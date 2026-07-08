from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from job_matcher.config import Settings, get_settings, load_linkedin_searches
from job_matcher.text_utils import (
    canonicalize_job_url,
    clean_text,
    extract_job_id,
    normalize_linkedin_search_url,
    parse_iso_date_to_timezone,
    split_paragraphs,
)

BASE_LINKEDIN_JOBS_URL = "https://www.linkedin.com/jobs/search/"
logger = logging.getLogger(__name__)


def make_linkedin_search_url(params: dict[str, Any]) -> str:
    return f"{BASE_LINKEDIN_JOBS_URL}?{urlencode(params)}"


def build_search_urls(settings: Settings | None = None) -> list[str]:
    active_settings = settings or get_settings()
    return [
        make_linkedin_search_url(params)
        for params in load_linkedin_searches(active_settings)
    ]


async def extract_cards_from_search_page(page) -> list[dict[str, Any]]:
    return await page.evaluate(
        """
        () => {
          const selectors = [
            'li.jobs-search-results__list-item',
            'ul.jobs-search__results-list li',
            'div.base-card',
            'div.job-search-card'
          ];

          const nodes = Array.from(new Set(
            selectors.flatMap(sel => Array.from(document.querySelectorAll(sel)))
          ));

          return nodes.map(card => {
            const linkEl =
              card.querySelector('a.base-card__full-link[href*="/jobs/view/"]') ||
              card.querySelector('a[href*="/jobs/view/"]');

            const titleEl =
              card.querySelector('.base-search-card__title') ||
              card.querySelector('.base-main-card__title') ||
              card.querySelector('h3');

            const companyEl =
              card.querySelector('.base-search-card__subtitle') ||
              card.querySelector('.base-main-card__subtitle') ||
              card.querySelector('h4');

            const locationEl =
              card.querySelector('.job-search-card__location') ||
              card.querySelector('.main-job-card__location') ||
              card.querySelector('[class*="location"]');

            const timeEl = card.querySelector('time');

            const urn =
              card.getAttribute('data-entity-urn') ||
              card.closest('[data-entity-urn]')?.getAttribute('data-entity-urn') ||
              '';

            return {
              title: titleEl?.innerText?.trim() || null,
              company: companyEl?.innerText?.trim() || null,
              location: locationEl?.innerText?.trim() || null,
              url: linkEl?.href || null,
              list_date_text: timeEl?.innerText?.trim() || null,
              list_date: timeEl?.getAttribute('datetime') || null,
              entity_urn: urn || null
            };
          }).filter(x => x.url && x.title);
        }
        """
    )


async def collect_search_results(
    search_urls: list[str], settings: Settings | None = None
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    now_tz = ZoneInfo(active_settings.timezone)
    all_rows: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=active_settings.headless_browser)
        context = await browser.new_context(
            locale="fr-CH",
            timezone_id=active_settings.timezone,
            viewport={"width": 1440, "height": 1000},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        for search_idx, raw_url in enumerate(search_urls):
            url = normalize_linkedin_search_url(raw_url)
            logger.info("Opening LinkedIn search %s/%s: %s", search_idx + 1, len(search_urls), url)
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(2_500)

            previous_count = 0
            stable_rounds = 0

            for _ in range(active_settings.scroll_rounds):
                cards = await extract_cards_from_search_page(page)
                if len(cards) > previous_count:
                    previous_count = len(cards)
                    stable_rounds = 0
                else:
                    stable_rounds += 1

                if previous_count >= active_settings.max_jobs_per_search:
                    break

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(random.randint(900, 1600))

                for selector in [
                    "button.infinite-scroller__show-more-button",
                    "button[aria-label*='Voir plus']",
                    "button:has-text('Voir plus')",
                    "button:has-text('Afficher plus')",
                    "button:has-text('Show more')",
                ]:
                    try:
                        button = page.locator(selector).first
                        if await button.count() > 0 and await button.is_visible():
                            await button.click(timeout=2_000)
                            await page.wait_for_timeout(random.randint(1200, 2200))
                            break
                    except Exception:
                        continue

                if stable_rounds >= 6:
                    break

            cards = await extract_cards_from_search_page(page)
            logger.info(
                "Search %s yielded %s visible cards before deduplication",
                search_idx + 1,
                len(cards),
            )
            for card in cards:
                clean_url = canonicalize_job_url(card.get("url"))
                job_id = extract_job_id(card.get("entity_urn")) or extract_job_id(clean_url)
                all_rows.append(
                    {
                        "search_idx": search_idx,
                        "search_url": url,
                        "job_id": job_id,
                        "title": card.get("title"),
                        "company": card.get("company"),
                        "location": card.get("location"),
                        "url": clean_url,
                        "list_date": card.get("list_date"),
                        "list_date_text": card.get("list_date_text"),
                        "entity_urn": card.get("entity_urn"),
                        "collected_at": datetime.now(now_tz),
                    }
                )

        await browser.close()

    if not all_rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(all_rows)
        .dropna(subset=["url"])
        .drop_duplicates(subset=["job_id", "url"])
        .reset_index(drop=True)
    )


def parse_job_detail_html(html: str, fallback_url: str | None = None) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    data: dict[str, Any] = {}
    json_ld = None

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            obj = json.loads(script.string or script.get_text())
            if isinstance(obj, dict) and obj.get("@type") == "JobPosting":
                json_ld = obj
                break
        except Exception:
            continue

    if json_ld:
        organization = json_ld.get("hiringOrganization") or {}
        location = json_ld.get("jobLocation") or {}
        address = location.get("address") or {}
        data.update(
            {
                "title_detail": clean_text(json_ld.get("title", "")),
                "company_detail": clean_text(organization.get("name", "")),
                "date_posted": json_ld.get("datePosted"),
                "valid_through": json_ld.get("validThrough"),
                "employment_type": json_ld.get("employmentType"),
                "industry": json_ld.get("industry"),
                "skills": json_ld.get("skills"),
                "education_requirements": json_ld.get("educationRequirements"),
                "address_country": address.get("addressCountry"),
                "address_locality": address.get("addressLocality"),
                "address_region": address.get("addressRegion"),
                "latitude": location.get("latitude"),
                "longitude": location.get("longitude"),
                "description_html": json_ld.get("description", ""),
                "source_parser": "json_ld",
            }
        )

    if not data.get("description_html"):
        description_element = soup.select_one(".show-more-less-html__markup")
        if description_element:
            data["description_html"] = str(description_element)
            data["source_parser"] = "dom_show_more_markup"

    data["description_text"] = clean_text(data.get("description_html", ""))

    criteria: dict[str, str] = {}
    for item in soup.select(".description__job-criteria-item"):
        key_element = item.select_one(".description__job-criteria-subheader")
        value_element = item.select_one(".description__job-criteria-text")
        key = clean_text(key_element.get_text(" ") if key_element else "")
        value = clean_text(value_element.get_text(" ") if value_element else "")
        if key and value:
            criteria[key] = value

    data["criteria_json"] = json.dumps(criteria, ensure_ascii=False)
    data["url_detail"] = canonicalize_job_url(fallback_url)

    canonical = soup.select_one("link[rel='canonical']")
    if canonical and canonical.get("href"):
        data["canonical_url"] = canonicalize_job_url(canonical["href"])
    else:
        data["canonical_url"] = data["url_detail"]

    data["job_id_detail"] = extract_job_id(data.get("canonical_url")) or extract_job_id(
        fallback_url
    )

    return data


async def collect_job_details(
    jobs_df: pd.DataFrame, settings: Settings | None = None
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    rows: list[dict[str, Any]] = []

    if jobs_df.empty:
        return pd.DataFrame()

    urls = jobs_df["url"].dropna().drop_duplicates().tolist()
    if active_settings.max_detail_pages > 0:
        urls = urls[: active_settings.max_detail_pages]

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=active_settings.headless_browser)
        context = await browser.new_context(
            locale="fr-CH",
            timezone_id=active_settings.timezone,
            viewport={"width": 1440, "height": 1000},
        )
        page = await context.new_page()

        for url in urls:
            try:
                logger.info("Collecting detail page: %s", url)
                await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_timeout(random.randint(1200, 2200))
                for selector in [
                    "button.show-more-less-html__button--more",
                    "button[data-tracking-control-name='public_jobs_show-more-html-btn']",
                    "button:has-text('Show more')",
                    "button:has-text('Afficher plus')",
                    "button:has-text('Voir plus')",
                ]:
                    try:
                        button = page.locator(selector).first
                        if await button.count() > 0 and await button.is_visible():
                            await button.click(timeout=3_000)
                            await page.wait_for_timeout(800)
                            break
                    except Exception:
                        continue

                parsed = parse_job_detail_html(await page.content(), fallback_url=url)
                parsed["detail_status"] = "ok"
                parsed["detail_error"] = None
            except Exception as exc:
                logger.exception("Failed to collect detail page for %s", url)
                parsed = {
                    "url_detail": url,
                    "canonical_url": canonicalize_job_url(url),
                    "job_id_detail": extract_job_id(url),
                    "detail_status": "error",
                    "detail_error": repr(exc),
                }

            rows.append(parsed)
            await page.wait_for_timeout(random.randint(1200, 2600))

        await browser.close()

    details_df = pd.DataFrame(rows)
    if details_df.empty:
        return details_df

    details_df["date_posted_dt"] = details_df["date_posted"].apply(
        lambda value: parse_iso_date_to_timezone(value, active_settings.timezone)
    )
    details_df["valid_through_dt"] = details_df["valid_through"].apply(
        lambda value: parse_iso_date_to_timezone(value, active_settings.timezone)
    )
    return details_df


def prepare_offers_dataframe(
    jobs_df: pd.DataFrame, details_df: pd.DataFrame
) -> pd.DataFrame:
    if jobs_df.empty:
        return pd.DataFrame()

    if details_df.empty:
        merged_df = jobs_df.copy()
    else:
        merged_df = jobs_df.merge(
            details_df,
            left_on="job_id",
            right_on="job_id_detail",
            how="left",
            suffixes=("", "_detail_merge"),
        )

    if "job_id_detail" not in merged_df:
        merged_df["job_id_detail"] = pd.NA
    if "canonical_url" not in merged_df:
        merged_df["canonical_url"] = pd.NA
    if "title_detail" not in merged_df:
        merged_df["title_detail"] = pd.NA
    if "company_detail" not in merged_df:
        merged_df["company_detail"] = pd.NA

    merged_df["final_job_id"] = merged_df["job_id"].fillna(merged_df["job_id_detail"])
    merged_df["final_url"] = merged_df["canonical_url"].fillna(merged_df["url"])
    merged_df["final_title"] = merged_df["title_detail"].replace("", pd.NA).fillna(merged_df["title"])
    merged_df["final_company"] = merged_df["company_detail"].replace("", pd.NA).fillna(merged_df["company"])

    return (
        merged_df.dropna(subset=["final_url"])
        .drop_duplicates(subset=["final_job_id", "final_url"])
        .reset_index(drop=True)
    )


def build_job_paragraphs(
    offers_df: pd.DataFrame, settings: Settings | None = None
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    rows: list[dict[str, Any]] = []

    for _, row in offers_df.iterrows():
        paragraphs = split_paragraphs(
            str(row.get("description_text") or ""),
            min_chars=active_settings.paragraph_min_chars,
        )
        for index, paragraph in enumerate(paragraphs):
            rows.append(
                {
                    "canonical_url": row["final_url"],
                    "external_job_id": row.get("final_job_id"),
                    "paragraph_idx": index,
                    "paragraph": paragraph,
                    "paragraph_chars": len(paragraph),
                }
            )

    return pd.DataFrame(rows)


def run_collection(settings: Settings | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    active_settings = settings or get_settings()
    search_urls = build_search_urls(active_settings)
    jobs_df = asyncio.run(collect_search_results(search_urls, active_settings))
    details_df = asyncio.run(collect_job_details(jobs_df, active_settings))
    offers_df = prepare_offers_dataframe(jobs_df, details_df)
    return offers_df, build_job_paragraphs(offers_df, active_settings)
