from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from job_matcher.config import Settings, get_settings
from job_matcher.text_utils import clean_text, split_paragraphs

logger = logging.getLogger(__name__)

DEFAULT_ETAT_GENEVE_RSS_URL = (
    "https://www.ge.ch/rss/offres-emploi-etat-geneve"
    "?departement=0&domaine_activite=0&classe_fonction_min=0"
    "&type_contrat=0&taux_activite_max=0"
)
ETAT_GENEVE_BASE_URL = "https://www.ge.ch"
SOURCE_NAME = "etat_geneve"

FEED_COLUMNS = [
    "search_url",
    "job_id",
    "title",
    "company",
    "location",
    "url",
    "list_date",
    "rss_summary",
    "collected_at",
]

DETAIL_DEFAULTS: dict[str, Any] = {
    "job_id_detail": None,
    "canonical_url": None,
    "title_detail": None,
    "company_detail": None,
    "location": None,
    "date_posted": None,
    "valid_through": None,
    "employment_type": None,
    "industry": None,
    "skills": None,
    "education_requirements": None,
    "address_country": None,
    "address_locality": None,
    "address_region": None,
    "latitude": None,
    "longitude": None,
    "description_text": "",
    "description_html": "",
    "criteria_json": "{}",
    "source": SOURCE_NAME,
    "source_parser": None,
    "detail_status": None,
    "detail_error": None,
}

FRENCH_MONTHS = {
    "janvier": "January",
    "février": "February",
    "fevrier": "February",
    "mars": "March",
    "avril": "April",
    "mai": "May",
    "juin": "June",
    "juillet": "July",
    "août": "August",
    "aout": "August",
    "septembre": "September",
    "octobre": "October",
    "novembre": "November",
    "décembre": "December",
    "decembre": "December",
}


def _normalize_inline(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def canonicalize_geneva_job_url(url: str | None) -> str | None:
    if not url:
        return None
    absolute = urljoin(ETAT_GENEVE_BASE_URL, url)
    parsed = urlparse(absolute)
    return urlunparse(parsed._replace(query="", fragment=""))


def extract_geneva_job_id(*values: str | None) -> str | None:
    for value in values:
        if not value:
            continue
        match = re.search(r"/(?:rss|liste-offres)/(\d+)(?:/|$)", value)
        if match:
            return match.group(1)
        trailing_match = re.search(r"(?:^|\D)(\d+)(?:\D*)$", value)
        if trailing_match:
            return trailing_match.group(1)
    return None


def create_http_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0 Safari/537.36"
            ),
            "Accept-Language": "fr-CH,fr;q=0.9,en;q=0.8",
        }
    )
    return session


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].casefold()


def _child_text(element, *names: str) -> str | None:
    accepted = {name.casefold() for name in names}
    for child in list(element):
        if _local_name(child.tag) not in accepted:
            continue
        text = "".join(child.itertext())
        normalized = _normalize_inline(text)
        if normalized:
            return normalized
    return None


def _child_link(element) -> str | None:
    for child in list(element):
        if _local_name(child.tag) != "link":
            continue
        value = child.attrib.get("href") or child.text
        normalized = _normalize_inline(value)
        if normalized:
            return normalized
    return None


def parse_geneva_feed(
    content: str | bytes,
    feed_url: str = DEFAULT_ETAT_GENEVE_RSS_URL,
    timezone_name: str = "Europe/Zurich",
    collected_at: datetime | None = None,
) -> pd.DataFrame:
    root = ElementTree.fromstring(content)
    now = collected_at or datetime.now(ZoneInfo(timezone_name))
    rows: list[dict[str, Any]] = []

    entries = [
        element
        for element in root.iter()
        if _local_name(element.tag) in {"item", "entry"}
    ]
    for entry in entries:
        link = canonicalize_geneva_job_url(_child_link(entry))
        guid = _child_text(entry, "guid", "id")
        title = _child_text(entry, "title")
        if not link or not title:
            continue

        author = _child_text(entry, "author", "creator")
        if not author:
            for child in list(entry):
                if _local_name(child.tag) != "author":
                    continue
                author = _child_text(child, "name")
                if author:
                    break

        rows.append(
            {
                "search_url": feed_url,
                "job_id": extract_geneva_job_id(guid, link),
                "title": title,
                "company": author or "État de Genève",
                "location": None,
                "url": link,
                "list_date": _child_text(
                    entry,
                    "pubDate",
                    "published",
                    "updated",
                ),
                "rss_summary": _child_text(
                    entry,
                    "description",
                    "summary",
                    "content",
                ),
                "collected_at": now,
            }
        )

    if not rows:
        return pd.DataFrame(columns=FEED_COLUMNS)

    return (
        pd.DataFrame(rows, columns=FEED_COLUMNS)
        .drop_duplicates(subset=["job_id", "url"])
        .reset_index(drop=True)
    )


def collect_feed_results(
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    feed_url = getattr(
        active_settings,
        "etat_geneve_rss_url",
        DEFAULT_ETAT_GENEVE_RSS_URL,
    )
    http = session or create_http_session()
    response = http.get(feed_url, timeout=(10, 30))
    response.raise_for_status()
    return parse_geneva_feed(
        response.content,
        feed_url=feed_url,
        timezone_name=active_settings.timezone,
    )


def _iter_json_objects(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_json_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_json_objects(child)


def _extract_job_posting(soup: BeautifulSoup) -> dict[str, Any] | None:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(script.string or script.get_text())
        except (TypeError, json.JSONDecodeError):
            continue
        for candidate in _iter_json_objects(payload):
            object_type = candidate.get("@type")
            if object_type == "JobPosting" or (
                isinstance(object_type, list) and "JobPosting" in object_type
            ):
                return candidate
    return None


def _find_heading(soup: BeautifulSoup, label: str) -> Tag | None:
    expected = _normalize_inline(label).casefold()
    return soup.find(
        lambda tag: (
            isinstance(tag, Tag)
            and tag.name in {"h2", "h3", "h4"}
            and _normalize_inline(tag.get_text(" ", strip=True)).casefold() == expected
        )
    )


def _extract_section_value(soup: BeautifulSoup, label: str) -> str | None:
    heading = _find_heading(soup, label)
    if heading is None:
        return None
    sibling = heading.find_next_sibling()
    while sibling is not None and not isinstance(sibling, Tag):
        sibling = sibling.next_sibling
    if sibling is None or sibling.name in {"h2", "h3", "h4"}:
        return None
    value = _normalize_inline(sibling.get_text(" ", strip=True))
    return value or None


def _extract_labeled_value(soup: BeautifulSoup, label: str) -> str | None:
    expected = re.sub(r"\s*:\s*$", "", _normalize_inline(label)).casefold()
    for text_node in soup.find_all(string=True):
        text = _normalize_inline(text_node)
        if not text:
            continue
        normalized = re.sub(r"\s*:\s*$", "", text).casefold()
        if normalized != expected and not normalized.startswith(f"{expected} :"):
            continue

        label_tag = text_node.parent
        container = label_tag.find_parent("p") or label_tag
        full_text = _normalize_inline(container.get_text(" ", strip=True))
        match = re.match(
            rf"^{re.escape(label)}\s*:\s*(.+)$",
            full_text,
            flags=re.IGNORECASE,
        )
        if match:
            return _normalize_inline(match.group(1)) or None

        sibling = label_tag.find_next_sibling()
        while sibling is not None and not isinstance(sibling, Tag):
            sibling = sibling.next_sibling
        if sibling is not None:
            value = _normalize_inline(sibling.get_text(" ", strip=True))
            if value:
                return value
    return None


def _extract_sidebar_value(soup: BeautifulSoup, label: str) -> str | None:
    expected = _normalize_inline(label).casefold()
    for text_node in soup.find_all(string=True):
        if _normalize_inline(text_node).casefold() != expected:
            continue
        label_tag = text_node.parent
        sibling = label_tag.find_next_sibling()
        while sibling is not None and not isinstance(sibling, Tag):
            sibling = sibling.next_sibling
        if sibling is None:
            continue
        value = _normalize_inline(sibling.get_text(" ", strip=True))
        if value:
            return value
    return None


def _extract_description(soup: BeautifulSoup) -> tuple[str, str]:
    start = _find_heading(soup, "Votre mission")
    if start is None:
        return "", ""

    html_parts: list[str] = []
    text_parts: list[str] = []
    current: Tag | None = start
    while current is not None:
        if current.name in {"h2", "h3", "h4"}:
            heading = _normalize_inline(current.get_text(" ", strip=True))
            html_parts.append(str(current))
            if heading:
                text_parts.append(heading)
        elif current.name == "div" and "text-body-large" in (
            current.get("class") or []
        ):
            html_parts.append(str(current))
            content = clean_text(str(current))
            if content:
                text_parts.append(content)
        elif current.name == "div" and "print:hidden" in (current.get("class") or []):
            break

        sibling = current.find_next_sibling()
        while sibling is not None and not isinstance(sibling, Tag):
            sibling = sibling.next_sibling
        current = sibling

    return "\n".join(html_parts), "\n\n".join(text_parts).strip()


def extract_address_locality(
    location: str | None,
    structured_locality: str | None = None,
) -> str | None:
    normalized_location = _normalize_inline(location)
    match = re.search(r"\b\d{4}\s+([^,;]+?)\s*$", normalized_location)
    if match:
        locality = _normalize_inline(match.group(1)).strip(" .")
        if locality:
            return locality
    if structured_locality:
        normalized = _normalize_inline(structured_locality)
        return normalized or None
    return None


def _clean_structured_text(value: Any) -> str:
    if isinstance(value, str):
        return clean_text(value)
    if isinstance(value, dict):
        return " ".join(
            text
            for child in value.values()
            if (text := _clean_structured_text(child))
        )
    if isinstance(value, list):
        return " ".join(
            text
            for child in value
            if (text := _clean_structured_text(child))
        )
    return _normalize_inline(value)


def parse_geneva_date(
    value: str | None,
    timezone_name: str,
    end_of_day: bool = False,
) -> pd.Timestamp:
    normalized = _normalize_inline(value)
    if not normalized:
        return pd.NaT
    translated = normalized
    for french, english in FRENCH_MONTHS.items():
        translated = re.sub(
            rf"\b{re.escape(french)}\b",
            english,
            translated,
            flags=re.IGNORECASE,
        )
    try:
        parsed = dateparser.parse(translated, dayfirst=True)
    except (TypeError, ValueError, OverflowError):
        return pd.NaT
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    else:
        parsed = parsed.astimezone(ZoneInfo(timezone_name))
    if end_of_day and not re.search(r"\d{1,2}:\d{2}", normalized):
        parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=0)
    return pd.Timestamp(parsed)


def _address_from_json_ld(job_posting: dict[str, Any]) -> dict[str, Any]:
    location = job_posting.get("jobLocation") or {}
    if isinstance(location, list):
        location = location[0] if location else {}
    if not isinstance(location, dict):
        return {}
    address = location.get("address") or {}
    if isinstance(address, str):
        return {"formatted": _normalize_inline(address)}
    if not isinstance(address, dict):
        return {}
    formatted = ", ".join(
        value
        for value in (
            _normalize_inline(address.get("streetAddress")),
            " ".join(
                value
                for value in (
                    _normalize_inline(address.get("postalCode")),
                    _normalize_inline(address.get("addressLocality")),
                )
                if value
            ),
        )
        if value
    )
    return {
        "formatted": formatted or None,
        "locality": address.get("addressLocality"),
        "region": address.get("addressRegion"),
        "country": address.get("addressCountry"),
    }


def parse_job_detail_html(
    html: str,
    fallback_url: str | None = None,
    timezone_name: str = "Europe/Zurich",
) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    job_posting = _extract_job_posting(soup) or {}
    data = dict(DETAIL_DEFAULTS)

    canonical_element = soup.select_one("link[rel='canonical']")
    canonical_url = canonicalize_geneva_job_url(
        canonical_element.get("href") if canonical_element else fallback_url
    )
    data["canonical_url"] = canonical_url
    data["job_id_detail"] = extract_geneva_job_id(canonical_url, fallback_url)

    title_element = soup.find("h1")
    data["title_detail"] = _normalize_inline(
        job_posting.get("title")
        or (title_element.get_text(" ", strip=True) if title_element else None)
    ) or None

    hiring_organization = job_posting.get("hiringOrganization") or {}
    hiring_name = (
        hiring_organization.get("name")
        if isinstance(hiring_organization, dict)
        else None
    )
    author = _extract_sidebar_value(soup, "Auteur")
    data["company_detail"] = author or _normalize_inline(hiring_name) or "État de Genève"

    json_address = _address_from_json_ld(job_posting)
    workplace = _extract_section_value(soup, "Lieu de travail")
    data["location"] = workplace or json_address.get("formatted") or None
    data["address_locality"] = extract_address_locality(
        data["location"],
        json_address.get("locality"),
    )
    data["address_region"] = _normalize_inline(json_address.get("region")) or None
    data["address_country"] = (
        _normalize_inline(json_address.get("country"))
        or ("CH" if re.search(r"\b\d{4}\b", data["location"] or "") else None)
    )

    location_data = job_posting.get("jobLocation") or {}
    if isinstance(location_data, list):
        location_data = location_data[0] if location_data else {}
    geo: dict[str, Any] = {}
    if isinstance(location_data, dict):
        raw_geo = location_data.get("geo") or {}
        if isinstance(raw_geo, dict):
            geo = raw_geo
    data["latitude"] = geo.get("latitude")
    data["longitude"] = geo.get("longitude")

    data["date_posted"] = job_posting.get("datePosted") or _extract_labeled_value(
        soup, "Date de publication"
    )
    data["valid_through"] = job_posting.get("validThrough") or _extract_labeled_value(
        soup, "Délai d'inscription"
    )
    data["employment_type"] = job_posting.get(
        "employmentType"
    ) or _extract_labeled_value(soup, "Type de contrat")

    description_html, description_text = _extract_description(soup)
    if not description_text:
        fallback_sections = [
            job_posting.get("description"),
            job_posting.get("experienceRequirements"),
            job_posting.get("jobBenefits"),
            data["location"],
        ]
        description_text = "\n\n".join(
            cleaned
            for value in fallback_sections
            if (cleaned := _clean_structured_text(value))
        )
    data["description_html"] = description_html or str(
        job_posting.get("description") or ""
    )
    data["description_text"] = description_text

    criteria = {
        "remuneration": _extract_labeled_value(soup, "Rémunération"),
        "activity_rate": _extract_labeled_value(soup, "Taux d'activité"),
        "job_start_date": job_posting.get("jobStartDate")
        or _extract_labeled_value(soup, "Entrée en fonction"),
        "publication_type": _extract_sidebar_value(soup, "Type de publication"),
        "author": author,
        "updated_at": _extract_sidebar_value(soup, "Mise à jour"),
        "experience_requirements": _clean_structured_text(
            job_posting.get("experienceRequirements")
        ),
        "job_benefits": _clean_structured_text(job_posting.get("jobBenefits")),
        "workplace": data["location"],
    }
    data["criteria_json"] = json.dumps(
        {key: value for key, value in criteria.items() if value},
        ensure_ascii=False,
    )
    data["source"] = SOURCE_NAME
    data["source_parser"] = (
        "etat_geneve_json_ld_dom" if job_posting else "etat_geneve_dom"
    )
    data["date_posted_dt"] = parse_geneva_date(
        data["date_posted"],
        timezone_name,
    )
    data["valid_through_dt"] = parse_geneva_date(
        data["valid_through"],
        timezone_name,
        end_of_day=True,
    )
    return data


def collect_job_details(
    jobs_df: pd.DataFrame,
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    if jobs_df.empty:
        return pd.DataFrame(
            columns=[*DETAIL_DEFAULTS, "date_posted_dt", "valid_through_dt"]
        )

    urls = jobs_df["url"].dropna().drop_duplicates().tolist()
    if active_settings.max_detail_pages > 0:
        urls = urls[: active_settings.max_detail_pages]

    http = session or create_http_session()
    rows: list[dict[str, Any]] = []
    for url in urls:
        try:
            logger.info("Collecting État de Genève detail page: %s", url)
            response = http.get(url, timeout=(10, 30))
            response.raise_for_status()
            parsed = parse_job_detail_html(
                response.text,
                fallback_url=url,
                timezone_name=active_settings.timezone,
            )
            parsed["detail_status"] = "ok"
            parsed["detail_error"] = None
        except Exception as exc:
            logger.exception("Failed to collect État de Genève detail page %s", url)
            parsed = dict(DETAIL_DEFAULTS)
            parsed.update(
                {
                    "job_id_detail": extract_geneva_job_id(url),
                    "canonical_url": canonicalize_geneva_job_url(url),
                    "detail_status": "error",
                    "detail_error": repr(exc),
                    "date_posted_dt": pd.NaT,
                    "valid_through_dt": pd.NaT,
                }
            )
        rows.append(parsed)

    return pd.DataFrame(rows)


def prepare_offers_dataframe(
    jobs_df: pd.DataFrame,
    details_df: pd.DataFrame,
    settings: Settings | None = None,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
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
            suffixes=("", "_detail"),
        )

    defaults = {
        "job_id_detail": pd.NA,
        "canonical_url": pd.NA,
        "title_detail": pd.NA,
        "company_detail": pd.NA,
        "description_text": "",
        "description_html": "",
        "date_posted_dt": pd.NaT,
        "valid_through_dt": pd.NaT,
        "criteria_json": "{}",
        "source_parser": "etat_geneve_rss",
        "detail_status": pd.NA,
        "detail_error": pd.NA,
    }
    for field, default in {**DETAIL_DEFAULTS, **defaults}.items():
        if field not in merged_df:
            merged_df[field] = default

    if "location_detail" in merged_df:
        merged_df["location"] = merged_df["location_detail"].replace(
            "", pd.NA
        ).combine_first(merged_df["location"])

    merged_df["final_job_id"] = merged_df["job_id"].fillna(
        merged_df["job_id_detail"]
    )
    merged_df["final_url"] = merged_df["canonical_url"].fillna(merged_df["url"])
    merged_df["final_title"] = merged_df["title_detail"].replace("", pd.NA).fillna(
        merged_df["title"]
    )
    merged_df["final_company"] = merged_df["company_detail"].replace(
        "", pd.NA
    ).fillna(merged_df["company"])
    merged_df["description_text"] = merged_df["description_text"].replace(
        "", pd.NA
    ).fillna(merged_df["rss_summary"].apply(clean_text))
    merged_df["description_html"] = merged_df["description_html"].replace(
        "", pd.NA
    ).fillna(merged_df["rss_summary"])
    feed_dates = merged_df["list_date"].apply(
        lambda value: parse_geneva_date(value, active_settings.timezone)
    )
    merged_df["date_posted_dt"] = merged_df["date_posted_dt"].combine_first(
        feed_dates
    )
    merged_df["source"] = SOURCE_NAME

    return (
        merged_df.dropna(subset=["final_url"])
        .drop_duplicates(subset=["final_job_id", "final_url"])
        .reset_index(drop=True)
    )


def build_job_paragraphs(
    offers_df: pd.DataFrame,
    settings: Settings | None = None,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    rows: list[dict[str, Any]] = []
    for _, row in offers_df.iterrows():
        description = row.get("description_text")
        if pd.isna(description):
            description = ""
        description = re.sub(r"\s*•\s*", "; ", str(description))
        description = re.sub(r"\s+", " ", description).strip()
        paragraphs = split_paragraphs(
            description,
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
