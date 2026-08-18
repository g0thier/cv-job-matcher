from __future__ import annotations

import asyncio
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from job_matcher.indeed import (  # noqa: E402
    INDEED_LOCATIONS,
    _solve_captcha,
    build_job_paragraphs,
    build_scheduled_window,
    build_search_url,
    build_startup_window,
    canonicalize_indeed_job_url,
    collect_job_details,
    collect_search_results,
    extract_indeed_job_key,
    parse_job_detail_html,
    parse_relative_posted_at,
    parse_search_page_html,
    prepare_offers_dataframe,
)


def settings(**overrides):
    values = {
        "timezone": "Europe/Zurich",
        "scroll_rounds": 1,
        "max_jobs_per_search": 500,
        "max_detail_pages": 0,
        "paragraph_min_chars": 40,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


async def no_sleep(_seconds: float) -> None:
    return None


def search_card(
    job_key: str,
    title: str,
    *,
    company: str = "Example SA",
    location: str = "Genève, GE",
    date_text: str = "il y a 5 minutes",
) -> str:
    return f"""
    <div data-testid="jobCard">
      <h2 data-testid="jobTitle"><a data-jk="{job_key}">{title}</a></h2>
      <span data-testid="companyName">{company}</span>
      <div data-testid="location">{location}</div>
      <span data-testid="salary">CHF 100'000</span>
      <span data-testid="postDate">{date_text}</span>
      <div data-testid="belowJobSnippet"><ul><li>Construire des services fiables.</li></ul></div>
      <div data-testid="jobCard-badge">Nouveau</div>
    </div>
    """


def search_page(*cards: str, next_start: int | None = None) -> str:
    next_link = (
        f'<a data-testid="pagination-page-next" href="/jobs?start={next_start}">Suivant</a>'
        if next_start is not None
        else ""
    )
    return f"<html><body>{''.join(cards)}{next_link}</body></html>"


DETAIL_HTML = """
<html>
  <head>
    <link rel="canonical" href="https://ch-fr.indeed.com/viewjob?jk=job-shared&amp;from=search">
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "JobPosting",
      "title": "Data Engineer",
      "datePosted": "2026-08-18T10:31:56.285Z",
      "validThrough": "2026-12-16T14:03:49.851Z",
      "employmentType": ["FULL_TIME"],
      "industry": "Technology",
      "skills": "Python, SQL",
      "educationRequirements": "Bachelor",
      "directApply": false,
      "hiringOrganization": {
        "@type": "Organization",
        "name": "Example SA",
        "sameAs": "https://ch-fr.indeed.com/cmp/example",
        "logo": "https://example.test/logo.png"
      },
      "jobLocation": {
        "@type": "Place",
        "address": {
          "addressCountry": "CH",
          "addressLocality": "Genève",
          "addressRegion": "GE"
        },
        "geo": {"latitude": 46.2, "longitude": 6.1}
      },
      "description": "<p>Construire et maintenir des pipelines de données fiables pour les utilisateurs.</p><p>Collaborer avec les équipes produit et plateforme au quotidien.</p>"
    }
    </script>
  </head>
  <body>
    <h1 data-testid="jobsearch-JobInfoHeader-title">Data Engineer</h1>
    <div id="jobDescriptionText">Description de secours</div>
    <div data-testid="inlineHeader-companyLocation">Genève, GE</div>
    <div data-testid="jobsearch-CompanyInfoContainer">Example SA 4,2 / 5</div>
    <a data-testid="companyReviewLink">1 234 avis</a>
    <div data-testid="salaryInfoAndJobType">100% | CHF 100'000</div>
  </body>
</html>
"""


def detail_html(job_key: str, title: str = "Data Engineer") -> str:
    return DETAIL_HTML.replace("job-shared", job_key).replace(
        "Data Engineer", title
    )


class FakePage:
    def __init__(self, url: str, html_values: list[str] | str):
        self.url = url
        self.html_values = (
            list(html_values) if isinstance(html_values, list) else [html_values]
        )
        self.index = 0
        self.captcha_solutions = 0
        self.screenshots: list[str] = []

    async def get_content(self):
        value = self.html_values[min(self.index, len(self.html_values) - 1)]
        if self.index < len(self.html_values) - 1:
            self.index += 1
        return value

    async def evaluate(self, _script):
        return None

    async def solve_captcha(self):
        self.captcha_solutions += 1

    async def get_current_url(self):
        return self.url

    async def save_screenshot(self, filename, full_page=True):
        del full_page
        self.screenshots.append(filename)


class FakeBrowser:
    def __init__(self, page_factory):
        self.page_factory = page_factory
        self.pages: list[FakePage] = []
        self.stopped = False

    async def get(self, url):
        page = self.page_factory(url)
        self.pages.append(page)
        return page

    def stop(self):
        self.stopped = True


class IndeedWindowTests(unittest.TestCase):
    def test_startup_window_begins_at_local_midnight(self) -> None:
        start, end = build_startup_window(
            datetime(2026, 8, 18, 10, 30, tzinfo=timezone.utc),
            "Europe/Zurich",
        )

        self.assertEqual(start.isoformat(), "2026-08-18T00:00:00+02:00")
        self.assertEqual(end.isoformat(), "2026-08-18T12:30:00+02:00")

    def test_scheduled_window_is_fifteen_minutes_plus_overlap(self) -> None:
        start, end = build_scheduled_window(
            datetime(2026, 8, 18, 10, 30, tzinfo=timezone.utc),
            "Europe/Zurich",
            overlap_minutes=5,
        )

        self.assertEqual(start.isoformat(), "2026-08-18T12:10:00+02:00")
        self.assertEqual(end.isoformat(), "2026-08-18T12:30:00+02:00")


class IndeedSearchParserTests(unittest.TestCase):
    def test_builds_and_canonicalizes_urls(self) -> None:
        search_url = build_search_url("Genève, GE", start=20)
        query = parse_qs(urlparse(search_url).query)

        self.assertEqual(query["l"], ["Genève, GE"])
        self.assertEqual(query["radius"], ["25"])
        self.assertEqual(query["start"], ["20"])
        self.assertEqual(
            canonicalize_indeed_job_url(
                "https://ch-fr.indeed.com/viewjob?jk=abc123&from=search"
            ),
            "https://ch-fr.indeed.com/viewjob?jk=abc123",
        )
        self.assertEqual(extract_indeed_job_key("/viewjob/abc123"), "abc123")

    def test_parses_relative_french_dates(self) -> None:
        reference = datetime(2026, 8, 18, 12, 30, tzinfo=ZoneInfo("Europe/Zurich"))

        self.assertEqual(
            parse_relative_posted_at("Publiée il y a 15 minutes", reference),
            reference - pd.Timedelta(minutes=15),
        )
        self.assertEqual(
            parse_relative_posted_at("il y a 2 heures", reference),
            reference - pd.Timedelta(hours=2),
        )
        self.assertIsNone(parse_relative_posted_at("Aujourd'hui", reference))

    def test_parses_card_shape_from_notebook_selectors(self) -> None:
        collected_at = datetime(
            2026, 8, 18, 12, 30, tzinfo=ZoneInfo("Europe/Zurich")
        )
        rows = parse_search_page_html(
            search_page(search_card("job-1", "Data Engineer")),
            search_url="https://example.test/search",
            search_location="Genève, GE",
            search_location_name="Genève",
            collected_at=collected_at,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["job_id"], "job-1")
        self.assertEqual(rows[0]["title"], "Data Engineer")
        self.assertEqual(rows[0]["company"], "Example SA")
        self.assertEqual(
            rows[0]["url"], "https://ch-fr.indeed.com/viewjob?jk=job-1"
        )
        self.assertIn("services fiables", rows[0]["description_snippet"])


class IndeedSearchCollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_collects_both_cities_paginates_and_aggregates_duplicates(self) -> None:
        def page_factory(url: str):
            query = parse_qs(urlparse(url).query)
            location = query.get("l", [""])[0]
            start = int(query.get("start", ["0"])[0])
            if start:
                return FakePage(url, '<div data-testid="no-results">Aucune offre</div>')
            if location == "Genève, GE":
                html = search_page(
                    search_card("job-shared", "Shared role"),
                    search_card("job-geneva", "Geneva role"),
                    next_start=10,
                )
            else:
                html = search_page(
                    search_card(
                        "job-shared",
                        "Shared role",
                        location="Lausanne, VD",
                    ),
                    search_card(
                        "job-lausanne",
                        "Lausanne role",
                        location="Lausanne, VD",
                    ),
                    next_start=10,
                )
            return FakePage(url, html)

        browser = FakeBrowser(page_factory)
        with TemporaryDirectory() as temp_dir:
            result = await collect_search_results(
                "2026-08-18T12:00:00+02:00",
                "2026-08-18T12:30:00+02:00",
                settings(),
                run_directory=temp_dir,
                browser_factory=lambda **_kwargs: browser,
                sleep=no_sleep,
            )

        self.assertTrue(browser.stopped)
        self.assertEqual(set(result["job_id"]), {"job-shared", "job-geneva", "job-lausanne"})
        shared = result[result["job_id"] == "job-shared"].iloc[0]
        self.assertEqual(
            json.loads(shared["search_locations_json"]),
            ["Genève", "Lausanne"],
        )
        self.assertEqual(tuple(name for _query, name in INDEED_LOCATIONS), ("Genève", "Lausanne"))
        paginated_urls = [page.url for page in browser.pages if "start=10" in page.url]
        self.assertEqual(len(paginated_urls), 2)
        for url in paginated_urls:
            query = parse_qs(urlparse(url).query)
            self.assertIn(query["l"][0], {"Genève, GE", "Lausanne, VD"})
            self.assertEqual(query["radius"], ["25"])
            self.assertEqual(query["sort"], ["date"])
            self.assertEqual(query["fromage"], ["1"])

    async def test_stops_on_terminal_page_without_synthetic_pagination(self) -> None:
        def page_factory(url: str):
            location = parse_qs(urlparse(url).query)["l"][0]
            job_key = "job-geneva" if location == "Genève, GE" else "job-lausanne"
            return FakePage(url, search_page(search_card(job_key, "Terminal role")))

        browser = FakeBrowser(page_factory)
        with TemporaryDirectory() as temp_dir:
            result = await collect_search_results(
                "2026-08-18T12:00:00+02:00",
                "2026-08-18T12:30:00+02:00",
                settings(max_jobs_per_search=1),
                run_directory=temp_dir,
                browser_factory=lambda **_kwargs: browser,
                sleep=no_sleep,
            )

        self.assertEqual(len(browser.pages), 2)
        self.assertEqual(set(result["job_id"]), {"job-geneva", "job-lausanne"})

    async def test_raises_at_configured_limit_only_when_next_page_exists(self) -> None:
        browser = FakeBrowser(
            lambda url: FakePage(
                url,
                search_page(search_card("job-1", "Limited role"), next_start=10),
            )
        )
        with TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(RuntimeError, "MAX_JOBS_PER_SEARCH"):
                await collect_search_results(
                    "2026-08-18T12:00:00+02:00",
                    "2026-08-18T12:30:00+02:00",
                    settings(max_jobs_per_search=1),
                    run_directory=temp_dir,
                    browser_factory=lambda **_kwargs: browser,
                    sleep=no_sleep,
                )

        self.assertTrue(browser.stopped)


class IndeedDetailTests(unittest.TestCase):
    def test_parses_json_ld_and_detail_metadata(self) -> None:
        parsed = parse_job_detail_html(DETAIL_HTML)

        self.assertEqual(parsed["job_id_detail"], "job-shared")
        self.assertEqual(parsed["title_detail"], "Data Engineer")
        self.assertEqual(parsed["company_detail"], "Example SA")
        self.assertEqual(parsed["address_locality_detail"], "Genève")
        self.assertEqual(parsed["latitude_detail"], 46.2)
        self.assertEqual(parsed["date_posted"], "2026-08-18T10:31:56.285Z")
        self.assertEqual(parsed["employment_type_detail"], "FULL_TIME")
        self.assertIn("pipelines de données", parsed["description_text"])
        metadata = json.loads(parsed["detail_metadata_json"])
        self.assertEqual(metadata["company_rating"], 4.2)
        self.assertEqual(metadata["company_reviews_count"], 1234)

    def test_parses_dom_fallback_without_json_ld(self) -> None:
        parsed = parse_job_detail_html(
            """
            <html><head>
              <link rel="canonical" href="https://ch-fr.indeed.com/viewjob?jk=dom-1">
            </head><body>
              <h1 data-testid="jobsearch-JobInfoHeader-title">DOM Engineer</h1>
              <div data-testid="inlineHeader-companyName">DOM SA</div>
              <div data-testid="inlineHeader-companyLocation">Lausanne, VD</div>
              <div id="jobDescriptionText"><p>Une description obtenue depuis le DOM.</p></div>
            </body></html>
            """
        )

        self.assertEqual(parsed["job_id_detail"], "dom-1")
        self.assertEqual(parsed["title_detail"], "DOM Engineer")
        self.assertEqual(parsed["company_detail"], "DOM SA")
        self.assertEqual(parsed["source_parser"], "indeed_dom")
        self.assertIn("description obtenue", parsed["description_text"])


class IndeedDetailCollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_native_cdp_click_when_page_solver_is_unavailable(self) -> None:
        class Element:
            async def get_position_async(self):
                return SimpleNamespace(left=490, top=180, width=300, height=65)

        class CompatiblePage:
            def __init__(self):
                self.commands = []
                self.front_calls = 0

            async def select(self, selector, timeout=1):
                del timeout
                if selector.startswith("iframe"):
                    return Element()
                raise asyncio.TimeoutError

            async def bring_to_front(self):
                self.front_calls += 1

            async def send(self, command):
                self.commands.append(command)

        page = CompatiblePage()

        self.assertTrue(await _solve_captcha(page))
        self.assertEqual(page.front_calls, 1)
        self.assertEqual(len(page.commands), 2)

    async def test_solves_captcha_and_isolates_one_detail_failure(self) -> None:
        captcha_html = "<html><body>Verify you are human captcha</body></html>"

        def page_factory(url: str):
            key = extract_indeed_job_key(url)
            if key == "job-ok":
                return FakePage(url, [captcha_html, detail_html("job-ok")])
            return FakePage(url, "<html><body>expired</body></html>")

        browser = FakeBrowser(page_factory)
        jobs = pd.DataFrame(
            {
                "job_id": ["job-ok", "job-error"],
                "url": [
                    canonicalize_indeed_job_url(job_key="job-ok"),
                    canonicalize_indeed_job_url(job_key="job-error"),
                ],
            }
        )
        with TemporaryDirectory() as temp_dir:
            details = await collect_job_details(
                jobs,
                settings(),
                run_directory=temp_dir,
                browser_factory=lambda **_kwargs: browser,
                sleep=no_sleep,
            )

        self.assertEqual(details["detail_status"].tolist(), ["ok", "error"])
        self.assertGreater(browser.pages[0].captcha_solutions, 0)
        self.assertIn("did not expose", details.iloc[1]["detail_error"])
        self.assertTrue(browser.stopped)

    async def test_accepts_complete_json_ld_without_dom_description(self) -> None:
        json_ld_only = detail_html("job-jsonld").replace(
            '<div id="jobDescriptionText">Description de secours</div>', ""
        )
        browser = FakeBrowser(lambda url: FakePage(url, json_ld_only))
        jobs = pd.DataFrame(
            {
                "job_id": ["job-jsonld"],
                "url": [canonicalize_indeed_job_url(job_key="job-jsonld")],
            }
        )

        with TemporaryDirectory() as temp_dir:
            details = await collect_job_details(
                jobs,
                settings(),
                run_directory=temp_dir,
                browser_factory=lambda **_kwargs: browser,
                sleep=no_sleep,
            )

        self.assertEqual(details.iloc[0]["detail_status"], "ok")
        self.assertEqual(details.iloc[0]["source_parser"], "indeed_json_ld")

    async def test_rejects_mismatched_search_url_and_job_key(self) -> None:
        browser = FakeBrowser(lambda url: FakePage(url, detail_html("job-expected")))
        jobs = pd.DataFrame(
            {
                "job_id": ["job-expected"],
                "url": [canonicalize_indeed_job_url(job_key="job-other")],
            }
        )

        with TemporaryDirectory() as temp_dir:
            details = await collect_job_details(
                jobs,
                settings(),
                run_directory=temp_dir,
                browser_factory=lambda **_kwargs: browser,
                sleep=no_sleep,
            )

        self.assertEqual(details.iloc[0]["detail_status"], "error")
        self.assertIn("search URL job key", details.iloc[0]["detail_error"])
        self.assertEqual(browser.pages, [])


class IndeedPreparationTests(unittest.TestCase):
    def _search_row(self, job_id: str = "job-shared") -> dict:
        return {
            "search_url": "https://ch-fr.indeed.com/jobs?l=Gen%C3%A8ve",
            "search_location": "Genève, GE",
            "search_location_name": "Genève",
            "search_locations_json": json.dumps(["Genève"]),
            "search_urls_json": json.dumps(["https://example.test/search"]),
            "job_id": job_id,
            "title": "List title",
            "company": "List company",
            "location": "Genève, GE",
            "url": canonicalize_indeed_job_url(job_key=job_id),
            "list_date_text": "il y a 5 minutes",
            "list_date_estimate": datetime.now(ZoneInfo("Europe/Zurich")),
            "salary": None,
            "description_snippet": "Snippet",
            "badges": None,
            "sponsored": False,
            "search_metadata_json": json.dumps(
                {"matched_search_locations": ["Genève"]}, ensure_ascii=False
            ),
            "collected_at": datetime.now(ZoneInfo("Europe/Zurich")),
        }

    def test_prepares_shared_offer_shape_and_paragraphs(self) -> None:
        detail = parse_job_detail_html(DETAIL_HTML)
        detail.update({"detail_status": "ok", "detail_error": None})

        offers = prepare_offers_dataframe(
            pd.DataFrame([self._search_row()]),
            pd.DataFrame([detail]),
            settings(),
            publication_date_from="2026-08-18T12:30:00+02:00",
            publication_date_to="2026-08-18T12:40:00+02:00",
        )
        paragraphs = build_job_paragraphs(offers, settings())

        self.assertEqual(len(offers), 1)
        self.assertEqual(offers.iloc[0]["source"], "indeed")
        self.assertEqual(offers.iloc[0]["final_title"], "Data Engineer")
        self.assertEqual(
            offers.iloc[0]["final_url"],
            "https://ch-fr.indeed.com/viewjob?jk=job-shared",
        )
        self.assertEqual(
            offers.iloc[0]["date_posted_dt"].isoformat(),
            "2026-08-18T12:31:56.285000+02:00",
        )
        criteria = json.loads(offers.iloc[0]["criteria_json"])
        self.assertEqual(
            criteria["search"]["matched_search_locations"],
            ["Genève"],
        )
        self.assertIn("json_ld", criteria["detail"])
        self.assertGreaterEqual(len(paragraphs), 1)
        self.assertTrue(
            paragraphs["paragraph"].str.contains("pipelines de données").any()
        )

    def test_excludes_failed_and_out_of_window_details(self) -> None:
        ok_detail = parse_job_detail_html(DETAIL_HTML)
        ok_detail.update({"detail_status": "ok", "detail_error": None})
        failed_detail = dict(ok_detail)
        failed_detail.update(
            {
                "job_id_detail": "job-error",
                "canonical_url": canonicalize_indeed_job_url(job_key="job-error"),
                "detail_status": "error",
                "detail_error": "blocked",
            }
        )
        jobs = pd.DataFrame(
            [self._search_row(), self._search_row(job_id="job-error")]
        )

        failed_only = prepare_offers_dataframe(
            jobs,
            pd.DataFrame([ok_detail, failed_detail]),
            settings(),
            publication_date_from="2026-08-18T12:32:00+02:00",
            publication_date_to="2026-08-18T13:00:00+02:00",
        )

        self.assertTrue(failed_only.empty)

    def test_applies_half_open_publication_bounds(self) -> None:
        at_start = parse_job_detail_html(detail_html("job-start"))
        at_start.update(
            {
                "date_posted": "2026-08-18T10:30:00Z",
                "detail_status": "ok",
                "detail_error": None,
            }
        )
        at_end = parse_job_detail_html(detail_html("job-end"))
        at_end.update(
            {
                "date_posted": "2026-08-18T10:40:00Z",
                "detail_status": "ok",
                "detail_error": None,
            }
        )

        offers = prepare_offers_dataframe(
            pd.DataFrame(
                [self._search_row("job-start"), self._search_row("job-end")]
            ),
            pd.DataFrame([at_start, at_end]),
            settings(),
            publication_date_from="2026-08-18T12:30:00+02:00",
            publication_date_to="2026-08-18T12:40:00+02:00",
        )

        self.assertEqual(offers["final_job_id"].tolist(), ["job-start"])


class IndeedUiRegistrationTests(unittest.TestCase):
    def test_streamlit_registers_indeed_label_and_existing_icon(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        streamlit_source = (repository_root / "streamlit_app.py").read_text()

        self.assertIn('"indeed": "Indeed"', streamlit_source)
        self.assertIn(
            '"indeed": "app/static/source-icons/indeed.png"',
            streamlit_source,
        )
        self.assertTrue(
            (repository_root / "static" / "source-icons" / "indeed.png").is_file()
        )


if __name__ == "__main__":
    unittest.main()
