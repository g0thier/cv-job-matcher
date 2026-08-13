from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import types
import unittest
from zoneinfo import ZoneInfo

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from job_matcher.jobup import (  # noqa: E402
    build_job_paragraphs,
    build_scheduled_window,
    build_startup_window,
    canonicalize_jobup_job_url,
    collect_job_details,
    collect_search_results,
    parse_job_detail_html,
    prepare_offers_dataframe,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _document(job_id: str, title: str, place: str = "Genève") -> dict:
    return {
        "id": job_id,
        "title": title,
        "company": {"id": "company-1", "name": "Example SA", "slug": "example-sa"},
        "place": place,
        "locations": [
            {
                "cantonCode": "GE",
                "city": place,
                "countryCode": "CH",
                "postalCode": "1200",
                "latitude": 46.2,
                "longitude": 6.1,
            }
        ],
        "initialPublicationDate": "2026-08-13T08:00:00+02:00",
        "publicationDate": "2026-08-13T08:15:00+02:00",
        "employmentPositionIds": [3],
        "employmentTypeIds": ["5"],
        "employmentGrades": [80, 100],
        "benefitIds": ["training"],
        "isPaid": True,
        "languageIds": ["fr"],
        "listingTags": [{"name": "quickApply"}],
        "coordinates": {"lat": 46.2, "lon": 6.1},
    }


class FakeResponse:
    def __init__(
        self,
        *,
        payload: dict | None = None,
        text: str = "",
        url: str = "https://job-search-api.jobup.ch/search/semantic",
    ) -> None:
        self._payload = payload
        self.text = text
        self.url = url

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        assert self._payload is not None
        return self._payload


class SearchSession:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def get(self, _url, params, timeout):
        self.calls.append(dict(params))
        region = params["regionIds"]
        page = params["page"]
        if (region, page) == (34, 1):
            documents = [_document("shared-id", "Shared role")]
            num_pages = 2
        elif (region, page) == (34, 2):
            documents = [_document("geneva-id", "Geneva role")]
            num_pages = 2
        elif (region, page) == (55, 1):
            documents = [_document("shared-id", "Shared role", "Lausanne")]
            num_pages = 1
        else:
            raise AssertionError((region, page))
        return FakeResponse(
            payload={
                "documents": documents,
                "numPages": num_pages,
                "totalHits": len(documents),
            },
            url=f"https://example.test/search?regionIds={region}&page={page}",
        )


class JobUpSearchTests(unittest.TestCase):
    def test_collects_every_page_for_two_separate_regions_and_deduplicates(self) -> None:
        session = SearchSession()
        settings = SimpleNamespace(timezone="Europe/Zurich")

        result = collect_search_results(
            "2026-08-13T08:00:00+02:00",
            "2026-08-13T08:15:00+02:00",
            settings=settings,
            session=session,
        )

        self.assertEqual(
            [(call["regionIds"], call["page"]) for call in session.calls],
            [(34, 1), (34, 2), (55, 1)],
        )
        self.assertTrue(all(call["rows"] == 200 for call in session.calls))
        self.assertEqual(len(result), 2)
        self.assertEqual(set(result["job_id"]), {"shared-id", "geneva-id"})
        self.assertEqual(
            result.loc[result["job_id"] == "shared-id", "url"].iloc[0],
            "https://www.jobup.ch/fr/emplois/detail/shared-id",
        )

    def test_canonical_url_removes_query_fragment_and_trailing_slash(self) -> None:
        self.assertEqual(
            canonicalize_jobup_job_url(
                "https://www.jobup.ch/fr/emplois/detail/abc/?x=1#description"
            ),
            "https://www.jobup.ch/fr/emplois/detail/abc",
        )


class JobUpWindowTests(unittest.TestCase):
    def test_startup_window_runs_from_local_midnight(self) -> None:
        start, end = build_startup_window(
            datetime(2026, 8, 13, 8, 45, tzinfo=ZoneInfo("UTC")),
            "Europe/Zurich",
        )

        self.assertEqual(start.isoformat(), "2026-08-13T00:00:00+02:00")
        self.assertEqual(end.isoformat(), "2026-08-13T10:45:00+02:00")

    def test_scheduled_window_is_fifteen_minutes(self) -> None:
        start, end = build_scheduled_window(
            datetime(2026, 8, 13, 10, 30, tzinfo=ZoneInfo("Europe/Zurich")),
            "Europe/Zurich",
        )

        self.assertEqual(start.isoformat(), "2026-08-13T10:15:00+02:00")
        self.assertEqual(end.isoformat(), "2026-08-13T10:30:00+02:00")


class JobUpDetailTests(unittest.TestCase):
    def setUp(self) -> None:
        self.html = """
        <html><head>
          <link rel="canonical" href="https://www.jobup.ch/fr/emplois/detail/job-1/?ref=x">
          <script type="application/ld+json">
          {
            "@type": "JobPosting",
            "title": "Data Engineer",
            "hiringOrganization": {"name": "Example SA"},
            "datePosted": "2026-08-13T08:00:00+02:00",
            "validThrough": "2026-09-01T23:59:59+02:00",
            "employmentType": "FULL_TIME",
            "industry": "Technology",
            "skills": "Python, SQL",
            "jobLocation": {
              "address": {
                "addressCountry": "CH",
                "addressRegion": "GE",
                "addressLocality": "Genève",
                "postalCode": "1200"
              },
              "geo": {"latitude": 46.2, "longitude": 6.1}
            }
          }
          </script>
        </head><body>
          <div data-cy="vacancy-description">
            <h2>À propos de cette offre</h2>
            <p>Construire et maintenir des pipelines de données fiables pour les utilisateurs.</p>
          </div>
        </body></html>
        """

    def test_parses_json_ld_and_vacancy_description(self) -> None:
        parsed = parse_job_detail_html(self.html)

        self.assertEqual(parsed["job_id_detail"], "job-1")
        self.assertEqual(parsed["canonical_url"], "https://www.jobup.ch/fr/emplois/detail/job-1")
        self.assertEqual(parsed["title_detail"], "Data Engineer")
        self.assertEqual(parsed["company_detail"], "Example SA")
        self.assertEqual(parsed["employment_type_detail"], "FULL_TIME")
        self.assertEqual(parsed["address_locality_detail"], "Genève")
        self.assertEqual(parsed["latitude_detail"], 46.2)
        self.assertIn("pipelines de données", parsed["description_text"])
        self.assertIn("vacancy_description_json_ld", parsed["source_parser"])

    def test_collect_details_records_one_error_and_continues(self) -> None:
        class DetailSession:
            def __init__(inner_self) -> None:
                inner_self.calls = 0

            def get(inner_self, _url, headers, timeout):
                inner_self.calls += 1
                if inner_self.calls == 1:
                    raise RuntimeError("temporary detail failure")
                return FakeResponse(text=self.html)

        jobs = pd.DataFrame(
            {
                "url": [
                    "https://www.jobup.ch/fr/emplois/detail/job-error",
                    "https://www.jobup.ch/fr/emplois/detail/job-1",
                ]
            }
        )
        settings = SimpleNamespace(
            timezone="Europe/Zurich",
            max_detail_pages=0,
        )

        details = collect_job_details(jobs, settings, DetailSession())

        self.assertEqual(details["detail_status"].tolist(), ["error", "ok"])
        self.assertIn("temporary detail failure", details.iloc[0]["detail_error"])

    def test_prepares_shared_offer_shape_and_paragraphs(self) -> None:
        jobs = pd.DataFrame([_document("job-1", "API title")])
        document = jobs.iloc[0].to_dict()
        search_session = SearchSession()
        del search_session
        search_row = {
            "search_url": "https://example.test/search?regionIds=34",
            "search_region_id": 34,
            "search_region_name": "Genève",
            "job_id": document["id"],
            "title": document["title"],
            "company": document["company"]["name"],
            "location": document["place"],
            "url": canonicalize_jobup_job_url(job_id=document["id"]),
            "list_date": document["publicationDate"],
            "initial_publication_date": document["initialPublicationDate"],
            "employment_type": json.dumps(document["employmentTypeIds"]),
            "address_country": "CH",
            "address_locality": "Genève",
            "address_region": "GE",
            "latitude": 46.2,
            "longitude": 6.1,
            "search_metadata_json": json.dumps(
                {"employmentGrades": [80, 100], "benefitIds": ["training"]}
            ),
            "collected_at": datetime.now(ZoneInfo("Europe/Zurich")),
        }
        details = pd.DataFrame(
            [
                {
                    **parse_job_detail_html(self.html),
                    "detail_status": "ok",
                    "detail_error": None,
                }
            ]
        )
        settings = SimpleNamespace(
            timezone="Europe/Zurich",
            paragraph_min_chars=40,
        )

        offers = prepare_offers_dataframe(
            pd.DataFrame([search_row]),
            details,
            settings,
        )
        paragraphs = build_job_paragraphs(offers, settings)

        self.assertEqual(offers.iloc[0]["source"], "jobup")
        self.assertEqual(offers.iloc[0]["final_title"], "Data Engineer")
        self.assertEqual(
            offers.iloc[0]["date_posted_dt"].isoformat(),
            "2026-08-13T08:15:00+02:00",
        )
        criteria = json.loads(offers.iloc[0]["criteria_json"])
        self.assertEqual(criteria["search"]["employmentGrades"], [80, 100])
        self.assertEqual(len(paragraphs), 1)


def _load_dag(filename: str):
    airflow_module = types.ModuleType("airflow")
    decorators_module = types.ModuleType("airflow.decorators")
    operators_module = types.ModuleType("airflow.operators")
    operators_python_module = types.ModuleType("airflow.operators.python")
    pendulum_module = types.ModuleType("pendulum")

    def fake_dag(**dag_kwargs):
        def decorator(_function):
            def wrapper(*_args, **_kwargs):
                return SimpleNamespace(**dag_kwargs)

            return wrapper

        return decorator

    decorators_module.dag = fake_dag
    decorators_module.task = lambda function=None, **_kwargs: function or (
        lambda wrapped: wrapped
    )
    operators_python_module.get_current_context = lambda: {}
    pendulum_module.datetime = lambda year, month, day, tz=None: SimpleNamespace(
        year=year,
        month=month,
        day=day,
        tz=tz,
    )

    config_module = types.ModuleType("job_matcher.config")
    config_module.get_settings = lambda: SimpleNamespace(timezone="Europe/Zurich")
    pipeline_module = types.ModuleType("job_matcher.pipeline")
    for name in (
        "collect_jobup_job_details_step",
        "collect_jobup_search_results_step",
        "filter_existing_jobs_step",
        "initialize_run",
        "persist_offers_step",
        "prepare_jobup_dataframes_step",
        "vectorize_paragraphs_step",
        "write_run_metadata",
    ):
        setattr(pipeline_module, name, lambda *args, **kwargs: {})

    module_names = (
        "airflow",
        "airflow.decorators",
        "airflow.operators",
        "airflow.operators.python",
        "pendulum",
        "job_matcher.config",
        "job_matcher.pipeline",
    )
    originals = {name: sys.modules.get(name) for name in module_names}
    sys.modules.update(
        {
            "airflow": airflow_module,
            "airflow.decorators": decorators_module,
            "airflow.operators": operators_module,
            "airflow.operators.python": operators_python_module,
            "pendulum": pendulum_module,
            "job_matcher.config": config_module,
            "job_matcher.pipeline": pipeline_module,
        }
    )
    try:
        path = REPO_ROOT / "dags" / filename
        spec = importlib.util.spec_from_file_location(f"test_{path.stem}", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        for name, original in originals.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


class JobUpDagDefinitionTests(unittest.TestCase):
    def test_scheduled_dag_runs_every_fifteen_minutes(self) -> None:
        module = _load_dag("jobup_jobs_ingestion.py")

        self.assertEqual(module.dag.dag_id, "jobup_jobs_ingestion")
        self.assertEqual(module.dag.schedule, "*/15 * * * *")
        self.assertFalse(module.dag.catchup)

    def test_startup_dag_is_unpaused_and_trigger_only(self) -> None:
        module = _load_dag("jobup_jobs_ingestion_startup.py")

        self.assertEqual(module.dag.dag_id, "jobup_jobs_ingestion_startup")
        self.assertIsNone(module.dag.schedule)
        self.assertFalse(module.dag.catchup)
        self.assertFalse(module.dag.is_paused_upon_creation)


if __name__ == "__main__":
    unittest.main()
