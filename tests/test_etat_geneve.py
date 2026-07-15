from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
import types
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from job_matcher.etat_geneve import (  # noqa: E402
    build_job_paragraphs,
    parse_geneva_date,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


class EtatGeneveDateParsingTests(unittest.TestCase):
    def test_parses_french_and_numeric_dates_in_local_timezone(self) -> None:
        published = parse_geneva_date("15 juillet 2026", "Europe/Zurich")
        deadline = parse_geneva_date(
            "31.07.2026",
            "Europe/Zurich",
            end_of_day=True,
        )

        self.assertEqual(published.isoformat(), "2026-07-15T00:00:00+02:00")
        self.assertEqual(deadline.isoformat(), "2026-07-31T23:59:59+02:00")


class EtatGeneveParagraphTests(unittest.TestCase):
    def test_groups_sections_like_linkedin_description(self) -> None:
        offers = pd.DataFrame(
            [
                {
                    "final_url": "https://example.test/jobs/geneva-1",
                    "final_job_id": "geneva-1",
                    "description_text": (
                        "Votre mission\n\n"
                        "Assurer le suivi administratif des dossiers avec qualité.\n\n"
                        "• Coordonner les échanges avec les différents partenaires.\n\n"
                        "Votre profil\n\n"
                        "Disposer d'une solide organisation et apprécier le travail en équipe."
                    ),
                }
            ]
        )
        settings = SimpleNamespace(paragraph_min_chars=40)

        paragraphs = build_job_paragraphs(offers, settings)

        self.assertEqual(len(paragraphs), 1)
        paragraph = paragraphs.iloc[0]["paragraph"]
        self.assertNotIn("\n", paragraph)
        self.assertIn("Votre mission", paragraph)
        self.assertIn("Votre profil", paragraph)


def _load_dag(filename: str):
    airflow_module = types.ModuleType("airflow")
    decorators_module = types.ModuleType("airflow.decorators")
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
    pendulum_module.datetime = lambda year, month, day, tz=None: SimpleNamespace(
        year=year,
        month=month,
        day=day,
        tz=tz,
    )

    config_module = types.ModuleType("job_matcher.config")
    config_module.get_settings = lambda: SimpleNamespace(
        timezone="Europe/Zurich",
        etat_geneve_rss_url="https://www.ge.ch/rss/offres-emploi-etat-geneve",
    )
    pipeline_module = types.ModuleType("job_matcher.pipeline")
    for name in (
        "collect_etat_geneve_feed_step",
        "collect_etat_geneve_job_details_step",
        "filter_existing_jobs_step",
        "initialize_run",
        "persist_offers_step",
        "prepare_etat_geneve_dataframes_step",
        "vectorize_paragraphs_step",
        "write_run_metadata",
    ):
        setattr(pipeline_module, name, lambda *args, **kwargs: {})

    module_names = (
        "airflow",
        "airflow.decorators",
        "pendulum",
        "job_matcher.config",
        "job_matcher.pipeline",
    )
    originals = {name: sys.modules.get(name) for name in module_names}
    sys.modules.update(
        {
            "airflow": airflow_module,
            "airflow.decorators": decorators_module,
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


class EtatGeneveDagDefinitionTests(unittest.TestCase):
    def test_scheduled_dag_runs_hourly(self) -> None:
        module = _load_dag("etat_geneve_jobs_ingestion.py")

        self.assertEqual(module.dag.dag_id, "etat_geneve_jobs_ingestion")
        self.assertEqual(module.dag.schedule, "0 * * * *")
        self.assertFalse(module.dag.catchup)

    def test_startup_dag_is_unpaused_and_trigger_only(self) -> None:
        module = _load_dag("etat_geneve_jobs_ingestion_startup.py")

        self.assertEqual(module.dag.dag_id, "etat_geneve_jobs_ingestion_startup")
        self.assertIsNone(module.dag.schedule)
        self.assertFalse(module.dag.catchup)
        self.assertFalse(module.dag.is_paused_upon_creation)


if __name__ == "__main__":
    unittest.main()
