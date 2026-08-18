from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_dag(filename: str):
    calls: dict[str, list] = {
        "scheduled_windows": [],
        "startup_windows": [],
        "search": [],
        "details": [],
        "prepare": [],
        "metadata": [],
    }
    state: dict[str, object] = {"context": {}}

    airflow_module = types.ModuleType("airflow")
    decorators_module = types.ModuleType("airflow.decorators")
    operators_module = types.ModuleType("airflow.operators")
    operators_python_module = types.ModuleType("airflow.operators.python")
    pendulum_module = types.ModuleType("pendulum")

    def fake_dag(**dag_kwargs):
        def decorator(function):
            def wrapper(*_args, **_kwargs):
                return SimpleNamespace(**dag_kwargs)

            wrapper.__wrapped__ = function
            return wrapper

        return decorator

    decorators_module.dag = fake_dag
    decorators_module.task = lambda function=None, **_kwargs: function or (
        lambda wrapped: wrapped
    )
    operators_python_module.get_current_context = lambda: state["context"]
    pendulum_module.datetime = lambda year, month, day, tz=None: SimpleNamespace(
        year=year,
        month=month,
        day=day,
        tz=tz,
    )

    config_module = types.ModuleType("job_matcher.config")
    config_module.get_settings = lambda: SimpleNamespace(timezone="Europe/Zurich")

    indeed_module = types.ModuleType("job_matcher.indeed")
    indeed_module.INDEED_LOCATIONS = (
        ("Genève, GE", "Genève"),
        ("Lausanne, VD", "Lausanne"),
    )
    indeed_module.INDEED_RADIUS_KM = 25

    def build_scheduled_window(interval_end, timezone_name, overlap_minutes=5):
        calls["scheduled_windows"].append(
            (interval_end, timezone_name, overlap_minutes)
        )
        return (
            datetime(2026, 8, 18, 10, 10, tzinfo=timezone.utc),
            datetime(2026, 8, 18, 10, 30, tzinfo=timezone.utc),
        )

    def build_startup_window(run_start, timezone_name):
        calls["startup_windows"].append((run_start, timezone_name))
        return (
            datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 8, 18, 9, 42, tzinfo=timezone.utc),
        )

    indeed_module.build_scheduled_window = build_scheduled_window
    indeed_module.build_startup_window = build_startup_window

    pipeline_module = types.ModuleType("job_matcher.pipeline")

    def initialize_run(*_args, **_kwargs):
        return {"run_key": "run-1", "run_dir": "runtime/airflow/run-1"}

    def write_run_metadata(run_key, filename, data):
        calls["metadata"].append((run_key, filename, data))
        return f"runtime/airflow/{run_key}/{filename}"

    def collect_search(run_key, publication_date_from, publication_date_to):
        calls["search"].append(
            (run_key, publication_date_from, publication_date_to)
        )
        return {"run_key": run_key, "jobs_path": "jobs.pkl", "jobs_count": 8}

    def filter_existing(run_key, jobs_path):
        return {
            "run_key": run_key,
            "jobs_path": "jobs_filtered.pkl",
            "jobs_count": 6,
            "jobs_skipped": 2,
        }

    def collect_details(run_key, jobs_path):
        calls["details"].append((run_key, jobs_path))
        return {
            "run_key": run_key,
            "jobs_path": jobs_path,
            "details_path": "details.pkl",
            "details_count": 5,
            "details_succeeded": 4,
            "details_failed": 1,
        }

    def prepare(
        run_key,
        jobs_path,
        details_path,
        publication_date_from,
        publication_date_to,
    ):
        calls["prepare"].append(
            (
                run_key,
                jobs_path,
                details_path,
                publication_date_from,
                publication_date_to,
            )
        )
        return {
            "run_key": run_key,
            "offers_path": "offers.pkl",
            "paragraphs_path": "paragraphs.pkl",
            "offers_count": 4,
            "paragraphs_count": 12,
        }

    pipeline_module.collect_indeed_search_results_step = collect_search
    pipeline_module.collect_indeed_job_details_step = collect_details
    pipeline_module.filter_existing_jobs_step = filter_existing
    pipeline_module.initialize_run = initialize_run
    pipeline_module.persist_offers_step = lambda *_args, **_kwargs: {
        "offers_seen": 4,
        "offers_saved": 3,
        "offers_skipped": 1,
        "paragraphs_saved": 9,
    }
    pipeline_module.prepare_indeed_dataframes_step = prepare
    pipeline_module.vectorize_paragraphs_step = lambda run_key, _path: {
        "run_key": run_key,
        "paragraphs_vectorized_path": "paragraphs_vectorized.pkl",
        "paragraphs_count": 12,
    }
    pipeline_module.write_run_metadata = write_run_metadata

    module_names = (
        "airflow",
        "airflow.decorators",
        "airflow.operators",
        "airflow.operators.python",
        "pendulum",
        "job_matcher.config",
        "job_matcher.indeed",
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
            "job_matcher.indeed": indeed_module,
            "job_matcher.pipeline": pipeline_module,
        }
    )
    try:
        path = REPO_ROOT / "dags" / filename
        spec = importlib.util.spec_from_file_location(f"test_{path.stem}", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module, calls, state
    finally:
        for name, original in originals.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


class IndeedDagDefinitionTests(unittest.TestCase):
    def test_scheduled_dag_configuration(self) -> None:
        module, _calls, _state = _load_dag("indeed_jobs_ingestion.py")

        self.assertEqual(module.dag.dag_id, "indeed_jobs_ingestion")
        self.assertEqual(module.dag.schedule, "*/15 * * * *")
        self.assertFalse(module.dag.catchup)
        self.assertEqual(module.dag.max_active_runs, 1)
        self.assertEqual(module.dag.default_args["retries"], 2)

    def test_startup_dag_configuration(self) -> None:
        module, _calls, _state = _load_dag("indeed_jobs_ingestion_startup.py")

        self.assertEqual(module.dag.dag_id, "indeed_jobs_ingestion_startup")
        self.assertIsNone(module.dag.schedule)
        self.assertFalse(module.dag.catchup)
        self.assertEqual(module.dag.max_active_runs, 1)
        self.assertFalse(module.dag.is_paused_upon_creation)
        self.assertEqual(module.dag.default_args["retries"], 2)

    def test_scheduled_dag_passes_effective_window_and_records_counts(self) -> None:
        module, calls, state = _load_dag("indeed_jobs_ingestion.py")
        interval_end = datetime(2026, 8, 18, 10, 30, tzinfo=timezone.utc)
        state["context"] = {
            "dag_run": SimpleNamespace(start_date=interval_end),
            "data_interval_end": interval_end,
            "logical_date": interval_end,
        }

        with patch.dict(os.environ, {"AIRFLOW_CTX_DAG_RUN_ID": "scheduled-1"}):
            module.indeed_jobs_ingestion.__wrapped__()

        effective_from = "2026-08-18T10:10:00+00:00"
        window_to = "2026-08-18T10:30:00+00:00"
        self.assertEqual(
            calls["scheduled_windows"],
            [(interval_end, "Europe/Zurich", 5)],
        )
        self.assertEqual(calls["search"], [("run-1", effective_from, window_to)])
        self.assertEqual(
            calls["prepare"],
            [
                (
                    "run-1",
                    "jobs_filtered.pkl",
                    "details.pkl",
                    effective_from,
                    window_to,
                )
            ],
        )

        initial_metadata = calls["metadata"][0]
        self.assertEqual(initial_metadata[1], "collection_window.json")
        self.assertEqual(
            initial_metadata[2]["nominal_publication_date_from"],
            "2026-08-18T10:15:00+00:00",
        )
        self.assertEqual(
            initial_metadata[2]["effective_publication_date_from"],
            effective_from,
        )
        self.assertEqual(initial_metadata[2]["locations"], ["Genève", "Lausanne"])
        self.assertEqual(
            initial_metadata[2]["search_locations"],
            ["Genève, GE", "Lausanne, VD"],
        )
        self.assertEqual(initial_metadata[2]["radius_km"], 25)

        summary = calls["metadata"][1]
        self.assertEqual(summary[1], "ingestion_summary.json")
        self.assertEqual(summary[2]["counts"]["jobs_discovered"], 8)
        self.assertEqual(summary[2]["counts"]["jobs_already_known"], 2)
        self.assertEqual(summary[2]["counts"]["details_succeeded"], 4)
        self.assertEqual(summary[2]["counts"]["details_failed"], 1)
        self.assertEqual(summary[2]["counts"]["offers_saved"], 3)
        self.assertEqual(summary[2]["counts"]["paragraphs_saved"], 9)

    def test_startup_dag_uses_midnight_window_for_search_and_prepare(self) -> None:
        module, calls, state = _load_dag("indeed_jobs_ingestion_startup.py")
        run_start = datetime(2026, 8, 18, 9, 42, tzinfo=timezone.utc)
        state["context"] = {
            "dag_run": SimpleNamespace(start_date=run_start),
            "logical_date": run_start,
        }

        with patch.dict(os.environ, {"AIRFLOW_CTX_DAG_RUN_ID": "startup-1"}):
            module.indeed_jobs_ingestion_startup.__wrapped__()

        window_from = "2026-08-18T00:00:00+00:00"
        window_to = "2026-08-18T09:42:00+00:00"
        self.assertEqual(
            calls["startup_windows"],
            [(run_start, "Europe/Zurich")],
        )
        self.assertEqual(calls["search"], [("run-1", window_from, window_to)])
        self.assertEqual(calls["prepare"][0][-2:], (window_from, window_to))
        self.assertEqual(calls["metadata"][0][1], "startup_context.json")
        self.assertEqual(
            calls["metadata"][0][2]["effective_publication_date_from"],
            window_from,
        )
        self.assertEqual(calls["metadata"][1][1], "ingestion_summary.json")


if __name__ == "__main__":
    unittest.main()
