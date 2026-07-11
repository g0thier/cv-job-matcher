from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))


def _load_startup_dag_module():
    airflow_module = types.ModuleType("airflow")
    decorators_module = types.ModuleType("airflow.decorators")
    operators_module = types.ModuleType("airflow.operators")
    operators_python_module = types.ModuleType("airflow.operators.python")
    pendulum_module = types.ModuleType("pendulum")

    def fake_dag(**dag_kwargs):
        def decorator(function):
            def wrapper(*_args, **_kwargs):
                return types.SimpleNamespace(**dag_kwargs)

            wrapper.__wrapped__ = function
            return wrapper

        return decorator

    def fake_task(function=None, **_kwargs):
        if function is None:
            return lambda wrapped: wrapped
        return function

    def fake_datetime(year, month, day, tz=None):
        return types.SimpleNamespace(year=year, month=month, day=day, tz=tz)

    decorators_module.dag = fake_dag
    decorators_module.task = fake_task
    operators_python_module.get_current_context = lambda: {}
    pendulum_module.datetime = fake_datetime

    config_module = types.ModuleType("job_matcher.config")
    config_module.get_settings = lambda: types.SimpleNamespace(timezone="Europe/Zurich")
    config_module.load_linkedin_searches = lambda _settings=None: []

    pipeline_module = types.ModuleType("job_matcher.pipeline")
    pipeline_module.collect_job_details_step = lambda *args, **kwargs: {}
    pipeline_module.collect_search_results_step = lambda *args, **kwargs: {}
    pipeline_module.filter_existing_jobs_step = lambda *args, **kwargs: {}
    pipeline_module.initialize_run = lambda *args, **kwargs: {"run_key": "run-1"}
    pipeline_module.persist_offers_step = lambda *args, **kwargs: {}
    pipeline_module.prepare_dataframes_step = lambda *args, **kwargs: {}
    pipeline_module.vectorize_paragraphs_step = lambda *args, **kwargs: {}
    pipeline_module.write_run_metadata = lambda *args, **kwargs: "runtime/airflow/run-1/meta.json"

    previous_modules = {
        name: sys.modules.get(name)
        for name in (
            "airflow",
            "airflow.decorators",
            "airflow.operators",
            "airflow.operators.python",
            "pendulum",
            "job_matcher.config",
            "job_matcher.pipeline",
        )
    }

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.decorators"] = decorators_module
    sys.modules["airflow.operators"] = operators_module
    sys.modules["airflow.operators.python"] = operators_python_module
    sys.modules["pendulum"] = pendulum_module
    sys.modules["job_matcher.config"] = config_module
    sys.modules["job_matcher.pipeline"] = pipeline_module

    try:
        module_path = REPO_ROOT / "dags" / "linkedin_jobs_ingestion_startup.py"
        spec = importlib.util.spec_from_file_location("test_startup_dag_module", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        for name, original in previous_modules.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


class StartupDagDefinitionTests(unittest.TestCase):
    def test_startup_dag_imports_with_expected_configuration(self) -> None:
        module = _load_startup_dag_module()

        self.assertEqual(module.dag.dag_id, "linkedin_jobs_ingestion_startup")
        self.assertIsNone(module.dag.schedule)
        self.assertFalse(module.dag.catchup)
        self.assertFalse(module.dag.is_paused_upon_creation)


if __name__ == "__main__":
    unittest.main()
