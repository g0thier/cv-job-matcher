from __future__ import annotations

import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from job_matcher.startup_trigger import (  # noqa: E402
    ALREADY_CLAIMED_EXIT_CODE,
    TRIGGER_STATUS_FAILED,
    TRIGGER_STATUS_TRIGGERED,
    build_run_id,
    build_startup_id,
    claim_startup,
    resolve_metadata_database_url,
    update_trigger_status,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "trigger_startup_dags.sh"


@dataclass
class ScriptRunResult:
    returncode: int
    stdout: str
    stderr: str
    state: dict[str, str]


class StartupTriggerHelperTests(unittest.TestCase):
    def test_resolve_metadata_database_url_prefers_airflow_connection(self) -> None:
        resolved = resolve_metadata_database_url(
            {
                "DATABASE_URL": "postgresql://app",
                "AIRFLOW_DATABASE_URL": "postgresql://airflow-alt",
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://airflow-main",
            }
        )

        self.assertEqual(resolved, "postgresql://airflow-main")

    def test_build_startup_id_uses_env_when_provided(self) -> None:
        startup_id = build_startup_id({"AIRFLOW_STARTUP_ID": "shared-startup"})

        self.assertEqual(startup_id, "shared-startup")
        self.assertEqual(build_run_id(startup_id), "startup__shared-startup")

    def test_build_startup_id_falls_back_to_timestamp_and_pid(self) -> None:
        startup_id = build_startup_id(
            env={},
            now=datetime(2026, 7, 11, 8, 45, 0, tzinfo=timezone.utc),
            pid=123,
        )

        self.assertEqual(startup_id, "20260711T084500Z-123")
        self.assertEqual(build_run_id(startup_id), "startup__20260711T084500Z-123")

    def test_claim_startup_returns_already_claimed_for_second_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_url = f"sqlite:///{Path(temp_dir) / 'startup.db'}"

            first = claim_startup(
                dag_id="linkedin_jobs_ingestion_startup",
                startup_id="shared-startup",
                run_id="startup__shared-startup",
                database_url=database_url,
            )
            second = claim_startup(
                dag_id="linkedin_jobs_ingestion_startup",
                startup_id="shared-startup",
                run_id="startup__shared-startup",
                database_url=database_url,
            )

        self.assertTrue(first.claimed)
        self.assertFalse(second.claimed)

    def test_concurrent_claims_only_allow_one_winner(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_url = f"sqlite:///{Path(temp_dir) / 'startup.db'}"
            barrier = threading.Barrier(2)

            def run_claim():
                barrier.wait()
                return claim_startup(
                    dag_id="linkedin_jobs_ingestion_startup",
                    startup_id="shared-startup",
                    run_id="startup__shared-startup",
                    database_url=database_url,
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _index: run_claim(), range(2)))

        self.assertEqual(sum(1 for result in results if result.claimed), 1)
        self.assertEqual(sum(1 for result in results if not result.claimed), 1)

    def test_update_trigger_status_persists_last_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_url = f"sqlite:///{Path(temp_dir) / 'startup.db'}"
            claim_startup(
                dag_id="linkedin_jobs_ingestion_startup",
                startup_id="shared-startup",
                run_id="startup__shared-startup",
                database_url=database_url,
            )

            update_trigger_status(
                dag_id="linkedin_jobs_ingestion_startup",
                startup_id="shared-startup",
                trigger_status=TRIGGER_STATUS_FAILED,
                database_url=database_url,
                last_error="trigger failed",
            )
            result = claim_startup(
                dag_id="linkedin_jobs_ingestion_startup",
                startup_id="shared-startup",
                run_id="startup__shared-startup",
                database_url=database_url,
            )

        self.assertFalse(result.claimed)
        self.assertEqual(result.trigger_status, TRIGGER_STATUS_FAILED)
        self.assertEqual(result.last_error, "trigger failed")


class StartupTriggerScriptTests(unittest.TestCase):
    def _write_fake_airflow(self, directory: Path) -> None:
        script = textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import os
            import sys
            from pathlib import Path

            state_dir = Path(os.environ["FAKE_AIRFLOW_STATE_DIR"])
            state_dir.mkdir(parents=True, exist_ok=True)
            args = sys.argv[1:]

            def read_count(name):
                path = state_dir / name
                if not path.exists():
                    return 0
                return int(path.read_text())

            def write_count(name, value):
                (state_dir / name).write_text(str(value))

            if args[:2] == ["db", "check"]:
                count = read_count("db_check_count") + 1
                write_count("db_check_count", count)
                ready_after = int(os.environ.get("FAKE_AIRFLOW_DB_READY_AFTER", "1"))
                if count >= ready_after:
                    print("db ok")
                    raise SystemExit(0)
                raise SystemExit(1)

            if args[:2] == ["dags", "list"]:
                count = read_count("dags_list_count") + 1
                write_count("dags_list_count", count)
                ready_after = int(os.environ.get("FAKE_AIRFLOW_DAG_READY_AFTER", "1"))
                dag_id = os.environ.get("FAKE_AIRFLOW_DAG_ID", "linkedin_jobs_ingestion_startup")
                paused_state = os.environ.get("FAKE_AIRFLOW_PAUSED_STATE", "true").lower() == "true"
                if (state_dir / "unpaused").exists():
                    paused_state = False
                if "--output" in args and "json" in args:
                    payload = []
                    if count >= ready_after:
                        payload.append({"dag_id": dag_id, "is_paused": paused_state})
                    print(json.dumps(payload))
                    raise SystemExit(0)
                if count >= ready_after:
                    print(f"{dag_id} | /opt/project/dags/{dag_id}.py | owner | False")
                raise SystemExit(0)

            if args[:2] == ["dags", "unpause"]:
                dag_id = args[2]
                (state_dir / "unpaused").write_text(dag_id)
                print(f"Dag: {dag_id}, paused: False")
                raise SystemExit(0)

            if args[:2] == ["dags", "trigger"]:
                run_id = args[args.index("--run-id") + 1]
                dag_id = args[-1]
                (state_dir / "last_trigger_run_id").write_text(run_id)
                (state_dir / "last_trigger_dag_id").write_text(dag_id)
                print(f"Triggered {dag_id} with {run_id}")
                raise SystemExit(0)

            raise SystemExit(f"Unsupported fake airflow invocation: {args}")
            """
        )
        airflow_path = directory / "airflow"
        airflow_path.write_text(script)
        airflow_path.chmod(airflow_path.stat().st_mode | stat.S_IEXEC)

    def _run_script(self, env_overrides: dict[str, str]) -> ScriptRunResult:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            self._write_fake_airflow(fake_bin)
            state_dir = temp_path / "state"
            database_path = temp_path / "startup.db"

            env = os.environ.copy()
            env.update(
                {
                    "PYTHONPATH": str(REPO_ROOT / "src"),
                    "PATH": f"{fake_bin}:{env['PATH']}",
                    "FAKE_AIRFLOW_STATE_DIR": str(state_dir),
                    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{database_path}",
                    "STARTUP_DAG_MAX_ATTEMPTS": "3",
                    "STARTUP_DAG_RETRY_DELAY": "0",
                }
            )
            env.update(env_overrides)

            completed = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )
            state = {
                path.name: path.read_text()
                for path in state_dir.iterdir()
                if path.is_file()
            } if state_dir.exists() else {}
            return ScriptRunResult(
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
                state=state,
            )

    def test_script_waits_for_database_and_dag_before_triggering(self) -> None:
        result = self._run_script(
            {
                "FAKE_AIRFLOW_DB_READY_AFTER": "2",
                "FAKE_AIRFLOW_DAG_READY_AFTER": "2",
            }
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("Airflow database unavailable", result.stdout)
        self.assertIn("not available yet", result.stdout)
        self.assertEqual(result.state["last_trigger_dag_id"], "linkedin_jobs_ingestion_startup")

    def test_script_uses_provided_startup_id_for_run_id(self) -> None:
        result = self._run_script({"AIRFLOW_STARTUP_ID": "shared-startup"})

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.state["last_trigger_run_id"], "startup__shared-startup")

    def test_script_fallback_run_id_changes_between_invocations(self) -> None:
        first = self._run_script({})
        second = self._run_script({})

        self.assertEqual(first.returncode, 0, msg=first.stderr)
        self.assertEqual(second.returncode, 0, msg=second.stderr)
        first_run_id = first.state["last_trigger_run_id"]
        second_run_id = second.state["last_trigger_run_id"]
        self.assertNotEqual(first_run_id, second_run_id)

    def test_script_exits_with_error_when_dag_is_never_discovered(self) -> None:
        result = self._run_script({"FAKE_AIRFLOW_DAG_READY_AFTER": "99"})

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("was not discovered", result.stdout)

    def test_script_unpauses_before_triggering(self) -> None:
        result = self._run_script({})

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("unpaused", result.state)

    def test_script_skips_trigger_when_startup_is_already_claimed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            self._write_fake_airflow(fake_bin)
            state_dir = temp_path / "state"
            database_path = temp_path / "startup.db"

            env = os.environ.copy()
            env.update(
                {
                    "PYTHONPATH": str(REPO_ROOT / "src"),
                    "PATH": f"{fake_bin}:{env['PATH']}",
                    "FAKE_AIRFLOW_STATE_DIR": str(state_dir),
                    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{database_path}",
                    "STARTUP_DAG_MAX_ATTEMPTS": "3",
                    "STARTUP_DAG_RETRY_DELAY": "0",
                    "AIRFLOW_STARTUP_ID": "shared-startup",
                }
            )

            first = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )
            first_run_id = (state_dir / "last_trigger_run_id").read_text()
            (state_dir / "last_trigger_run_id").unlink()

            second = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(first.returncode, 0, msg=first.stderr)
            self.assertEqual(second.returncode, 0, msg=second.stderr)
            self.assertEqual(first_run_id, "startup__shared-startup")
            self.assertFalse((state_dir / "last_trigger_run_id").exists())
            self.assertIn("already claimed", second.stdout)


class DeploymentConfigurationTests(unittest.TestCase):
    def test_docker_compose_declares_single_startup_trigger_service(self) -> None:
        compose_text = (REPO_ROOT / "docker-compose.yml").read_text()

        self.assertEqual(compose_text.count("airflow-startup-trigger:"), 1)
        self.assertIn('command: ["bash", "/opt/project/scripts/trigger_startup_dags.sh"]', compose_text)
        scheduler_block = compose_text.split("airflow-scheduler:")[1].split("airflow-startup-trigger:")[0]
        self.assertNotIn("trigger_startup_dags.sh", scheduler_block)


if __name__ == "__main__":
    unittest.main()
