from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Mapping

from sqlalchemy import Column, DateTime, Integer, MetaData, Table, Text, UniqueConstraint, create_engine, select
from sqlalchemy.exc import IntegrityError, OperationalError

ALREADY_CLAIMED_EXIT_CODE = 10
TRIGGER_STATUS_CLAIMED = "claimed"
TRIGGER_STATUS_TRIGGERED = "triggered"
TRIGGER_STATUS_FAILED = "failed"

metadata = MetaData()
startup_dag_triggers = Table(
    "startup_dag_triggers",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("dag_id", Text, nullable=False),
    Column("startup_id", Text, nullable=False),
    Column("run_id", Text, nullable=False),
    Column("claimed_at", DateTime(timezone=True), nullable=False),
    Column("trigger_status", Text, nullable=False),
    Column("triggered_at", DateTime(timezone=True), nullable=True),
    Column("last_error", Text, nullable=True),
    UniqueConstraint("dag_id", "startup_id", name="uq_startup_dag_triggers_dag_startup"),
)


@dataclass(frozen=True)
class ClaimResult:
    claimed: bool
    dag_id: str
    startup_id: str
    run_id: str
    trigger_status: str
    claimed_at: str | None = None
    triggered_at: str | None = None
    last_error: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)


def resolve_metadata_database_url(env: Mapping[str, str] | None = None) -> str:
    active_env = env or os.environ
    for key in (
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW_DATABASE_URL",
        "DATABASE_URL",
    ):
        value = active_env.get(key)
        if value:
            return value
    raise ValueError(
        "No metadata database URL found in AIRFLOW__DATABASE__SQL_ALCHEMY_CONN, "
        "AIRFLOW_DATABASE_URL, or DATABASE_URL."
    )


def build_startup_id(
    env: Mapping[str, str] | None = None,
    now: datetime | None = None,
    pid: int | None = None,
) -> str:
    active_env = env or os.environ
    configured_startup_id = active_env.get("AIRFLOW_STARTUP_ID")
    if configured_startup_id:
        return configured_startup_id

    current_time = now or datetime.now(timezone.utc)
    current_pid = os.getpid() if pid is None else pid
    return f"{current_time.strftime('%Y%m%dT%H%M%SZ')}-{current_pid}"


def build_run_id(startup_id: str) -> str:
    return f"startup__{startup_id}"


def _build_engine(database_url: str):
    return create_engine(database_url, pool_pre_ping=True)


def _ensure_control_table(engine) -> None:
    try:
        metadata.create_all(engine, tables=[startup_dag_triggers])
    except OperationalError as exc:
        if "already exists" not in str(exc).lower():
            raise


def _serialize_timestamp(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def claim_startup(
    dag_id: str,
    startup_id: str,
    run_id: str,
    database_url: str | None = None,
) -> ClaimResult:
    active_database_url = database_url or resolve_metadata_database_url()
    engine = _build_engine(active_database_url)
    _ensure_control_table(engine)
    claimed_at = datetime.now(timezone.utc)

    try:
        with engine.begin() as connection:
            connection.execute(
                startup_dag_triggers.insert().values(
                    dag_id=dag_id,
                    startup_id=startup_id,
                    run_id=run_id,
                    claimed_at=claimed_at,
                    trigger_status=TRIGGER_STATUS_CLAIMED,
                    triggered_at=None,
                    last_error=None,
                )
            )
        return ClaimResult(
            claimed=True,
            dag_id=dag_id,
            startup_id=startup_id,
            run_id=run_id,
            trigger_status=TRIGGER_STATUS_CLAIMED,
            claimed_at=_serialize_timestamp(claimed_at),
        )
    except IntegrityError:
        with engine.begin() as connection:
            existing_row = (
                connection.execute(
                    select(startup_dag_triggers).where(
                        startup_dag_triggers.c.dag_id == dag_id,
                        startup_dag_triggers.c.startup_id == startup_id,
                    )
                )
                .mappings()
                .one()
            )
        return ClaimResult(
            claimed=False,
            dag_id=dag_id,
            startup_id=startup_id,
            run_id=existing_row["run_id"],
            trigger_status=existing_row["trigger_status"],
            claimed_at=_serialize_timestamp(existing_row["claimed_at"]),
            triggered_at=_serialize_timestamp(existing_row["triggered_at"]),
            last_error=existing_row["last_error"],
        )
    finally:
        engine.dispose()


def update_trigger_status(
    dag_id: str,
    startup_id: str,
    trigger_status: str,
    database_url: str | None = None,
    last_error: str | None = None,
) -> None:
    active_database_url = database_url or resolve_metadata_database_url()
    engine = _build_engine(active_database_url)
    try:
        _ensure_control_table(engine)

        values: dict[str, object] = {
            "trigger_status": trigger_status,
            "last_error": last_error,
        }
        if trigger_status == TRIGGER_STATUS_TRIGGERED:
            values["triggered_at"] = datetime.now(timezone.utc)

        with engine.begin() as connection:
            connection.execute(
                startup_dag_triggers.update()
                .where(
                    startup_dag_triggers.c.dag_id == dag_id,
                    startup_dag_triggers.c.startup_id == startup_id,
                )
                .values(**values)
            )
    finally:
        engine.dispose()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage startup DAG trigger claims.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    claim_parser = subparsers.add_parser("claim", help="Claim a startup DAG trigger slot.")
    claim_parser.add_argument("--dag-id", required=True)
    claim_parser.add_argument("--startup-id", required=True)
    claim_parser.add_argument("--run-id", required=True)

    status_parser = subparsers.add_parser(
        "mark-status",
        help="Update the status of an existing startup DAG trigger claim.",
    )
    status_parser.add_argument("--dag-id", required=True)
    status_parser.add_argument("--startup-id", required=True)
    status_parser.add_argument(
        "--status",
        required=True,
        choices=[TRIGGER_STATUS_TRIGGERED, TRIGGER_STATUS_FAILED],
    )
    status_parser.add_argument("--last-error", default=None)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "claim":
        result = claim_startup(
            dag_id=args.dag_id,
            startup_id=args.startup_id,
            run_id=args.run_id,
        )
        print(result.to_json())
        return 0 if result.claimed else ALREADY_CLAIMED_EXIT_CODE

    if args.command == "mark-status":
        update_trigger_status(
            dag_id=args.dag_id,
            startup_id=args.startup_id,
            trigger_status=args.status,
            last_error=args.last_error,
        )
        print(
            json.dumps(
                {
                    "dag_id": args.dag_id,
                    "startup_id": args.startup_id,
                    "trigger_status": args.status,
                    "last_error": args.last_error,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
