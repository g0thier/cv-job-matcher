from __future__ import annotations

import logging
import os
from typing import Any
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from pendulum import datetime

from job_matcher.config import get_settings, load_linkedin_searches
from job_matcher.pipeline import (
    collect_job_details_step,
    collect_search_results_step,
    initialize_run,
    persist_offers_step,
    prepare_dataframes_step,
    vectorize_paragraphs_step,
    write_run_metadata,
)

logger = logging.getLogger(__name__)


def _build_startup_searches(
    lookback_seconds: int,
    searches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            **params,
            "f_TPR": f"r{lookback_seconds}",
        }
        for params in searches
    ]


@dag(
    dag_id="linkedin_jobs_ingestion_startup",
    description=(
        "Collect LinkedIn public jobs once at Airflow startup using a dynamic "
        "lookback window from local midnight."
    ),
    schedule="@once",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["jobs", "linkedin", "pgvector", "startup"],
)
def linkedin_jobs_ingestion_startup():
    @task
    def setup_run():
        active_settings = get_settings()
        context = get_current_context()
        dag_run = context["dag_run"]
        run_start = dag_run.start_date or context["logical_date"]
        if run_start.tzinfo is None:
            run_start = run_start.replace(tzinfo=ZoneInfo("UTC"))
        start_local = run_start.astimezone(ZoneInfo(active_settings.timezone))
        midnight_local = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
        lookback_seconds = max(
            1,
            int(start_local.timestamp() - midnight_local.timestamp()),
        )
        searches = _build_startup_searches(
            lookback_seconds,
            load_linkedin_searches(active_settings),
        )

        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        run_context = initialize_run(settings=active_settings, run_key=run_id)
        metadata_path = write_run_metadata(
            run_context["run_key"],
            "startup_context.json",
            {
                "timezone": active_settings.timezone,
                "dag_run_start_date": run_start.isoformat(),
                "dag_run_start_date_local": start_local.isoformat(),
                "lookback_seconds": lookback_seconds,
                "searches": searches,
            },
        )
        logger.info(
            "Initialized startup run context: %s with lookback_seconds=%s",
            run_context,
            lookback_seconds,
        )
        logger.info("Startup metadata persisted to %s", metadata_path)
        return {
            **run_context,
            "lookback_seconds": lookback_seconds,
            "searches": searches,
        }

    @task
    def collect_jobs(context: dict[str, Any]):
        result = collect_search_results_step(
            context["run_key"],
            searches=context["searches"],
        )
        logger.info("Search collection result: %s", result)
        return result

    @task
    def collect_details(search_result: dict[str, str | int]):
        result = collect_job_details_step(
            search_result["run_key"],
            search_result["jobs_path"],
        )
        logger.info("Detail collection result: %s", result)
        return result

    @task
    def prepare_payloads(detail_result: dict[str, str | int]):
        result = prepare_dataframes_step(
            detail_result["run_key"],
            detail_result["jobs_path"],
            detail_result["details_path"],
        )
        logger.info("Preparation result: %s", result)
        return result

    @task
    def vectorize_paragraphs(prepared_result: dict[str, str | int]):
        result = vectorize_paragraphs_step(
            prepared_result["run_key"],
            prepared_result["paragraphs_path"],
        )
        logger.info("Vectorization result: %s", result)
        return result

    @task
    def persist_results(
        prepared_result: dict[str, str | int],
        vectorized_result: dict[str, str | int],
    ):
        result = persist_offers_step(
            prepared_result["run_key"],
            prepared_result["offers_path"],
            vectorized_result["paragraphs_vectorized_path"],
        )
        logger.info("Persistence result: %s", result)
        return result

    run_context = setup_run()
    search_result = collect_jobs(run_context)
    detail_result = collect_details(search_result)
    prepared_result = prepare_payloads(detail_result)
    vectorized_result = vectorize_paragraphs(prepared_result)
    persist_results(prepared_result, vectorized_result)


dag = linkedin_jobs_ingestion_startup()
