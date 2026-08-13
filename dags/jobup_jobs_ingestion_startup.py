from __future__ import annotations

import logging
import os
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from pendulum import datetime

from job_matcher.config import get_settings
from job_matcher.jobup import JOBUP_REGIONS, build_startup_window
from job_matcher.pipeline import (
    collect_jobup_job_details_step,
    collect_jobup_search_results_step,
    filter_existing_jobs_step,
    initialize_run,
    persist_offers_step,
    prepare_jobup_dataframes_step,
    vectorize_paragraphs_step,
    write_run_metadata,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="jobup_jobs_ingestion_startup",
    description=(
        "Collect JobUp jobs published since local midnight once at Airflow startup."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["jobs", "jobup", "pgvector", "startup"],
)
def jobup_jobs_ingestion_startup():
    @task
    def setup_run():
        active_settings = get_settings()
        context = get_current_context()
        dag_run = context["dag_run"]
        run_start = dag_run.start_date or context["logical_date"]
        window_start, window_end = build_startup_window(
            run_start,
            active_settings.timezone,
        )

        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        run_key = f"jobup_jobs_ingestion_startup__{run_id}" if run_id else None
        run_context = initialize_run(settings=active_settings, run_key=run_key)
        metadata_path = write_run_metadata(
            run_context["run_key"],
            "startup_context.json",
            {
                "timezone": active_settings.timezone,
                "dag_run_start_date": run_start.isoformat(),
                "publication_date_from": window_start.isoformat(),
                "publication_date_to": window_end.isoformat(),
                "regions": [
                    {"region_id": region_id, "name": name}
                    for region_id, name in JOBUP_REGIONS
                ],
            },
        )
        logger.info(
            "Initialized JobUp startup interval %s -> %s; metadata=%s",
            window_start,
            window_end,
            metadata_path,
        )
        return {
            **run_context,
            "publication_date_from": window_start.isoformat(),
            "publication_date_to": window_end.isoformat(),
        }

    @task
    def collect_jobs(context: dict[str, Any]):
        result = collect_jobup_search_results_step(
            context["run_key"],
            context["publication_date_from"],
            context["publication_date_to"],
        )
        logger.info("JobUp startup search collection result: %s", result)
        return result

    @task
    def filter_known_jobs(search_result: dict[str, Any]):
        result = filter_existing_jobs_step(
            search_result["run_key"],
            search_result["jobs_path"],
        )
        logger.info("JobUp startup known-job filtering result: %s", result)
        return result

    @task
    def collect_details(filtered_result: dict[str, Any]):
        result = collect_jobup_job_details_step(
            filtered_result["run_key"],
            filtered_result["jobs_path"],
        )
        logger.info("JobUp startup detail collection result: %s", result)
        return result

    @task
    def prepare_payloads(detail_result: dict[str, Any]):
        result = prepare_jobup_dataframes_step(
            detail_result["run_key"],
            detail_result["jobs_path"],
            detail_result["details_path"],
        )
        logger.info("JobUp startup preparation result: %s", result)
        return result

    @task
    def vectorize_paragraphs(prepared_result: dict[str, Any]):
        result = vectorize_paragraphs_step(
            prepared_result["run_key"],
            prepared_result["paragraphs_path"],
        )
        logger.info("JobUp startup vectorization result: %s", result)
        return result

    @task
    def persist_results(
        prepared_result: dict[str, Any],
        vectorized_result: dict[str, Any],
    ):
        result = persist_offers_step(
            prepared_result["run_key"],
            prepared_result["offers_path"],
            vectorized_result["paragraphs_vectorized_path"],
        )
        logger.info("JobUp startup persistence result: %s", result)
        return result

    run_context = setup_run()
    search_result = collect_jobs(run_context)
    filtered_result = filter_known_jobs(search_result)
    detail_result = collect_details(filtered_result)
    prepared_result = prepare_payloads(detail_result)
    vectorized_result = vectorize_paragraphs(prepared_result)
    persist_results(prepared_result, vectorized_result)


dag = jobup_jobs_ingestion_startup()
