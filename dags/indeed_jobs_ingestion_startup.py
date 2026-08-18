from __future__ import annotations

import logging
import os
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from pendulum import datetime

from job_matcher.config import get_settings
from job_matcher.indeed import (
    INDEED_LOCATIONS,
    INDEED_RADIUS_KM,
    build_startup_window,
)
from job_matcher.pipeline import (
    collect_indeed_job_details_step,
    collect_indeed_search_results_step,
    filter_existing_jobs_step,
    initialize_run,
    persist_offers_step,
    prepare_indeed_dataframes_step,
    vectorize_paragraphs_step,
    write_run_metadata,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="indeed_jobs_ingestion_startup",
    description=(
        "Collect Indeed jobs in Geneva and Lausanne published since local "
        "midnight once at Airflow startup."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 2},
    tags=["jobs", "indeed", "pgvector", "startup"],
)
def indeed_jobs_ingestion_startup():
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
        run_key = f"indeed_jobs_ingestion_startup__{run_id}" if run_id else None
        run_context = initialize_run(settings=active_settings, run_key=run_key)
        window_context = {
            **run_context,
            "timezone": active_settings.timezone,
            "dag_run_start_date": run_start.isoformat(),
            "nominal_publication_date_from": window_start.isoformat(),
            "nominal_publication_date_to": window_end.isoformat(),
            "publication_date_from": window_start.isoformat(),
            "publication_date_to": window_end.isoformat(),
            "locations": [name for _query, name in INDEED_LOCATIONS],
            "search_locations": [query for query, _name in INDEED_LOCATIONS],
            "radius_km": INDEED_RADIUS_KM,
        }
        metadata_path = write_run_metadata(
            run_context["run_key"],
            "startup_context.json",
            {
                "timezone": window_context["timezone"],
                "dag_run_start_date": window_context["dag_run_start_date"],
                "interval_semantics": "[from,to)",
                "nominal_publication_date_from": window_context[
                    "nominal_publication_date_from"
                ],
                "nominal_publication_date_to": window_context[
                    "nominal_publication_date_to"
                ],
                "effective_publication_date_from": window_context[
                    "publication_date_from"
                ],
                "effective_publication_date_to": window_context[
                    "publication_date_to"
                ],
                "locations": window_context["locations"],
                "search_locations": window_context["search_locations"],
                "radius_km": window_context["radius_km"],
            },
        )
        logger.info(
            "Initialized Indeed startup interval %s -> %s; metadata=%s",
            window_start,
            window_end,
            metadata_path,
        )
        return window_context

    @task
    def collect_jobs(context: dict[str, Any]):
        result = collect_indeed_search_results_step(
            context["run_key"],
            context["publication_date_from"],
            context["publication_date_to"],
        )
        logger.info("Indeed startup search collection result: %s", result)
        return result

    @task
    def filter_known_jobs(search_result: dict[str, Any]):
        result = filter_existing_jobs_step(
            search_result["run_key"],
            search_result["jobs_path"],
        )
        logger.info("Indeed startup known-job filtering result: %s", result)
        return result

    @task
    def collect_details(filtered_result: dict[str, Any]):
        result = collect_indeed_job_details_step(
            filtered_result["run_key"],
            filtered_result["jobs_path"],
        )
        logger.info("Indeed startup detail collection result: %s", result)
        return result

    @task
    def prepare_payloads(
        detail_result: dict[str, Any],
        context: dict[str, Any],
    ):
        result = prepare_indeed_dataframes_step(
            detail_result["run_key"],
            detail_result["jobs_path"],
            detail_result["details_path"],
            context["publication_date_from"],
            context["publication_date_to"],
        )
        logger.info("Indeed startup preparation result: %s", result)
        return result

    @task
    def vectorize_paragraphs(prepared_result: dict[str, Any]):
        result = vectorize_paragraphs_step(
            prepared_result["run_key"],
            prepared_result["paragraphs_path"],
        )
        logger.info("Indeed startup vectorization result: %s", result)
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
        logger.info("Indeed startup persistence result: %s", result)
        return result

    @task
    def record_summary(
        context: dict[str, Any],
        search_result: dict[str, Any],
        filtered_result: dict[str, Any],
        detail_result: dict[str, Any],
        prepared_result: dict[str, Any],
        vectorized_result: dict[str, Any],
        persisted_result: dict[str, Any],
    ):
        summary = {
            "timezone": context["timezone"],
            "dag_run_start_date": context["dag_run_start_date"],
            "interval_semantics": "[from,to)",
            "nominal_publication_date_from": context[
                "nominal_publication_date_from"
            ],
            "nominal_publication_date_to": context["nominal_publication_date_to"],
            "effective_publication_date_from": context["publication_date_from"],
            "effective_publication_date_to": context["publication_date_to"],
            "locations": context["locations"],
            "search_locations": context["search_locations"],
            "radius_km": context["radius_km"],
            "counts": {
                "jobs_discovered": search_result.get("jobs_count", 0),
                "jobs_after_existing_filter": filtered_result.get("jobs_count", 0),
                "jobs_already_known": filtered_result.get("jobs_skipped", 0),
                "details_collected": detail_result.get("details_count", 0),
                "details_succeeded": detail_result.get("details_succeeded", 0),
                "details_failed": detail_result.get("details_failed", 0),
                "offers_prepared": prepared_result.get("offers_count", 0),
                "paragraphs_prepared": prepared_result.get("paragraphs_count", 0),
                "paragraphs_vectorized": vectorized_result.get(
                    "paragraphs_count", 0
                ),
                "offers_seen": persisted_result.get("offers_seen", 0),
                "offers_saved": persisted_result.get("offers_saved", 0),
                "offers_skipped": persisted_result.get("offers_skipped", 0),
                "paragraphs_saved": persisted_result.get("paragraphs_saved", 0),
            },
        }
        metadata_path = write_run_metadata(
            context["run_key"],
            "ingestion_summary.json",
            summary,
        )
        logger.info("Indeed startup summary persisted to %s", metadata_path)
        return {"run_key": context["run_key"], "metadata_path": metadata_path}

    run_context = setup_run()
    search_result = collect_jobs(run_context)
    filtered_result = filter_known_jobs(search_result)
    detail_result = collect_details(filtered_result)
    prepared_result = prepare_payloads(detail_result, run_context)
    vectorized_result = vectorize_paragraphs(prepared_result)
    persisted_result = persist_results(prepared_result, vectorized_result)
    record_summary(
        run_context,
        search_result,
        filtered_result,
        detail_result,
        prepared_result,
        vectorized_result,
        persisted_result,
    )


dag = indeed_jobs_ingestion_startup()
