from __future__ import annotations

import logging
import os

from airflow.decorators import dag, task
from pendulum import datetime

from job_matcher.config import get_settings
from job_matcher.pipeline import (
    collect_etat_geneve_feed_step,
    collect_etat_geneve_job_details_step,
    filter_existing_jobs_step,
    initialize_run,
    persist_offers_step,
    prepare_etat_geneve_dataframes_step,
    vectorize_paragraphs_step,
    write_run_metadata,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="etat_geneve_jobs_ingestion_startup",
    description="Collect all active État de Genève jobs once at Airflow startup.",
    schedule=None,
    start_date=datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["jobs", "etat-geneve", "rss", "pgvector", "startup"],
)
def etat_geneve_jobs_ingestion_startup():
    @task
    def setup_run():
        active_settings = get_settings()
        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        run_key = (
            f"etat_geneve_jobs_ingestion_startup__{run_id}" if run_id else None
        )
        run_context = initialize_run(settings=active_settings, run_key=run_key)
        metadata_path = write_run_metadata(
            run_context["run_key"],
            "startup_context.json",
            {
                "timezone": active_settings.timezone,
                "feed_url": active_settings.etat_geneve_rss_url,
                "collection_scope": "all_active_feed_entries",
            },
        )
        logger.info(
            "Initialized État de Genève startup context %s; metadata=%s",
            run_context,
            metadata_path,
        )
        return run_context

    @task
    def collect_jobs(context: dict[str, str]):
        result = collect_etat_geneve_feed_step(context["run_key"])
        logger.info("État de Genève startup feed collection result: %s", result)
        return result

    @task
    def filter_known_jobs(search_result: dict[str, str | int]):
        result = filter_existing_jobs_step(
            search_result["run_key"],
            search_result["jobs_path"],
        )
        logger.info("État de Genève startup known-job filtering result: %s", result)
        return result

    @task
    def collect_details(filtered_result: dict[str, str | int]):
        result = collect_etat_geneve_job_details_step(
            filtered_result["run_key"],
            filtered_result["jobs_path"],
        )
        logger.info("État de Genève startup detail collection result: %s", result)
        return result

    @task
    def prepare_payloads(detail_result: dict[str, str | int]):
        result = prepare_etat_geneve_dataframes_step(
            detail_result["run_key"],
            detail_result["jobs_path"],
            detail_result["details_path"],
        )
        logger.info("État de Genève startup preparation result: %s", result)
        return result

    @task
    def vectorize_paragraphs(prepared_result: dict[str, str | int]):
        result = vectorize_paragraphs_step(
            prepared_result["run_key"],
            prepared_result["paragraphs_path"],
        )
        logger.info("État de Genève startup vectorization result: %s", result)
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
        logger.info("État de Genève startup persistence result: %s", result)
        return result

    run_context = setup_run()
    search_result = collect_jobs(run_context)
    filtered_result = filter_known_jobs(search_result)
    detail_result = collect_details(filtered_result)
    prepared_result = prepare_payloads(detail_result)
    vectorized_result = vectorize_paragraphs(prepared_result)
    persist_results(prepared_result, vectorized_result)


dag = etat_geneve_jobs_ingestion_startup()
