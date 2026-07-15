from __future__ import annotations

import logging
import os

from airflow.decorators import dag, task
from pendulum import datetime

from job_matcher.pipeline import (
    collect_etat_geneve_feed_step,
    collect_etat_geneve_job_details_step,
    filter_existing_jobs_step,
    initialize_run,
    persist_offers_step,
    prepare_etat_geneve_dataframes_step,
    vectorize_paragraphs_step,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="etat_geneve_jobs_ingestion",
    description=(
        "Collect État de Genève jobs, vectorize descriptions and save them "
        "in Postgres."
    ),
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["jobs", "etat-geneve", "rss", "pgvector"],
)
def etat_geneve_jobs_ingestion():
    @task
    def setup_run():
        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        run_key = f"etat_geneve_jobs_ingestion__{run_id}" if run_id else None
        run_context = initialize_run(run_key=run_key)
        logger.info("Initialized État de Genève run context: %s", run_context)
        return run_context

    @task
    def collect_jobs(context: dict[str, str]):
        result = collect_etat_geneve_feed_step(context["run_key"])
        logger.info("État de Genève feed collection result: %s", result)
        return result

    @task
    def filter_known_jobs(search_result: dict[str, str | int]):
        result = filter_existing_jobs_step(
            search_result["run_key"],
            search_result["jobs_path"],
        )
        logger.info("État de Genève known-job filtering result: %s", result)
        return result

    @task
    def collect_details(filtered_result: dict[str, str | int]):
        result = collect_etat_geneve_job_details_step(
            filtered_result["run_key"],
            filtered_result["jobs_path"],
        )
        logger.info("État de Genève detail collection result: %s", result)
        return result

    @task
    def prepare_payloads(detail_result: dict[str, str | int]):
        result = prepare_etat_geneve_dataframes_step(
            detail_result["run_key"],
            detail_result["jobs_path"],
            detail_result["details_path"],
        )
        logger.info("État de Genève preparation result: %s", result)
        return result

    @task
    def vectorize_paragraphs(prepared_result: dict[str, str | int]):
        result = vectorize_paragraphs_step(
            prepared_result["run_key"],
            prepared_result["paragraphs_path"],
        )
        logger.info("État de Genève vectorization result: %s", result)
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
        logger.info("État de Genève persistence result: %s", result)
        return result

    run_context = setup_run()
    search_result = collect_jobs(run_context)
    filtered_result = filter_known_jobs(search_result)
    detail_result = collect_details(filtered_result)
    prepared_result = prepare_payloads(detail_result)
    vectorized_result = vectorize_paragraphs(prepared_result)
    persist_results(prepared_result, vectorized_result)


dag = etat_geneve_jobs_ingestion()
