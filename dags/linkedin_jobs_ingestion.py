from __future__ import annotations

import logging
import os

from airflow.decorators import dag, task
from pendulum import datetime

from job_matcher.pipeline import (
    collect_job_details_step,
    collect_search_results_step,
    initialize_run,
    persist_offers_step,
    prepare_dataframes_step,
    vectorize_paragraphs_step,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="linkedin_jobs_ingestion",
    description="Collect LinkedIn public jobs, vectorize descriptions and save them in Postgres.",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["jobs", "linkedin", "pgvector"],
)
def linkedin_jobs_ingestion():
    @task
    def setup_run():
        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        run_context = initialize_run(run_key=run_id)
        logger.info("Initialized Airflow run context: %s", run_context)
        return run_context

    @task
    def collect_jobs(context: dict[str, str]):
        result = collect_search_results_step(context["run_key"])
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


dag = linkedin_jobs_ingestion()
