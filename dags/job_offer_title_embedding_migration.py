from __future__ import annotations

import logging

from airflow.decorators import dag, task
from pendulum import datetime

from job_matcher.migrations import (
    add_job_offer_title_embedding_column,
    backfill_job_offer_title_embeddings,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="job_offer_title_embedding_migration",
    description=(
        "Manually add job_offers.title_embedding and backfill embeddings for existing titles."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["jobs", "linkedin", "pgvector", "migration", "manual"],
)
def job_offer_title_embedding_migration():
    @task
    def migrate_schema():
        add_job_offer_title_embedding_column()
        logger.info("Schema migration completed for job_offers.title_embedding")
        return {"status": "ok"}

    @task
    def backfill_embeddings(_migration_result: dict[str, str]):
        result = backfill_job_offer_title_embeddings()
        logger.info("Backfill result: %s", result)
        return result

    migration_result = migrate_schema()
    backfill_embeddings(migration_result)


dag = job_offer_title_embedding_migration()
