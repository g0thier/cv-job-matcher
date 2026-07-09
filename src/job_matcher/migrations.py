from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from sqlalchemy import text

from job_matcher.config import Settings, get_settings
from job_matcher.database import build_engine, session_scope
from job_matcher.embeddings import encode_texts
from job_matcher.models import JobOffer
from job_matcher.text_utils import utcnow

logger = logging.getLogger(__name__)


def _normalize_text(value: Any) -> str | None:
    if pd.isna(value):
        return None

    text_value = str(value).strip()
    return text_value or None


def add_job_offer_title_embedding_column(settings: Settings | None = None) -> None:
    active_settings = settings or get_settings()
    engine = build_engine(active_settings)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                ALTER TABLE job_offers
                ADD COLUMN IF NOT EXISTS title_embedding vector(384)
                """
            )
        )
    logger.info("Ensured column job_offers.title_embedding exists")


def backfill_job_offer_title_embeddings(
    settings: Settings | None = None,
    batch_size: int = 256,
) -> dict[str, int]:
    active_settings = settings or get_settings()
    now = utcnow()

    with session_scope(active_settings) as session:
        offers = (
            session.query(JobOffer)
            .filter(JobOffer.title.isnot(None))
            .filter(JobOffer.title_embedding.is_(None))
            .all()
        )

        normalized_titles = [_normalize_text(offer.title) for offer in offers]
        pending = [
            (offer, title)
            for offer, title in zip(offers, normalized_titles, strict=False)
            if title is not None
        ]

        skipped = len(offers) - len(pending)
        updated = 0

        for start in range(0, len(pending), batch_size):
            chunk = pending[start : start + batch_size]
            embeddings = encode_texts(
                [title for _, title in chunk],
                settings=active_settings,
                batch_size=batch_size,
            ).tolist()

            for (offer, _title), embedding in zip(chunk, embeddings, strict=False):
                offer.title_embedding = embedding
                offer.updated_at = now
                updated += 1

    result = {
        "offers_seen": len(offers),
        "offers_updated": updated,
        "offers_skipped": skipped,
    }
    logger.info("Title embedding backfill complete: %s", result)
    return result
