from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from job_matcher.config import Settings, get_settings
from job_matcher.database import ensure_database, session_scope
from job_matcher.embeddings import encode_texts
from job_matcher.linkedin import (
    build_job_paragraphs,
    build_search_urls,
    collect_job_details,
    collect_search_results,
    prepare_offers_dataframe,
)
from job_matcher.models import JobOffer, JobParagraph
from job_matcher.text_utils import utcnow

logger = logging.getLogger(__name__)

RUNTIME_ROOT = Path("runtime/airflow")


@dataclass
class PipelineResult:
    offers_seen: int
    offers_saved: int
    offers_skipped: int
    paragraphs_saved: int

    def to_dict(self) -> dict[str, int]:
        return {
            "offers_seen": self.offers_seen,
            "offers_saved": self.offers_saved,
            "offers_skipped": self.offers_skipped,
            "paragraphs_saved": self.paragraphs_saved,
        }


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if pd.isna(value):
        return None
    return value


def _sanitize_run_key(run_key: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", run_key)


def _normalize_text(value: Any) -> str | None:
    if pd.isna(value):
        return None

    text_value = str(value).strip()
    return text_value or None


def get_run_directory(run_key: str) -> Path:
    run_dir = RUNTIME_ROOT / _sanitize_run_key(run_key)
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _write_dataframe(df: pd.DataFrame, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(path)
    return str(path)


def _read_dataframe(path: str | Path) -> pd.DataFrame:
    return pd.read_pickle(Path(path))


def _write_json(data: dict[str, Any], path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str))
    return str(path)


def write_run_metadata(run_key: str, filename: str, data: dict[str, Any]) -> str:
    return _write_json(data, get_run_directory(run_key) / filename)


def initialize_run(
    settings: Settings | None = None,
    run_key: str | None = None,
) -> dict[str, str]:
    active_settings = settings or get_settings()
    logger.info("Initializing ingestion run and ensuring database schema is ready")
    ensure_database(active_settings)
    run_key = run_key or utcnow().strftime("%Y%m%dT%H%M%S")
    run_dir = get_run_directory(run_key)
    _write_json(
        {
            "run_key": run_key,
            "database_url": active_settings.database_url,
            "timezone": active_settings.timezone,
            "embedding_model": active_settings.embedding_model_name,
        },
        run_dir / "context.json",
    )
    logger.info("Run initialized in %s", run_dir)
    return {"run_key": run_key, "run_dir": str(run_dir)}


def collect_search_results_step(
    run_key: str,
    settings: Settings | None = None,
    searches: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    run_dir = get_run_directory(run_key)
    search_urls = build_search_urls(active_settings, searches=searches)
    logger.info("Collecting LinkedIn search results from %s search URLs", len(search_urls))
    for index, url in enumerate(search_urls, start=1):
        logger.info("Search URL %s/%s: %s", index, len(search_urls), url)

    jobs_df = asyncio.run(collect_search_results(search_urls, active_settings))
    jobs_path = run_dir / "jobs.pkl"
    _write_dataframe(jobs_df, jobs_path)

    logger.info(
        "Collected %s unique job cards and saved them to %s",
        len(jobs_df),
        jobs_path,
    )
    return {
        "run_key": run_key,
        "jobs_path": str(jobs_path),
        "jobs_count": int(len(jobs_df)),
    }


def filter_existing_jobs_step(
    run_key: str, jobs_path: str, settings: Settings | None = None
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    run_dir = get_run_directory(run_key)
    jobs_df = _read_dataframe(jobs_path)

    if jobs_df.empty:
        filtered_path = run_dir / "jobs_filtered.pkl"
        _write_dataframe(jobs_df, filtered_path)
        logger.info("No job cards to filter before detail collection")
        return {
            "run_key": run_key,
            "jobs_path": str(filtered_path),
            "jobs_count": 0,
            "jobs_skipped": 0,
        }

    with session_scope(active_settings) as session:
        existing_urls = {
            canonical_url
            for (canonical_url,) in session.query(JobOffer.canonical_url).all()
            if canonical_url
        }

    filtered_jobs_df = jobs_df[~jobs_df["url"].isin(existing_urls)].reset_index(drop=True)
    skipped_jobs = int(len(jobs_df) - len(filtered_jobs_df))
    filtered_path = run_dir / "jobs_filtered.pkl"
    _write_dataframe(filtered_jobs_df, filtered_path)

    logger.info(
        "Pre-filtered known offers before detail collection: %s found, %s already known, %s remaining",
        len(jobs_df),
        skipped_jobs,
        len(filtered_jobs_df),
    )
    return {
        "run_key": run_key,
        "jobs_path": str(filtered_path),
        "jobs_count": int(len(filtered_jobs_df)),
        "jobs_skipped": skipped_jobs,
    }


def collect_job_details_step(
    run_key: str, jobs_path: str, settings: Settings | None = None
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    run_dir = get_run_directory(run_key)
    jobs_df = _read_dataframe(jobs_path)
    logger.info("Collecting details for %s job cards", len(jobs_df))

    details_df = asyncio.run(collect_job_details(jobs_df, active_settings))
    details_path = run_dir / "details.pkl"
    _write_dataframe(details_df, details_path)

    status_counts = (
        details_df["detail_status"].fillna("missing").value_counts().to_dict()
        if not details_df.empty and "detail_status" in details_df
        else {}
    )
    logger.info(
        "Collected %s detail pages with status distribution: %s",
        len(details_df),
        status_counts,
    )

    if not details_df.empty and "detail_error" in details_df:
        errors = details_df[details_df["detail_error"].notna()][
            ["canonical_url", "detail_error"]
        ].head(5)
        if not errors.empty:
            logger.warning("Sample detail errors: %s", errors.to_dict(orient="records"))

    return {
        "run_key": run_key,
        "jobs_path": jobs_path,
        "details_path": str(details_path),
        "details_count": int(len(details_df)),
    }


def prepare_dataframes_step(
    run_key: str,
    jobs_path: str,
    details_path: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    run_dir = get_run_directory(run_key)
    jobs_df = _read_dataframe(jobs_path)
    details_df = _read_dataframe(details_path)

    logger.info(
        "Preparing merged offers dataframe from %s jobs and %s detail rows",
        len(jobs_df),
        len(details_df),
    )
    offers_df = prepare_offers_dataframe(jobs_df, details_df)
    if not offers_df.empty:
        offers_before_dedup = len(offers_df)
        offers_df = offers_df.drop_duplicates(subset=["final_url"]).reset_index(drop=True)
        duplicate_offers = offers_before_dedup - len(offers_df)
        if duplicate_offers:
            logger.warning(
                "Dropped %s duplicate offers from prepared dataframe based on final_url",
                duplicate_offers,
            )
    paragraphs_df = build_job_paragraphs(offers_df, active_settings)

    offers_path = run_dir / "offers.pkl"
    paragraphs_path = run_dir / "paragraphs.pkl"
    _write_dataframe(offers_df, offers_path)
    _write_dataframe(paragraphs_df, paragraphs_path)

    logger.info(
        "Prepared %s offers and %s paragraphs",
        len(offers_df),
        len(paragraphs_df),
    )
    return {
        "run_key": run_key,
        "offers_path": str(offers_path),
        "paragraphs_path": str(paragraphs_path),
        "offers_count": int(len(offers_df)),
        "paragraphs_count": int(len(paragraphs_df)),
    }


def vectorize_paragraphs_step(
    run_key: str, paragraphs_path: str, settings: Settings | None = None
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    run_dir = get_run_directory(run_key)
    paragraphs_df = _read_dataframe(paragraphs_path)
    logger.info("Vectorizing %s paragraphs", len(paragraphs_df))

    vectorized_df = paragraphs_df.copy()
    if vectorized_df.empty:
        vectorized_df["embedding"] = []
    else:
        vectorized_df["embedding"] = encode_texts(
            vectorized_df["paragraph"].tolist(),
            settings=active_settings,
        ).tolist()

    vectorized_path = run_dir / "paragraphs_vectorized.pkl"
    _write_dataframe(vectorized_df, vectorized_path)
    logger.info("Saved vectorized paragraphs to %s", vectorized_path)
    return {
        "run_key": run_key,
        "paragraphs_vectorized_path": str(vectorized_path),
        "paragraphs_count": int(len(vectorized_df)),
    }


def _attach_title_embeddings(
    offers_df: pd.DataFrame, settings: Settings | None = None
) -> pd.DataFrame:
    vectorized_df = offers_df.copy()
    vectorized_df["title_embedding"] = None

    if vectorized_df.empty:
        return vectorized_df

    normalized_titles = vectorized_df["final_title"].apply(_normalize_text)
    title_mask = normalized_titles.notna()
    if not title_mask.any():
        return vectorized_df

    embeddings = encode_texts(
        normalized_titles[title_mask].tolist(),
        settings=settings,
    ).tolist()
    for row_index, embedding in zip(vectorized_df.index[title_mask], embeddings, strict=False):
        vectorized_df.at[row_index, "title_embedding"] = embedding
    return vectorized_df


def persist_offers_step(
    run_key: str,
    offers_path: str,
    paragraphs_vectorized_path: str,
    settings: Settings | None = None,
) -> dict[str, int]:
    active_settings = settings or get_settings()
    offers_df = _attach_title_embeddings(
        _read_dataframe(offers_path),
        settings=active_settings,
    )
    paragraphs_df = _read_dataframe(paragraphs_vectorized_path)
    logger.info(
        "Persisting %s offers, their title embeddings, and %s vectorized paragraphs to Postgres",
        len(offers_df),
        len(paragraphs_df),
    )

    saved_offers = 0
    skipped_offers = 0
    saved_paragraphs = 0
    now = utcnow()
    seen_urls: set[str] = set()

    with session_scope(active_settings) as session:
        for _, row in offers_df.iterrows():
            canonical_url = row["final_url"]
            if canonical_url in seen_urls:
                skipped_offers += 1
                logger.info(
                    "Skipping duplicate offer within current batch: %s",
                    canonical_url,
                )
                continue

            seen_urls.add(canonical_url)
            with session.no_autoflush:
                job_offer = (
                    session.query(JobOffer)
                    .filter(JobOffer.canonical_url == canonical_url)
                    .one_or_none()
                )
            if job_offer is not None:
                skipped_offers += 1
                logger.info(
                    "Skipping existing offer already stored in database: %s",
                    canonical_url,
                )
                continue

            job_offer = JobOffer(
                canonical_url=canonical_url,
                collected_at=now,
                updated_at=now,
            )
            session.add(job_offer)

            job_offer.external_job_id = _normalize_scalar(row.get("final_job_id"))
            job_offer.source_url = _normalize_scalar(row.get("url"))
            job_offer.search_url = _normalize_scalar(row.get("search_url"))
            job_offer.title = _normalize_scalar(row.get("final_title"))
            job_offer.title_embedding = row.get("title_embedding")
            job_offer.company = _normalize_scalar(row.get("final_company"))
            job_offer.location = _normalize_scalar(row.get("location"))
            job_offer.date_posted = _normalize_scalar(row.get("date_posted_dt"))
            job_offer.valid_through = _normalize_scalar(row.get("valid_through_dt"))
            job_offer.employment_type = _normalize_scalar(row.get("employment_type"))
            job_offer.industry = _normalize_scalar(row.get("industry"))
            job_offer.skills = _normalize_scalar(
                str(row.get("skills")) if row.get("skills") else None
            )
            job_offer.education_requirements = _normalize_scalar(
                row.get("education_requirements")
            )
            job_offer.address_country = _normalize_scalar(row.get("address_country"))
            job_offer.address_locality = _normalize_scalar(row.get("address_locality"))
            job_offer.address_region = _normalize_scalar(row.get("address_region"))
            job_offer.latitude = _normalize_scalar(row.get("latitude"))
            job_offer.longitude = _normalize_scalar(row.get("longitude"))
            job_offer.description_text = _normalize_scalar(row.get("description_text"))
            job_offer.description_html = _normalize_scalar(row.get("description_html"))
            job_offer.criteria_json = _normalize_scalar(row.get("criteria_json"))
            job_offer.source_parser = _normalize_scalar(row.get("source_parser"))
            job_offer.detail_status = _normalize_scalar(row.get("detail_status"))
            job_offer.detail_error = _normalize_scalar(row.get("detail_error"))
            job_offer.updated_at = now
            if job_offer.collected_at is None:
                job_offer.collected_at = _normalize_scalar(row.get("collected_at")) or now

            saved_offers += 1

            if paragraphs_df.empty:
                continue

            matching_rows = paragraphs_df[
                paragraphs_df["canonical_url"] == canonical_url
            ].to_dict(orient="records")
            for paragraph_row in matching_rows:
                job_offer.paragraphs.append(
                    JobParagraph(
                        paragraph_idx=int(paragraph_row["paragraph_idx"]),
                        paragraph=paragraph_row["paragraph"],
                        paragraph_chars=int(paragraph_row["paragraph_chars"]),
                        embedding=paragraph_row["embedding"],
                        created_at=now,
                    )
                )
                saved_paragraphs += 1

    result = PipelineResult(
        offers_seen=len(offers_df),
        offers_saved=saved_offers,
        offers_skipped=skipped_offers,
        paragraphs_saved=saved_paragraphs,
    ).to_dict()
    logger.info("Persistence complete: %s", result)
    _write_json(result, get_run_directory(run_key) / "result.json")
    return result


def run_ingestion(settings: Settings | None = None) -> dict[str, int]:
    context = initialize_run(settings)
    search_result = collect_search_results_step(context["run_key"], settings)
    details_result = collect_job_details_step(
        context["run_key"],
        search_result["jobs_path"],
        settings,
    )
    prepared_result = prepare_dataframes_step(
        context["run_key"],
        details_result["jobs_path"],
        details_result["details_path"],
        settings,
    )
    vectorized_result = vectorize_paragraphs_step(
        context["run_key"],
        prepared_result["paragraphs_path"],
        settings,
    )
    return persist_offers_step(
        context["run_key"],
        prepared_result["offers_path"],
        vectorized_result["paragraphs_vectorized_path"],
        settings,
    )
