from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import timedelta, timezone
from typing import Literal

from sqlalchemy import func, select

from job_matcher.config import Settings, get_settings
from job_matcher.cv import extract_cv_text_from_bytes
from job_matcher.database import ensure_database, session_scope
from job_matcher.embeddings import encode_texts
from job_matcher.models import JobOffer, JobParagraph
from job_matcher.text_utils import chunk_text, utcnow


SortOrder = Literal["relevance", "newest", "title_asc", "text_score_final"]
VALID_SORT_ORDERS: tuple[SortOrder, ...] = (
    "relevance",
    "newest",
    "title_asc",
    "text_score_final",
)


@dataclass
class SearchResult:
    canonical_url: str
    source: str | None
    title: str | None
    company: str | None
    location: str | None
    employment_type: str | None
    industry: str | None
    date_posted: object
    title_score: float
    score_max: float
    score_top3_mean: float
    score_top5_mean: float
    score_final: float
    top_paragraph: str | None
    top_cv_chunk: str | None


def _rank_jobs_by_title(
    session,
    cv_chunks: list[str],
    cv_embeddings: list[list[float]],
    min_date,
) -> list[dict]:
    published_at = func.coalesce(JobOffer.date_posted, JobOffer.collected_at)
    per_job_titles: dict[str, dict] = {}

    metadata_stmt = select(
        JobOffer.canonical_url,
        JobOffer.source,
        JobOffer.title,
        JobOffer.company,
        JobOffer.location,
        JobOffer.employment_type,
        JobOffer.industry,
        JobOffer.date_posted,
        published_at.label("published_at"),
    ).where(published_at >= min_date)
    for row in session.execute(metadata_stmt):
        per_job_titles[row.canonical_url] = {
            "canonical_url": row.canonical_url,
            "source": row.source,
            "title": row.title,
            "company": row.company,
            "location": row.location,
            "employment_type": row.employment_type,
            "industry": row.industry,
            "date_posted": row.date_posted,
            "published_at": row.published_at,
            "title_score": 0.0,
            "has_title_score": False,
            "top_cv_chunk": None,
        }

    for chunk_index, embedding in enumerate(cv_embeddings):
        distance = JobOffer.title_embedding.cosine_distance(embedding)
        stmt = (
            select(
                JobOffer.canonical_url,
                (1 - distance).label("title_score"),
            )
            .where(published_at >= min_date)
            .where(JobOffer.title_embedding.isnot(None))
        )
        for row in session.execute(stmt):
            score = float(row.title_score)
            current = per_job_titles.get(row.canonical_url)
            if current is None:
                continue
            if not current["has_title_score"] or score > current["title_score"]:
                current["title_score"] = score
                current["has_title_score"] = True
                current["top_cv_chunk"] = cv_chunks[chunk_index]

    return sorted(
        per_job_titles.values(),
        key=lambda item: (-item["title_score"], item["canonical_url"]),
    )


def _normalize_title(title: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", (title or "").strip())
    return "".join(
        character for character in normalized if not unicodedata.combining(character)
    ).casefold()


def _published_at_sort_key(job: dict) -> tuple[bool, float, float, str]:
    published_at = job["published_at"]
    if published_at is not None and published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    return (
        published_at is None,
        -published_at.timestamp() if published_at is not None else 0.0,
        -job["title_score"],
        job["canonical_url"],
    )


def _title_sort_key(job: dict) -> tuple[bool, str, float, str]:
    normalized_title = _normalize_title(job["title"])
    return (
        not normalized_title,
        normalized_title,
        -job["title_score"],
        job["canonical_url"],
    )


def _select_jobs_for_scoring(
    title_ranked_jobs: list[dict],
    sort_order: SortOrder,
    result_limit: int,
) -> list[dict]:
    if result_limit <= 0:
        return []
    if sort_order == "text_score_final":
        return list(title_ranked_jobs)
    if sort_order == "newest":
        return sorted(title_ranked_jobs, key=_published_at_sort_key)[:result_limit]
    if sort_order == "title_asc":
        return sorted(title_ranked_jobs, key=_title_sort_key)[:result_limit]

    scored_title_jobs = [
        job for job in title_ranked_jobs if job.get("has_title_score", True)
    ]
    relevance_jobs = scored_title_jobs or title_ranked_jobs
    if len(relevance_jobs) <= result_limit:
        return list(relevance_jobs)

    cutoff_score = relevance_jobs[result_limit - 1]["title_score"]
    return [
        job for job in relevance_jobs if job["title_score"] >= cutoff_score
    ]


def _sort_search_results(
    results: list[SearchResult],
    job_states: dict[str, dict],
    sort_order: SortOrder,
) -> list[SearchResult]:
    if sort_order == "newest":
        return sorted(
            results,
            key=lambda item: _published_at_sort_key(
                {
                    "published_at": job_states[item.canonical_url]["published_at"],
                    "title_score": item.title_score,
                    "canonical_url": item.canonical_url,
                }
            ),
        )
    if sort_order == "title_asc":
        return sorted(
            results,
            key=lambda item: (
                not _normalize_title(item.title),
                _normalize_title(item.title),
                -item.title_score,
                item.canonical_url,
            ),
        )
    if sort_order == "text_score_final":
        return sorted(
            results,
            key=lambda item: (
                -item.score_final,
                -item.title_score,
                -item.score_max,
                item.canonical_url,
            ),
        )
    return sorted(
        results,
        key=lambda item: (
            -item.title_score,
            -item.score_final,
            -item.score_max,
            item.canonical_url,
        ),
    )


def search_jobs_for_cv(
    cv_bytes: bytes,
    lookback_hours: int,
    result_limit: int = 25,
    settings: Settings | None = None,
    sort_order: SortOrder = "relevance",
) -> tuple[str, list[str], list[SearchResult]]:
    if sort_order not in VALID_SORT_ORDERS:
        raise ValueError(f"Unsupported sort order: {sort_order}")

    active_settings = settings or get_settings()
    ensure_database(active_settings)

    cv_text = extract_cv_text_from_bytes(cv_bytes)
    cv_chunks = chunk_text(
        cv_text,
        chunk_size=active_settings.cv_chunk_size,
        overlap=active_settings.cv_chunk_overlap,
    )
    if not cv_chunks:
        raise ValueError("No usable CV chunks were extracted.")

    cv_embeddings = encode_texts(cv_chunks, settings=active_settings).tolist()
    min_date = utcnow() - timedelta(hours=lookback_hours)

    with session_scope(active_settings) as session:
        title_ranked_jobs = _rank_jobs_by_title(session, cv_chunks, cv_embeddings, min_date)
        relevance_paragraph_fallback = sort_order == "relevance" and not any(
            job.get("has_title_score", True) for job in title_ranked_jobs
        )
        candidate_jobs = _select_jobs_for_scoring(
            title_ranked_jobs,
            sort_order,
            result_limit,
        )
        candidate_urls = [job["canonical_url"] for job in candidate_jobs]

        per_job_matches: dict[str, dict] = {
            job["canonical_url"]: {
                "source": job["source"],
                "title": job["title"],
                "company": job["company"],
                "location": job["location"],
                "employment_type": job["employment_type"],
                "industry": job["industry"],
                "date_posted": job["date_posted"],
                "published_at": job["published_at"],
                "title_score": job["title_score"],
                "scores": [],
                "top_row": None,
                "top_cv_chunk": job["top_cv_chunk"],
            }
            for job in candidate_jobs
        }

        if candidate_jobs:
            for chunk_index, embedding in enumerate(cv_embeddings):
                distance = JobParagraph.embedding.cosine_distance(embedding)
                published_at = func.coalesce(JobOffer.date_posted, JobOffer.collected_at)
                stmt = (
                    select(
                        JobOffer.canonical_url,
                        JobOffer.source,
                        JobOffer.title,
                        JobOffer.company,
                        JobOffer.location,
                        JobOffer.employment_type,
                        JobOffer.industry,
                        JobOffer.date_posted,
                        JobParagraph.paragraph,
                        JobParagraph.paragraph_idx,
                        (1 - distance).label("paragraph_score"),
                    )
                    .join(JobParagraph, JobParagraph.job_offer_id == JobOffer.id)
                    .where(published_at >= min_date)
                    .where(JobOffer.canonical_url.in_(candidate_urls))
                )
                for row in session.execute(stmt):
                    job_state = per_job_matches.get(row.canonical_url)
                    if job_state is None:
                        continue

                    score = float(row.paragraph_score)
                    match = {
                        "paragraph_score": score,
                        "paragraph": row.paragraph,
                        "cv_chunk": cv_chunks[chunk_index],
                    }
                    job_state["scores"].append(score)
                    current_top = job_state["top_row"]
                    if current_top is None or score > current_top["paragraph_score"]:
                        job_state["top_row"] = match

        if relevance_paragraph_fallback:
            per_job_matches = {
                canonical_url: job_state
                for canonical_url, job_state in per_job_matches.items()
                if job_state["scores"]
            }

    results: list[SearchResult] = []
    for canonical_url, job_state in per_job_matches.items():
        scores = sorted(job_state["scores"], reverse=True)
        top_row = job_state["top_row"]
        if scores:
            top3 = scores[:3]
            top5 = scores[:5]
            score_max = scores[0]
            score_top3 = sum(top3) / len(top3)
            score_top5 = sum(top5) / len(top5)
            score_final = 0.45 * score_max + 0.55 * score_top5
        else:
            score_max = 0.0
            score_top3 = 0.0
            score_top5 = 0.0
            score_final = 0.0

        results.append(
            SearchResult(
                canonical_url=canonical_url,
                source=job_state["source"],
                title=job_state["title"],
                company=job_state["company"],
                location=job_state["location"],
                employment_type=job_state["employment_type"],
                industry=job_state["industry"],
                date_posted=job_state["date_posted"],
                title_score=job_state["title_score"],
                score_max=score_max,
                score_top3_mean=score_top3,
                score_top5_mean=score_top5,
                score_final=score_final,
                top_paragraph=top_row["paragraph"] if top_row else None,
                top_cv_chunk=top_row["cv_chunk"] if top_row else job_state["top_cv_chunk"],
            )
        )

    results = _sort_search_results(results, per_job_matches, sort_order)
    return cv_text, cv_chunks, results[:result_limit]
