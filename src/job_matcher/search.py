from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import func, select

from job_matcher.config import Settings, get_settings
from job_matcher.cv import extract_cv_text_from_bytes
from job_matcher.database import ensure_database, session_scope
from job_matcher.embeddings import encode_texts
from job_matcher.models import JobOffer, JobParagraph
from job_matcher.text_utils import chunk_text, utcnow


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

    for chunk_index, embedding in enumerate(cv_embeddings):
        distance = JobOffer.title_embedding.cosine_distance(embedding)
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
                (1 - distance).label("title_score"),
            )
            .where(published_at >= min_date)
            .where(JobOffer.title_embedding.isnot(None))
        )
        for row in session.execute(stmt):
            score = float(row.title_score)
            current = per_job_titles.get(row.canonical_url)
            if current is None or score > current["title_score"]:
                per_job_titles[row.canonical_url] = {
                    "canonical_url": row.canonical_url,
                    "source": row.source,
                    "title": row.title,
                    "company": row.company,
                    "location": row.location,
                    "employment_type": row.employment_type,
                    "industry": row.industry,
                    "date_posted": row.date_posted,
                    "title_score": score,
                    "top_cv_chunk": cv_chunks[chunk_index],
                }

    return sorted(
        per_job_titles.values(),
        key=lambda item: item["title_score"],
        reverse=True,
    )


def search_jobs_for_cv(
    cv_bytes: bytes,
    lookback_hours: int,
    result_limit: int = 25,
    settings: Settings | None = None,
) -> tuple[str, list[str], list[SearchResult]]:
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
        candidate_jobs = title_ranked_jobs[:result_limit]
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
                "title_score": job["title_score"],
                "scores": [],
                "top_row": None,
                "top_cv_chunk": job["top_cv_chunk"],
            }
            for job in candidate_jobs
        }

        if not candidate_urls:
            per_job_matches = defaultdict(
                lambda: {
                    "source": None,
                    "title": None,
                    "company": None,
                    "location": None,
                    "employment_type": None,
                    "industry": None,
                    "date_posted": None,
                    "title_score": 0.0,
                    "scores": [],
                    "top_row": None,
                    "top_cv_chunk": None,
                }
            )

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
            )
            if candidate_urls:
                stmt = stmt.where(JobOffer.canonical_url.in_(candidate_urls))
            for row in session.execute(stmt):
                job_state = per_job_matches[row.canonical_url]
                if job_state["title"] is None:
                    job_state["source"] = row.source
                    job_state["title"] = row.title
                    job_state["company"] = row.company
                    job_state["location"] = row.location
                    job_state["employment_type"] = row.employment_type
                    job_state["industry"] = row.industry
                    job_state["date_posted"] = row.date_posted

                score = float(row.paragraph_score)
                match = {
                    "paragraph_score": score,
                    "paragraph": row.paragraph,
                    "cv_chunk": cv_chunks[chunk_index],
                    "title": row.title,
                    "company": row.company,
                    "location": row.location,
                    "employment_type": row.employment_type,
                    "industry": row.industry,
                    "date_posted": row.date_posted,
                }
                job_state["scores"].append(score)
                current_top = job_state["top_row"]
                if current_top is None or score > current_top["paragraph_score"]:
                    job_state["top_row"] = match

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

    results.sort(
        key=lambda item: (item.title_score, item.score_final, item.score_max),
        reverse=True,
    )
    return cv_text, cv_chunks, results[:result_limit]
