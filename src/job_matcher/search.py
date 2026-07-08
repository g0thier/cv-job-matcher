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
    title: str | None
    company: str | None
    location: str | None
    employment_type: str | None
    industry: str | None
    date_posted: object
    score_max: float
    score_top3_mean: float
    score_top5_mean: float
    score_final: float
    top_paragraph: str | None
    top_cv_chunk: str | None


def search_jobs_for_cv(
    cv_bytes: bytes,
    lookback_days: int,
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
    min_date = utcnow() - timedelta(days=lookback_days)

    per_job_matches: dict[str, dict] = defaultdict(
        lambda: {
            "scores": [],
            "top_row": None,
        }
    )

    with session_scope(active_settings) as session:
        for chunk_index, embedding in enumerate(cv_embeddings):
            distance = JobParagraph.embedding.cosine_distance(embedding)
            published_at = func.coalesce(JobOffer.date_posted, JobOffer.collected_at)
            stmt = (
                select(
                    JobOffer.canonical_url,
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
                .order_by(distance)
                .limit(active_settings.streamlit_candidate_limit)
            )
            for row in session.execute(stmt):
                job_state = per_job_matches[row.canonical_url]
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
        if not scores:
            continue
        top_row = job_state["top_row"]
        top3 = scores[:3]
        top5 = scores[:5]
        score_max = scores[0]
        score_top3 = sum(top3) / len(top3)
        score_top5 = sum(top5) / len(top5)
        score_final = 0.45 * score_max + 0.55 * score_top5
        results.append(
            SearchResult(
                canonical_url=canonical_url,
                title=top_row["title"],
                company=top_row["company"],
                location=top_row["location"],
                employment_type=top_row["employment_type"],
                industry=top_row["industry"],
                date_posted=top_row["date_posted"],
                score_max=score_max,
                score_top3_mean=score_top3,
                score_top5_mean=score_top5,
                score_final=score_final,
                top_paragraph=top_row["paragraph"],
                top_cv_chunk=top_row["cv_chunk"],
            )
        )

    results.sort(key=lambda item: item.score_final, reverse=True)
    return cv_text, cv_chunks, results[: active_settings.streamlit_result_limit]
