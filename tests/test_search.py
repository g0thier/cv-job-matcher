from __future__ import annotations

import inspect
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from job_matcher import search  # noqa: E402


NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)


class _Expression:
    def __init__(self, name: str):
        self.name = name

    def cosine_distance(self, _embedding):
        return _Expression(f"cosine_distance({self.name})")

    def label(self, name: str):
        return _Expression(name)

    def isnot(self, value):
        return _Predicate("isnot", self, value)

    def in_(self, values):
        return _Predicate("in", self, tuple(values))

    def __eq__(self, other):
        return _Predicate("eq", self, other)

    def __ge__(self, other):
        return _Predicate("ge", self, other)

    def __rsub__(self, other):
        return _Expression(f"{other} - {self.name}")


class _Predicate:
    def __init__(self, operator: str, left, right):
        self.operator = operator
        self.left = left
        self.right = right


class _Statement:
    def __init__(self, *columns):
        self.columns = columns
        self.predicates: list[_Predicate] = []

    def join(self, *_args, **_kwargs):
        return self

    def where(self, predicate):
        self.predicates.append(predicate)
        return self


class _Func:
    @staticmethod
    def coalesce(first, second):
        return _Expression(f"coalesce({first.name}, {second.name})")


FAKE_JOB_OFFER = SimpleNamespace(
    id=_Expression("job_offers.id"),
    canonical_url=_Expression("job_offers.canonical_url"),
    source=_Expression("job_offers.source"),
    title=_Expression("job_offers.title"),
    title_embedding=_Expression("job_offers.title_embedding"),
    company=_Expression("job_offers.company"),
    location=_Expression("job_offers.location"),
    employment_type=_Expression("job_offers.employment_type"),
    industry=_Expression("job_offers.industry"),
    date_posted=_Expression("job_offers.date_posted"),
    collected_at=_Expression("job_offers.collected_at"),
)
FAKE_JOB_PARAGRAPH = SimpleNamespace(
    job_offer_id=_Expression("job_paragraphs.job_offer_id"),
    paragraph=_Expression("job_paragraphs.paragraph"),
    paragraph_idx=_Expression("job_paragraphs.paragraph_idx"),
    embedding=_Expression("job_paragraphs.embedding"),
)


class _Session:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements: list[_Statement] = []

    def execute(self, statement):
        self.statements.append(statement)
        return list(self.rows)


def _job(
    name: str,
    *,
    title: str | None,
    title_score: float,
    date_posted: datetime | None = None,
    collected_at: datetime | None = None,
) -> dict:
    effective_collected_at = collected_at or NOW
    return {
        "canonical_url": f"https://example.test/jobs/{name}",
        "source": "test",
        "title": title,
        "company": "Example SA",
        "location": "Geneva",
        "employment_type": "Full-time",
        "industry": "Technology",
        "date_posted": date_posted,
        "collected_at": effective_collected_at,
        "published_at": date_posted or effective_collected_at,
        "title_score": title_score,
        "top_cv_chunk": "CV chunk",
    }


def _paragraph_row(job: dict, score: float):
    return SimpleNamespace(
        canonical_url=job["canonical_url"],
        source=job["source"],
        title=job["title"],
        company=job["company"],
        location=job["location"],
        employment_type=job["employment_type"],
        industry=job["industry"],
        date_posted=job["date_posted"],
        collected_at=job["collected_at"],
        paragraph=f"Paragraph for {job['canonical_url']}",
        paragraph_idx=0,
        paragraph_score=score,
    )


class SearchOrderingTests(unittest.TestCase):
    def _run_search(
        self,
        jobs: list[dict],
        *,
        rows=(),
        result_limit: int = 25,
        lookback_hours: int = 24,
        sort_order: str | None = None,
    ):
        session = _Session(rows)

        @contextmanager
        def fake_session_scope(_settings=None):
            yield session

        settings = SimpleNamespace(cv_chunk_size=300, cv_chunk_overlap=60)
        ranked_jobs = patch.object(
            search,
            "_rank_jobs_by_title",
            return_value=jobs,
        )
        with patch.object(search, "ensure_database"), patch.object(
            search,
            "extract_cv_text_from_bytes",
            return_value="Extracted CV text",
        ), patch.object(
            search,
            "chunk_text",
            return_value=["CV chunk"],
        ), patch.object(
            search,
            "encode_texts",
            return_value=SimpleNamespace(tolist=lambda: [[0.1, 0.2]]),
        ), patch.object(
            search,
            "utcnow",
            return_value=NOW,
        ), patch.object(
            search,
            "session_scope",
            fake_session_scope,
        ), ranked_jobs as rank_mock, patch.multiple(
            search,
            select=lambda *columns: _Statement(*columns),
            func=_Func(),
            JobOffer=FAKE_JOB_OFFER,
            JobParagraph=FAKE_JOB_PARAGRAPH,
        ):
            kwargs = {
                "lookback_hours": lookback_hours,
                "result_limit": result_limit,
                "settings": settings,
            }
            if sort_order is not None:
                kwargs["sort_order"] = sort_order
            result = search.search_jobs_for_cv(b"pdf", **kwargs)

        return result, session, rank_mock

    def test_api_defaults_to_relevance_and_rejects_unknown_order(self) -> None:
        parameter = inspect.signature(search.search_jobs_for_cv).parameters["sort_order"]
        self.assertEqual(parameter.default, "relevance")

        with self.assertRaisesRegex(ValueError, "sort|order"):
            self._run_search([], sort_order="not-a-sort-order")

    def test_default_relevance_scores_every_title_tie_at_the_limit(self) -> None:
        first = _job("first", title="First", title_score=0.90)
        better_text = _job("better-text", title="Better text", title_score=0.90)
        below_title_cutoff = _job("below", title="Below", title_score=0.89)

        (_, _, results), _, _ = self._run_search(
            [first, better_text, below_title_cutoff],
            rows=[
                _paragraph_row(first, 0.20),
                _paragraph_row(better_text, 0.95),
            ],
            result_limit=1,
        )

        self.assertEqual(
            [result.canonical_url for result in results],
            [better_text["canonical_url"]],
        )

    def test_relevance_excludes_unscored_titles_when_scored_titles_exist(self) -> None:
        scored = _job("scored", title="Scored", title_score=-0.10)
        scored["has_title_score"] = True
        unscored = _job("unscored", title="Unscored", title_score=0.0)
        unscored["has_title_score"] = False

        selected = search._select_jobs_for_scoring(
            [unscored, scored],
            "relevance",
            result_limit=25,
        )

        self.assertEqual(
            [job["canonical_url"] for job in selected],
            [scored["canonical_url"]],
        )

    def test_text_score_final_selects_global_top_n_after_scoring(self) -> None:
        best_title = _job("best-title", title="Best title", title_score=0.95)
        second_title = _job("second-title", title="Second title", title_score=0.80)
        best_text = _job("best-text", title="Best text", title_score=0.10)

        (_, _, results), _, _ = self._run_search(
            [best_title, second_title, best_text],
            rows=[
                _paragraph_row(best_title, 0.25),
                _paragraph_row(second_title, 0.50),
                _paragraph_row(best_text, 0.99),
            ],
            result_limit=1,
            sort_order="text_score_final",
        )

        self.assertEqual(
            [result.canonical_url for result in results],
            [best_text["canonical_url"]],
        )
        self.assertAlmostEqual(results[0].score_final, 0.99)

    def test_newest_uses_collected_at_only_as_date_posted_fallback(self) -> None:
        fallback = _job(
            "fallback",
            title="Fallback",
            title_score=0.10,
            date_posted=None,
            collected_at=NOW - timedelta(hours=1),
        )
        tied_lower_score = _job(
            "z-tie",
            title="Tie",
            title_score=0.80,
            date_posted=NOW - timedelta(hours=2),
            collected_at=NOW,
        )
        tied_url_b = _job(
            "b-tie",
            title="Tie",
            title_score=0.90,
            date_posted=NOW - timedelta(hours=2),
        )
        tied_url_a = _job(
            "a-tie",
            title="Tie",
            title_score=0.90,
            date_posted=NOW - timedelta(hours=2),
        )
        old_posted_but_recently_collected = _job(
            "old-posted",
            title="Old posted",
            title_score=0.99,
            date_posted=NOW - timedelta(hours=6),
            collected_at=NOW + timedelta(days=1),
        )

        (_, _, results), _, _ = self._run_search(
            [
                old_posted_but_recently_collected,
                tied_lower_score,
                tied_url_b,
                fallback,
                tied_url_a,
            ],
            sort_order="newest",
        )

        self.assertEqual(
            [result.canonical_url for result in results],
            [
                fallback["canonical_url"],
                tied_url_a["canonical_url"],
                tied_url_b["canonical_url"],
                tied_lower_score["canonical_url"],
                old_posted_but_recently_collected["canonical_url"],
            ],
        )

    def test_title_asc_ignores_whitespace_case_and_accents_and_puts_none_last(
        self,
    ) -> None:
        alpha = _job("alpha", title="  Álpha ", title_score=0.10)
        beta = _job("beta", title="beta", title_score=0.20)
        eclair = _job("eclair", title=" Éclair", title_score=0.30)
        ecole_lower_score = _job("z-ecole", title="eCOLE", title_score=0.40)
        ecole_url_b = _job("b-ecole", title="École", title_score=0.90)
        ecole_url_a = _job("a-ecole", title="ecole", title_score=0.90)
        missing = _job("missing", title=None, title_score=1.00)

        (_, _, results), _, _ = self._run_search(
            [
                missing,
                ecole_lower_score,
                beta,
                ecole_url_b,
                alpha,
                eclair,
                ecole_url_a,
            ],
            sort_order="title_asc",
        )

        self.assertEqual(
            [result.canonical_url for result in results],
            [
                alpha["canonical_url"],
                beta["canonical_url"],
                eclair["canonical_url"],
                ecole_url_a["canonical_url"],
                ecole_url_b["canonical_url"],
                ecole_lower_score["canonical_url"],
                missing["canonical_url"],
            ],
        )

    def test_lookback_precedes_ranking_scoring_and_result_limit(self) -> None:
        inside = _job(
            "inside",
            title="Inside",
            title_score=0.50,
            date_posted=NOW - timedelta(hours=2),
        )

        (_, _, results), session, rank_mock = self._run_search(
            [inside],
            rows=[_paragraph_row(inside, 0.70)],
            lookback_hours=6,
            result_limit=1,
            sort_order="text_score_final",
        )

        self.assertEqual(len(results), 1)
        min_date = NOW - timedelta(hours=6)
        self.assertEqual(rank_mock.call_args.args[-1], min_date)
        self.assertTrue(session.statements, "Paragraph scoring query was not executed")
        for statement in session.statements:
            self.assertTrue(
                any(
                    predicate.operator == "ge"
                    and predicate.left.name.startswith("coalesce(")
                    and predicate.right == min_date
                    for predicate in statement.predicates
                ),
                "Every paragraph scoring query must enforce the Lookback window",
            )


class TitleLookbackQueryTests(unittest.TestCase):
    def test_title_ranking_filters_by_posted_or_collected_date(self) -> None:
        min_date = NOW - timedelta(hours=24)
        inside_row = SimpleNamespace(
            canonical_url="https://example.test/jobs/inside",
            source="test",
            title="Inside",
            company="Example SA",
            location="Geneva",
            employment_type="Full-time",
            industry="Technology",
            date_posted=NOW - timedelta(hours=1),
            collected_at=NOW - timedelta(minutes=30),
            published_at=NOW - timedelta(hours=1),
            title_score=0.80,
        )
        outside_row = SimpleNamespace(
            canonical_url="https://example.test/jobs/outside",
            source="test",
            title="Outside",
            company="Example SA",
            location="Geneva",
            employment_type="Full-time",
            industry="Technology",
            date_posted=NOW - timedelta(days=2),
            collected_at=NOW - timedelta(days=2),
            published_at=NOW - timedelta(days=2),
            title_score=1.00,
        )

        class LookbackAwareSession(_Session):
            def execute(self, statement):
                self.statements.append(statement)
                has_lookback = any(
                    predicate.operator == "ge"
                    and predicate.left.name.startswith("coalesce(")
                    and predicate.right == min_date
                    for predicate in statement.predicates
                )
                return [inside_row] if has_lookback else [inside_row, outside_row]

        session = LookbackAwareSession()
        with patch.multiple(
            search,
            select=lambda *columns: _Statement(*columns),
            func=_Func(),
            JobOffer=FAKE_JOB_OFFER,
        ):
            ranked = search._rank_jobs_by_title(
                session,
                ["CV chunk"],
                [[0.1, 0.2]],
                min_date,
            )

        self.assertEqual(
            [job["canonical_url"] for job in ranked],
            [inside_row.canonical_url],
        )
        self.assertEqual(ranked[0]["published_at"], inside_row.published_at)


class StreamlitSortControlTests(unittest.TestCase):
    def test_streamlit_maps_every_sort_label_to_the_search_api(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        streamlit_source = (repository_root / "streamlit_app.py").read_text()

        for label, value in {
            "Relevance": "relevance",
            "Newest first": "newest",
            "Title A–Z": "title_asc",
            "Text score final": "text_score_final",
        }.items():
            self.assertIn(f'"{label}": "{value}"', streamlit_source)
        self.assertIn('"Sort offers by"', streamlit_source)
        self.assertIn("sort_order=sort_order", streamlit_source)


if __name__ == "__main__":
    unittest.main()
