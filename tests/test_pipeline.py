from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import types
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


database_stub = types.ModuleType("job_matcher.database")
database_stub.ensure_database = lambda *args, **kwargs: None
database_stub.session_scope = lambda *args, **kwargs: None
sys.modules["job_matcher.database"] = database_stub

embeddings_stub = types.ModuleType("job_matcher.embeddings")
embeddings_stub.encode_texts = lambda *args, **kwargs: None
sys.modules["job_matcher.embeddings"] = embeddings_stub

linkedin_stub = types.ModuleType("job_matcher.linkedin")
linkedin_stub.build_job_paragraphs = lambda *args, **kwargs: None
linkedin_stub.build_search_urls = lambda *args, **kwargs: None
linkedin_stub.collect_job_details = lambda *args, **kwargs: None
linkedin_stub.collect_search_results = lambda *args, **kwargs: None
linkedin_stub.prepare_offers_dataframe = lambda *args, **kwargs: None
sys.modules["job_matcher.linkedin"] = linkedin_stub

models_stub = types.ModuleType("job_matcher.models")
models_stub.JobOffer = type("JobOffer", (), {})
models_stub.JobParagraph = type("JobParagraph", (), {})
sys.modules["job_matcher.models"] = models_stub

from job_matcher.pipeline import (
    _attach_title_embeddings,
    filter_existing_jobs_step,
    persist_offers_step,
)


class AttachTitleEmbeddingsTests(unittest.TestCase):
    @patch("job_matcher.pipeline.encode_texts")
    def test_assigns_embeddings_for_multiple_titles(self, mock_encode_texts) -> None:
        offers_df = pd.DataFrame(
            {
                "final_title": ["Data Scientist", "ML Engineer"],
            }
        )
        mock_encode_texts.return_value.tolist.return_value = [
            [0.1, 0.2],
            [0.3, 0.4],
        ]

        result = _attach_title_embeddings(offers_df)

        self.assertEqual(result["title_embedding"].tolist(), [[0.1, 0.2], [0.3, 0.4]])
        mock_encode_texts.assert_called_once_with(
            ["Data Scientist", "ML Engineer"],
            settings=None,
        )

    @patch("job_matcher.pipeline.encode_texts")
    def test_only_assigns_embeddings_for_non_empty_titles(self, mock_encode_texts) -> None:
        offers_df = pd.DataFrame(
            {
                "final_title": ["Data Scientist", None, "   ", "ML Engineer"],
            }
        )
        mock_encode_texts.return_value.tolist.return_value = [
            [0.1, 0.2],
            [0.3, 0.4],
        ]

        result = _attach_title_embeddings(offers_df)

        self.assertEqual(
            result["title_embedding"].tolist(),
            [[0.1, 0.2], None, None, [0.3, 0.4]],
        )
        mock_encode_texts.assert_called_once_with(
            ["Data Scientist", "ML Engineer"],
            settings=None,
        )

    @patch("job_matcher.pipeline.encode_texts")
    def test_returns_empty_dataframe_without_calling_encoder(self, mock_encode_texts) -> None:
        offers_df = pd.DataFrame({"final_title": []})

        result = _attach_title_embeddings(offers_df)

        self.assertTrue(result.empty)
        self.assertIn("title_embedding", result.columns)
        mock_encode_texts.assert_not_called()


class FilterExistingJobsStepTests(unittest.TestCase):
    @patch("job_matcher.pipeline._write_dataframe")
    @patch("job_matcher.pipeline._read_dataframe")
    def test_filters_out_known_urls_before_details(
        self,
        mock_read_dataframe,
        mock_write_dataframe,
    ) -> None:
        jobs_df = pd.DataFrame(
            {
                "url": [
                    "https://www.linkedin.com/jobs/view/1",
                    "https://www.linkedin.com/jobs/view/2",
                ],
                "job_id": ["1", "2"],
            }
        )
        mock_read_dataframe.return_value = jobs_df
        mock_write_dataframe.return_value = "runtime/airflow/run-1/jobs_filtered.pkl"

        class Query:
            def all(self):
                return [("https://www.linkedin.com/jobs/view/1",)]

        @contextmanager
        def fake_session_scope(_settings=None):
            yield SimpleNamespace(query=lambda *_args, **_kwargs: Query())

        with patch("job_matcher.pipeline.session_scope", fake_session_scope), patch(
            "job_matcher.pipeline.get_run_directory",
            return_value=Path("runtime/airflow/run-1"),
        ), patch(
            "job_matcher.pipeline.JobOffer",
            SimpleNamespace(canonical_url="canonical_url"),
        ):
            result = filter_existing_jobs_step("run-1", "runtime/airflow/run-1/jobs.pkl")

        self.assertEqual(result["jobs_count"], 1)
        self.assertEqual(result["jobs_skipped"], 1)
        written_df = mock_write_dataframe.call_args.args[0]
        self.assertEqual(
            written_df["url"].tolist(),
            ["https://www.linkedin.com/jobs/view/2"],
        )

    @patch("job_matcher.pipeline._write_dataframe")
    @patch("job_matcher.pipeline._read_dataframe")
    def test_handles_empty_dataframe_without_querying_database(
        self,
        mock_read_dataframe,
        mock_write_dataframe,
    ) -> None:
        mock_read_dataframe.return_value = pd.DataFrame({"url": []})
        mock_write_dataframe.return_value = "runtime/airflow/run-1/jobs_filtered.pkl"

        @contextmanager
        def forbidden_session_scope(_settings=None):
            raise AssertionError("session_scope should not be called for empty dataframes")
            yield

        with patch("job_matcher.pipeline.session_scope", forbidden_session_scope), patch(
            "job_matcher.pipeline.get_run_directory",
            return_value=Path("runtime/airflow/run-1"),
        ):
            result = filter_existing_jobs_step("run-1", "runtime/airflow/run-1/jobs.pkl")

        self.assertEqual(result["jobs_count"], 0)
        self.assertEqual(result["jobs_skipped"], 0)
        written_df = mock_write_dataframe.call_args.args[0]
        self.assertTrue(written_df.empty)


class PersistOffersStepTests(unittest.TestCase):
    @patch("job_matcher.pipeline._write_json")
    @patch("job_matcher.pipeline._read_dataframe")
    @patch("job_matcher.pipeline._attach_title_embeddings")
    def test_keeps_final_duplicate_guard(
        self,
        mock_attach_title_embeddings,
        mock_read_dataframe,
        mock_write_json,
    ) -> None:
        offers_df = pd.DataFrame(
            {
                "final_url": ["https://www.linkedin.com/jobs/view/1"],
                "final_job_id": ["1"],
                "url": ["https://www.linkedin.com/jobs/view/1"],
                "search_url": ["https://www.linkedin.com/jobs/search"],
                "final_title": ["Data Scientist"],
                "final_company": ["OpenAI"],
            }
        )
        mock_attach_title_embeddings.return_value = offers_df
        mock_read_dataframe.side_effect = [offers_df, pd.DataFrame()]

        class ExistingOfferQuery:
            def filter(self, *_args, **_kwargs):
                return self

            def one_or_none(self):
                return object()

        @contextmanager
        def fake_session_scope(_settings=None):
            session = SimpleNamespace(
                no_autoflush=contextmanager(lambda: iter([None]))(),
                query=lambda *_args, **_kwargs: ExistingOfferQuery(),
            )
            yield session

        with patch("job_matcher.pipeline.session_scope", fake_session_scope), patch(
            "job_matcher.pipeline.JobOffer",
            SimpleNamespace(canonical_url="canonical_url"),
        ), patch(
            "job_matcher.pipeline.get_run_directory",
            return_value=Path("runtime/airflow/run-1"),
        ):
            result = persist_offers_step(
                "run-1",
                "runtime/airflow/run-1/offers.pkl",
                "runtime/airflow/run-1/paragraphs_vectorized.pkl",
            )

        self.assertEqual(result["offers_seen"], 1)
        self.assertEqual(result["offers_saved"], 0)
        self.assertEqual(result["offers_skipped"], 1)
        self.assertEqual(result["paragraphs_saved"], 0)
        mock_write_json.assert_called_once()

    @patch("job_matcher.pipeline._write_json")
    @patch("job_matcher.pipeline._read_dataframe")
    @patch("job_matcher.pipeline._attach_title_embeddings")
    def test_skips_duplicate_urls_within_same_batch(
        self,
        mock_attach_title_embeddings,
        mock_read_dataframe,
        mock_write_json,
    ) -> None:
        offers_df = pd.DataFrame(
            {
                "final_url": [
                    "https://www.linkedin.com/jobs/view/1",
                    "https://www.linkedin.com/jobs/view/1",
                ],
                "final_job_id": ["1", "1"],
                "url": [
                    "https://www.linkedin.com/jobs/view/1",
                    "https://www.linkedin.com/jobs/view/1",
                ],
                "search_url": [
                    "https://www.linkedin.com/jobs/search",
                    "https://www.linkedin.com/jobs/search",
                ],
                "final_title": ["Data Scientist", "Data Scientist"],
                "final_company": ["OpenAI", "OpenAI"],
            }
        )
        mock_attach_title_embeddings.return_value = offers_df
        mock_read_dataframe.side_effect = [offers_df, pd.DataFrame()]

        class EmptyOfferQuery:
            def filter(self, *_args, **_kwargs):
                return self

            def one_or_none(self):
                return None

        class FakeJobOffer:
            canonical_url = "canonical_url"

            def __init__(self, canonical_url, collected_at, updated_at):
                self.canonical_url = canonical_url
                self.collected_at = collected_at
                self.updated_at = updated_at
                self.paragraphs = []

        @contextmanager
        def fake_session_scope(_settings=None):
            session = SimpleNamespace(
                no_autoflush=contextmanager(lambda: iter([None]))(),
                query=lambda *_args, **_kwargs: EmptyOfferQuery(),
                add=lambda *_args, **_kwargs: None,
            )
            yield session

        with patch("job_matcher.pipeline.session_scope", fake_session_scope), patch(
            "job_matcher.pipeline.JobOffer",
            FakeJobOffer,
        ), patch(
            "job_matcher.pipeline.get_run_directory",
            return_value=Path("runtime/airflow/run-1"),
        ):
            result = persist_offers_step(
                "run-1",
                "runtime/airflow/run-1/offers.pkl",
                "runtime/airflow/run-1/paragraphs_vectorized.pkl",
            )

        self.assertEqual(result["offers_seen"], 2)
        self.assertEqual(result["offers_saved"], 1)
        self.assertEqual(result["offers_skipped"], 1)
        self.assertEqual(result["paragraphs_saved"], 0)
        mock_write_json.assert_called_once()


if __name__ == "__main__":
    unittest.main()
