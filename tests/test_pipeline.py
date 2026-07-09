from __future__ import annotations

import sys
from pathlib import Path
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

from job_matcher.pipeline import _attach_title_embeddings


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


if __name__ == "__main__":
    unittest.main()
