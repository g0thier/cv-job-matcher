from __future__ import annotations

from functools import lru_cache

import numpy as np
from sentence_transformers import SentenceTransformer

from job_matcher.config import Settings, get_settings


@lru_cache(maxsize=1)
def get_embedding_model(settings: Settings | None = None) -> SentenceTransformer:
    active_settings = settings or get_settings()
    return SentenceTransformer(active_settings.embedding_model_name)


def encode_texts(
    texts: list[str], settings: Settings | None = None, batch_size: int = 32
) -> np.ndarray:
    if not texts:
        active_settings = settings or get_settings()
        return np.empty((0, active_settings.embedding_dimension))

    model = get_embedding_model(settings)
    return model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
