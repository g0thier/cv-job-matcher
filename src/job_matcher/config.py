from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Settings:
    timezone: str = os.getenv("APP_TIMEZONE", "Europe/Zurich")
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://jobmatcher:jobmatcher@postgres:5432/jobmatcher",
    )
    linkedin_searches_file: Path = Path(
        os.getenv("LINKEDIN_SEARCHES_FILE", "config/linkedin_searches.json")
    )
    etat_geneve_rss_url: str = os.getenv(
        "ETAT_GENEVE_RSS_URL",
        (
            "https://www.ge.ch/rss/offres-emploi-etat-geneve"
            "?departement=0&domaine_activite=0&classe_fonction_min=0"
            "&type_contrat=0&taux_activite_max=0"
        ),
    )
    embedding_model_name: str = os.getenv(
        "EMBEDDING_MODEL_NAME",
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
    )
    embedding_dimension: int = int(os.getenv("EMBEDDING_DIMENSION", "384"))
    headless_browser: bool = os.getenv("HEADLESS_BROWSER", "true").lower() == "true"
    max_jobs_per_search: int = int(os.getenv("MAX_JOBS_PER_SEARCH", "500"))
    max_detail_pages: int = int(os.getenv("MAX_DETAIL_PAGES", "0"))
    scroll_rounds: int = int(os.getenv("SCROLL_ROUNDS", "40"))
    cv_chunk_size: int = int(os.getenv("CV_CHUNK_SIZE", "300"))
    cv_chunk_overlap: int = int(os.getenv("CV_CHUNK_OVERLAP", "60"))
    paragraph_min_chars: int = int(os.getenv("PARAGRAPH_MIN_CHARS", "40"))

    @property
    def resolved_search_file(self) -> Path:
        return self.linkedin_searches_file.resolve()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def load_linkedin_searches(settings: Settings | None = None) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    raw = json.loads(settings.linkedin_searches_file.read_text())
    if not isinstance(raw, list):
        raise ValueError("linkedin_searches.json must contain a list of search objects")
    return raw
