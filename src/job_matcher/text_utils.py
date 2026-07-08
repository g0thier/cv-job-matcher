from __future__ import annotations

import re
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dateparser


def normalize_linkedin_search_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.urlencode(
        dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)),
        doseq=True,
    )
    return urllib.parse.urlunparse(parsed._replace(query=query))


def canonicalize_job_url(url: str | None) -> str | None:
    if not url:
        return None
    absolute = urllib.parse.urljoin("https://www.linkedin.com", url)
    parsed = urllib.parse.urlparse(absolute)
    return urllib.parse.urlunparse(parsed._replace(query="", fragment=""))


def extract_job_id(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"(\d{8,})", value)
    return match.group(1) if match else None


def clean_text(text: str | None) -> str:
    if not text:
        return ""
    normalized = BeautifulSoup(text, "lxml").get_text("\n")
    normalized = re.sub(r"\xa0", " ", normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def split_paragraphs(text: str | None, min_chars: int = 40) -> list[str]:
    if not text:
        return []

    raw_parts = re.split(r"\n\s*\n|(?<=\.)\s*\n|•|\u2022", text)
    parts: list[str] = []

    for part in raw_parts:
        paragraph = re.sub(r"\s+", " ", part).strip(" -\t\n")
        if len(paragraph) >= min_chars:
            parts.append(paragraph)

    seen: set[str] = set()
    unique_parts: list[str] = []
    for part in parts:
        key = part.lower()
        if key not in seen:
            seen.add(key)
            unique_parts.append(part)

    return unique_parts


def chunk_text(text: str, chunk_size: int = 300, overlap: int = 60) -> list[str]:
    normalized = clean_text(text)
    if not normalized:
        return []

    paragraphs = split_paragraphs(normalized, min_chars=30)
    chunks: list[str] = []
    current = ""

    for paragraph in paragraphs:
        if len(current) + len(paragraph) + 1 <= chunk_size:
            current = f"{current}\n{paragraph}".strip()
        else:
            if current:
                chunks.append(current)
            current = paragraph

    if current:
        chunks.append(current)

    if len(chunks) <= 1 and len(normalized) > chunk_size:
        chunks = []
        start = 0
        while start < len(normalized):
            chunks.append(normalized[start : start + chunk_size])
            start += max(1, chunk_size - overlap)

    return [chunk for chunk in chunks if len(chunk.strip()) >= 50]


def parse_iso_date_to_timezone(value: str | None, timezone_name: str) -> pd.Timestamp:
    if not value:
        return pd.NaT
    try:
        dt = dateparser.parse(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return pd.Timestamp(dt.astimezone(ZoneInfo(timezone_name)))
    except Exception:
        return pd.NaT


def utcnow() -> datetime:
    return datetime.now(tz=ZoneInfo("UTC"))
