from __future__ import annotations

from io import BytesIO
from pathlib import Path

from pypdf import PdfReader

from job_matcher.text_utils import clean_text


def extract_cv_text_from_pdf(pdf_path: Path) -> str:
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"CV not found: {pdf_path.resolve()}")
    return extract_cv_text_from_bytes(pdf_path.read_bytes())


def extract_cv_text_from_bytes(pdf_bytes: bytes) -> str:
    reader = PdfReader(BytesIO(pdf_bytes))
    pages = [page.extract_text() or "" for page in reader.pages]
    text = clean_text("\n\n".join(pages))
    if len(text) < 100:
        raise ValueError("CV extraction is too short. Check the PDF content.")
    return text
