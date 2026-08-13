from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from job_matcher.config import Settings, get_settings
from job_matcher.text_utils import parse_iso_date_to_timezone, split_paragraphs

logger = logging.getLogger(__name__)

JOBUP_SEARCH_URL = "https://job-search-api.jobup.ch/search/semantic"
JOBUP_DETAIL_BASE_URL = "https://www.jobup.ch/fr/emplois/detail"
JOBUP_REGIONS = ((34, "Genève"), (55, "Lausanne"))
JOBUP_PAGE_SIZE = 200
SOURCE_NAME = "jobup"

SEARCH_COLUMNS = [
    "search_url",
    "search_region_id",
    "search_region_name",
    "job_id",
    "title",
    "company",
    "location",
    "url",
    "list_date",
    "initial_publication_date",
    "employment_type",
    "address_country",
    "address_locality",
    "address_region",
    "latitude",
    "longitude",
    "search_metadata_json",
    "collected_at",
]

DETAIL_DEFAULTS: dict[str, Any] = {
    "job_id_detail": None,
    "canonical_url": None,
    "title_detail": None,
    "company_detail": None,
    "location_detail": None,
    "valid_through": None,
    "employment_type_detail": "",
    "industry": None,
    "skills": None,
    "education_requirements": None,
    "address_country_detail": None,
    "address_locality_detail": None,
    "address_region_detail": None,
    "latitude_detail": None,
    "longitude_detail": None,
    "description_text": "",
    "description_html": "",
    "detail_metadata_json": "{}",
    "source_parser": None,
    "detail_status": None,
    "detail_error": None,
}

REQUEST_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "fr",
    "Origin": "https://www.jobup.ch",
    "Referer": "https://www.jobup.ch/",
    "X-Node-Request": "false",
    "X-Source": "jobup_ch_desktop",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    ),
}


def create_http_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(REQUEST_HEADERS)
    return session


def canonicalize_jobup_job_url(
    url: str | None = None,
    job_id: str | None = None,
) -> str | None:
    if job_id:
        return f"{JOBUP_DETAIL_BASE_URL}/{str(job_id).strip()}"
    if not url:
        return None
    parsed = urlparse(urljoin("https://www.jobup.ch", url))
    clean_path = parsed.path.rstrip("/")
    return urlunparse(parsed._replace(path=clean_path, query="", fragment=""))


def extract_jobup_job_id(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value)
    candidate = parsed.path.rstrip("/").rsplit("/", 1)[-1] if parsed.path else value
    candidate = candidate.strip()
    return candidate or None


def _as_local_datetime(value: datetime | str, timezone_name: str) -> datetime:
    parsed = dateparser.parse(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    return parsed.astimezone(ZoneInfo(timezone_name))


def format_api_datetime(value: datetime | str, timezone_name: str) -> str:
    return _as_local_datetime(value, timezone_name).strftime("%Y-%m-%d %H:%M:%S")


def build_startup_window(
    run_start: datetime,
    timezone_name: str,
) -> tuple[datetime, datetime]:
    end = _as_local_datetime(run_start, timezone_name)
    start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, end


def build_scheduled_window(
    interval_end: datetime,
    timezone_name: str,
) -> tuple[datetime, datetime]:
    end = _as_local_datetime(interval_end, timezone_name)
    return end - timedelta(minutes=15), end


def _first_location(document: dict[str, Any]) -> dict[str, Any]:
    locations = document.get("locations") or []
    return locations[0] if locations and isinstance(locations[0], dict) else {}


def _normalize_employment_type(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and (text := item.strip()):
                return text
    return ""


def _document_to_row(
    document: dict[str, Any],
    search_url: str,
    region_id: int,
    region_name: str,
    collected_at: datetime,
) -> dict[str, Any] | None:
    job_id = str(document.get("id") or "").strip()
    title = str(document.get("title") or "").strip()
    if not job_id or not title:
        return None

    company = document.get("company") or {}
    company_name = company.get("name") if isinstance(company, dict) else None
    address = _first_location(document)
    coordinates = document.get("coordinates") or {}
    employment_type_ids = document.get("employmentTypeIds") or []
    metadata = {
        "initialPublicationDate": document.get("initialPublicationDate"),
        "employmentPositionIds": document.get("employmentPositionIds") or [],
        "employmentTypeIds": employment_type_ids,
        "employmentGrades": document.get("employmentGrades") or [],
        "benefitIds": document.get("benefitIds") or [],
        "isPaid": document.get("isPaid"),
        "languageIds": document.get("languageIds") or [],
        "listingTags": document.get("listingTags") or [],
        "companyId": company.get("id") if isinstance(company, dict) else None,
        "companySlug": company.get("slug") if isinstance(company, dict) else None,
    }
    return {
        "search_url": search_url,
        "search_region_id": region_id,
        "search_region_name": region_name,
        "job_id": job_id,
        "title": title,
        "company": company_name,
        "location": document.get("place") or address.get("city"),
        "url": canonicalize_jobup_job_url(job_id=job_id),
        "list_date": document.get("publicationDate"),
        "initial_publication_date": document.get("initialPublicationDate"),
        "employment_type": _normalize_employment_type(employment_type_ids),
        "address_country": address.get("countryCode"),
        "address_locality": address.get("city"),
        "address_region": address.get("cantonCode"),
        "latitude": address.get("latitude") or coordinates.get("lat"),
        "longitude": address.get("longitude") or coordinates.get("lon"),
        "search_metadata_json": json.dumps(metadata, ensure_ascii=False),
        "collected_at": collected_at,
    }


def collect_search_results(
    publication_date_from: datetime | str,
    publication_date_to: datetime | str,
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    http = session or create_http_session()
    collected_at = datetime.now(ZoneInfo(active_settings.timezone))
    date_from = format_api_datetime(publication_date_from, active_settings.timezone)
    date_to = format_api_datetime(publication_date_to, active_settings.timezone)
    rows: list[dict[str, Any]] = []

    for region_id, region_name in JOBUP_REGIONS:
        page = 1
        expected_pages = 1
        region_rows_before = len(rows)
        while page <= expected_pages:
            params = {
                "page": page,
                "publicationDateFrom": date_from,
                "publicationDateTo": date_to,
                "regionIds": region_id,
                "rows": JOBUP_PAGE_SIZE,
                "sort": "date",
            }
            logger.info(
                "Collecting JobUp region %s (%s), page %s/%s, window %s -> %s",
                region_name,
                region_id,
                page,
                expected_pages,
                date_from,
                date_to,
            )
            response = http.get(JOBUP_SEARCH_URL, params=params, timeout=(10, 30))
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(
                payload.get("documents"), list
            ):
                raise ValueError(
                    f"Invalid JobUp response for region {region_id}, page {page}"
                )

            try:
                expected_pages = max(1, int(payload.get("numPages", 1)))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid JobUp numPages for region {region_id}, page {page}"
                ) from exc

            search_url = getattr(response, "url", JOBUP_SEARCH_URL)
            documents = payload["documents"]
            logger.info(
                "JobUp region %s page %s returned %s documents (totalHits=%s)",
                region_name,
                page,
                len(documents),
                payload.get("totalHits"),
            )
            for document in documents:
                if not isinstance(document, dict):
                    continue
                row = _document_to_row(
                    document,
                    search_url,
                    region_id,
                    region_name,
                    collected_at,
                )
                if row is not None:
                    rows.append(row)
            page += 1

        logger.info(
            "Collected %s JobUp rows for region %s before global deduplication",
            len(rows) - region_rows_before,
            region_name,
        )

    if not rows:
        return pd.DataFrame(columns=SEARCH_COLUMNS)

    result = pd.DataFrame(rows, columns=SEARCH_COLUMNS)
    before_dedup = len(result)
    result = result.drop_duplicates(subset=["job_id", "url"]).reset_index(drop=True)
    logger.info(
        "Collected %s JobUp rows and removed %s duplicates across pages/regions",
        len(result),
        before_dedup - len(result),
    )
    return result


def _iter_json_objects(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_json_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_json_objects(child)


def _extract_job_posting(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(script.string or script.get_text())
        except (TypeError, json.JSONDecodeError):
            continue
        for candidate in _iter_json_objects(payload):
            object_type = candidate.get("@type")
            if object_type == "JobPosting" or (
                isinstance(object_type, list) and "JobPosting" in object_type
            ):
                return candidate
    return {}


def _clean_html_text(value: Any) -> str:
    if value is None:
        return ""
    return BeautifulSoup(str(value), "html.parser").get_text("\n").strip()


def _job_location(job_posting: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    location = job_posting.get("jobLocation") or {}
    if isinstance(location, list):
        location = location[0] if location else {}
    if not isinstance(location, dict):
        return {}, {}
    address = location.get("address") or {}
    geo = location.get("geo") or {}
    return (
        address if isinstance(address, dict) else {},
        geo if isinstance(geo, dict) else {},
    )


def parse_job_detail_html(
    html: str,
    fallback_url: str | None = None,
) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    posting = _extract_job_posting(soup)
    data = dict(DETAIL_DEFAULTS)

    canonical_element = soup.select_one("link[rel='canonical']")
    canonical_url = canonicalize_jobup_job_url(
        canonical_element.get("href") if canonical_element else fallback_url
    )
    data["canonical_url"] = canonical_url
    data["job_id_detail"] = extract_jobup_job_id(canonical_url or fallback_url)

    title_element = soup.find("h1")
    data["title_detail"] = _clean_html_text(
        posting.get("title")
        or (title_element.get_text(" ", strip=True) if title_element else None)
    ) or None

    organization = posting.get("hiringOrganization") or {}
    if isinstance(organization, dict):
        data["company_detail"] = _clean_html_text(organization.get("name")) or None

    description_element = soup.select_one('div[data-cy="vacancy-description"]')
    if description_element is not None:
        data["description_html"] = str(description_element)
        data["description_text"] = description_element.get_text("\n", strip=True)
        parser_name = "jobup_vacancy_description"
    else:
        data["description_html"] = str(posting.get("description") or "")
        data["description_text"] = _clean_html_text(posting.get("description"))
        parser_name = "jobup_json_ld" if posting else "jobup_dom_missing_description"

    address, geo = _job_location(posting)
    data["address_country_detail"] = address.get("addressCountry")
    data["address_locality_detail"] = address.get("addressLocality")
    data["address_region_detail"] = address.get("addressRegion")
    data["latitude_detail"] = geo.get("latitude")
    data["longitude_detail"] = geo.get("longitude")
    data["location_detail"] = ", ".join(
        value
        for value in (
            str(address.get("postalCode") or "").strip(),
            str(address.get("addressLocality") or "").strip(),
        )
        if value
    ) or None
    data["valid_through"] = posting.get("validThrough")
    data["employment_type_detail"] = _normalize_employment_type(
        posting.get("employmentType")
    )
    data["industry"] = posting.get("industry")
    data["skills"] = posting.get("skills")
    data["education_requirements"] = posting.get("educationRequirements")
    data["detail_metadata_json"] = json.dumps(
        {
            key: posting.get(key)
            for key in (
                "datePosted",
                "validThrough",
                "employmentType",
                "experienceRequirements",
                "qualifications",
                "jobBenefits",
            )
            if posting.get(key) is not None
        },
        ensure_ascii=False,
    )
    data["source_parser"] = (
        f"{parser_name}_json_ld" if posting and description_element is not None else parser_name
    )
    return data


def collect_job_details(
    jobs_df: pd.DataFrame,
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    if jobs_df.empty:
        return pd.DataFrame(
            columns=[*DETAIL_DEFAULTS, "valid_through_dt"]
        )

    urls = jobs_df["url"].dropna().drop_duplicates().tolist()
    if active_settings.max_detail_pages > 0:
        urls = urls[: active_settings.max_detail_pages]

    http = session or create_http_session()
    rows: list[dict[str, Any]] = []
    for url in urls:
        try:
            logger.info("Collecting JobUp detail page: %s", url)
            response = http.get(
                url,
                headers={"Accept": "text/html", "Accept-Language": "fr"},
                timeout=(10, 30),
            )
            response.raise_for_status()
            parsed = parse_job_detail_html(response.text, fallback_url=url)
            parsed["detail_status"] = "ok"
            parsed["detail_error"] = None
        except Exception as exc:
            logger.exception("Failed to collect JobUp detail page %s", url)
            parsed = dict(DETAIL_DEFAULTS)
            parsed.update(
                {
                    "job_id_detail": extract_jobup_job_id(url),
                    "canonical_url": canonicalize_jobup_job_url(url),
                    "detail_status": "error",
                    "detail_error": repr(exc),
                }
            )
        parsed["valid_through_dt"] = parse_iso_date_to_timezone(
            parsed.get("valid_through"),
            active_settings.timezone,
        )
        rows.append(parsed)

    return pd.DataFrame(rows)


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if not isinstance(value, (list, tuple, set)):
        try:
            if pd.isna(value):
                return {}
        except (TypeError, ValueError):
            pass
    if value == "":
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def prepare_offers_dataframe(
    jobs_df: pd.DataFrame,
    details_df: pd.DataFrame,
    settings: Settings | None = None,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    if jobs_df.empty:
        return pd.DataFrame()

    if details_df.empty:
        merged = jobs_df.copy()
    else:
        merged = jobs_df.merge(
            details_df,
            left_on="job_id",
            right_on="job_id_detail",
            how="left",
        )

    defaults = {
        **DETAIL_DEFAULTS,
        "valid_through_dt": pd.NaT,
    }
    for field, default in defaults.items():
        if field not in merged:
            merged[field] = default

    for target, detail_field in (
        ("location", "location_detail"),
        ("employment_type", "employment_type_detail"),
        ("address_country", "address_country_detail"),
        ("address_locality", "address_locality_detail"),
        ("address_region", "address_region_detail"),
        ("latitude", "latitude_detail"),
        ("longitude", "longitude_detail"),
    ):
        merged[target] = merged[detail_field].replace("", pd.NA).combine_first(
            merged[target]
        )

    merged["employment_type"] = merged["employment_type"].apply(
        _normalize_employment_type
    )

    merged["final_job_id"] = merged["job_id"].fillna(merged["job_id_detail"])
    merged["final_url"] = merged["canonical_url"].fillna(merged["url"])
    merged["final_title"] = merged["title_detail"].replace("", pd.NA).fillna(
        merged["title"]
    )
    merged["final_company"] = merged["company_detail"].replace("", pd.NA).fillna(
        merged["company"]
    )
    merged["date_posted_dt"] = merged["list_date"].apply(
        lambda value: parse_iso_date_to_timezone(value, active_settings.timezone)
    )
    merged["criteria_json"] = merged.apply(
        lambda row: json.dumps(
            {
                "search": _json_object(row.get("search_metadata_json")),
                "detail": _json_object(row.get("detail_metadata_json")),
            },
            ensure_ascii=False,
        ),
        axis=1,
    )
    merged["source"] = SOURCE_NAME

    return (
        merged.dropna(subset=["final_url"])
        .drop_duplicates(subset=["final_job_id", "final_url"])
        .reset_index(drop=True)
    )


def build_job_paragraphs(
    offers_df: pd.DataFrame,
    settings: Settings | None = None,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    rows: list[dict[str, Any]] = []
    for _, row in offers_df.iterrows():
        description = row.get("description_text")
        if pd.isna(description):
            description = ""
        description = re.sub(r"\s*•\s*", "; ", str(description))
        description = re.sub(r"\s+", " ", description).strip()
        paragraphs = split_paragraphs(
            description,
            min_chars=active_settings.paragraph_min_chars,
        )
        for index, paragraph in enumerate(paragraphs):
            rows.append(
                {
                    "canonical_url": row["final_url"],
                    "external_job_id": row.get("final_job_id"),
                    "paragraph_idx": index,
                    "paragraph": paragraph,
                    "paragraph_chars": len(paragraph),
                }
            )
    return pd.DataFrame(rows)
