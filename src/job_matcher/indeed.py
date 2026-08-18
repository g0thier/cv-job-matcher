from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
import shutil
import sys
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlparse, urlunparse
from zoneinfo import ZoneInfo

import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from playwright.async_api import async_playwright

from job_matcher.config import Settings, get_settings
from job_matcher.text_utils import parse_iso_date_to_timezone, split_paragraphs

logger = logging.getLogger(__name__)

INDEED_BASE_URL = "https://ch-fr.indeed.com"
INDEED_SEARCH_URL = f"{INDEED_BASE_URL}/jobs"
INDEED_DETAIL_URL = f"{INDEED_BASE_URL}/viewjob"
INDEED_LOCATIONS = (("Genève, GE", "Genève"), ("Lausanne, VD", "Lausanne"))
INDEED_RADIUS_KM = 25
INDEED_PAGE_STEP = 10
INDEED_FROMAGE_DAYS = 1
INDEED_SCHEDULE_MINUTES = 15
INDEED_OVERLAP_MINUTES = 5
SEARCH_LOAD_ATTEMPTS = 4
DETAIL_LOAD_ATTEMPTS = 6
BROWSER_START_ATTEMPTS = 3
BROWSER_START_BACKOFF_SECONDS = 1.0
BROWSER_STOP_TIMEOUT_SECONDS = 2.0
SOURCE_NAME = "indeed"

CAPTCHA_MARKERS = (
    "captcha",
    "cf-turnstile",
    "just a moment",
    "verify you are human",
    "vérifiez que vous êtes humain",
)
JOB_CARD_SELECTORS = (
    '[data-testid="jobCard"]',
    ".job_seen_beacon",
    ".jobsearch-ResultsList > div",
    ".result",
)
NO_RESULTS_SELECTORS = (
    '[data-testid="no-results"]',
    ".jobsearch-NoResult-messageContainer",
    ".jobsearch-ResultsList-empty",
)

SEARCH_COLUMNS = [
    "search_url",
    "search_location",
    "search_location_name",
    "search_locations_json",
    "search_urls_json",
    "job_id",
    "title",
    "company",
    "location",
    "url",
    "list_date_text",
    "list_date_estimate",
    "salary",
    "description_snippet",
    "badges",
    "sponsored",
    "search_metadata_json",
    "collected_at",
]

DETAIL_DEFAULTS: dict[str, Any] = {
    "job_id_detail": None,
    "canonical_url": None,
    "title_detail": None,
    "company_detail": None,
    "company_url": None,
    "company_logo": None,
    "location_detail": None,
    "address_country_detail": None,
    "address_locality_detail": None,
    "address_region_detail": None,
    "latitude_detail": None,
    "longitude_detail": None,
    "date_posted": None,
    "valid_through": None,
    "employment_type_detail": "",
    "industry": None,
    "skills": None,
    "education_requirements": None,
    "description_text": "",
    "description_html": "",
    "detail_metadata_json": "{}",
    "source_parser": None,
    "detail_status": None,
    "detail_error": None,
}

SleepCallable = Callable[[float], Awaitable[None]]
BrowserFactory = Callable[..., Any]


def _as_local_datetime(value: datetime | str, timezone_name: str) -> datetime:
    parsed = dateparser.parse(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    return parsed.astimezone(ZoneInfo(timezone_name))


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
    overlap_minutes: int = INDEED_OVERLAP_MINUTES,
) -> tuple[datetime, datetime]:
    end = _as_local_datetime(interval_end, timezone_name)
    start = end - timedelta(
        minutes=INDEED_SCHEDULE_MINUTES + max(0, int(overlap_minutes))
    )
    return start, end


def build_search_url(location: str, start: int = 0) -> str:
    params = {
        "q": "",
        "l": location,
        "radius": INDEED_RADIUS_KM,
        "sort": "date",
        "limit": 240,
        "fromage": INDEED_FROMAGE_DAYS,
        "start": max(0, int(start)),
    }
    return f"{INDEED_SEARCH_URL}?{urlencode(params)}"


def extract_indeed_job_key(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    parsed = urlparse(text)
    query_key = parse_qs(parsed.query).get("jk", [None])[0]
    if query_key and str(query_key).strip():
        return str(query_key).strip()
    match = re.search(r"(?:[?&]jk=|/viewjob/)([A-Za-z0-9_-]+)", text)
    return match.group(1) if match else None


def _is_indeed_host(hostname: str | None) -> bool:
    host = (hostname or "").lower()
    return host == "indeed.com" or host.startswith("indeed.") or ".indeed." in host


def canonicalize_indeed_job_url(
    url: str | None = None,
    job_key: str | None = None,
) -> str | None:
    normalized_key = str(job_key or "").strip() or extract_indeed_job_key(url)
    if normalized_key:
        return f"{INDEED_DETAIL_URL}?{urlencode({'jk': normalized_key})}"
    if not url:
        return None
    absolute = urljoin(INDEED_BASE_URL, str(url).strip())
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"} or not _is_indeed_host(parsed.hostname):
        return None
    return absolute


def _normalize_inline(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if not isinstance(value, (dict, list, tuple, set)) and pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()
    return text or None


def _html_to_plain_text(value: Any) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(str(value), "html.parser")
    lines = [_normalize_inline(line) for line in soup.get_text("\n").splitlines()]
    return "\n".join(line for line in lines if line)


def _first_text(container: BeautifulSoup, selectors: tuple[str, ...]) -> str | None:
    for selector in selectors:
        element = container.select_one(selector)
        if element is not None:
            text = _normalize_inline(element.get_text(" ", strip=True))
            if text:
                return text
    return None


def find_job_cards(html: str) -> list[Any]:
    soup = BeautifulSoup(html, "html.parser")
    for selector in JOB_CARD_SELECTORS:
        cards = soup.select(selector)
        if cards:
            return cards
    return []


def _strip_accents(value: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(character)
    )


def parse_relative_posted_at(
    value: str | None,
    reference: datetime,
) -> datetime | None:
    if not value:
        return None
    text = _strip_accents(_normalize_inline(value) or "").lower()
    if not text:
        return None
    if any(marker in text for marker in ("a l'instant", "just now", "a few seconds")):
        return reference
    if "aujourd'hui" in text or text == "today":
        return None

    match = re.search(
        r"(?:il y a|posted|active)\s*(\d+)\s*"
        r"(minute|minutes|heure|heures|hour|hours|jour|jours|day|days)",
        text,
    )
    if not match:
        match = re.search(
            r"(\d+)\s*"
            r"(minute|minutes|heure|heures|hour|hours|jour|jours|day|days)"
            r"(?:\s+ago)?",
            text,
        )
    if not match:
        return None

    amount = int(match.group(1))
    unit = match.group(2)
    if unit.startswith("minute"):
        delta = timedelta(minutes=amount)
    elif unit.startswith(("heure", "hour")):
        delta = timedelta(hours=amount)
    else:
        delta = timedelta(days=amount)
    return reference - delta


def _card_snippet(card: Any) -> str | None:
    container = card.select_one('[data-testid="belowJobSnippet"]')
    if container is None:
        return None
    values = [
        _normalize_inline(item.get_text(" ", strip=True))
        for item in container.select("li")
    ]
    values = [value for value in values if value]
    if values:
        return " | ".join(values)
    return _normalize_inline(container.get_text(" ", strip=True))


def parse_search_page_html(
    html: str,
    *,
    search_url: str,
    search_location: str,
    search_location_name: str,
    collected_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for card in find_job_cards(html):
        link = card.select_one("a[data-jk]")
        job_key = (
            _normalize_inline(link.get("data-jk")) if link is not None else None
        )
        if not job_key:
            continue

        title = _first_text(
            card,
            (
                'h2[data-testid="jobTitle"]',
                'a[data-testid="jobTitle"]',
                "h3.jobTitle",
                "a.jcs-JobTitle",
                'span[id^="jobTitle-"]',
            ),
        )
        company = _first_text(
            card,
            (
                'span[data-testid="companyName"]',
                'div[data-testid="companyName"]',
                "span.companyName",
                'span[data-testid="company-name"]',
            ),
        )
        location = _first_text(
            card,
            (
                'div[data-testid="location"]',
                'span[data-testid="location"]',
                "div.location",
                'div[data-testid="text-location"]',
            ),
        )
        salary = _first_text(
            card,
            (
                'div[data-testid="salary"]',
                'span[data-testid="salary"]',
                "div.salary-snippet",
                "li.salary-snippet-container",
            ),
        )
        date_text = _first_text(
            card,
            (
                'span[data-testid="postDate"]',
                'div[data-testid="postDate"]',
                "span.date",
            ),
        )
        estimate = parse_relative_posted_at(date_text, collected_at)
        badges = [
            value
            for value in (
                _normalize_inline(element.get_text(" ", strip=True))
                for element in card.select(
                    '[data-testid*="badge"], .mosaic-provider-jobcards-1f1q1js'
                )
            )
            if value
        ]
        sponsored = card.select_one(".sponTapItem") is not None or bool(
            re.search(r"sponsoris|sponsored", card.get_text(" ", strip=True), re.I)
        )
        canonical_url = canonicalize_indeed_job_url(job_key=job_key)
        rows.append(
            {
                "search_url": search_url,
                "search_location": search_location,
                "search_location_name": search_location_name,
                "job_id": job_key,
                "title": title,
                "company": company,
                "location": location,
                "url": canonical_url,
                "list_date_text": date_text,
                "list_date_estimate": estimate,
                "salary": salary,
                "description_snippet": _card_snippet(card),
                "badges": ", ".join(dict.fromkeys(badges)) or None,
                "sponsored": sponsored,
                "collected_at": collected_at,
            }
        )
    return rows


def _contains_captcha(html: str) -> bool:
    lowered = html.lower()
    return any(marker in lowered for marker in CAPTCHA_MARKERS)


def _contains_no_results(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    if any(soup.select_one(selector) is not None for selector in NO_RESULTS_SELECTORS):
        return True
    text = _strip_accents(soup.get_text(" ", strip=True)).lower()
    return any(
        marker in text
        for marker in (
            "aucune offre",
            "aucun emploi",
            "no jobs found",
            "did not match any jobs",
        )
    )


def _page_contains_offer(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    posting = _find_job_posting(soup)
    has_title = bool(
        soup.select_one('[data-testid="jobsearch-JobInfoHeader-title"], h1')
        or _normalize_inline(posting.get("title"))
    )
    has_description = bool(
        soup.select_one("#jobDescriptionText")
        or _html_to_plain_text(posting.get("description"))
    )
    return has_title and has_description


async def _resolve_chromium_executable() -> str | None:
    configured = os.getenv("INDEED_BROWSER_EXECUTABLE")
    if configured:
        path = Path(configured).expanduser()
        if not path.is_file():
            raise FileNotFoundError(
                f"INDEED_BROWSER_EXECUTABLE does not exist: {path}"
            )
        return str(path)

    async with async_playwright() as playwright:
        path = Path(playwright.chromium.executable_path)
    return str(path) if path.is_file() else None


def _browser_profile_directory(run_directory: Path, launch_attempt: int) -> Path:
    task_id = re.sub(
        r"[^A-Za-z0-9_.-]+",
        "_",
        os.getenv("AIRFLOW_CTX_TASK_ID", "local"),
    ).strip("_") or "local"
    try_number = re.sub(
        r"[^A-Za-z0-9_.-]+",
        "_",
        os.getenv("AIRFLOW_CTX_TRY_NUMBER", "1"),
    ).strip("_") or "1"
    return run_directory / (
        f"indeed_browser_profile_{task_id}_try_{try_number}_launch_{launch_attempt}"
    )


def _is_retryable_browser_start_error(error: Exception) -> bool:
    message = str(error).casefold()
    return isinstance(error, (ConnectionError, OSError, TimeoutError)) or any(
        marker in message
        for marker in (
            "failed to connect to the browser",
            "connection refused",
            "browser closed",
        )
    )


async def _cleanup_failed_browser_instances(
    instances: set[Any],
    registry: set[Any],
) -> bool:
    all_stopped = True
    for instance in instances:
        try:
            await _terminate_browser_process(instance, log_stderr=True)
        except Exception:
            all_stopped = False
            logger.exception("Unable to clean up a failed Chromium process")
        finally:
            registry.discard(instance)
    return all_stopped


async def _terminate_browser_process(
    browser: Any,
    *,
    log_stderr: bool,
) -> None:
    process = getattr(browser, "_process", None)
    if process is None:
        return
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
    try:
        _stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=BROWSER_STOP_TIMEOUT_SECONDS,
        )
    except asyncio.CancelledError:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        raise
    except TimeoutError:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        try:
            _stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=BROWSER_STOP_TIMEOUT_SECONDS,
            )
        except TimeoutError as error:
            raise RuntimeError(
                "Chromium did not terminate after being killed"
            ) from error
    if log_stderr and stderr:
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        if stderr_text:
            logger.warning("Chromium startup stderr:\n%s", stderr_text[-4000:])


def _start_virtual_display() -> Any:
    from sbvirtualdisplay import Display

    display = Display(visible=0, size=(1366, 840))
    display.start()
    return display


def _stop_virtual_display(display: Any) -> None:
    if display is None:
        return
    try:
        display.stop()
    except Exception:
        logger.exception("Unable to stop the Indeed Xvfb display")


def _remove_browser_profile(profile_directory: Path | None) -> None:
    if profile_directory is None:
        return
    try:
        shutil.rmtree(profile_directory)
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception(
            "Unable to remove the Indeed browser profile %s",
            profile_directory,
        )


async def _start_browser(
    run_directory: Path,
    browser_factory: BrowserFactory | None,
    sleep: SleepCallable = asyncio.sleep,
):
    executable = None
    registry: set[Any] | None = None
    if browser_factory is None:
        from seleniumbase.undetected.cdp_driver import cdp_util
        from seleniumbase.undetected.cdp_driver.browser import (
            get_registered_instances,
        )

        executable = await _resolve_chromium_executable()
        registry = get_registered_instances()

    for launch_attempt in range(1, BROWSER_START_ATTEMPTS + 1):
        profile_directory = _browser_profile_directory(
            run_directory,
            launch_attempt,
        )
        profile_directory.mkdir(parents=True, exist_ok=True)
        kwargs = {
            "headless": False,
            "xvfb": sys.platform.startswith("linux"),
            "user_data_dir": str(profile_directory),
            "lang": "fr-CH",
        }
        if sys.platform.startswith("linux"):
            kwargs["sandbox"] = False
        if executable:
            kwargs["browser_executable_path"] = executable

        registered_before = set(registry or ())
        virtual_display = None
        try:
            if browser_factory is not None:
                browser = browser_factory(**kwargs)
                return await browser if inspect.isawaitable(browser) else browser
            if sys.platform.startswith("linux"):
                virtual_display = _start_virtual_display()
                kwargs["xvfb"] = False
                kwargs["headed"] = True
            browser = await cdp_util.start_async(**kwargs)
            browser._indeed_virtual_display = virtual_display
            browser._indeed_registry = registry
            browser._indeed_profile_directory = profile_directory
            return browser
        except BaseException as error:
            browser_instances_stopped = True
            if registry is not None:
                browser_instances_stopped = await _cleanup_failed_browser_instances(
                    set(registry) - registered_before,
                    registry,
                )
            _stop_virtual_display(virtual_display)
            if browser_instances_stopped:
                _remove_browser_profile(profile_directory)
            if not isinstance(error, Exception):
                raise
            if (
                launch_attempt >= BROWSER_START_ATTEMPTS
                or not _is_retryable_browser_start_error(error)
            ):
                raise
            delay = BROWSER_START_BACKOFF_SECONDS * launch_attempt
            logger.warning(
                "Indeed browser startup attempt %d/%d failed: %s. "
                "Retrying in %.1f seconds with a fresh profile.",
                launch_attempt,
                BROWSER_START_ATTEMPTS,
                error,
                delay,
            )
            await sleep(delay)

    raise RuntimeError("Indeed browser startup exhausted all attempts")


async def _stop_browser(browser: Any) -> None:
    if browser is None:
        return
    virtual_display = getattr(browser, "_indeed_virtual_display", None)
    registry = getattr(browser, "_indeed_registry", None)
    profile_directory = getattr(browser, "_indeed_profile_directory", None)
    browser_process_stopped = False
    try:
        try:
            result = browser.stop()
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception("Unable to stop the Indeed browser cleanly")
        try:
            await _terminate_browser_process(browser, log_stderr=False)
            browser_process_stopped = True
        except Exception:
            logger.exception("Unable to terminate the Indeed Chromium process")
    finally:
        if registry is not None:
            registry.discard(browser)
        _stop_virtual_display(virtual_display)
        if browser_process_stopped:
            _remove_browser_profile(profile_directory)


async def _save_screenshot(page: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        await page.save_screenshot(filename=str(path), full_page=True)
    except Exception:
        logger.exception("Unable to save Indeed diagnostic screenshot to %s", path)


async def _current_page_url(page: Any, fallback: str) -> str:
    getter = getattr(page, "get_current_url", None)
    if callable(getter):
        value = getter()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return str(value)
    value = getattr(page, "url", None)
    if inspect.isawaitable(value):
        value = await value
    if value:
        return str(value)
    target = getattr(page, "target", None)
    value = getattr(target, "url", None)
    return str(value or fallback)


async def _solve_captcha(page: Any) -> bool:
    """Use SeleniumBase's solver when available, with a CDP click fallback."""
    solver = getattr(page, "solve_captcha", None)
    if callable(solver):
        result = solver()
        if inspect.isawaitable(result):
            await result
        return True

    selectors = (
        'iframe[src*="challenges.cloudflare.com"]',
        'iframe[title*="Cloudflare"]',
        'iframe[title*="challenge"]',
        '[class="cf-turnstile"]',
        "#challenge-form div > div",
        '[style="display: grid;"] div div',
        '[data-testid*="challenge-"] div',
        ".cf-turnstile-wrapper",
        '[id*="turnstile"] div:not([class])',
        '[class*="turnstile"] div:not([class])',
        '[data-callback="onCaptchaSuccess"]',
    )
    select = getattr(page, "select", None)
    evaluate = getattr(page, "evaluate", None)
    if not callable(select) and not callable(evaluate):
        logger.warning("Indeed CAPTCHA detected but this CDP page has no solver")
        return False

    async def native_click(x: float, y: float) -> None:
        import mycdp as cdp
        import mycdp.input_  # noqa: F401

        bring_to_front = getattr(page, "bring_to_front", None)
        if callable(bring_to_front):
            result = bring_to_front()
            if inspect.isawaitable(result):
                await result
        button = cdp.input_.MouseButton("left")
        for event_type in ("mousePressed", "mouseReleased"):
            await page.send(
                cdp.input_.dispatch_mouse_event(
                    event_type,
                    x=x,
                    y=y,
                    button=button,
                    buttons=1,
                    click_count=1,
                )
            )

    for selector in selectors:
        if callable(evaluate):
            try:
                expression = (
                    "(() => { const element = document.querySelector("
                    f"{json.dumps(selector)}"
                    "); if (!element) return null; "
                    "const rect = element.getBoundingClientRect(); "
                    "return {left: rect.left, top: rect.top, "
                    "width: rect.width, height: rect.height}; })()"
                )
                rectangle = evaluate(expression)
                if inspect.isawaitable(rectangle):
                    rectangle = await rectangle
                if rectangle and rectangle.get("width") and rectangle.get("height"):
                    x = float(rectangle["left"]) + min(
                        32, max(1, float(rectangle["width"]) / 2)
                    )
                    y = float(rectangle["top"]) + min(
                        32, max(1, float(rectangle["height"]) / 2)
                    )
                    await native_click(x, y)
                    return True
            except Exception:
                logger.debug(
                    "Indeed evaluated CAPTCHA click failed for selector %s",
                    selector,
                    exc_info=True,
                )

        if not callable(select):
            continue
        try:
            element = select(selector, timeout=1)
            if inspect.isawaitable(element):
                element = await element
        except Exception:
            continue

        try:
            position = await element.get_position_async()
            x = position.left + min(32, max(1, position.width / 2))
            y = position.top + min(32, max(1, position.height / 2))
            await native_click(x, y)
            return True
        except Exception:
            logger.debug(
                "Indeed native CAPTCHA click failed for selector %s",
                selector,
                exc_info=True,
            )

        for method_name in ("mouse_click_async", "click_async", "click"):
            click = getattr(element, method_name, None)
            if not callable(click):
                continue
            try:
                result = click()
                if inspect.isawaitable(result):
                    await result
                return True
            except Exception:
                logger.debug(
                    "Indeed CAPTCHA click failed for selector %s via %s",
                    selector,
                    method_name,
                    exc_info=True,
                )
    logger.warning("Indeed CAPTCHA detected but no challenge control was clickable")
    return False


async def _load_search_html(
    browser: Any,
    url: str,
    debug_path: Path,
    sleep: SleepCallable,
) -> tuple[Any, str]:
    page = await browser.get(url)
    await sleep(3)
    last_html = ""
    for attempt in range(1, SEARCH_LOAD_ATTEMPTS + 1):
        last_html = await page.get_content()
        if find_job_cards(last_html) or _contains_no_results(last_html):
            return page, last_html
        if _contains_captcha(last_html):
            logger.warning(
                "Indeed CAPTCHA detected for %s (%s/%s)",
                url,
                attempt,
                SEARCH_LOAD_ATTEMPTS,
            )
            await _solve_captcha(page)
            await sleep(4)
        else:
            await sleep(2)

    await _save_screenshot(page, debug_path)
    current_url = await _current_page_url(page, url)
    raise RuntimeError(
        "Indeed search page did not expose job cards or an empty-results marker "
        f"after {SEARCH_LOAD_ATTEMPTS} attempts: {current_url}"
    )


async def _scroll_search_page(
    page: Any,
    html: str,
    settings: Settings,
    sleep: SleepCallable,
) -> str:
    previous_count = len(find_job_cards(html))
    stable_rounds = 0
    for _ in range(max(1, active_scroll_rounds(settings))):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await sleep(1)
        html = await page.get_content()
        current_count = len(find_job_cards(html))
        if current_count > previous_count:
            previous_count = current_count
            stable_rounds = 0
        else:
            stable_rounds += 1
        if stable_rounds >= 2:
            break
    await page.evaluate("window.scrollTo(0, 0)")
    await sleep(0.25)
    return await page.get_content()


def active_scroll_rounds(settings: Settings) -> int:
    return min(max(1, int(settings.scroll_rounds)), 40)


def _next_page_url(html: str, current_url: str, current_start: int) -> str | None:
    def preserve_search_parameters(href: str) -> str:
        absolute = urljoin(current_url, href)
        current = urlparse(current_url)
        target = urlparse(absolute)
        current_query = dict(parse_qsl(current.query, keep_blank_values=True))
        target_query = dict(parse_qsl(target.query, keep_blank_values=True))
        current_query.update(target_query)
        return urlunparse(target._replace(query=urlencode(current_query)))

    soup = BeautifulSoup(html, "html.parser")
    for selector in (
        'a[data-testid="pagination-page-next"]',
        'a[aria-label*="Suivant"]',
        'a[aria-label*="Next"]',
    ):
        element = soup.select_one(selector)
        if element is not None and element.get("href"):
            return preserve_search_parameters(element.get("href"))

    candidates: list[tuple[int, str]] = []
    for element in soup.select('a[href*="start="]'):
        href = preserve_search_parameters(element.get("href"))
        try:
            start = int(parse_qs(urlparse(href).query).get("start", ["0"])[0])
        except (TypeError, ValueError):
            continue
        if start > current_start:
            candidates.append((start, href))
    if candidates:
        return min(candidates, key=lambda item: item[0])[1]
    return None


def _row_is_definitely_too_old(row: dict[str, Any], window_start: datetime) -> bool:
    estimate = row.get("list_date_estimate")
    if not isinstance(estimate, datetime):
        return False
    return estimate < window_start - timedelta(minutes=2)


def _aggregate_search_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=SEARCH_COLUMNS)

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row["job_id"])
        current = grouped.get(key)
        if current is None:
            current = dict(row)
            current["_locations"] = [row["search_location_name"]]
            current["_search_urls"] = [row["search_url"]]
            grouped[key] = current
            continue

        if row["search_location_name"] not in current["_locations"]:
            current["_locations"].append(row["search_location_name"])
        if row["search_url"] not in current["_search_urls"]:
            current["_search_urls"].append(row["search_url"])
        for column in (
            "title",
            "company",
            "location",
            "list_date_text",
            "list_date_estimate",
            "salary",
            "description_snippet",
            "badges",
        ):
            if not current.get(column) and row.get(column):
                current[column] = row[column]
        current["sponsored"] = bool(current.get("sponsored") or row.get("sponsored"))

    result_rows: list[dict[str, Any]] = []
    for current in grouped.values():
        locations = current.pop("_locations")
        search_urls = current.pop("_search_urls")
        current["search_locations_json"] = json.dumps(
            locations, ensure_ascii=False
        )
        current["search_urls_json"] = json.dumps(search_urls, ensure_ascii=False)
        current["search_metadata_json"] = json.dumps(
            {
                "matched_search_locations": locations,
                "search_urls": search_urls,
                "list_date_text": current.get("list_date_text"),
                "salary": current.get("salary"),
                "description_snippet": current.get("description_snippet"),
                "badges": current.get("badges"),
                "sponsored": bool(current.get("sponsored")),
            },
            ensure_ascii=False,
        )
        result_rows.append(current)

    return pd.DataFrame(result_rows, columns=SEARCH_COLUMNS)


async def collect_search_results(
    publication_date_from: datetime | str,
    publication_date_to: datetime | str,
    settings: Settings | None = None,
    *,
    run_directory: str | Path | None = None,
    browser_factory: BrowserFactory | None = None,
    sleep: SleepCallable = asyncio.sleep,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    window_start = _as_local_datetime(
        publication_date_from, active_settings.timezone
    )
    window_end = _as_local_datetime(publication_date_to, active_settings.timezone)
    if window_start >= window_end:
        raise ValueError("Indeed collection window must have a positive duration")

    run_dir = Path(run_directory or "runtime/airflow/indeed")
    debug_dir = run_dir / "indeed_debug"
    browser = await _start_browser(run_dir, browser_factory)
    all_rows: list[dict[str, Any]] = []
    try:
        for search_location, location_name in INDEED_LOCATIONS:
            current_start = 0
            next_url: str | None = build_search_url(search_location, current_start)
            region_keys: set[str] = set()
            page_number = 0
            while next_url:
                page_number += 1
                logger.info(
                    "Collecting Indeed location %s, page %s: %s",
                    location_name,
                    page_number,
                    next_url,
                )
                page, html = await _load_search_html(
                    browser,
                    next_url,
                    debug_dir / f"search_{location_name}_{page_number}.png",
                    sleep,
                )
                html = await _scroll_search_page(page, html, active_settings, sleep)
                collected_at = datetime.now(ZoneInfo(active_settings.timezone))
                page_rows = parse_search_page_html(
                    html,
                    search_url=next_url,
                    search_location=search_location,
                    search_location_name=location_name,
                    collected_at=collected_at,
                )
                if not page_rows:
                    break

                candidate_rows = [
                    row
                    for row in page_rows
                    if not _row_is_definitely_too_old(row, window_start)
                ]
                page_keys = {str(row["job_id"]) for row in candidate_rows}
                new_keys = page_keys - region_keys
                for row in candidate_rows:
                    if str(row["job_id"]) in new_keys:
                        all_rows.append(row)
                region_keys.update(new_keys)

                discovered_next = _next_page_url(html, next_url, current_start)
                if (
                    len(region_keys) >= active_settings.max_jobs_per_search
                    and discovered_next
                ):
                    raise RuntimeError(
                        "Indeed MAX_JOBS_PER_SEARCH was reached for "
                        f"{location_name} while more results may exist"
                    )

                known_estimates = [
                    row.get("list_date_estimate")
                    for row in page_rows
                    if isinstance(row.get("list_date_estimate"), datetime)
                ]
                page_is_older = bool(known_estimates) and len(known_estimates) == len(
                    page_rows
                ) and all(
                    estimate < window_start - timedelta(minutes=2)
                    for estimate in known_estimates
                )
                if page_is_older or not new_keys or not discovered_next:
                    break

                next_url = discovered_next
                try:
                    current_start = int(
                        parse_qs(urlparse(next_url).query).get("start", ["0"])[0]
                    )
                except (TypeError, ValueError):
                    current_start += INDEED_PAGE_STEP
    finally:
        await _stop_browser(browser)

    result = _aggregate_search_rows(all_rows)
    logger.info(
        "Collected %s unique Indeed jobs across Geneva and Lausanne",
        len(result),
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


def _find_job_posting(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.select('script[type="application/ld+json"]'):
        payload = script.string or script.get_text()
        try:
            decoded = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            continue
        for candidate in _iter_json_objects(decoded):
            object_types = candidate.get("@type", [])
            if isinstance(object_types, str):
                object_types = [object_types]
            if "JobPosting" in object_types:
                return candidate
    return {}


def _first_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return next((item for item in value if isinstance(item, dict)), {})
    return {}


def _normalize_string_list(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        values = [_normalize_inline(item) for item in value]
        return " | ".join(item for item in values if item)
    return ""


def _extract_company_metrics(soup: BeautifulSoup) -> tuple[float | None, int | None]:
    container = soup.select_one('[data-testid="jobsearch-CompanyInfoContainer"]')
    header_text = _normalize_inline(container.get_text(" ", strip=True)) if container else ""
    rating_match = re.search(r"(\d+(?:[.,]\d+)?)\s*/\s*5", header_text or "")
    rating = float(rating_match.group(1).replace(",", ".")) if rating_match else None
    reviews_text = _first_text(
        soup,
        ('[data-testid="companyReviewLink"]', 'a[href*="/reviews"]'),
    )
    reviews_match = re.search(r"([\d\s.,]+)\s+(?:avis|reviews?)\b", reviews_text or "", re.I)
    reviews_digits = re.sub(r"\D", "", reviews_match.group(1)) if reviews_match else ""
    return rating, int(reviews_digits) if reviews_digits else None


def parse_job_detail_html(
    html: str,
    fallback_url: str | None = None,
) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    posting = _find_job_posting(soup)
    organization = _first_mapping(posting.get("hiringOrganization"))
    job_location = _first_mapping(posting.get("jobLocation"))
    address = _first_mapping(job_location.get("address"))
    geo = _first_mapping(job_location.get("geo"))

    canonical_element = soup.select_one('link[rel="canonical"]')
    raw_canonical = (
        canonical_element.get("href") if canonical_element is not None else fallback_url
    )
    job_key = extract_indeed_job_key(raw_canonical) or extract_indeed_job_key(
        fallback_url
    )
    canonical_url = canonicalize_indeed_job_url(raw_canonical, job_key=job_key)

    description_html = str(posting.get("description") or "")
    description_text = _html_to_plain_text(description_html)
    parser_name = "indeed_json_ld" if posting else "indeed_dom"
    if not description_text:
        description_element = soup.select_one("#jobDescriptionText")
        if description_element is not None:
            description_html = str(description_element)
            description_text = _html_to_plain_text(description_html)
            parser_name = "indeed_dom"

    location = _first_text(
        soup,
        (
            '[data-testid="inlineHeader-companyLocation"]',
            '[data-testid="jobLocationText"]',
            "#jobLocationText",
            ".jobsearch-JobInfoHeader-companyLocation",
        ),
    )
    if not location:
        location = ", ".join(
            value
            for value in (
                _normalize_inline(address.get("addressLocality")),
                _normalize_inline(address.get("addressRegion")),
                _normalize_inline(address.get("addressCountry")),
            )
            if value
        ) or None

    rating, reviews_count = _extract_company_metrics(soup)
    metadata = {
        "company_url": organization.get("sameAs"),
        "company_logo": organization.get("logo"),
        "direct_apply": posting.get("directApply"),
        "salary_and_job_type": _first_text(
            soup,
            ('[data-testid="salaryInfoAndJobType"]', "#salaryInfoAndJobType"),
        ),
        "company_rating": rating,
        "company_reviews_count": reviews_count,
        "json_ld": posting,
    }
    data = dict(DETAIL_DEFAULTS)
    data.update(
        {
            "job_id_detail": job_key,
            "canonical_url": canonical_url,
            "title_detail": _normalize_inline(posting.get("title"))
            or _first_text(
                soup,
                ('[data-testid="jobsearch-JobInfoHeader-title"]', "h1"),
            ),
            "company_detail": _normalize_inline(organization.get("name"))
            or _first_text(
                soup,
                ('[data-testid="inlineHeader-companyName"]', '[data-company-name="true"]'),
            ),
            "company_url": _normalize_inline(organization.get("sameAs")),
            "company_logo": _normalize_inline(organization.get("logo")),
            "location_detail": location,
            "address_country_detail": _normalize_inline(
                address.get("addressCountry")
            ),
            "address_locality_detail": _normalize_inline(
                address.get("addressLocality")
            ),
            "address_region_detail": _normalize_inline(address.get("addressRegion")),
            "latitude_detail": geo.get("latitude"),
            "longitude_detail": geo.get("longitude"),
            "date_posted": _normalize_inline(posting.get("datePosted")),
            "valid_through": _normalize_inline(posting.get("validThrough")),
            "employment_type_detail": _normalize_string_list(
                posting.get("employmentType")
            ),
            "industry": _normalize_string_list(posting.get("industry")) or None,
            "skills": _normalize_string_list(posting.get("skills")) or None,
            "education_requirements": _normalize_string_list(
                posting.get("educationRequirements")
            )
            or None,
            "description_text": description_text,
            "description_html": description_html,
            "detail_metadata_json": json.dumps(
                metadata, ensure_ascii=False, default=str
            ),
            "source_parser": parser_name,
        }
    )
    return data


async def _load_detail_html(
    browser: Any,
    url: str,
    debug_path: Path,
    sleep: SleepCallable,
) -> str:
    page = await browser.get(url)
    last_html = ""
    for attempt in range(1, DETAIL_LOAD_ATTEMPTS + 1):
        await sleep(2 if attempt == 1 else 1)
        last_html = await page.get_content()
        if _page_contains_offer(last_html):
            return last_html
        if _contains_captcha(last_html):
            logger.warning(
                "Indeed CAPTCHA detected on detail %s (%s/%s)",
                url,
                attempt,
                DETAIL_LOAD_ATTEMPTS,
            )
            await _solve_captcha(page)
            await sleep(3)

    await _save_screenshot(page, debug_path)
    current_url = await _current_page_url(page, url)
    raise RuntimeError(
        "Indeed detail did not expose a title and description after "
        f"{DETAIL_LOAD_ATTEMPTS} attempts: {current_url}"
    )


async def collect_job_details(
    jobs_df: pd.DataFrame,
    settings: Settings | None = None,
    *,
    run_directory: str | Path | None = None,
    browser_factory: BrowserFactory | None = None,
    sleep: SleepCallable = asyncio.sleep,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    if jobs_df.empty:
        return pd.DataFrame(columns=list(DETAIL_DEFAULTS))

    jobs = (
        jobs_df.dropna(subset=["url"])
        .drop_duplicates(subset=["job_id", "url"])
        .to_dict(orient="records")
    )
    if active_settings.max_detail_pages > 0:
        jobs = jobs[: active_settings.max_detail_pages]

    run_dir = Path(run_directory or "runtime/airflow/indeed")
    debug_dir = run_dir / "indeed_debug"
    browser = await _start_browser(run_dir, browser_factory)
    rows: list[dict[str, Any]] = []
    try:
        for job in jobs:
            source_url = job.get("url")
            expected_job_key = str(job.get("job_id") or "").strip()
            url = canonicalize_indeed_job_url(
                source_url, job_key=expected_job_key
            )
            try:
                if not url or not expected_job_key:
                    raise ValueError("Indeed offer is missing its canonical URL or job key")
                source_job_key = extract_indeed_job_key(source_url)
                if source_job_key and source_job_key != expected_job_key:
                    raise ValueError(
                        "Indeed search URL job key does not match the selected offer: "
                        f"{source_job_key!r} != {expected_job_key!r}"
                    )
                html = await _load_detail_html(
                    browser,
                    url,
                    debug_dir / f"detail_{expected_job_key}.png",
                    sleep,
                )
                parsed = parse_job_detail_html(html, fallback_url=url)
                if parsed["job_id_detail"] != expected_job_key:
                    raise ValueError(
                        "Indeed detail job key does not match the selected offer: "
                        f"{parsed['job_id_detail']!r} != {expected_job_key!r}"
                    )
                if not parsed["title_detail"]:
                    raise ValueError("Indeed detail title is missing")
                if not parsed["description_text"]:
                    raise ValueError("Indeed detail description is missing")
                posted_at = parse_iso_date_to_timezone(
                    parsed.get("date_posted"), active_settings.timezone
                )
                if pd.isna(posted_at):
                    raise ValueError("Indeed detail publication date is missing or invalid")
                parsed["detail_status"] = "ok"
                parsed["detail_error"] = None
            except Exception as exc:
                logger.exception("Failed to collect Indeed detail page %s", url)
                parsed = dict(DETAIL_DEFAULTS)
                parsed.update(
                    {
                        "job_id_detail": expected_job_key or extract_indeed_job_key(url),
                        "canonical_url": canonicalize_indeed_job_url(
                            url, job_key=expected_job_key
                        ),
                        "detail_status": "error",
                        "detail_error": repr(exc),
                    }
                )
            rows.append(parsed)
    finally:
        await _stop_browser(browser)

    return pd.DataFrame(rows, columns=list(DETAIL_DEFAULTS))


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        if pd.isna(value):
            return {}
    except (TypeError, ValueError):
        pass
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def prepare_offers_dataframe(
    jobs_df: pd.DataFrame,
    details_df: pd.DataFrame,
    settings: Settings | None = None,
    *,
    publication_date_from: datetime | str,
    publication_date_to: datetime | str,
) -> pd.DataFrame:
    active_settings = settings or get_settings()
    if jobs_df.empty or details_df.empty:
        return pd.DataFrame()

    successful_details = details_df[
        details_df.get("detail_status", pd.Series(index=details_df.index, dtype=str)).eq(
            "ok"
        )
    ].copy()
    if successful_details.empty:
        return pd.DataFrame()

    successful_details = successful_details.drop_duplicates(
        subset=["job_id_detail", "canonical_url"]
    )
    merged = jobs_df.merge(
        successful_details,
        left_on="job_id",
        right_on="job_id_detail",
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame()

    merged["date_posted_dt"] = merged["date_posted"].apply(
        lambda value: parse_iso_date_to_timezone(value, active_settings.timezone)
    )
    merged["valid_through_dt"] = merged["valid_through"].apply(
        lambda value: parse_iso_date_to_timezone(value, active_settings.timezone)
    )
    window_start = pd.Timestamp(
        _as_local_datetime(publication_date_from, active_settings.timezone)
    )
    window_end = pd.Timestamp(
        _as_local_datetime(publication_date_to, active_settings.timezone)
    )
    merged = merged[
        merged["date_posted_dt"].notna()
        & (merged["date_posted_dt"] >= window_start)
        & (merged["date_posted_dt"] < window_end)
    ].copy()
    if merged.empty:
        return pd.DataFrame()

    for target, detail_field in (
        ("location", "location_detail"),
        ("address_country", "address_country_detail"),
        ("address_locality", "address_locality_detail"),
        ("address_region", "address_region_detail"),
        ("latitude", "latitude_detail"),
        ("longitude", "longitude_detail"),
    ):
        list_values = (
            merged[target]
            if target in merged
            else pd.Series(pd.NA, index=merged.index)
        )
        detail_values = merged[detail_field].replace("", pd.NA)
        merged[target] = detail_values.where(detail_values.notna(), list_values)

    merged["final_job_id"] = merged["job_id_detail"].fillna(merged["job_id"])
    merged["final_url"] = merged["canonical_url"].fillna(merged["url"])
    merged["final_title"] = merged["title_detail"].replace("", pd.NA).fillna(
        merged["title"]
    )
    merged["final_company"] = merged["company_detail"].replace(
        "", pd.NA
    ).fillna(merged["company"])
    merged["employment_type"] = merged["employment_type_detail"].fillna("")
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

    required = (
        merged["final_url"].notna()
        & merged["final_title"].notna()
        & merged["description_text"].fillna("").astype(str).str.strip().ne("")
    )
    return (
        merged[required]
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
        if description is None or pd.isna(description):
            continue
        normalized = re.sub(r"\s*[\u2022‣◦⁃∙]\s*", "\n\n", str(description))
        normalized = re.sub(r"[ \t]+", " ", normalized)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        paragraphs = split_paragraphs(
            normalized,
            min_chars=active_settings.paragraph_min_chars,
        )
        if (
            not paragraphs
            and len(re.sub(r"\s+", " ", normalized))
            >= active_settings.paragraph_min_chars
        ):
            paragraphs = [re.sub(r"\s+", " ", normalized).strip()]
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
    return pd.DataFrame(
        rows,
        columns=(
            "canonical_url",
            "external_job_id",
            "paragraph_idx",
            "paragraph",
            "paragraph_chars",
        ),
    )
