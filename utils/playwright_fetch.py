"""Shared Playwright browser utilities for all UFC pipeline scrapers."""

import contextlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ.setdefault("PLAYWRIGHT_HOST_PLATFORM_OVERRIDE", "ubuntu22.04-x64")

log = logging.getLogger(__name__)

_DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _ensure_lib_path() -> None:
    """Add bundled Chromium libs to LD_LIBRARY_PATH on Cloud Run if needed."""
    lib_path = os.environ.get(
        "CHROMIUM_LIB_PATH",
        "/tmp/chromium_libs/usr/lib/x86_64-linux-gnu",
    )
    if lib_path and lib_path not in os.environ.get("LD_LIBRARY_PATH", ""):
        current = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = f"{lib_path}:{current}".rstrip(":")


def fetch_page(
    url: str,
    wait_until: str = "networkidle",
    timeout: int = 30_000,
) -> str:
    """Fetch a single URL using a fresh Playwright Chromium instance. Returns HTML."""
    from playwright.sync_api import sync_playwright

    _ensure_lib_path()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=_DEFAULT_UA)
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout)
            return page.content()
        finally:
            browser.close()


def fetch_pages(
    urls: list[str],
    max_workers: int = 5,
    wait_until: str = "networkidle",
    timeout: int = 30_000,
) -> dict[str, str]:
    """Fetch multiple URLs concurrently. Returns {url: html} for successful fetches."""
    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(fetch_page, url, wait_until, timeout): url
            for url in urls
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                results[url] = future.result()
            except Exception as exc:
                log.warning("fetch_pages: failed %s: %s", url, exc)
    return results


@contextlib.contextmanager
def open_browser_page(
    user_agent: str = _DEFAULT_UA,
    headless: bool = True,
):
    """Context manager that yields a Playwright page for persistent multi-page scraping."""
    from playwright.sync_api import sync_playwright

    _ensure_lib_path()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page(user_agent=user_agent)
        try:
            yield page
        finally:
            browser.close()
