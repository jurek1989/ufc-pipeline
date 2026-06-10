"""
backfill_bfo_odds.py — jednorazowy backfill historycznych kursów z BestFightOdds.

Strategia:
  Faza 0 — Seeds: pobierz top 200 zawodników z BQ (wg liczby walk),
            wyszukaj ich profil na BFO (/search?query=name).
  Faza 1 — Discovery: odwiedź profile zawodników na BFO, zbierz linki
            do eventów UFC w ich historii walk.
  Faza 2 — Scraping: dla każdego znalezionego eventu sparsuj tabelę
            kursów (identyczny parser jak scrape_bfo_odds.py) i zapisz
            do UFC_data.odds_snapshots z is_historical=True.

Resumable: eventy już obecne w odds_snapshots są pomijane.
Polite:    2–3 s delay między requestami, batch write co 50 eventów,
           retry 3× z 60 s czekania na HTTP 429/403.

Uruchomienie:
  LD_LIBRARY_PATH=/tmp/chromium_libs/usr/lib/x86_64-linux-gnu \\
  PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu22.04-x64 \\
  python3 backfill_bfo_odds.py
"""

import os
import re
import sys
import time
import random
import logging
import urllib.parse
from datetime import datetime, timezone

import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import bigquery
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from config import PROJECT_ID, DATASET, TABLE_ODDS_SNAPSHOTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

os.environ.setdefault("PLAYWRIGHT_HOST_PLATFORM_OVERRIDE", "ubuntu22.04-x64")

BFO_BASE = "https://www.bestfightodds.com"
TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE_ODDS_SNAPSHOTS}"
SKIP_BOOKMAKERS = {"Props", "Future Events"}
BATCH_SIZE = 50
DELAY_MIN, DELAY_MAX = 2.0, 3.5
RETRY_COUNT = 3
RETRY_WAIT = 60
SEED_LIMIT = 200

client = bigquery.Client(project=PROJECT_ID)


# ── Helpers ───────────────────────────────────────────────────────────────────

def sleep_polite():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def fetch_with_retry(pg, url: str, retries: int = RETRY_COUNT) -> str | None:
    for attempt in range(retries):
        try:
            resp = pg.goto(url, timeout=30000)
            if resp and resp.status in (429, 403):
                logging.warning(f"HTTP {resp.status} on {url}, waiting {RETRY_WAIT}s...")
                time.sleep(RETRY_WAIT)
                continue
            return pg.content()
        except PWTimeout:
            logging.warning(f"Timeout on {url} (attempt {attempt+1}/{retries})")
            time.sleep(10)
    logging.error(f"Failed to fetch {url} after {retries} attempts")
    return None


def clean_odds(text: str) -> int | None:
    v = re.sub(r"[▲▼\s]", "", text)
    try:
        return int(v)
    except ValueError:
        return None


# ── Phase 0: Seed fighter names from BigQuery ────────────────────────────────

def get_seed_names() -> list[str]:
    """Top SEED_LIMIT fighters by UFC fight count."""
    q = f"""
    SELECT name FROM (
      SELECT f_1 AS name, COUNT(*) AS cnt
        FROM `{PROJECT_ID}.{DATASET}.UFC_fights_data` GROUP BY 1
      UNION ALL
      SELECT f_2, COUNT(*)
        FROM `{PROJECT_ID}.{DATASET}.UFC_fights_data` GROUP BY 1
    )
    GROUP BY 1 ORDER BY SUM(cnt) DESC LIMIT {SEED_LIMIT}
    """
    return [row.name for row in client.query(q).result()]


def search_bfo_profile(name: str, pg) -> str | None:
    """Return BFO profile path (/fighters/Name-NNN) for a fighter, or None."""
    encoded = urllib.parse.quote_plus(name)
    html = fetch_with_retry(pg, f"{BFO_BASE}/search?query={encoded}")
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=re.compile(r"^/fighters/")):
        return a["href"]
    return None


# ── Phase 1: Discover event URLs from fighter profiles ───────────────────────

def collect_event_urls_from_profile(html: str) -> dict[str, str]:
    """Return {bfo_url: event_name} from a fighter's profile page."""
    soup = BeautifulSoup(html, "html.parser")
    tst = soup.find("table", class_="team-stats-table")
    if not tst:
        return {}
    events = {}
    for a in tst.find_all("a", href=lambda h: h and "/events/" in h):
        href = a["href"]
        if "future-events" in href:
            continue
        full_url = BFO_BASE + href
        name = a.get_text(strip=True)
        name = re.sub(r"\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+.*", "", name).strip()
        events[full_url] = name
    return events


def discover_all_events(pg) -> dict[str, str]:
    """
    Phase 0: query BQ for top SEED_LIMIT fighters.
    Phase 1: search BFO for each fighter → visit profile → collect event URLs.
    """
    logging.info(f"=== Phase 0: Loading top {SEED_LIMIT} fighters from BigQuery ===")
    seed_names = get_seed_names()
    logging.info(f"  {len(seed_names)} seed fighters loaded")

    all_events: dict[str, str] = {}
    valid_seeds = found_profiles = 0

    for name in seed_names:
        profile_path = search_bfo_profile(name, pg)
        sleep_polite()

        if not profile_path:
            logging.debug(f"  {name}: not found on BFO")
            continue

        found_profiles += 1
        url = BFO_BASE + profile_path
        html = fetch_with_retry(pg, url)
        sleep_polite()

        if not html:
            continue

        events = collect_event_urls_from_profile(html)
        if not events:
            logging.debug(f"  {name}: profile found but no events table")
        else:
            new = {k: v for k, v in events.items() if k not in all_events}
            all_events.update(events)
            logging.info(
                f"  {name:<32} {len(events):3d} events, {len(new):3d} new"
                f"  |  total: {len(all_events)}"
            )
            valid_seeds += 1

    logging.info(
        f"Discovery complete: {valid_seeds} seeds with events "
        f"({found_profiles} profiles found) → {len(all_events)} unique events"
    )
    return all_events


# ── Phase 2: Parse event page odds ───────────────────────────────────────────

_MONTH = (
    r"(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)"
)

def parse_event_date(html: str) -> str:
    """Extract date from event page.

    Returns "Month D YYYY" when the year is visible on the page,
    or "Month D" (no year) as a fallback. The year variant lets
    callers set scraped_at correctly without guessing.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ")
    # Full date: "April 13, 2024" or "April 13 2024"
    m = re.search(
        _MONTH + r"\s+\d{1,2}(?:st|nd|rd|th)?,?\s*(20\d{2})",
        text,
    )
    if m:
        month_day_m = re.search(_MONTH + r"\s+\d{1,2}", m.group(0))
        if month_day_m:
            return f"{month_day_m.group(0)} {m.group(2)}"
    # Fallback: month+day only (year unknown)
    m2 = re.search(_MONTH + r"\s+\d{1,2}", text)
    return m2.group(0) if m2 else ""


def parse_event_odds(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_="odds-table")
    if len(tables) < 2:
        return []
    main = tables[1]
    rows = main.find_all("tr")
    if not rows:
        return []

    header_cells = rows[0].find_all(["th", "td"])
    bookmakers = [
        re.sub(r"\$[\d]+\s*\w+", "", c.get_text(strip=True)).strip()
        for c in header_cells[1:]
    ]

    non_pr = [r for r in rows[1:] if "pr" not in (r.get("class") or [])]
    results = []
    i = 0
    while i + 1 < len(non_pr):
        row_f1, row_f2 = non_pr[i], non_pr[i + 1]
        cells_f1 = row_f1.find_all(["th", "td"])
        cells_f2 = row_f2.find_all(["th", "td"])
        f1 = re.sub(r"^\d+", "", cells_f1[0].get_text(strip=True)).strip()
        f2 = cells_f2[0].get_text(strip=True).strip()
        if not f1 or not f2:
            i += 2
            continue
        for k, bm in enumerate(bookmakers):
            if bm in SKIP_BOOKMAKERS or not bm:
                continue
            idx = k + 2
            o1 = clean_odds(cells_f1[idx].get_text()) if idx < len(cells_f1) else None
            o2 = clean_odds(cells_f2[idx].get_text()) if idx < len(cells_f2) else None
            if o1 is None and o2 is None:
                continue
            results.append({
                "fighter_1": f1,
                "fighter_2": f2,
                "bookmaker": bm,
                "odds_f1_american": o1,
                "odds_f2_american": o2,
            })
        i += 2
    return results


# ── BQ helpers ────────────────────────────────────────────────────────────────

def get_already_scraped_urls() -> set[str]:
    df = client.query(
        f"SELECT DISTINCT event_url FROM `{TABLE_REF}` WHERE is_historical = TRUE"
    ).to_dataframe()
    return set(df["event_url"].dropna())


def write_batch(rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True)
    df["odds_f1_american"] = pd.array(df["odds_f1_american"], dtype=pd.Int64Dtype())
    df["odds_f2_american"] = pd.array(df["odds_f2_american"], dtype=pd.Int64Dtype())
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    for attempt in range(3):
        try:
            client.load_table_from_dataframe(
                df, TABLE_REF, job_config=job_config
            ).result(timeout=120)
            logging.info(f"  → Saved {len(df)} rows to BQ")
            return
        except Exception as e:
            logging.warning(f"  BQ write attempt {attempt+1}/3 failed: {e}")
            time.sleep(15)
    logging.error(f"  BQ write failed after 3 attempts — {len(df)} rows LOST")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    lib_path = "/tmp/chromium_libs/usr/lib/x86_64-linux-gnu"
    current_ld = os.environ.get("LD_LIBRARY_PATH", "")
    if lib_path not in current_ld:
        os.environ["LD_LIBRARY_PATH"] = f"{lib_path}:{current_ld}".rstrip(":")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        # ── Phase 1: Discover events ──────────────────────────────────────
        logging.info("=== Phase 1: Discovering UFC events from fighter profiles ===")
        all_events = discover_all_events(page)

        # Filter to UFC events only
        ufc_events = {
            url: name for url, name in all_events.items()
            if re.search(r"\bufc\b", name, re.I) or re.search(r"\bufc\b", url, re.I)
        }
        logging.info(f"UFC events discovered: {len(ufc_events)} out of {len(all_events)} total")

        # ── Resumability: skip already scraped ───────────────────────────
        already_done = get_already_scraped_urls()
        to_scrape = {url: name for url, name in ufc_events.items() if url not in already_done}

        logging.info(
            f"\n{'='*60}\n"
            f"  Events discovered : {len(ufc_events)}\n"
            f"  Already in BQ     : {len(already_done)}\n"
            f"  To scrape now     : {len(to_scrape)}\n"
            f"{'='*60}"
        )

        if not to_scrape:
            logging.info("Nothing to scrape. All discovered events already in BQ.")
            browser.close()
            return

        # ── Phase 2: Scrape event pages ───────────────────────────────────
        logging.info("=== Phase 2: Scraping event pages ===")
        batch: list[dict] = []
        total_rows = 0
        events_done = 0

        for event_url, event_name in sorted(to_scrape.items()):
            html = fetch_with_retry(page, event_url)
            if not html:
                continue

            event_date = parse_event_date(html)
            odds_rows = parse_event_odds(html)

            if not odds_rows:
                logging.info(f"  [skip-empty] {event_name}")
                events_done += 1
                sleep_polite()
                continue

            # scraped_at: use full date when year is available,
            # else 1970-01-01 as a sentinel (means "year unknown").
            scraped_at = pd.Timestamp("1970-01-01", tz="UTC")
            if event_date:
                parts = event_date.split()
                if len(parts) == 3 and parts[-1].isdigit() and len(parts[-1]) == 4:
                    try:
                        scraped_at = pd.Timestamp(event_date, tz="UTC")
                    except Exception:
                        pass

            for row in odds_rows:
                batch.append({
                    "event_name":       event_name,
                    "event_url":        event_url,
                    "event_date":       event_date,
                    "fighter_1":        row["fighter_1"],
                    "fighter_2":        row["fighter_2"],
                    "bookmaker":        row["bookmaker"],
                    "odds_f1_american": row["odds_f1_american"],
                    "odds_f2_american": row["odds_f2_american"],
                    "scraped_at":       scraped_at,
                    "is_historical":    True,
                })

            events_done += 1
            total_rows += len(odds_rows)
            logging.info(
                f"  [{events_done}/{len(to_scrape)}] {event_name:<50} "
                f"{len(odds_rows):3d} rows  |  date={event_date}"
            )

            # Write after every event — avoids losing many events on BQ timeout
            write_batch(batch)
            batch = []

            sleep_polite()

        # Final flush
        if batch:
            write_batch(batch)

        browser.close()

    logging.info(
        f"\n{'='*60}\n"
        f"  Events scraped : {events_done}\n"
        f"  Total rows     : {total_rows}\n"
        f"{'='*60}"
    )


if __name__ == "__main__":
    main()
