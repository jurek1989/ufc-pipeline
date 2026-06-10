"""
scrape_bfo_odds_watcher.py — Cloud Run Job: daily BFO upcoming odds snapshot + line movement alerts.

Cadence: daily — third cadence per ARCHITECTURE.md §5 / §15.
What it does:
  1. Scrape upcoming UFC events from BestFightOdds.com (same parser as scrape_bfo_odds.py).
  2. Write new rows to odds_snapshots (is_historical=False, WRITE_APPEND).
  3. Compare new snapshot with previous per fight; log movements > 5 pp.
  4. send_value_alert() skeleton — logs ALERT line, email TODO when model is ready (Phase 4).

Usage:
  python scrape_bfo_odds_watcher.py
"""

import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery
from playwright.sync_api import sync_playwright

from config import DATASET, PROJECT_ID, TABLE_ODDS_SNAPSHOTS
from scrape_bfo_odds import BFO_BASE, fetch_page, parse_event_odds, parse_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

os.environ.setdefault("PLAYWRIGHT_HOST_PLATFORM_OVERRIDE", "ubuntu22.04-x64")

TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE_ODDS_SNAPSHOTS}"
VIEW_CONSENSUS = f"{PROJECT_ID}.{DATASET}.odds_consensus"

client = bigquery.Client(project=PROJECT_ID)


# ── Alert skeleton ────────────────────────────────────────────────────────────

def send_value_alert(fight: str, model_prob: float, market_prob: float, edge: float) -> None:
    """Log a value bet alert. Email integration TODO: Phase 4 (after model is ready)."""
    logging.info("ALERT: edge %.1f%% on %s", edge, fight)


# ── Line movement detection ───────────────────────────────────────────────────

def _detect_line_movements(event_urls: list[str]) -> None:
    """
    After writing a new snapshot, compare it with the previous one per fight.
    Logs any movement > 5 pp as: "Ruch linii: <fighter>: <old>%→<new>%, ruch <delta>pp"
    """
    if not event_urls:
        return

    url_list = ", ".join(f"'{u}'" for u in event_urls)
    sql = f"""
    WITH ranked AS (
      SELECT
        event_name,
        fighter_1,
        fighter_2,
        scraped_at,
        consensus_prob_f1,
        consensus_prob_f2,
        ROW_NUMBER() OVER (
          PARTITION BY event_url, fighter_1, fighter_2
          ORDER BY scraped_at DESC
        ) AS rn
      FROM `{VIEW_CONSENSUS}`
      WHERE event_url IN ({url_list})
    )
    SELECT * FROM ranked WHERE rn <= 2
    ORDER BY fighter_1, fighter_2, rn
    """
    try:
        rows = client.query(sql).to_dataframe()
    except Exception as exc:
        logging.warning("Line movement query failed: %s", exc)
        return

    if rows.empty:
        logging.info("No previous snapshots found — skipping line movement check.")
        return

    movements_found = 0
    for (f1, f2), group in rows.groupby(["fighter_1", "fighter_2"]):
        curr = group[group["rn"] == 1]
        prev = group[group["rn"] == 2]
        if curr.empty or prev.empty:
            continue

        curr_p1 = float(curr["consensus_prob_f1"].iloc[0])
        prev_p1 = float(prev["consensus_prob_f1"].iloc[0])
        delta = curr_p1 - prev_p1

        if abs(delta) > 0.05:
            movements_found += 1
            direction = "+" if delta > 0 else ""
            logging.info(
                "Ruch linii: %s: %.0f%%→%.0f%%, ruch %s%.0fpp",
                f1,
                prev_p1 * 100,
                curr_p1 * 100,
                direction,
                delta * 100,
            )

    if movements_found == 0:
        logging.info("Brak ruchów linii > 5pp.")


# ── BQ write ──────────────────────────────────────────────────────────────────

def _write_rows(rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True)
    df["odds_f1_american"] = pd.array(df["odds_f1_american"], dtype=pd.Int64Dtype())
    df["odds_f2_american"] = pd.array(df["odds_f2_american"], dtype=pd.Int64Dtype())
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, TABLE_REF, job_config=job_config).result()
    logging.info("Saved %d rows to %s", len(df), TABLE_REF)


# ── Entry ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.info("=" * 60)
    logging.info("scrape_bfo_odds_watcher  start")
    logging.info("=" * 60)

    lib_path = "/tmp/chromium_libs/usr/lib/x86_64-linux-gnu"
    current_ld = os.environ.get("LD_LIBRARY_PATH", "")
    if lib_path not in current_ld:
        os.environ["LD_LIBRARY_PATH"] = f"{lib_path}:{current_ld}".rstrip(":")

    scraped_at = datetime.now(timezone.utc)
    all_rows: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        logging.info("Fetching BFO event list...")
        home_html = fetch_page(BFO_BASE, page)
        events = parse_events(home_html)
        logging.info("Found %d upcoming events: %s", len(events), [e["name"] for e in events])

        for event in events:
            logging.info("Scraping: %s (%s)", event["name"], event["url"])
            event_html = fetch_page(event["url"], page)
            odds_rows = parse_event_odds(event_html)
            logging.info("  → %d bookmaker rows", len(odds_rows))
            for row in odds_rows:
                all_rows.append({
                    "event_name":       event["name"],
                    "event_url":        event["url"],
                    "event_date":       event["date"],
                    "fighter_1":        row["fighter_1"],
                    "fighter_2":        row["fighter_2"],
                    "bookmaker":        row["bookmaker"],
                    "odds_f1_american": row["odds_f1_american"],
                    "odds_f2_american": row["odds_f2_american"],
                    "scraped_at":       scraped_at,
                    "is_historical":    False,
                })

        browser.close()

    if not all_rows:
        logging.info("No upcoming odds scraped — nothing to write.")
        sys.exit(0)

    _write_rows(all_rows)

    event_urls = list({r["event_url"] for r in all_rows})
    _detect_line_movements(event_urls)

    logging.info("=" * 60)
    logging.info("scrape_bfo_odds_watcher complete — %d rows written.", len(all_rows))
    sys.exit(0)


if __name__ == "__main__":
    main()
