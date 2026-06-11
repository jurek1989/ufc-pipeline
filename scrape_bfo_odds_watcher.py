"""
scrape_bfo_odds_watcher.py — Cloud Run Job: daily BFO upcoming odds snapshot + alerts.

Cadence: daily — third cadence per ARCHITECTURE.md §5 / §15.
What it does:
  1. Scrape upcoming UFC events from BestFightOdds.com.
  2. Write new rows to odds_snapshots (WRITE_APPEND).
  3. Compare new snapshot with previous per fight; log movements > 5 pp,
     email movements > 10 pp.
  4. send_value_alert() — email when model edge exceeds threshold.
"""

import logging
import smtplib
import sys
from datetime import datetime, timezone
from email.mime.text import MIMEText

import pandas as pd
from google.cloud import bigquery

from config import DATASET, GMAIL_APP_PASSWORD, GMAIL_USER, PROJECT_ID, TABLE_ODDS_SNAPSHOTS
from scrape_bfo_odds import BFO_BASE, parse_event_odds, parse_events
from utils.playwright_fetch import open_browser_page

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE_ODDS_SNAPSHOTS}"
VIEW_CONSENSUS = f"{PROJECT_ID}.{DATASET}.odds_consensus"

client = bigquery.Client(project=PROJECT_ID)


def _nav(url: str, page) -> str:
    """Navigate to URL and return rendered HTML."""
    page.goto(url, timeout=30_000)
    return page.content()


# ── Email ─────────────────────────────────────────────────────────────────────

def send_email_alert(subject: str, body: str) -> None:
    """Send email via Gmail SMTP. Silently skips if credentials not configured."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        logging.warning("Email alert skipped — GMAIL_USER or GMAIL_APP_PASSWORD not set")
        return
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = GMAIL_USER
        msg["To"] = GMAIL_USER
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        logging.info("Email sent: %s", subject)
    except Exception as exc:
        logging.warning("Email send failed: %s", exc)


# ── Value alert ───────────────────────────────────────────────────────────────

def send_value_alert(
    fighter_1: str,
    fighter_2: str,
    model_prob: float,
    market_prob: float,
    edge: float,
    recommended_side: str,
) -> None:
    """Send value bet alert via email."""
    subject = f"UFC Value Alert: {fighter_1} vs {fighter_2}"
    body = (
        f"Walka: {fighter_1} vs {fighter_2}\n"
        f"Model prob: {model_prob:.1%}\n"
        f"Market prob: {market_prob:.1%}\n"
        f"Edge: {edge:.1%}\n"
        f"Recommendation: BET {recommended_side}\n"
        f"\nŹródło: odds-watcher pipeline"
    )
    logging.info(
        "VALUE ALERT: %s vs %s — edge %.1f%% — BET %s",
        fighter_1, fighter_2, edge * 100, recommended_side,
    )
    send_email_alert(subject, body)


# ── Line movement detection ───────────────────────────────────────────────────

def _detect_line_movements(event_urls: list[str]) -> None:
    """
    Compare latest snapshot with previous per fight.
    Logs movements > 5 pp; emails movements > 10 pp.
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

        if abs(delta) <= 0.05:
            continue

        movements_found += 1
        direction = "+" if delta > 0 else ""
        logging.info(
            "Ruch linii: %s: %.0f%%→%.0f%%, ruch %s%.0fpp",
            f1, prev_p1 * 100, curr_p1 * 100, direction, delta * 100,
        )

        if abs(delta) > 0.10:
            old_str = f"{prev_p1:.0%}"
            new_str = f"{curr_p1:.0%}"
            send_email_alert(
                subject=f"UFC Line Movement: {f1} {old_str}→{new_str}",
                body=(
                    f"Ruch linii: {f1} vs {f2}\n"
                    f"Poprzednie: {old_str}\n"
                    f"Aktualne:   {new_str}\n"
                    f"Zmiana:     {direction}{delta * 100:.0f}pp\n"
                    f"\nŹródło: odds-watcher pipeline"
                ),
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

    scraped_at = datetime.now(timezone.utc)
    all_rows: list[dict] = []

    with open_browser_page() as page:
        logging.info("Fetching BFO event list...")
        home_html = _nav(BFO_BASE, page)
        events = parse_events(home_html)
        logging.info("Found %d upcoming events: %s", len(events), [e["name"] for e in events])

        for event in events:
            logging.info("Scraping: %s (%s)", event["name"], event["url"])
            event_html = _nav(event["url"], page)
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
