import re
import sys
import logging
from datetime import datetime, timezone

import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import bigquery

from config import PROJECT_ID, DATASET, TABLE_ODDS_SNAPSHOTS
from utils.playwright_fetch import open_browser_page

logging.basicConfig(level=logging.INFO)

BFO_BASE = "https://www.bestfightodds.com"
TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE_ODDS_SNAPSHOTS}"
SKIP_BOOKMAKERS = {"Props", "Future Events"}

client = bigquery.Client(project=PROJECT_ID)


def _nav(url: str, page) -> str:
    page.goto(url, timeout=30000)
    return page.content()


def parse_events(html: str) -> list[dict]:
    """Return list of {name, url, date} for upcoming UFC events."""
    soup = BeautifulSoup(html, "html.parser")
    events = []
    for block in soup.find_all("div", class_=re.compile(r"table-header")):
        link = block.find("a", href=re.compile(r"/events/"))
        if not link:
            continue
        name = link.get_text(strip=True)
        if name == "Future Events":
            continue
        href = BFO_BASE + link["href"]
        date_text = block.get_text(" ", strip=True)
        m = re.search(
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+\d+",
            date_text,
        )
        events.append({"name": name, "url": href, "date": m.group(0) if m else ""})
    return events


def clean_odds(text: str) -> int | None:
    """Strip trend arrows, return American odds integer or None."""
    v = re.sub(r"[▲▼\s]", "", text)
    try:
        return int(v)
    except ValueError:
        return None


def parse_event_odds(html: str) -> list[dict]:
    """Parse all h2h fight rows from an event page. Returns list of row dicts."""
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_="odds-table")
    if len(tables) < 2:
        return []

    main = tables[1]
    rows = main.find_all("tr")
    if not rows:
        return []

    # Header row: cell[0]=empty, cells[1..] = bookmaker names
    header_cells = rows[0].find_all(["th", "td"])
    bookmakers = [
        re.sub(r"\$[\d]+\s*\w+", "", c.get_text(strip=True)).strip()
        for c in header_cells[1:]
    ]

    # Non-prop rows only
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
            idx = k + 2  # cells[0]=fighter, cells[1]=gap, cells[2..]=odds
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


def main():
    scraped_at = datetime.now(timezone.utc)
    all_rows = []

    with open_browser_page() as page:
        logging.info("Fetching BFO event list...")
        home_html = _nav(BFO_BASE, page)
        events = parse_events(home_html)
        logging.info(f"Found {len(events)} upcoming events: {[e['name'] for e in events]}")

        for event in events:
            logging.info(f"Scraping: {event['name']} ({event['url']})")
            event_html = _nav(event["url"], page)
            rows = parse_event_odds(event_html)
            logging.info(f"  → {len(rows)} bookmaker rows")
            for row in rows:
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
                })

    if not all_rows:
        logging.info("No odds data scraped.")
        return

    df = pd.DataFrame(all_rows)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True)
    df["odds_f1_american"] = pd.array(df["odds_f1_american"], dtype=pd.Int64Dtype())
    df["odds_f2_american"] = pd.array(df["odds_f2_american"], dtype=pd.Int64Dtype())

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, TABLE_REF, job_config=job_config).result()
    logging.info(f"Saved {len(df)} rows to {TABLE_REF}")


if __name__ == "__main__":
    main()
