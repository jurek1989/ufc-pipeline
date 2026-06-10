import os
import sys
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import logging

from config import PROJECT_ID, DATASET, TABLE_EVENTS_URLS
from utils.playwright_fetch import fetch_page

logging.basicConfig(level=logging.INFO)
client = bigquery.Client(project=PROJECT_ID)


def main():
    limit = os.environ.get("LIMIT", "15").upper()
    if limit != "ALL":
        try:
            limit = int(limit)
        except ValueError:
            logging.error("Zmienna LIMIT musi być liczbą całkowitą lub 'ALL'")
            sys.exit(1)

    url = "http://ufcstats.com/statistics/events/completed?page=all"
    soup = BeautifulSoup(fetch_page(url), "html.parser")
    rows = [
        row for row in soup.select("tr.b-statistics__table-row")
        if row.select_one("a") and row.select_one("span")
    ]

    events = []
    for row in rows:
        a_tag = row.select_one("a")
        date_span = row.select_one("span")

        event_url = a_tag["href"].strip().rstrip("/")
        event_title = a_tag.text.strip()

        try:
            event_date = datetime.strptime(date_span.text.strip(), "%B %d, %Y").date()
        except Exception as e:
            logging.warning(f"Błąd parsowania daty: {e}")
            continue

        events.append((event_url, event_date, event_title))

    if limit != "ALL":
        events = events[:limit]

    df = pd.DataFrame(events, columns=["event_url", "Date", "Title"])
    df["event_url"] = df["event_url"].astype(str).str.strip().str.rstrip("/")
    df["Title"] = df["Title"].astype(str).str.strip()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime('%Y-%m-%d')

    query = f"SELECT DISTINCT event_url FROM `{PROJECT_ID}.{DATASET}.{TABLE_EVENTS_URLS}`"
    existing_df = client.query(query).to_dataframe()
    existing_urls = set(u.strip().rstrip("/") for u in existing_df["event_url"])

    new_df = df[~df["event_url"].isin(existing_urls)]

    if new_df.empty:
        logging.info("Brak nowych gal do dodania.")
        return

    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE_EVENTS_URLS}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(new_df, table_ref, job_config=job_config).result()
    logging.info(f"Dodano {len(new_df)} nowych gal do tabeli.")


if __name__ == "__main__":
    main()
