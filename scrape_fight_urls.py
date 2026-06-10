import sys
import logging
from google.cloud import bigquery
import pandas as pd
from bs4 import BeautifulSoup

from config import PROJECT_ID, DATASET, TABLE_EVENTS_URLS, TABLE_FIGHTS_URLS
from utils.playwright_fetch import fetch_page

logging.basicConfig(level=logging.INFO)
client = bigquery.Client(project=PROJECT_ID)


def main():
    events_df = client.query(f"""
        SELECT DISTINCT event_url, Title, Date
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_EVENTS_URLS}`
    """).to_dataframe()
    events_df["event_url"] = events_df["event_url"].astype(str).str.strip().str.rstrip("/")
    events_df["Title"] = events_df["Title"].astype(str).str.strip()
    events_df["Date"] = pd.to_datetime(events_df["Date"])

    existing_df = client.query(f"""
        SELECT DISTINCT event_url
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_FIGHTS_URLS}`
    """).to_dataframe()
    existing_df["event_url"] = existing_df["event_url"].astype(str).str.strip().str.rstrip("/")
    existing_event_urls = set(existing_df["event_url"])

    missing_df = events_df[~events_df["event_url"].isin(existing_event_urls)]
    if missing_df.empty:
        logging.info("Brak brakujących eventów do uzupełnienia.")
        return

    results = []
    for _, row in missing_df.iterrows():
        event_url = row["event_url"]
        try:
            soup = BeautifulSoup(fetch_page(event_url), "html.parser")
            for tr in soup.select("tr.b-fight-details__table-row"):
                cols = tr.find_all("td")
                if len(cols) < 2:
                    continue
                fight_tag = cols[0].find("a", href=True)
                if fight_tag:
                    results.append({
                        "Event_URL": event_url,
                        "Title": row["Title"],
                        "Date": row["Date"].strftime('%Y-%m-%d'),
                        "Fight_URL": fight_tag["href"].strip(),
                    })
        except Exception as e:
            logging.warning(f"Błąd scrapowania dla {event_url}: {e}")
            continue

    if not results:
        logging.info("Nie znaleziono żadnych walk do dodania.")
        return

    df = pd.DataFrame(results)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE_FIGHTS_URLS}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    logging.info(f"Dodano {len(df)} walk do tabeli.")


if __name__ == "__main__":
    main()
