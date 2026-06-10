import sys
import logging
import requests
import bs4
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import time

from config import PROJECT_ID, DATASET, TABLE_FIGHTERS_URLS

logging.basicConfig(level=logging.INFO)

FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_FIGHTERS_URLS}"
client = bigquery.Client(project=PROJECT_ID)


def get_existing_fighter_urls():
    query = f"SELECT fighter_url FROM `{FULL_TABLE_ID}`"
    return set(client.query(query).to_dataframe()["fighter_url"].tolist())


def fetch_letter_page(letter, retries=3, delay=5):
    url = f"http://ufcstats.com/statistics/fighters?char={letter}&page=all"
    for attempt in range(retries):
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if res.status_code == 429:
                logging.warning(f"429 przy literze {letter}, próba {attempt+1}/{retries} – czekam {delay}s")
                time.sleep(delay)
                continue
            res.raise_for_status()
            return letter, res.text
        except Exception as e:
            logging.warning(f"Błąd przy literze {letter}, próba {attempt+1}: {e}")
            time.sleep(delay)
    return letter, None


def extract_fighter_urls_from_html(html):
    soup = bs4.BeautifulSoup(html, "html.parser")
    return [a["href"] for a in soup.select("a.b-link")[1::3]]


def main():
    logging.info("Start scrapowania zawodników (A–Z)")
    existing_urls = get_existing_fighter_urls()
    all_urls = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_letter_page, letter) for letter in "abcdefghijklmnopqrstuvwxyz"]
        for future in as_completed(futures):
            letter, html = future.result()
            if html:
                all_urls.extend(extract_fighter_urls_from_html(html))

    new_urls = set(all_urls) - existing_urls
    logging.info(f"Łącznie: {len(set(all_urls))}, w bazie: {len(existing_urls)}, nowych: {len(new_urls)}")

    if not new_urls:
        logging.info("Brak nowych zawodników do dodania.")
        return

    now_utc = datetime.now(pytz.UTC)
    table = client.get_table(FULL_TABLE_ID)
    col_names = [field.name for field in table.schema]
    df = pd.DataFrame([(url, now_utc) for url in new_urls], columns=col_names)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, table, job_config=job_config).result()
    logging.info(f"Dodano {len(new_urls)} rekordów.")


if __name__ == "__main__":
    main()
