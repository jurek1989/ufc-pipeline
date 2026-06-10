import logging
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import bs4
import pytz

from config import PROJECT_ID, DATASET, TABLE_FIGHTERS_URLS
from utils.playwright_fetch import fetch_pages

logging.basicConfig(level=logging.INFO)

FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_FIGHTERS_URLS}"
client = bigquery.Client(project=PROJECT_ID)


def get_existing_fighter_urls():
    query = f"SELECT fighter_url FROM `{FULL_TABLE_ID}`"
    return set(client.query(query).to_dataframe()["fighter_url"].tolist())


def extract_fighter_urls_from_html(html):
    soup = bs4.BeautifulSoup(html, "html.parser")
    return [a["href"] for a in soup.select("a.b-link")[1::3]]


def main():
    logging.info("Start scrapowania zawodników (A–Z)")
    existing_urls = get_existing_fighter_urls()

    letter_urls = [
        f"http://ufcstats.com/statistics/fighters?char={letter}&page=all"
        for letter in "abcdefghijklmnopqrstuvwxyz"
    ]
    html_map = fetch_pages(letter_urls, max_workers=5)

    all_urls = []
    for html in html_map.values():
        all_urls.extend(extract_fighter_urls_from_html(html))

    new_urls = set(all_urls) - existing_urls
    logging.info(
        "Łącznie: %d, w bazie: %d, nowych: %d",
        len(set(all_urls)), len(existing_urls), len(new_urls),
    )

    if not new_urls:
        logging.info("Brak nowych zawodników do dodania.")
        return

    now_utc = datetime.now(pytz.UTC)
    table = client.get_table(FULL_TABLE_ID)
    col_names = [field.name for field in table.schema]
    df = pd.DataFrame([(url, now_utc) for url in new_urls], columns=col_names)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, table, job_config=job_config).result()
    logging.info("Dodano %d rekordów.", len(new_urls))


if __name__ == "__main__":
    main()
