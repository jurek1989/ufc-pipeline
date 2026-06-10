import sys
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import bigquery
from unidecode import unidecode

from config import PROJECT_ID, DATASET, TABLE_RANKINGS

logging.basicConfig(level=logging.INFO)

FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_RANKINGS}"
client = bigquery.Client(project=PROJECT_ID)


def main():
    logging.info("Rozpoczęto scrapowanie rankingów UFC.")

    try:
        response = requests.get("https://www.ufc.com/rankings")
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Błąd podczas pobierania strony: {e}")
        sys.exit(1)

    soup = BeautifulSoup(response.content, "html.parser")
    ranking_date = datetime.now().strftime("%Y-%m-%d")
    rankings_data = []

    for section in soup.find_all("div", class_="view-grouping"):
        division = section.find("div", class_="view-grouping-header").get_text(strip=True)

        champion_tag = section.find("div", class_="rankings--athlete--champion")
        if champion_tag:
            rankings_data.append({
                "date": ranking_date,
                "weightclass": division,
                "fighter": champion_tag.find("h5").get_text(strip=True),
                "rank": 0,
            })

        for rank, fighter_td in enumerate(
            section.find_all("td", class_="views-field views-field-title"), start=1
        ):
            rankings_data.append({
                "date": ranking_date,
                "weightclass": division,
                "fighter": fighter_td.get_text(strip=True),
                "rank": rank,
            })

    if not rankings_data:
        logging.info("Brak danych do wstawienia.")
        return

    df = pd.DataFrame(rankings_data)
    df["fighter_normalized"] = df["fighter"].apply(unidecode)

    # UPSERT via temp table + MERGE (§10.4)
    temp_table = f"{FULL_TABLE_ID}_tmp_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, temp_table, job_config=job_config).result()

    merge_sql = f"""
    MERGE `{FULL_TABLE_ID}` T
    USING `{temp_table}` S
      ON T.date = S.date AND T.weightclass = S.weightclass AND T.fighter = S.fighter
    WHEN MATCHED THEN
      UPDATE SET T.rank = S.rank, T.fighter_normalized = S.fighter_normalized
    WHEN NOT MATCHED THEN
      INSERT (date, weightclass, fighter, rank, fighter_normalized)
      VALUES (S.date, S.weightclass, S.fighter, S.rank, S.fighter_normalized)
    """
    try:
        client.query(merge_sql).result()
        logging.info(f"Upsert {len(rankings_data)} rekordów rankingów.")
    finally:
        client.delete_table(temp_table, not_found_ok=True)


if __name__ == "__main__":
    main()
