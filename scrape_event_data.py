import sys
import logging
from google.cloud import bigquery
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

from config import PROJECT_ID, DATASET, TABLE_EVENTS_URLS, TABLE_EVENTS_DATA
from utils.playwright_fetch import fetch_page

logging.basicConfig(level=logging.INFO)
client = bigquery.Client(project=PROJECT_ID)


def main():
    query = f"""
        SELECT DISTINCT event_url
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_EVENTS_URLS}`
        WHERE event_url NOT IN (
            SELECT DISTINCT event_url FROM `{PROJECT_ID}.{DATASET}.{TABLE_EVENTS_DATA}`
        )
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        logging.info("Brak nowych eventów do przetworzenia.")
        return

    rows = []
    for url in df["event_url"]:
        try:
            soup = BeautifulSoup(fetch_page(url), "html.parser")

            event_name = soup.find("h2", class_="b-content__title").text.strip()
            details_box = soup.find("div", class_="b-list__info-box")

            event_date = None
            event_city = event_state = event_country = None

            if details_box:
                for li in details_box.find_all("li"):
                    if "Date:" in li.text:
                        try:
                            date_text = li.text.split("Date:")[1].strip()
                            event_date = datetime.strptime(date_text, "%B %d, %Y").date()
                        except Exception:
                            pass
                    if "Location:" in li.text:
                        try:
                            location_text = li.text.split("Location:")[1].strip()
                            parts = [p.strip() for p in location_text.split(",")]
                            event_city = parts[0] if len(parts) > 0 else None
                            event_state = parts[1] if len(parts) > 1 else None
                            event_country = parts[2] if len(parts) > 2 else None
                        except Exception:
                            pass

            rows.append({
                "event_url": url,
                "event_name": event_name,
                "event_date": event_date.isoformat() if event_date else None,
                "event_city": event_city,
                "event_state": event_state,
                "event_country": event_country,
            })
        except Exception as e:
            logging.warning(f"Błąd przy {url}: {e}")
            continue

    if not rows:
        logging.warning("Brak poprawnych danych do wstawienia.")
        return

    df_out = pd.DataFrame(rows)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE_EVENTS_DATA}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df_out, table_ref, job_config=job_config).result()
    logging.info(f"Dodano {len(rows)} nowych eventów.")


if __name__ == "__main__":
    main()
