import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

from config import PROJECT_ID, DATASET, TABLE_BETTING_ODDS_API, ODDS_API_KEY

logging.basicConfig(level=logging.INFO)

BQ_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE_BETTING_ODDS_API}"
client = bigquery.Client(project=PROJECT_ID)

BOOKMAKER_REGIONS = {
    "DraftKings": "us", "FanDuel": "us", "BetMGM": "us", "BetUS": "us",
    "BetOnline.ag": "us", "BetRivers": "us", "BetAnySports": "us",
    "888sport": "uk", "Betfair": "uk", "Betway": "uk", "Paddy Power": "uk",
    "Virgin Bet": "uk", "Grosvenor": "uk", "LiveScore Bet": "uk", "Matchbook": "uk",
    "Unibet": "eu", "Unibet (FR)": "eu", "Unibet (NL)": "eu", "Betclic (FR)": "eu",
    "LeoVegas": "eu", "Marathon Bet": "eu", "Nordic Bet": "eu", "Coolbet": "eu",
    "Betsson": "eu",
}


def main():
    if not ODDS_API_KEY:
        logging.error("ODDS_API_KEY nie jest ustawiony.")
        sys.exit(1)

    params = {
        "regions": "us,uk,eu",
        "markets": "h2h",
        "oddsFormat": "decimal",
        "apiKey": ODDS_API_KEY,
    }
    res = requests.get(
        "https://api.the-odds-api.com/v4/sports/mma_mixed_martial_arts/odds",
        params=params,
    )
    if res.status_code != 200:
        logging.error(f"Błąd API: {res.text}")
        sys.exit(1)

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for match in res.json():
        for bookmaker in match.get("bookmakers", []):
            title = bookmaker["title"]
            region = BOOKMAKER_REGIONS.get(title, "unknown")
            for market in bookmaker.get("markets", []):
                if market["key"] != "h2h" or len(market["outcomes"]) != 2:
                    continue
                rows.append({
                    "fighter_1": market["outcomes"][0]["name"],
                    "fighter_2": market["outcomes"][1]["name"],
                    "odds_1": market["outcomes"][0]["price"],
                    "odds_2": market["outcomes"][1]["price"],
                    "event_date": match.get("commence_time", "")[:10],
                    "adding_date": now,
                    "bookmaker": title,
                    "region": region,
                })

    if not rows:
        logging.info("Brak danych do zapisania.")
        return

    df = pd.DataFrame(rows)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, BQ_TABLE, job_config=job_config).result()
    logging.info(f"Zapisano {len(rows)} rekordów.")


if __name__ == "__main__":
    main()
