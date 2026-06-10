import sys
import logging
import bs4
import pandas as pd
from google.cloud import bigquery

from config import PROJECT_ID, DATASET, TABLE_FIGHTS_URLS, TABLE_FIGHTS_STATS
from utils.playwright_fetch import fetch_page

logging.basicConfig(level=logging.INFO)
client = bigquery.Client(project=PROJECT_ID)


def get_new_fight_urls():
    query = f"""
    SELECT DISTINCT Fight_URL AS fight_url
    FROM `{PROJECT_ID}.{DATASET}.{TABLE_FIGHTS_URLS}`
    WHERE Fight_URL NOT IN (
        SELECT DISTINCT fight_url FROM `{PROJECT_ID}.{DATASET}.{TABLE_FIGHTS_STATS}`
    )
    """
    return [row.fight_url for row in client.query(query).result()]


def get_fighter_id(fight_soup, fight_stats, fighter):
    idx = 0 if fighter == 1 else 1
    try:
        return fight_stats[idx].text.strip()
    except Exception:
        return fight_soup.select('a.b-fight-details__person-link')[idx].text.strip()


def get_striking_stats(fight_stats, fighter):
    idx = 0 if fighter == 1 else 1
    try:
        return (
            fight_stats[2 + idx].text.strip(),
            fight_stats[8 + idx].text.split(' of ')[1].strip(),
            fight_stats[8 + idx].text.split(' of ')[0].strip(),
            fight_stats[4 + idx].text.split(' of ')[1].strip(),
            fight_stats[4 + idx].text.split(' of ')[0].strip(),
        )
    except Exception:
        return ('NULL', 'NULL', 'NULL', 'NULL', 'NULL')


def get_grappling_stats(fight_stats, fighter):
    idx = 0 if fighter == 1 else 1
    try:
        return (
            fight_stats[10 + idx].text.split(' of ')[1].strip(),
            fight_stats[10 + idx].text.split(' of ')[0].strip(),
            fight_stats[14 + idx].text.strip(),
            fight_stats[16 + idx].text.strip(),
            fight_stats[18 + idx].text.strip(),
        )
    except Exception:
        return ('NULL', 'NULL', 'NULL', 'NULL', 'NULL')


def main():
    fight_urls = get_new_fight_urls()
    logging.info(f"Znaleziono {len(fight_urls)} nowych URL walk.")

    if not fight_urls:
        logging.info("Brak nowych walk do scrapowania.")
        return

    fights_stats_data = []

    for url in fight_urls:
        logging.info(f"Przetwarzanie URL: {url}")
        fight_soup = bs4.BeautifulSoup(fetch_page(url), 'html.parser')
        fight_stats = fight_soup.select('p.b-fight-details__table-text')

        for fighter_num in (1, 2):
            fighter_name = get_fighter_id(fight_soup, fight_stats, fighter_num)
            kd, ts_att, ts_succ, ss_att, ss_succ = get_striking_stats(fight_stats, fighter_num)
            td_att, td_succ, sub_att, reversals, ctrl = get_grappling_stats(fight_stats, fighter_num)
            fights_stats_data.append({
                "fighter_id": fighter_name,
                "knockdowns": kd,
                "total_strikes_att": ts_att,
                "total_strikes_succ": ts_succ,
                "sig_strikes_att": ss_att,
                "sig_strikes_succ": ss_succ,
                "takedown_att": td_att,
                "takedown_succ": td_succ,
                "submission_att": sub_att,
                "reversals": reversals,
                "ctrl_time": ctrl,
                "fight_url": url,
            })

    if not fights_stats_data:
        logging.info("Brak nowych danych do wstawienia.")
        return

    df = pd.DataFrame(fights_stats_data)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE_FIGHTS_STATS}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    logging.info(f"Dodano {len(fights_stats_data)} nowych statystyk walk.")


if __name__ == "__main__":
    main()
