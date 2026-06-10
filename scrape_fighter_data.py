import sys
import logging
import requests
import bs4
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

from config import PROJECT_ID, DATASET, TABLE_FIGHTERS_URLS, TABLE_FIGHTERS_DATA

logging.basicConfig(level=logging.INFO)

FULL_URLS = f"{PROJECT_ID}.{DATASET}.{TABLE_FIGHTERS_URLS}"
FULL_DATA = f"{PROJECT_ID}.{DATASET}.{TABLE_FIGHTERS_DATA}"
client = bigquery.Client(project=PROJECT_ID)


def parse_l_name(name):
    if len(name) <= 1:
        return 'NULL'
    if len(name) == 2:
        return name[-1]
    return ' '.join(name[1:])  # fix: was name[-(len-1):-0] which always returned ''


def parse_nickname(n):
    return n.strip() if n != '\n' else 'NULL'


def parse_height(h):
    return None if '--' in h else int((int(h[0]) * 12 + int(h.split("'")[1].strip().strip('"'))) * 2.54)


def parse_reach(r):
    return None if '--' in r else int(r.strip().strip('"')) * 2.54


def parse_weight(w):
    return None if '--' in w else int(w.split()[0].strip())


def parse_stance(s):
    return s.strip() if s.strip() else 'NULL'


def parse_dob(d):
    return None if d == '--' else datetime.strptime(d, '%b %d, %Y').strftime('%Y-%m-%d')


def scrape_fighter_from_url(url):
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = bs4.BeautifulSoup(res.text, "html.parser")

        name = soup.select_one('span')
        if not name:
            return None
        name_parts = name.text.split()
        f_name = name_parts[0]
        l_name = parse_l_name(name_parts)

        nick = soup.select_one('p.b-content__Nickname')
        nickname = parse_nickname(nick.text) if nick else 'NULL'

        details = soup.select('li.b-list__box-list-item')
        if len(details) < 5:
            return None
        height = parse_height(details[0].text.split(":")[1].strip())
        weight = parse_weight(details[1].text.split(":")[1].strip())
        reach = parse_reach(details[2].text.split(":")[1].strip())
        stance = parse_stance(details[3].text.split(":")[1].strip())
        dob = parse_dob(details[4].text.split(":")[1].strip())

        record = soup.select_one('span.b-content__title-record')
        if not record:
            return None
        rec = record.text.split(":")[1].strip().split("-")
        w_count, l_count = int(rec[0]), int(rec[1])
        d_count = int(rec[-1][0]) if len(rec[-1]) > 1 else int(rec[-1])
        nc = int(rec[-1].split("(")[-1][0]) if '(' in rec[-1] else 0

        stats_dict = {}
        for stat in soup.select('ul.b-list__box-list.b-list__box-list_margin-top li.b-list__box-list-item'):
            try:
                k, v = stat.text.split(":")
                stats_dict[k.strip()] = v.strip()
            except Exception:
                continue

        return {
            "fighter_f_name": f_name,
            "fighter_l_name": l_name,
            "fighter_nickname": nickname,
            "fighter_height_cm": height,
            "fighter_weight_lbs": weight,
            "fighter_reach_cm": reach,
            "fighter_stance": stance,
            "fighter_dob": dob,
            "fighter_w": w_count,
            "fighter_l": l_count,
            "fighter_d": d_count,
            "fighter_nc_dq": nc,
            "fighter_SlpM": float(stats_dict.get('SLpM', 0)),
            "fighter_Str_Acc": float(stats_dict.get('Str. Acc.', '0').strip('%')) / 100,
            "fighter_SApM": float(stats_dict.get('SApM', 0)),
            "fighter_Str_Def": float(stats_dict.get('Str. Def', '0').strip('%')) / 100,
            "fighter_TD_Avg": float(stats_dict.get('TD Avg.', 0)),
            "fighter_TD_Acc": float(stats_dict.get('TD Acc.', '0').strip('%')) / 100,
            "fighter_TD_Def": float(stats_dict.get('TD Def.', '0').strip('%')) / 100,
            "fighter_Sub_Avg": float(stats_dict.get('Sub. Avg.', 0)),
            "fighter_url": url,
        }
    except Exception as e:
        logging.warning(f"Błąd przy {url}: {e}")
        return None


def get_urls_to_scrape():
    query = f"""
    SELECT fighter_url FROM `{FULL_URLS}`
    WHERE fighter_url NOT IN (
        SELECT DISTINCT fighter_url FROM `{FULL_DATA}`
    )
    """
    return client.query(query).to_dataframe()["fighter_url"].tolist()


def main():
    urls = get_urls_to_scrape()
    logging.info(f"URL-i do przetworzenia: {len(urls)}")

    if not urls:
        logging.info("Brak nowych zawodników.")
        return

    fighters = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(scrape_fighter_from_url, url) for url in urls]
        for future in as_completed(futures):
            result = future.result()
            if result:
                fighters.append(result)

    if not fighters:
        logging.info("Brak danych do zapisania.")
        return

    df = pd.DataFrame(fighters)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, FULL_DATA, job_config=job_config).result()
    logging.info(f"Dodano {len(fighters)} rekordów.")


if __name__ == "__main__":
    main()
