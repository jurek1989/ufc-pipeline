import sys
import logging
import bs4
import pandas as pd
from google.cloud import bigquery
import re

from config import PROJECT_ID, DATASET, TABLE_FIGHTS_DATA, TABLE_FIGHTS_URLS
from utils.playwright_fetch import fetch_page

logging.basicConfig(level=logging.INFO)
client = bigquery.Client(project=PROJECT_ID)

TABLE_REF_FIGHTS = f"{PROJECT_ID}.{DATASET}.{TABLE_FIGHTS_DATA}"
TABLE_REF_URLS = f"{PROJECT_ID}.{DATASET}.{TABLE_FIGHTS_URLS}"


def get_info(label, soup):
    for p in soup.select("p.b-fight-details__text"):
        for tag in p.find_all("i", class_="b-fight-details__label"):
            if tag.text.strip() == label:
                next_node = tag.next_sibling
                while next_node:
                    if isinstance(next_node, str):
                        text = next_node.strip()
                        if text:
                            return text
                    elif hasattr(next_node, "get_text"):
                        text = next_node.get_text(strip=True)
                        if text:
                            return text
                    next_node = next_node.next_sibling
    return None


def get_result_details(soup):
    for p in soup.select("p.b-fight-details__text"):
        if "Details:" in p.text:
            text = p.get_text(separator=" ", strip=True)
            if "Details:" in text:
                return text.split("Details:")[-1].strip()
    return None


def get_event_name(soup):
    header = soup.select_one("h2.b-content__title")
    if header:
        return header.get_text(strip=True).replace("Event:", "").strip()
    return None


def get_winner(f1, f2, fighters):
    status1 = fighters[0].select_one(".b-fight-details__person-status")
    status2 = fighters[1].select_one(".b-fight-details__person-status")
    s1 = status1.get_text(strip=True).upper() if status1 else ""
    s2 = status2.get_text(strip=True).upper() if status2 else ""
    if s1 == "W":
        return f1
    if s2 == "W":
        return f2
    if "DRAW" in s1 or "DRAW" in s2:
        return "Draw"
    if "DQ" in s1 or "DQ" in s2:
        return "DQ"
    return "NC"


def main():
    df_urls = client.query(f"""
        SELECT DISTINCT Fight_URL AS url
        FROM `{TABLE_REF_URLS}`
        WHERE Fight_URL NOT IN (
            SELECT DISTINCT fight_url FROM `{TABLE_REF_FIGHTS}`
        )
    """).to_dataframe()
    logging.info(f"Liczba URL-i do scrapowania: {len(df_urls)}")

    if df_urls.empty:
        logging.info("Brak nowych walk do scrapowania.")
        return

    records = []
    for url in df_urls["url"]:
        logging.info(f"Przetwarzanie: {url}")
        try:
            soup = bs4.BeautifulSoup(fetch_page(url), "html.parser")

            fighters = soup.select("div.b-fight-details__person")
            if len(fighters) != 2:
                logging.warning(f"Brak poprawnych danych o zawodnikach: {url}")
                continue

            f1 = fighters[0].select_one("h3").text.strip()
            f2 = fighters[1].select_one("h3").text.strip()
            f1_url = fighters[0].select_one("a")["href"]
            f2_url = fighters[1].select_one("a")["href"]

            winner = get_winner(f1, f2, fighters)
            referee = get_info("Referee:", soup)

            time_format_tag = next(
                (p for p in soup.select("p.b-fight-details__text") if "Time format:" in p.text), None
            )
            num_rounds = 0
            if time_format_tag:
                m = re.search(r'(\d+)\s*Rnd', time_format_tag.text)
                if m:
                    num_rounds = int(m.group(1))

            finish_round = int(get_info("Round:", soup) or 0)
            finish_time = get_info("Time:", soup)

            weight_tag = soup.select_one("i.b-fight-details__fight-title")
            weight_class = weight_tag.text.strip().replace(" Bout", "") if weight_tag else None
            title_fight = "Title Bout" in weight_class if weight_class else False
            gender = "F" if weight_class and "Women" in weight_class else "M"

            records.append({
                "event_name": get_event_name(soup),
                "referee": referee,
                "f_1": f1,
                "f_2": f2,
                "f_1_url": f1_url,
                "f_2_url": f2_url,
                "winner": winner,
                "num_rounds": num_rounds,
                "title_fight": title_fight,
                "weight_class": weight_class,
                "gender": gender,
                "result": get_info("Method:", soup),
                "result_details": get_result_details(soup),
                "finish_round": finish_round,
                "finish_time": finish_time,
                "fight_url": url,
            })
        except Exception as e:
            logging.error(f"Błąd przy {url}: {e}")
            continue

    if not records:
        logging.warning("Brak rekordów do zapisania.")
        return

    df = pd.DataFrame(records)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, TABLE_REF_FIGHTS, job_config=job_config).result()
    logging.info(f"Zapisano {len(df)} rekordów.")


if __name__ == "__main__":
    main()
