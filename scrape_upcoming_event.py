import sys
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

from config import PROJECT_ID, DATASET, TABLE_COMING_EVENT

logging.basicConfig(level=logging.INFO)

FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_COMING_EVENT}"
client = bigquery.Client(project=PROJECT_ID)


def get_latest_event():
    try:
        response = requests.get('http://ufcstats.com/statistics/events/completed?page=all')
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_link = soup.select_one('i.b-statistics__table-content a.b-link[href*="event-details"]')
        if not event_link:
            return None
        return {'url': event_link['href'].strip(), 'name': event_link.text.strip()}
    except Exception as e:
        logging.error(f'Error getting events: {e}')
        return None


def get_event_details(event_url):
    try:
        response = requests.get(event_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        event_name = soup.find('h2', class_='b-content__title').get_text(strip=True)

        details_section = soup.find('div', class_='b-list__info-box')
        date_str = location_str = 'N/A'
        if details_section:
            for item in details_section.find_all('li'):
                text = item.get_text(strip=True)
                if 'Date:' in text:
                    date_str = text.split('Date:')[-1].strip()
                if 'Location:' in text:
                    location_str = text.split('Location:')[-1].strip()

        try:
            event_date = datetime.strptime(date_str, '%B %d, %Y').strftime('%Y-%m-%d')
        except Exception:
            event_date = date_str

        location_parts = [p.strip() for p in location_str.split(',')]

        fights = []
        for row in soup.select('tr.b-fight-details__table-row')[1:]:
            fighters = row.select('a.b-link_style_black')
            weight_cell = row.select_one('td:nth-of-type(7) p')
            fights.append({
                'event_name': event_name,
                'event_date': event_date,
                'event_city': location_parts[0] if location_parts else None,
                'event_state': location_parts[1] if len(location_parts) > 1 else None,
                'event_country': location_parts[-1] if location_parts else None,
                'fighter1_name': fighters[0].text.strip() if len(fighters) > 0 else 'N/A',
                'fighter1_link': fighters[0]['href'] if len(fighters) > 0 else 'N/A',
                'fighter2_name': fighters[1].text.strip() if len(fighters) > 1 else 'N/A',
                'fighter2_link': fighters[1]['href'] if len(fighters) > 1 else 'N/A',
                'weight_class': weight_cell.get_text(strip=True) if weight_cell else 'N/A',
                'f1_odds': None,
                'f2_odds': None,
            })
        return fights
    except Exception as e:
        logging.error(f'Error: {e}')
        return []


def main():
    event = get_latest_event()
    if not event:
        logging.error("Brak eventów.")
        sys.exit(1)

    logging.info(f"Znaleziono event: {event['name']}")
    fights = get_event_details(event['url'])
    if not fights:
        logging.error("Brak danych o walkach.")
        sys.exit(1)

    df = pd.DataFrame(fights)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, FULL_TABLE_ID, job_config=job_config).result()
    logging.info(f"OK: {len(fights)} walk.")


if __name__ == "__main__":
    main()
