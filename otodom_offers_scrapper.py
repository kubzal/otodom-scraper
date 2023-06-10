import argparse
import datetime
import logging
import sys
import time

import pandas as pd
import psycopg2
import requests
import unidecode
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

from utils import get_creds

APP_NAME = "otodom_offers_scrapper"

LOG_TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILENAME = f"logs/scrapper_{LOG_TIMESTAMP}.log"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")

logging.basicConfig(
    format=LOG_FORMAT,
    handlers=[logging.FileHandler(LOG_FILENAME), logging.StreamHandler()],
    encoding="utf-8",
    level=logging.INFO,
)

logger = logging.getLogger(APP_NAME)


def get_offer_ids_from_db(credentials, dt=CURRENT_DATE):
    """
    Connects with PostgreSQl DB and get offer ids for selected day
    """
    conn = psycopg2.connect(
        database=credentials["database"],
        user=credentials["username"],
        password=credentials["password"],
        host=credentials["host"],
        port=credentials["port"],
    )

    logger.info("Getting offers from database")

    cursor = conn.cursor()

    query = f"""
    select offer_id
    from public.otodom_offers_ids
    where date(create_timestamp) = '{dt}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    offer_ids = [item[0] for item in result]

    logger.info(f"{len(offer_ids)} offers in database for {dt}: ")

    return offer_ids


def get_offer_params(offer_url):
    """
    Gets offer params by offer ID
    """
    r = requests.get(offer_url)
    soup = BeautifulSoup(r.content, "html.parser")

    offer_params = soup.find_all("div", {"class": "css-1wi2w6s enb64yk4"})

    results = dict()

    price = soup.find("strong", {"aria-label": "Cena"}).get_text()
    results["price"] = unidecode.unidecode(price)

    price_m2 = soup.find("div", {"aria-label": "Cena za metr kwadratowy"}).get_text()
    results["price_m2"] = unidecode.unidecode(price_m2)

    address = soup.find("a", {"aria-label": "Adres"}).get_text()
    results["address"] = unidecode.unidecode(address)

    for line in offer_params:
        k = unidecode.unidecode(
            str(line.parent.parent["aria-label"])
            .replace(" / ", "_")
            .replace(" ", "_")
            .lower()
        )
        v = unidecode.unidecode(str(line.get_text()).strip())
        results[k] = v

    return results


def save_offers_params_to_db(df, credentials):
    engine = create_engine(
        f"postgresql://{credentials['username']}:{credentials['password']}"
        f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
    )
    table_name = "otodom_offers_params"

    logger.info(
        f"Saving {len(df.index)} rows into table: "
        f"{credentials['database']}.{table_name}"
    )
    df.to_sql(table_name, engine, if_exists="append", index=False)


def create_offers_df(offer_ids, wait=1):
    """
    Get offers params using get_offer_params() for offers
    from offer_ids list and prepare pandas data frame with
    them.
    """
    results = list()
    for id in offer_ids:
        url = f"https://www.otodom.pl/pl/oferta/{id}"

        try:
            offer_params = get_offer_params(url)
        except AttributeError:
            logger.warning(f"Broken URL: {url}")
            pass

        offer = dict()
        offer["create_timestamp"] = datetime.datetime.now()
        offer["id"] = id
        offer = {**offer, **offer_params}
        results.append(offer)

        time.sleep(wait)

    df = pd.DataFrame(results)

    return df


def validate_date(date_text):
    try:
        datetime.date.fromisoformat(date_text)
        return True
    except ValueError:
        logger.error("Incorrect date format, should be YYYY-MM-DD")


def main(argv):
    logger.info(f"Starting {APP_NAME}")

    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="extract date from database")
    group.add_argument("--url", help="otodom offer URL")
    args = parser.parse_args()

    if args.date and validate_date(args.date):
        logger.info(f"[date] {args.date}")

        offer_ids = get_offer_ids_from_db(get_creds(), args.date)
        df = create_offers_df(offer_ids)
        save_offers_params_to_db(df, get_creds())

    if args.url:
        logger.info(f"[url] {args.url}")
        try:
            offer_params = get_offer_params(args.url)

            for key, value in offer_params.items():
                print(f"{key}: {value}")

        except AttributeError:
            logger.error(f"Incorrect otodom offer URL: {args.url}")

    logger.info(f"{APP_NAME} finished")


if __name__ == "__main__":
    main(sys.argv)
