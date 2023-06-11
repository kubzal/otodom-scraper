import argparse
import datetime
import logging
import os
import sys
import time

import pandas as pd
import requests
import unidecode
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

from utils import get_creds

APP_NAME = "otodom_offers_scrapper"


def get_offer_ids_from_db(logger, credentials, dt):
    """
    Connects with PostgreSQl DB and get offer ids for selected day
    """

    engine = create_engine(
        f"postgresql://{credentials['username']}:{credentials['password']}"
        f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
    )

    logger.info("Getting offers from database")

    query = f"""
    select offer_id
    from public.otodom_offers_ids
    where date(create_timestamp) = '{dt}'
    """

    with engine.connect() as conn:
        result = conn.execute(text(query)) 
        offer_ids = [item[0] for item in result]

    logger.info(f"{len(offer_ids)} offers in database for {dt}")

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


def save_offers_params_to_db(logger, df, credentials, dry_run=False):
    if dry_run is False:
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
    else:
        logger.info("Dry run, results not saved")


def create_offers_df(logger, offer_ids, wait, dry_run):
    """
    Get offers params using get_offer_params() for offers
    from offer_ids list and prepare pandas data frame with
    them.
    """
    offer_ids_count = len(offer_ids)
    runtime_timedelta = datetime.timedelta(seconds=(offer_ids_count * wait))
    logger.info(f"Estimated runtime {runtime_timedelta}")

    if dry_run is False:
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
    else:
        df = pd.DataFrame()

    return df


def validate_date(logger, date_text):
    try:
        datetime.date.fromisoformat(date_text)
        return True
    except ValueError:
        logger.error("Incorrect date format, should be YYYY-MM-DD")


def main(argv):
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="extract date from database")
    group.add_argument("--url", help="otodom offer URL")
    parser.add_argument("--wait", help="wait")
    parser.add_argument("--dry_run", help="dry run", nargs="?", const=True, type=bool)
    args = parser.parse_args()

    log_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = "logs"
    log_path = f"{log_dir}/{APP_NAME}_{log_timestamp}.log"
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Create logs directory if does not exist
    try:
        os.makedirs(log_dir)
    except FileExistsError:
        pass

    logging.basicConfig(
        format=log_format,
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
        encoding="utf-8",
        level=logging.INFO,
    )

    logger = logging.getLogger(APP_NAME)
    logger.info(f"Starting {APP_NAME}")

    if args.wait is not None:
        wait = int(args.wait)
    else:
        wait = 1

    logger.info(f"Wait between offers {wait}s")

    if args.dry_run is None:
        dry_run = False
    else:
        dry_run = args.dry_run
        logger.info(f"Dry run {dry_run}")

    if args.date and validate_date(logger, args.date):
        logger.info(f"[date] {args.date}")

        offer_ids = get_offer_ids_from_db(logger, get_creds(), args.date)
        df = create_offers_df(logger, offer_ids, wait, dry_run)
        save_offers_params_to_db(logger, df, get_creds(), dry_run)

    if args.url:
        url = args.url
        logger.info(f"[url] {url}")
        try:
            offer_params = get_offer_params(url)

            for key, value in offer_params.items():
                print(f"{key}: {value}")

        except AttributeError:
            logger.error(f"Incorrect otodom offer URL: {url}")

    logger.info(f"{APP_NAME} finished")


if __name__ == "__main__":
    main(sys.argv)
