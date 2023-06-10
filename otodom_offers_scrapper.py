import datetime
import getopt
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

LOG_TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILENAME = f"scrapper_{LOG_TIMESTAMP}.log"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")

logging.basicConfig(
    format=LOG_FORMAT,
    handlers=[logging.FileHandler(LOG_FILENAME), logging.StreamHandler()],
    encoding="utf-8",
    level=logging.WARNING,
)

logger = logging.getLogger("otodom_offers_scrapper")


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

    print(
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
            logger.warning(f"Broken url: {url}")
            pass

        offer = dict()
        offer["create_timestamp"] = datetime.datetime.now()
        offer["id"] = id
        offer = {**offer, **offer_params}
        results.append(offer)

        time.sleep(wait)

    df = pd.DataFrame(results)

    return df


def main(argv):
    arg_date = ""
    arg_help = f"{argv[0]} -d <date>"

    try:
        opts, args = getopt.getopt(argv[1:], "hl:", ["help", "date="])
    except:
        print(f"Except: {arg_help}")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-d", "--date"):
            arg_date = arg

    # Wykonanie
    if len(arg_date) > 0:
        offer_ids = get_offer_ids_from_db(get_creds(), arg_date)
        df = create_offers_df(offer_ids)
        save_offers_params_to_db(df, get_creds())
    else:
        print("Can't get --date arg")


if __name__ == "__main__":
    main(sys.argv)
