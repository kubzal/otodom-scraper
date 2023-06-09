import sys
import getopt

import datetime
import time

import pandas as pd
import psycopg2
import requests
import unidecode
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

from utils import get_creds


def get_offer_ids_from_db(credentials, dt=datetime.datetime.now().strftime("%Y-%m-%d")):
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


def get_offers_loop(offer_ids, bulk=10):
    results = list()
    for id in offer_ids:
        url = f"https://www.otodom.pl/pl/oferta/{id}"

        offer = dict()
        offer["create_timestamp"] = datetime.datetime.now()
        offer["id"] = id
        offer = {**offer, **get_offer_params(url)}

        results.append(offer)
        if len(results) == bulk:
            df = pd.DataFrame(results)
            save_offers_params_to_db(df, get_creds())
            results.clear()

        time.sleep(2)

    if len(results) < bulk and len(results) > 0:
        df = pd.DataFrame(results)
        save_offers_params_to_db(df, get_creds())


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
        get_offers_loop(offer_ids)
    else:
        print("Can't get --date arg")


if __name__ == "__main__":
    main(sys.argv)
