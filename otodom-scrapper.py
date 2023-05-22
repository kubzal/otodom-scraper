import sys
import getopt

import time
import requests
import json
import unidecode

import pandas as pd
from bs4 import BeautifulSoup


# Pobieranie parametrów oferty
def get_offer_params(offer_url):
    """
    Docstring
    """
    r = requests.get(offer_url)
    soup = BeautifulSoup(r.content, "html.parser")

    offer_params = soup.find_all("div", {"class": "css-1wi2w6s enb64yk4"})

    results = dict()

    price = soup.find("strong", {"aria-label": "Cena"})
    results["price"] = unidecode.unidecode(price.text)

    price_m2 = soup.find("div", {"aria-label": "Cena za metr kwadratowy"})
    results["price_m2"] = unidecode.unidecode(price_m2.text)

    address = soup.find("a", {"aria-label": "Adres"})
    results["address"] = unidecode.unidecode(address.text)

    for line in offer_params:
        k = unidecode.unidecode(
            str(line.parent.parent["aria-label"])
            .replace(" / ", "_")
            .replace(" ", "_")
            .lower()
        )
        v = unidecode.unidecode(str(line.text).strip())
        results[k] = v

    return results


# Pobieranie linków z listingu
def get_offers_urls(listing_url):
    """
    Docstring
    """

    r = requests.get(listing_url)
    soup = BeautifulSoup(r.content, "html.parser")

    urls_json = soup.find("script", {"id": "__NEXT_DATA__"}).get_text()

    json_dict = json.loads(urls_json)
    offers = json_dict["props"]["pageProps"]["schemaMarkupData"]["@graph"][2]["offers"][
        "offers"
    ]

    offers_list = []
    for offer in offers:
        offers_list.append({"name": offer["name"], "url": offer["url"]})

    return offers_list


# Pobieranie ofert z linków z listingu
def create_offers_table(offers_list):
    """
    Docstring
    """
    results = list()
    for offer in offers_list:
        enriched_offer = dict()

        enriched_offer["offer_name"] = offer["name"]
        enriched_offer["offer_url"] = offer["url"]

        enriched_offer = {**enriched_offer, **get_offer_params(offer["url"])}
        results.append(enriched_offer)

    return results


def main(argv):
    arg_listing = ""
    arg_help = f"{argv[0]} -l <otodom_listing>"

    try:
        opts, args = getopt.getopt(argv[1:], "hl:", ["help", "listing="])
    except:
        print(f"Except: {arg_help}")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-l", "--listing"):
            arg_listing = arg

    # Wykonanie
    if len(arg_listing) > 0:
        print(f"otodom_listing: {arg_listing}")
        offers_listing = create_offers_table(get_offers_urls(arg_listing))

        df = pd.DataFrame(offers_listing)

        save_path = f"results_{int(time.time())}.csv"
        df.to_csv(save_path, index=False)
        print(f"Results saved into {save_path}")
    else:
        print("Can't get --listing arg")


if __name__ == "__main__":
    main(sys.argv)
