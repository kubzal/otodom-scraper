import datetime
import getopt
import re
import sys
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine

from utils import get_creds


def crawler(driver, actions, url):
    """
    Crawls otodom listing to do some actions and
    use pagination till its end
    """
    driver.get(url)  # open URL in Browser

    # Cookies
    try:
        print("Accepting cookies")
        driver.find_element(
            By.ID, "onetrust-accept-btn-handler"
        ).click()  # accept cookies
    except NoSuchElementException:
        print("Cookies already accepted")

    pagination_button = driver.find_element(
        By.XPATH, "//*[@data-cy='pagination.next-page']"
    )
    actions.move_to_element(pagination_button).perform()

    soup = BeautifulSoup(driver.page_source, "html.parser")  # get html

    # Find pages number
    pages = soup.find_all("button", {"data-cy": re.compile("^pagination.go-to-page-")})
    pages_numbers = [int(x.get_text()) for x in pages]
    last_page_number = max(pages_numbers)

    print(f"Total pages: {last_page_number}")

    for i in range(0, last_page_number):
        print(f"Current URL: {driver.current_url}")

        # Move to pagination button
        pagination_button = driver.find_element(
            By.XPATH, "//*[@data-cy='pagination.next-page']"
        )
        actions.move_to_element(pagination_button).perform()

        # Get page source
        html = driver.page_source

        # Get offers ids
        offers_ids = get_offers_ids(html)

        # Creatinfg df with offers ids
        df = pd.DataFrame(offers_ids, columns=["offer_id"])
        df.insert(loc=0, column="create_timestamp", value=datetime.datetime.now())
        df.insert(loc=1, column="listing_url", value=driver.current_url)

        # Saving to DB
        save_df(df, get_creds(), csv=False, db=True)

        # Wait and go to the next page
        time.sleep(10)

        if pagination_button.is_enabled():
            pagination_button.click()
        else:
            break


def get_offers_ids(page_source):
    """
    Takes otodom listing page and get all offer ids from that page
    """

    soup = BeautifulSoup(page_source, "html.parser")

    listing_items = soup.find_all("li", {"data-cy": "listing-item"})

    offer_urls = []
    for item in listing_items:
        a = item.find("a", {"data-cy": "listing-item-link"})
        href_array = a["href"].split("/")

        offer_id = href_array[len(href_array) - 1]

        offer_urls.append(offer_id)

    return offer_urls


def save_df(df, credentials, csv=True, db=True):
    """
    Saves otodom offer ids into CSV or/and DB table
    """
    # Save to CSV
    if csv:
        csv_path = f"results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_path, index=False)
        print(f"Results saved into {csv_path}")

    # Save to DB
    if db:
        # Saving to PostgreSQL DB
        engine = create_engine(
            f"postgresql://{credentials['username']}:{credentials['password']}"
            f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
        )

        table_name = "otodom_offers_ids"
        df.to_sql(table_name, engine, if_exists="append", index=False)
        print(
            f"Results saved to PostgreSQL DB into table: {credentials['database']}.{table_name}"
        )


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

        driver = webdriver.Chrome()
        actions = ActionChains(driver)

        crawler(driver, actions, arg_listing)

    else:
        print("Can't get --listing arg")


if __name__ == "__main__":
    main(sys.argv)
