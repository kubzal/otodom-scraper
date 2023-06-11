import argparse
import datetime
import logging
import os
import re
import sys
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine

from utils import get_creds

APP_NAME = "otodom_listing_crawler"

LOG_TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR = "logs"
LOG_PATH = f"{LOG_DIR}/{APP_NAME}_{LOG_TIMESTAMP}.log"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")

CHROMEDRIVER_PATH = "/usr/local/bin/chromedriver"
WINDOW_SIZE = "1920,1080"
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)
chrome_options.add_argument("--no-sandbox")

# Create logs directory if does not exist
try:
    os.makedirs(LOG_DIR)
except FileExistsError:
    pass

logging.basicConfig(
    format=LOG_FORMAT,
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
    encoding="utf-8",
    level=logging.INFO,
)

logger = logging.getLogger(APP_NAME)


def crawler(driver, actions, url, wait=5):
    """
    Crawls otodom listing to do some actions and
    use pagination till its end
    """
    driver.get(url)  # open URL in Browser

    # Cookies
    try:
        logger.info("Accepting cookies")
        driver.find_element(
            By.ID, "onetrust-accept-btn-handler"
        ).click()  # accept cookies
    except NoSuchElementException:
        logger.info("Cookies already accepted")

    pagination_button = driver.find_element(
        By.XPATH, "//*[@data-cy='pagination.next-page']"
    )
    actions.move_to_element(pagination_button).perform()

    soup = BeautifulSoup(driver.page_source, "html.parser")  # get html

    # Find pages number
    pages = soup.find_all("button", {"data-cy": re.compile("^pagination.go-to-page-")})
    pages_numbers = [int(x.get_text()) for x in pages]
    last_page_number = max(pages_numbers)

    logger.info(f"Total pages: {last_page_number}")

    for i in range(0, last_page_number):
        logger.info(f"Current URL: {driver.current_url}")

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
        time.sleep(wait)

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
        logger.info(f"Results saved into {csv_path}")

    # Save to DB
    if db:
        # Saving to PostgreSQL DB
        engine = create_engine(
            f"postgresql://{credentials['username']}:{credentials['password']}"
            f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
        )

        table_name = "otodom_offers_ids"
        df.to_sql(table_name, engine, if_exists="append", index=False)
        logger.info(
            f"Results saved to PostgreSQL DB into table: {credentials['database']}.{table_name}"
        )


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--listing", help="otodom listing URL", required=True)
    parser.add_argument("--wait", help="wait time between listing pages in seconds")
    args = parser.parse_args()

    logger.info(f"Starting {APP_NAME}")
    if args.listing:
        logger.info(f"[listing] {args.listing}")
        driver = webdriver.Chrome(
            executable_path=CHROMEDRIVER_PATH, chrome_options=chrome_options
        )
        actions = ActionChains(driver)

        if args.wait and isinstance(int(args.wait), int):
            logger.info(f"Wait between listing pages {args.wait}s")
            crawler(driver, actions, args.listing, wait=int(args.wait))
        else:
            logger.info("Default wait time between listing pages")
            crawler(driver, actions, args.listing)

    logger.info(f"{APP_NAME} finished")


if __name__ == "__main__":
    main(sys.argv)
