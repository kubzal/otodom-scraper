# otodom-scrapper
Welcome to `otodom-scrapper`!

## Installation
Recommended Python version is 3.9.10.

First clone the repository.
```
git clone git@github.com:kubzal/otodom-scrapper.git
```
Then enter the directory and create virtual environment. 
```
cd otodom-scrapper
python -m venv env
```
Then activate virual environment. 
```
source env/bin/activate
```
Finally install the requirements.
```
pip install -r requirements.txt
```

Right now `otodom-scraper` consists of two scripts.

## _otodom_listing_crawler_
Gets otodom listing URL, crawl through all pages of that listing and gets offers ids. After that all offers ids are saved into table on PostgreSQL database. Offer id looks like `przestronne-3-pokoje-m-57m2-cicha-zielona-okolica-ID4lL7J` and it is a part of offer URL. By injecting it after `https://www.otodom.pl/pl/oferta/` we get offer URL.

```
offer_id = "przestronne-3-pokoje-m-57m2-cicha-zielona-okolica-ID4lL7J"
offer_url = f"https://www.otodom.pl/pl/oferta/{offer_id}"
```

Below there is an example how to run that script locally.
```
python otodom_listing_crawler.py --listing "https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/bialystok?distanceRadius=0&locations=%5Bcities_6-204%5D&viewType=listing" --run local
```

Possible options
```
python otodom_listing_crawler.py [-h] --listing LISTING [--wait SECONDS] --run local | server [--dry_run]
```

## _otodom_offers_scrapper_
Option `--date` gets offers ids from PostgreSQL database for specified date, and gets offer params and save them into another table in database.

Below there is an example how to use it.
```
python otodom_offers_scrapper.py --date 2023-06-09
```

Option `--url` gets offer params for the offer from URL.

Below there is an example how to use it.
```
python otodom_offers_scrapper.py --url "https://www.otodom.pl/pl/oferta/penthouse-z-tarasem-na-zoliborzu-ul-rydygiera-ID4lKUX"
```
Possible options
```
 otodom_offers_scrapper.py [-h] (--date DATE | --url URL) [--wait WAIT] [--dry_run]
```