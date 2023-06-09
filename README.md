# otodom-scrapper
Right now `otodom-scraper` consists of two scripts.

## Saving offers ids
Gets otodom listing URL, crawl through all pages and gets offer ids. After that all offer ids are saved into PostgreSQL database. Offer id looks like this `przestronne-3-pokoje-m-57m2-cicha-zielona-okolica-ID4lL7J` and it is a part of offer URL. By injecting it after `https://www.otodom.pl/pl/oferta/` we get offer URL.

```
offer_id = "przestronne-3-pokoje-m-57m2-cicha-zielona-okolica-ID4lL7J"
offer_url = f"https://www.otodom.pl/pl/oferta/{offer_id}"
```

Below there is an example how to run that script localy.
```
python otodom-crawl-and-get-offer-ids.py --listing "https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/bialystok?distanceRadius=0&locations=%5Bcities_6-204%5D&viewType=listing"
```
## Saving offers params
Gets offers ids from PostgreSQL database for specified date, and gets offer params and save them into another table in database.

Below there is an example how to run that script localy.
```
python otodom-offer-scrapper.py --date 2023-06-09
```
