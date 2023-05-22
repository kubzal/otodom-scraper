import requests
from bs4 import BeautifulSoup

url = "https://www.otodom.pl/pl/oferta/2-pok-55-m2-z-widokiem-na-ogrod-krasinskich-ID4lszi"
# url = "https://www.otodom.pl/pl/oferta/widokowe-3-pokoje-przy-metrze-wawrzyszew-ID4lxZ0"

r = requests.get(url)
soup = BeautifulSoup(r.content, "html.parser")

offer_params = soup.find_all("div", {"class": "css-1wi2w6s enb64yk4"})

price = soup.find("strong", {"aria-label": "Cena"})
print(f"Cena: {price.text}")

price_m2 = soup.find("div", {"aria-label": "Cena za metr kwadratowy"})
print(f"Cena/m2: {price_m2.text}")

address = soup.find("a", {"aria-label": "Adres"})
print(f"Lokalizacja: {address.text}")

for line in offer_params:
    print(f"{line.parent.parent['aria-label']}: {str(line.text).strip()}")



