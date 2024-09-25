import os
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Directory setup
DATA_DIR = os.path.join(os.getcwd(), "data")
NOC_COUNTRIES_CSV = os.path.join(DATA_DIR, "noc_countries.csv")

def scrape_noc_countries():
    """Scrape NOC countries and save them to a CSV file."""
    url = "https://www.olympedia.org/countries"
    print(f"Fetching data from {url}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return

    soup = BeautifulSoup(response.content, "lxml")
    table_body = soup.find("tbody")
    if not table_body:
        print("Error: Could not find the table body in the page.")
        return

    rows = table_body.find_all("tr")

    all_nocs = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 2:
            noc_code_link = cells[0].find("a")
            country_name_link = cells[1].find("a")
            participation_icon = row.find("span", class_="glyphicon glyphicon-ok")
            
            # Log specific reasons for skipping
            if not noc_code_link:
                print(f"Skipping NOC due to missing NOC code: {row}")
                continue
            if not country_name_link:
                print(f"Skipping NOC due to missing country name: {row}")
                continue
            if not participation_icon:
                print(f"Skipping NOC due to non-participation: {country_name_link.text.strip()}")

            noc_code = noc_code_link.text.strip()
            country_name = country_name_link.text.strip()
            noc_country = {
                "noc": noc_code,
                "country": country_name,
            }
            all_nocs.append(noc_country)
        else:
            print(f"Warning: Skipping a row due to insufficient data: {row}")

    if all_nocs:
        # Ensure the data directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        noc_df = pd.DataFrame(all_nocs)
        noc_df.to_csv(NOC_COUNTRIES_CSV, index=False)
        print(f"NOC countries data saved to {NOC_COUNTRIES_CSV}")
    else:
        print("No NOC countries data found.")
