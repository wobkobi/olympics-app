import os
import requests
import queue
from bs4 import BeautifulSoup
from utils import fetch_page, save_json

# Directory setup
RAW_DATA_DIR = os.path.join(os.getcwd(), "raw_data")
COUNTRIES_URLS_FILE = os.path.join(RAW_DATA_DIR, "countries_urls.json")

# Create a queue to hold country URLs
country_queue = queue.Queue()

def get_country_urls_worker(base_url, session):
    """Worker function to fetch country URLs from the queue."""
    while True:
        country_row = country_queue.get()
        if country_row is None:
            break  # Exit if None is passed to signal termination
        
        if country_row.find(attrs={'class': 'glyphicon glyphicon-ok'}):
            country_url = base_url + country_row.find_all('a')[1]['href']
            country_queue.task_done()
            return country_url

def get_country_urls(base_url, session):
    """Fetch and save the URLs of all countries that competed in the Olympics."""
    initial_url = f"{base_url}/countries"
    content = fetch_page(initial_url, session)

    if content:
        page = BeautifulSoup(content, "lxml")
        table_countries = page.find('tbody').find_all('tr')
        
        # Enqueue the country rows for processing
        for country_row in table_countries:
            country_queue.put(country_row)

        countries = set()

        # Process each country in the queue
        while not country_queue.empty():
            country_url = get_country_urls_worker(base_url, session)
            if country_url:
                countries.add(country_url)

        save_json(list(countries), COUNTRIES_URLS_FILE)
        print(f"Total countries saved: {len(countries)}")
        return countries
    else:
        print(f"Failed to fetch country URLs from {initial_url}.")
        return set()

def fetch_and_save_countries():
    base_url = "https://www.olympedia.org"
    session = requests.Session()
    
    print("Fetching country URLs...")
    get_country_urls(base_url, session)

    # Stop worker (if using multi-threading, this would be important to signal termination)
    country_queue.put(None)
