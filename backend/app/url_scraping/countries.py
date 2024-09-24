import os
import concurrent.futures
import requests
import threading
from bs4 import BeautifulSoup
from app.utils import save_json

# Directory setup
RAW_DATA_DIR = os.path.join(os.getcwd(), "raw_data")
COUNTRIES_URLS_FILE = os.path.join(RAW_DATA_DIR, "countries_urls.json")

max_threads = 10  # Adjust as needed
country_list = []  # Thread-safe list for country URLs
country_list_lock = threading.Lock()

def get_country_urls_worker(base_url, session, country_rows):
    """Worker function to fetch country URLs."""
    for country_row in country_rows:
        try:
            if country_row.find(attrs={'class': 'glyphicon glyphicon-ok'}):
                country_url = base_url + country_row.find_all('a')[1]['href']

                # Safely add the country URL to the list
                with country_list_lock:
                    country_list.append(country_url)
        except Exception as e:
            print(f"Error processing country row: {e}")

def fetch_and_save_countries():
    base_url = "https://www.olympedia.org"
    session = requests.Session()

    print("Fetching country URLs...")
    initial_url = f"{base_url}/countries"
    content = session.get(initial_url).content

    if content:
        page = BeautifulSoup(content, "lxml")
        table_countries = page.find('tbody').find_all('tr')

        # Divide the country rows among threads
        num_countries = len(table_countries)
        chunk_size = max(1, num_countries // max_threads)
        chunks = [table_countries[i:i + chunk_size] for i in range(0, num_countries, chunk_size)]

        # Process URLs using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = []
            for chunk in chunks:
                futures.append(executor.submit(get_country_urls_worker, base_url, session, chunk))

            # Wait for all threads to complete
            concurrent.futures.wait(futures)

        # Remove duplicates and save to file
        unique_country_urls = list(set(country_list))
        save_json(unique_country_urls, COUNTRIES_URLS_FILE, append=False)
        print(f"Country URLs collection completed. Total unique countries: {len(unique_country_urls)}")
    else:
        print(f"Failed to fetch country URLs from {initial_url}.")