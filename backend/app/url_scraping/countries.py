import os
import concurrent.futures
import requests
import threading
from bs4 import BeautifulSoup
from app.utils import save_json

# Directory setup
RAW_DATA_DIR = os.path.join(os.getcwd(), "raw_data")
COUNTRIES_URLS_FILE = os.path.join(RAW_DATA_DIR, "countries_urls.json")

max_threads = 100  # Adjust as needed
country_list = set()  # Thread-safe set for country URLs
country_list_lock = threading.Lock()

def get_country_urls_worker(base_url, session, country_rows):
    """Worker function to fetch country URLs."""
    for country_row in country_rows:
        try:
            # Check for the glyphicon indicating a valid country entry
            if country_row.find(attrs={'class': 'glyphicon glyphicon-ok'}):
                country_url = base_url + country_row.find_all('a')[1]['href']

                # Safely add the country URL to the set (set ensures uniqueness)
                with country_list_lock:
                    country_list.add(country_url)
        except Exception as e:
            print(f"Error processing country row: {e}")

def fetch_and_save_countries():
    base_url = "https://www.olympedia.org"
    session = requests.Session()

    print("Fetching country URLs...")
    initial_url = f"{base_url}/countries"
    try:
        response = session.get(initial_url)
        response.raise_for_status()  # Check for request errors
        content = response.content
    except requests.RequestException as e:
        print(f"Failed to fetch country URLs from {initial_url}. Error: {e}")
        return

    if content:
        page = BeautifulSoup(content, "lxml")
        table_countries = page.find('tbody').find_all('tr')

        # Divide the country rows among threads
        num_countries = len(table_countries)
        chunk_size = max(1, num_countries // max_threads)
        chunks = [table_countries[i:i + chunk_size] for i in range(0, num_countries, chunk_size)]

        # Process URLs using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(get_country_urls_worker, base_url, session, chunk) for chunk in chunks]

            # Wait for all threads to complete
            concurrent.futures.wait(futures)

        # Convert the set to a sorted list and save to file
        unique_country_urls = sorted(country_list)
        save_json(unique_country_urls, COUNTRIES_URLS_FILE, append=False)
        print(f"Country URLs collection completed. Total unique country URLs: {len(unique_country_urls)}")
    else:
        print(f"Failed to fetch country URLs content from {initial_url}.")