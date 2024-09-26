import os
import concurrent.futures
import requests
import threading
import queue
import json
from bs4 import BeautifulSoup
from app.utils import (
    fetch_page,
    save_json,
    load_json,
    init_progress,
    increment_progress,
    progress_lock,
)

# Directory setup
RAW_DATA_DIR = os.path.join(os.getcwd(), "raw_data")
EVENTS_URLS_FILE = os.path.join(RAW_DATA_DIR, "events_urls.json")
COUNTRIES_URLS_FILE = os.path.join(RAW_DATA_DIR, "countries_urls.json")

max_threads = 100  # Adjust as needed
event_queue = queue.Queue()
file_lock = threading.Lock()  # Lock for synchronizing file writes
events_urls_set = set()  # Set to track all unique event URLs
events_urls_set_lock = threading.Lock()  # Lock for thread-safe access to the set

# Shared variable to track if this is the first URL being written
first_url_written = threading.Event()

# Global progress data
progress_data = {
    "total": 0,
    "current": 0,
    "start_time": None,
    "last_logged_percentage": 0.0,
    "last_print_time": 0,
}

def get_event_urls_worker(base_url):
    """Worker function to process event URLs from the queue."""
    session = requests.Session()  # Create a session per thread
    while True:
        country_url = event_queue.get()

        try:
            if country_url is None:
                # Mark the task as done and exit
                event_queue.task_done()
                break

            # Fetch and process the country URL
            try:
                content = fetch_page(country_url, session)

                if content:
                    country_page = BeautifulSoup(content, "lxml")
                    events_urls = set()

                    if country_page.find("tbody"):
                        for game in country_page.find("tbody").find_all("tr"):
                            event_url = base_url + game.find_all("a")[1]["href"]
                            events_urls.add(event_url)

                    # Append the events URLs directly to the file
                    if events_urls:
                        serialized_urls = [json.dumps(url) for url in events_urls]

                        with file_lock:
                            with open(EVENTS_URLS_FILE, 'a', encoding='utf-8') as f:
                                for url_str in serialized_urls:
                                    if first_url_written.is_set():
                                        f.write(',\n' + url_str)
                                    else:
                                        f.write('\n' + url_str)
                                        first_url_written.set()

                        # Add to the global set of event URLs
                        with events_urls_set_lock:
                            events_urls_set.update(events_urls)

                else:
                    print(f"No content fetched for {country_url}")

            except Exception as e:
                print(f"Error processing {country_url}: {e}")

            # Increment progress after processing a country URL
            with progress_lock:
                increment_progress("Fetching Events", progress_data)

        finally:
            # Mark the task as done
            event_queue.task_done()

def fetch_and_save_events():
    print("Fetching event URLs...")
    base_url = "https://www.olympedia.org"

    # Load country URLs from file
    if not os.path.exists(COUNTRIES_URLS_FILE):
        print("No countries URLs file found. Please run country scraping first.")
        return

    countries_urls = load_json(COUNTRIES_URLS_FILE)

    # Initialize progress tracking
    init_progress(len(countries_urls), progress_data)

    # Initialize the JSON file with an opening bracket
    with open(EVENTS_URLS_FILE, 'w', encoding='utf-8') as f:
        f.write('[')

    # Enqueue the country URLs for processing
    for url in countries_urls:
        event_queue.put(url)

    # Add termination signals to the queue BEFORE starting threads
    for _ in range(max_threads):
        event_queue.put(None)

    # Process URLs using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for _ in range(max_threads):
            executor.submit(get_event_urls_worker, base_url)

    # Wait until all tasks are done
    event_queue.join()

    # Close the JSON array in the file
    with open(EVENTS_URLS_FILE, 'a', encoding='utf-8') as f:
        f.write('\n]')

    # Print the total number of unique event URLs
    print("Event URLs collection completed. Total unique event URLs: ", len(events_urls_set))

