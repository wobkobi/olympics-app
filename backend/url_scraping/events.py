import os
import concurrent.futures
import requests
import queue
from bs4 import BeautifulSoup
from utils import (
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

max_threads = 80
event_queue = queue.Queue()

# Global progress data
progress_data = {
    "total": 0,
    "current": 0,
    "start_time": None,
    "ema_time_per_url": None,
    "historical_avg_time_per_url": None,
    "last_logged_percentage": 0.0,
}


def get_event_urls_worker(base_url, session):
    """Worker function to process event URLs from the queue."""
    while True:
        country_url = event_queue.get()

        if country_url is None:
            # Signal the worker to exit and mark as done
            event_queue.task_done()
            print("Worker received termination signal.")
            break

        content = fetch_page(country_url, session)

        if content:
            country_page = BeautifulSoup(content, "lxml")
            events_urls = set()

            if country_page.find("tbody"):
                for game in country_page.find("tbody").find_all("tr"):
                    event_url = base_url + game.find_all("a")[1]["href"]
                    events_urls.add(event_url)

            # Safely save the events URLs and handle progress
            with progress_lock:
                save_json(
                    list(events_urls), EVENTS_URLS_FILE, append=True
                )  # Ensure it appends instead of overwriting
                increment_progress("Getting Events", progress_data)

        else:
            # Still increment progress for failed requests
            with progress_lock:
                increment_progress(progress_data)

        # Mark task as done for the queue
        event_queue.task_done()

        # Exit if no more tasks in the queue
        if event_queue.empty():
            break


def fetch_and_save_events():
    print("Fetching event URLs...")
    base_url = "https://www.olympedia.org"
    session = requests.Session()

    # Load country URLs from file
    countries_urls_file = os.path.join(RAW_DATA_DIR, "countries_urls.json")

    if not os.path.exists(countries_urls_file):
        print("No countries URLs file found. Please run country scraping first.")
        return

    countries_urls = load_json(countries_urls_file)

    # Initialize progress
    init_progress(len(countries_urls), progress_data)

    # Fill the queue with URLs
    for url in countries_urls:
        event_queue.put(url)

    # Process URLs using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for _ in range(max_threads):
            executor.submit(get_event_urls_worker, base_url, session)

    # Wait until all tasks are done
    event_queue.join()

    # Send stop signals to all workers
    for _ in range(max_threads):
        event_queue.put(None)

    print("Event URLs collection completed.")
