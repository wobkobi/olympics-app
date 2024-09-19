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
ATHLETES_URLS_FILE = os.path.join(RAW_DATA_DIR, "athletes_urls.json")

max_threads = 80
athletes_queue = queue.Queue()

# Global progress data
progress_data = {
    "total": 0,
    "current": 0,
    "start_time": None,
    "ema_time_per_url": None,
    "historical_avg_time_per_url": None,
    "last_logged_percentage": 0.0,
}


def get_athletes_urls_worker(base_url, session):
    """Worker function to process athletes URLs from the queue."""
    while True:
        event_url = athletes_queue.get()

        if event_url is None:
            # Signal the worker to exit and mark as done
            athletes_queue.task_done()
            print("Worker received termination signal.")
            break

        content = fetch_page(event_url, session)

        if content:
            game_page = BeautifulSoup(content, "lxml")
            athletes_urls = set()

            table_athletes = game_page.find("tbody").find_all("a")
            for row in table_athletes:
                if "athlete" in row["href"]:
                    athlete_url = base_url + row["href"]
                    athletes_urls.add(athlete_url)

            # Append to the file in a thread-safe manner
            with progress_lock:
                save_json(
                    list(athletes_urls), ATHLETES_URLS_FILE, append=True
                )  # Ensure it appends instead of overwriting
                increment_progress("Getting Athletes", progress_data)
        else:
            # Still increment progress for failed requests
            with progress_lock:
                increment_progress(progress_data)

        # Mark task as done for the queue
        athletes_queue.task_done()

        # Exit if no more tasks in the queue
        if athletes_queue.empty():
            break


def fetch_and_save_athletes():
    print("Fetching athlete URLs...")
    base_url = "https://www.olympedia.org"
    session = requests.Session()

    # Load event URLs from file
    events_urls_file = os.path.join(RAW_DATA_DIR, "events_urls.json")

    if not os.path.exists(events_urls_file):
        print("No event URLs file found. Please run event scraping first.")
        return

    events_urls = load_json(events_urls_file)

    # Initialize progress
    init_progress(len(events_urls), progress_data)

    # Fill the queue with event URLs
    for url in events_urls:
        athletes_queue.put(url)

    # Process event URLs and fetch athlete URLs using a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for _ in range(max_threads):
            executor.submit(get_athletes_urls_worker, base_url, session)

    # Wait until all tasks are done
    athletes_queue.join()

    # Ensure that all workers receive the stop signal
    for _ in range(max_threads):
        athletes_queue.put(None)

    print("Athlete URLs collection completed.")
