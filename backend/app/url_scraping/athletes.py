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
ATHLETES_URLS_FILE = os.path.join(RAW_DATA_DIR, "athletes_urls.json")
EVENTS_URLS_FILE = os.path.join(RAW_DATA_DIR, "events_urls.json")

max_threads = 100  # Adjust as needed
athletes_queue = queue.Queue()
file_lock = threading.Lock()  # Lock for synchronizing file writes

# Shared variable to track if the first URL has been written
first_url_written = threading.Event()

# Global progress data
progress_data = {
    "total": 0,
    "current": 0,
    "start_time": None,
    "last_logged_percentage": 0.0,
    "last_print_time": 0,
}

def get_athletes_urls_worker(base_url):
    """Worker function to process athlete URLs from the queue."""
    session = requests.Session()  # Create a session per thread
    while True:
        event_url = athletes_queue.get()

        try:
            if event_url is None:
                # Mark the task as done and exit
                athletes_queue.task_done()
                break

            # Fetch and process the event URL
            try:
                content = fetch_page(event_url, session)

                if content:
                    game_page = BeautifulSoup(content, "lxml")
                    local_athletes_urls = set()

                    table_body = game_page.find("tbody")
                    if table_body:
                        table_athletes = table_body.find_all("a")
                        for row in table_athletes:
                            href = row.get("href", "")
                            if "athlete" in href:
                                athlete_url = base_url + href
                                local_athletes_urls.add(athlete_url)

                    if local_athletes_urls:
                        serialized_urls = [json.dumps(url) for url in local_athletes_urls]
                        with file_lock:
                            with open(ATHLETES_URLS_FILE, 'a', encoding='utf-8') as f:
                                for url_str in serialized_urls:
                                    if first_url_written.is_set():
                                        f.write(',\n' + url_str)
                                    else:
                                        f.write('\n' + url_str)
                                        first_url_written.set()
                else:
                    print(f"No content fetched for {event_url}")

            except Exception as e:
                print(f"Error processing {event_url}: {e}")

            # Increment progress after processing an event URL
            with progress_lock:
                increment_progress("Fetching Athletes", progress_data)

        finally:
            # Mark the task as done
            athletes_queue.task_done()

def fetch_and_save_athletes():
    print("Fetching athlete URLs...")
    base_url = "https://www.olympedia.org"

    if not os.path.exists(EVENTS_URLS_FILE):
        print("No event URLs file found. Please run event scraping first.")
        return

    events_urls = load_json(EVENTS_URLS_FILE)
    init_progress(len(events_urls), progress_data)

    # Initialize the JSON file with an opening bracket
    with open(ATHLETES_URLS_FILE, 'w', encoding='utf-8') as f:
        f.write('[')

    # Enqueue the event URLs for processing
    for url in events_urls:
        athletes_queue.put(url)

    # Add termination signals to the queue BEFORE starting threads
    for _ in range(max_threads):
        athletes_queue.put(None)

    # Process URLs using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for _ in range(max_threads):
            executor.submit(get_athletes_urls_worker, base_url)

    # Wait until all tasks are done
    athletes_queue.join()

    # Close the JSON array in the file
    with open(ATHLETES_URLS_FILE, 'a', encoding='utf-8') as f:
        f.write('\n]')

    # Optionally, remove duplicates after scraping
    remove_duplicate_athlete_urls()

def remove_duplicate_athlete_urls():
    """Remove duplicate athlete URLs from the JSON file."""
    print("Removing duplicate athlete URLs...")
    if os.path.exists(ATHLETES_URLS_FILE):
        with open(ATHLETES_URLS_FILE, 'r', encoding='utf-8') as f:
            # Load the JSON array
            try:
                urls = json.load(f)
            except json.JSONDecodeError:
                print("Error reading athlete URLs JSON file.")
                return

        # Remove duplicates by converting to a set, then back to a list
        unique_urls = list(set(urls))

        # Save the cleaned list back to the file
        with open(ATHLETES_URLS_FILE, 'w', encoding='utf-8') as f:
            json.dump(unique_urls, f, indent=4)

        print(f"Athletes URLs collection completed. Total unique athlete URLs: {len(unique_urls)}")
    else:
        print("Athlete URLs file does not exist.")
