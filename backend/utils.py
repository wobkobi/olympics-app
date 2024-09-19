import concurrent.futures
import queue
import requests
import random
import os
import json
import time
import threading

# Directory setup
RAW_DATA_DIR = os.path.join(os.getcwd(), "raw_data")
proxies_list = []
failed_proxies = set()
progress_lock = threading.Lock()
log_lock = threading.Lock()
url_queue = queue.Queue()

def load_proxies(proxy_file='proxies.txt', max_workers=20):
    """Load proxies from a file and check if they work by calling icanhazip.com."""
    proxies = []
    try:
        with open(proxy_file, 'r') as f:
            proxy_lines = [line.strip() for line in f if line.strip()]  # Remove empty lines
    except FileNotFoundError:
        raise FileNotFoundError(f"Proxy file '{proxy_file}' not found.")
    
    # Split and validate proxies in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_proxy, line): line for line in proxy_lines}
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                proxies.append(result)  # Only append valid proxies

    if not proxies:
        raise ValueError("No valid proxies found.")
    
    print(f"Loaded {len(proxies)} working proxies.")
    return proxies

def check_proxy(proxy_line):
    """Check if a proxy works by calling icanhazip.com."""
    parts = proxy_line.strip().split(':')
    if len(parts) == 4:
        ip, port, username, password = parts
        proxy = f"http://{username}:{password}@{ip}:{port}"
        test_url = "http://icanhazip.com"
        proxy_dict = {"http": proxy, "https": proxy}
        
        try:
            response = requests.get(test_url, proxies=proxy_dict, timeout=5)
            if response.status_code == 200:
                return proxy
            else:
                print(f"Proxy {proxy} failed with status code: {response.status_code}")
                return None
        except requests.RequestException as e:
            print(f"Proxy {proxy} failed due to an error: {e}")
            return None
    else:
        print(f"Invalid proxy format in line: {proxy_line}")
        return None


def get_random_proxy():
    """Randomly select a proxy that hasn't failed."""
    if not proxies_list:
        raise ValueError("No proxies available.")
    
    available_proxies = [proxy for proxy in proxies_list if proxy not in failed_proxies]
    
    if not available_proxies:
        raise ValueError("All proxies have failed.")
    
    proxy = random.choice(available_proxies)
    return {"http": proxy, "https": proxy}

def fetch_page(url, session):
    proxy = get_random_proxy()
    retries = 0  # Keep track of how many retries we have attempted

    while True:
        try:
            response = session.get(url, proxies=proxy)

            # Success case: return the content if the response is good
            if response.status_code == 200 and response.content:
                return response.content

            # Non-recoverable errors: no retries
            if response.status_code in [403, 404]:
                print(f"Non-recoverable error for {url}: Status code {response.status_code}")
                return None

            # Recoverable errors: retry with exponential backoff
            if response.status_code in [500, 502, 503, 504]:
                retries += 1
                wait_time = 2 ** retries  # Exponentially increasing sleep time
                print(f"Recoverable error {response.status_code} for {url}, retrying... Retry #{retries}, waiting {wait_time}s")
                time.sleep(wait_time)

        except requests.exceptions.ProxyError:
            print(f"Proxy error for proxy {proxy['http']}, marking it as failed.")
            failed_proxies.add(proxy['http'])
            # Fetch a new proxy for the next retry
            proxy = get_random_proxy()
            retries += 1
            wait_time = 2 ** retries
            print(f"Retrying with new proxy... Retry #{retries}, waiting {wait_time}s")
            time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            print(f"Request error for {url}: {e}")
            retries += 1
            wait_time = 2 ** retries
            print(f"Retrying after request error... Retry #{retries}, waiting {format_time(wait_time)}")
            time.sleep(wait_time)

# Worker function that continuously processes URLs from the queue
def worker(session):
    while True:
        url = url_queue.get()  # Get a URL from the queue
        if url is None:  # Break the loop if a sentinel value (None) is found
            break

        fetch_page(url, session)  # Fetch the page using retries
        url_queue.task_done()  # Mark the task as done in the queue

def format_time(seconds):
    """Format time (in seconds) into hours, minutes, and seconds."""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    elif minutes:
        return f"{int(minutes)}m {int(seconds)}s"
    else:
        return f"{int(seconds)}s"


def init_progress(total_count, progress_data):
    """Initialize the progress tracking for the scraping process."""
    progress_data["total"] = total_count
    progress_data["current"] = 0
    progress_data["start_time"] = time.time()
    progress_data["ema_time_per_url"] = None
    progress_data["historical_avg_time_per_url"] = None
    progress_data["last_logged_percentage"] = 0.0
    
def increment_progress(task_name, progress_data):
    """Increment the current progress count and call the print progress function."""
    progress_data["current"] += 1
    print_progress(task_name, progress_data)

    
    
def print_progress(task_name, progress_data):
    """Prints progress at specified intervals."""
    current = progress_data["current"]
    total = progress_data["total"]
    start_time = progress_data["start_time"]

    if total == 0:
        print(f"{task_name}: No progress. Total count is zero.")
        return

    percentage_done = (current / total) * 100
    elapsed_time = time.time() - start_time
    time_per_item = elapsed_time / current if current > 0 else 0
    remaining_items = total - current
    eta = remaining_items * time_per_item

    # Print progress every 1% change
    if current == total or percentage_done - progress_data.get("last_logged_percentage", 0) >= 1:
        print(f"{task_name} Progress: {percentage_done:.2f}% ({current}/{total}), ETA: {format_time(eta)}")
        progress_data["last_logged_percentage"] = percentage_done

def sanitize_urls(url_list):
    """Clean up extraneous characters from URLs."""
    return [url.strip().strip('"').strip(',') for url in url_list if url.startswith('http')]

def save_json(data, filename, append=False):
    """Save data to a JSON file, append if required."""
    if append and os.path.exists(filename):
        with open(filename, 'r+') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []
            existing_data.extend(data)
            file.seek(0)
            json.dump(existing_data, file, indent=4)
    else:
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)

def load_json(filename):
    """Load data from a JSON file."""
    with open(filename, 'r') as file:
        return json.load(file)

def log_skipped_url(url, filename):
    """Log skipped URLs to a JSON file in a thread-safe way."""
    with log_lock:
        try:
            skipped_urls = load_json(filename)
        except FileNotFoundError:
            skipped_urls = []

        skipped_urls.append(url)
        save_json(skipped_urls, filename)

proxies_list = load_proxies()
