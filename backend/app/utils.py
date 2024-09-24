import os
import concurrent.futures
import requests
import random
import json
import time
import threading
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Directory setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw_data")
DATA_DIR = os.path.join(BASE_DIR, "data")

proxies_list = set()
proxies_lock = threading.Lock()
reload_lock = threading.Lock()
progress_lock = threading.Lock()
failed_urls_lock = threading.Lock()

# Configuration variables
max_wait_time = 60   # Maximum wait time between retries (in seconds)
retry_delay = 5      # Delay between retries (in seconds)
max_retries = 30     # Maximum number of retries before giving up
failed_urls = []
original_proxy_count = 0

def load_proxies(max_workers=20, retry_delay=60):
    """Load proxies from the URL specified in the .env file and check their functionality."""
    global proxies_list, original_proxy_count

    proxy_url = os.getenv('PROXY_URL')
    if not proxy_url:
        raise ValueError("PROXY_URL environment variable is not set in the .env file.")

    while True:
        try:
            response = requests.get(proxy_url)
            response.raise_for_status()
            proxy_lines = {line.strip() for line in response.text.splitlines() if line.strip()}
            break  # Exit the loop if successful
        except requests.RequestException as e:
            print(f"Failed to download proxies: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    proxies = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_proxy, line): line for line in proxy_lines}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                proxies.add(result)

    if not proxies:
        raise ValueError("No valid proxies found.")

    with proxies_lock:
        proxies_list = proxies
        original_proxy_count = len(proxies_list)

    print(f"Loaded {len(proxies_list)} working proxies.")
    return proxies_list

def check_proxy(proxy_line):
    """Check if a proxy works by calling icanhazip.com."""
    parts = proxy_line.strip().split(':')
    if len(parts) == 4:
        ip, port, username, password = parts
        proxy = f"http://{username}:{password}@{ip}:{port}"
        proxy_dict = {"http": proxy, "https": proxy}

        try:
            response = requests.get("http://icanhazip.com", proxies=proxy_dict, timeout=5)
            if response.status_code == 200:
                return proxy
        except requests.RequestException:
            pass  # Ignore failed proxies
    return None

def get_random_proxy():
    global proxies_list, original_proxy_count

    with proxies_lock:
        if not proxies_list:
            proxies_list = load_proxies()
        current_proxy_count = len(proxies_list)

    if current_proxy_count < (original_proxy_count / 2):
        with reload_lock:
            with proxies_lock:
                if len(proxies_list) < (original_proxy_count / 2):
                    print("Proxy count dropped below 50%. Retesting proxies.")
                    proxies_list = load_proxies()

    if not proxies_list:
        raise ValueError("No proxies available.")

    with proxies_lock:
        proxy_choice = random.choice(list(proxies_list))
    return {"http": proxy_choice, "https": proxy_choice}

def fetch_page(url, session):
    global failed_urls
    retries = 0  # Keep track of how many retries we have attempted

    while retries < max_retries:
        proxy = get_random_proxy()
        try:
            response = session.get(url, proxies=proxy)

            # Success case: return the content if the response is good
            if response.status_code == 200 and response.content:
                return response.content

            # Handle specific HTTP errors without printing
            if response.status_code in [403, 404]:
                # Non-recoverable error; no need to retry
                return None

            if response.status_code in [500, 502, 503, 504]:
                retries += 1
                wait_time = min(1.5 ** retries, max_wait_time)
                # Only print on the last retry
                if retries == max_retries:
                    print(f"Error for {url}: Status code {response.status_code}, reached max retries ({max_retries}).")
                time.sleep(wait_time)

        except requests.exceptions.Timeout:
            retries += 1
            # Only print on the last retry
            if retries == max_retries:
                print(f"Timeout occurred for {url}, reached max retries ({max_retries}).")
            time.sleep(retry_delay)

        except requests.exceptions.ProxyError:
            retries += 1
            # Only print on the last retry
            if retries == max_retries:
                print(f"Proxy error for {url}, reached max retries ({max_retries}).")
            time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            retries += 1
            # Only print on the last retry
            if retries == max_retries:
                print(f"Request error for {url}: {e}, reached max retries ({max_retries}).")
            time.sleep(retry_delay)

    else:
        # Only print once when all retries have been exhausted
        print(f"Failed to fetch {url} after {max_retries} retries. Saving to failed_urls.json")
        with failed_urls_lock:
            failed_urls.append(url)
            save_failed_urls(failed_urls)
        return None

def save_failed_urls(failed_urls):
    """Save the failed URLs to a JSON file in the RAW_DATA_DIR directory."""
    failed_urls_file = os.path.join(RAW_DATA_DIR, 'failed_urls.json')
    save_json(failed_urls, failed_urls_file, append=False)

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

def init_progress(total, progress_data):
    """Initialize progress tracking."""
    progress_data["total"] = total
    progress_data["current"] = 0
    progress_data["start_time"] = time.time()
    progress_data["last_logged_percentage"] = 0.0
    progress_data["last_print_time"] = 0

def increment_progress(task_name, progress_data):
    """Increment the current progress count and print the progress."""
    progress_data["current"] += 1
    print_progress(task_name, progress_data)

def print_progress(task_name, progress_data):
    """Prints progress every 1% but waits at least 5 seconds between prints to avoid spamming."""
    current = progress_data["current"]
    total = progress_data["total"]
    start_time = progress_data["start_time"]

    if total == 0:
        print(f"{task_name}: No progress. Total count is zero.")
        return

    # Calculate percentage done
    percentage_done = (current / total) * 100

    # Calculate elapsed time and ETA
    elapsed_time = time.time() - start_time
    time_per_item = elapsed_time / current if current > 0 else 0
    remaining_items = total - current
    eta = remaining_items * time_per_item

    current_time = time.time()
    time_since_last_print = current_time - progress_data.get("last_print_time", 0)
    percentage_change = percentage_done - progress_data.get("last_logged_percentage", 0)

    # Print progress if at least 1% progress made and at least 5 seconds have passed
    if current == total or (percentage_change >= 1 and time_since_last_print >= 5):
        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(current_time))
        print(f"[{formatted_time}], {task_name}: {percentage_done:.2f}% ({current}/{total}), ETA: {format_time(eta)}")

        # Update last print time and percentage
        progress_data["last_print_time"] = current_time
        progress_data["last_logged_percentage"] = percentage_done

def save_json(data, filename, append=False):
    """Save data to a JSON file, appending if required."""
    if append and os.path.exists(filename):
        with open(filename, 'r+', encoding='utf-8') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []
            if isinstance(existing_data, list):
                existing_data.extend(data)
            else:
                existing_data = data
            file.seek(0)
            json.dump(existing_data, file, indent=4)
            file.truncate()
    else:
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)

def load_json(filename):
    """Load data from a JSON file."""
    with open(filename, 'r', encoding='utf-8') as file:
        return json.load(file)

# Initialize proxies once at the beginning
proxies_list = load_proxies()
