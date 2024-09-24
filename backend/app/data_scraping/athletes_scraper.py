import os
import concurrent.futures
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import gzip
from app.utils import fetch_page, init_progress, increment_progress, progress_lock

# Directory setup
DATA_DIR = os.path.join(os.getcwd(), "data")
RAW_DATA_DIR = os.path.join(os.getcwd(), "raw_data")
ATHLETES_CSV = os.path.join(DATA_DIR, "athletes.csv")
ATHLETES_URLS_JSON = os.path.join(RAW_DATA_DIR, "athletes_urls.json")
ATHLETES_CONTENT_JSON_GZ = os.path.join(RAW_DATA_DIR, "athletes_content.json.gz")

# Maximum number of workers for concurrent processing
max_workers = 50  # Adjusted to a reasonable number

# Initialize progress data
progress_data = {
    "total": 0,
    "current": 0,
    "start_time": None,
    "last_logged_percentage": 0.0,
    "last_print_time": 0,  # Added this field for progress tracking
}

def get_content(url):
    """Scrape athlete content and return it as a list of dictionaries."""
    session = requests.Session()
    page_content = fetch_page(url, session)
    if not page_content:
        print(f"Error fetching {url}")
        with progress_lock:
            increment_progress("Scraping Athlete Data", progress_data)
        return []
    
    page = BeautifulSoup(page_content, "lxml")
    
    # Extract biographical data
    athlete_id = int(url.split('/')[-1])
    name_tag = page.find("h1")
    name = name_tag.text.strip() if name_tag else None
    gender, born, died, height, weight, noc, roles, image_url = None, None, None, None, None, None, None, None
    
    # Extract image URL
    img_tag = page.find("img", class_="photo")
    image_url = img_tag.get("src") if img_tag else None
    
    # Extract biographical info
    bio_section = page.find(attrs={"class": "biodata"})
    bio_summary = bio_section.find_all('tr') if bio_section else []
    noc_list = []
    roles_list = []
    
    for row in bio_summary:
        header_tag = row.find("th")
        data_tag = row.find("td")
        header = header_tag.text.strip() if header_tag else None
        data = data_tag.text.strip() if data_tag else None
        
        if header == 'Sex':
            gender = data
        elif header == 'Born':
            born = data.split('in')[0].strip()
        elif header == 'Died':
            died = data.split('in')[0].strip()
        elif header == 'Measurements':
            measurements = data.split(' / ')
            if len(measurements) == 2: 
                height, weight = measurements 
            elif "kg" in data:
                weight = data
            elif "cm" in data:
                height = data
        elif header == 'NOC':
            noc_list.extend([link.text.strip() for link in row.find_all('a')])
        elif header == 'Roles':
            roles_list.extend([role.strip() for role in data.split('•')])  # Split roles and strip whitespace
    
    noc = ', '.join(noc_list) if noc_list else None
    roles = ' • '.join(roles_list) if roles_list else None  # Join roles with separator
    
    # Extract events and positions
    results = []
    tables = page.find_all("table", class_="table")

    for table in tables:
        rows = table.find_all("tr", class_="active")
        for row in rows:
            tds = row.find_all("td")
            if len(tds) < 3:
                continue
            game_info = tds[0].text.strip() if tds[0] else None
            discipline = tds[1].text.strip() if tds[1] else None
            noc_team = tds[2].text.strip() if tds[2] else None
            
            event_row = row.find_next_sibling("tr")
            if event_row and event_row.find("a"):
                event_link = event_row.find("a")
                event = event_link.text.strip() if event_link else None
                small_tag = event_row.find('small')
                small_text = small_tag.text.strip() if small_tag else ""
                full_event = f"{event} ({small_text})"
            else:
                full_event = discipline
            
            pos = ""
            if event_row:
                event_tds = event_row.find_all("td")
                if len(event_tds) > 3 and event_tds[3]:
                    pos = event_tds[3].text.strip()
            result = {
                'id': athlete_id,
                'name': name,
                'gender': gender,
                'born': born,
                'died': died,
                'height': height,
                'weight': weight,
                'noc': noc,
                'roles': roles,
                'game': game_info,
                'team': noc_team,
                'sport': discipline,
                'event': full_event,
                'position': pos,
                'image_url': image_url
            }
            results.append(result)
    
    with progress_lock:
        increment_progress("Scraping Athlete Data", progress_data)
    return results

def scrape_athlete_data():
    """Main function to scrape athlete data and save it to CSV and JSON."""
    if not os.path.exists(ATHLETES_URLS_JSON):
        print("Athlete URLs file not found. Please ensure it exists at:", ATHLETES_URLS_JSON)
        return

    with open(ATHLETES_URLS_JSON, 'r') as file:
        athlete_urls = json.load(file)
    
    total_urls = len(athlete_urls)
    print(f"Total athlete URLs to process: {total_urls}")

    init_progress(total_urls, progress_data)
    print("Initiated progress tracking")
    
    # Define the columns and data types
    columns = [
        "id", "name", "gender", "born", "died", "height", "weight", "noc", "roles",
        "game", "team", "sport", "event", "position", "image_url"
    ]
    dtypes = {
        "id": int,
        "name": str,
        "gender": str,
        "born": str,
        "died": str,
        "height": str,
        "weight": str,
        "noc": str,
        "roles": str,
        "game": str,
        "team": str,
        "sport": str,
        "event": str,
        "position": str,
        "image_url": str
    }
    
    # Create the CSV file with headers if it doesn't exist
    if not os.path.exists(ATHLETES_CSV):
        os.makedirs(os.path.dirname(ATHLETES_CSV), exist_ok=True)
        pd.DataFrame(columns=columns).to_csv(ATHLETES_CSV, index=False)
        print(f"CSV file created at {ATHLETES_CSV}")
    else:
        print(f"CSV file already exists at {ATHLETES_CSV}")

    try:
        with gzip.open(ATHLETES_CONTENT_JSON_GZ, 'wt', encoding='utf-8') as gz_file:
            gz_file.write('[')  # Start the JSON array
            
            print("Starting data collection")

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(get_content, url): url for url in athlete_urls}
                first_entry = True
                for future in concurrent.futures.as_completed(futures):
                    url = futures[future]
                    try:
                        athlete_stats = future.result()
                        if athlete_stats:
                            # Convert to DataFrame
                            df = pd.DataFrame(athlete_stats, columns=columns)
                            
                            # Ensure data types are correct
                            for col, dtype in dtypes.items():
                                if col in df.columns:
                                    try:
                                        df[col] = df[col].astype(dtype)
                                    except ValueError:
                                        # Handle cases where conversion fails
                                        df[col] = df[col].where(df[col].notnull(), None)
                            
                            # Write to CSV
                            df.to_csv(ATHLETES_CSV, mode='a', header=False, index=False)
                            
                            # Write each athlete's data to the gzipped JSON file
                            for athlete_data in athlete_stats:
                                if not first_entry:
                                    gz_file.write(',\n')  # Add a comma before each new entry
                                json.dump(athlete_data, gz_file)
                                first_entry = False
                    except Exception as e:
                        print(f"Error processing {url}: {e}")
            
            gz_file.write('\n]')  # End the JSON array
        print(f"Scraping completed. Data saved to {ATHLETES_CSV} and {ATHLETES_CONTENT_JSON_GZ}")

    except (OSError, EOFError, json.JSONDecodeError) as e:
        print(f"Error occurred during file writing: {e}")
        print("Please check the file and ensure it is saved correctly.")
