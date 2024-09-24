import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from app.utils import init_progress, increment_progress, progress_lock

# Directory setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
HOST_CITIES_CSV = os.path.join(DATA_DIR, "host_cities.csv")

# Initialize progress data
progress_data = {
    "total": 0,
    "current": 0,
    "start_time": None,
    "last_logged_percentage": 0.0,
    "last_print_time": 0,
}

def get_host_cities(season_table_element, season):
    """Extract host cities from the given season table."""
    host_cities = []
    rows = season_table_element.find_all("tr")[1:]  # Skip header row
    total_rows = len(rows)
    init_progress(total_rows, progress_data)

    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 3:
            year = cells[1].text.strip()
            host_city = cells[2].text.strip()
            game = {
                "year": year,
                "season": season,
                "game": f"{year} {season} Olympics",
                "host_city": host_city,
            }
            host_cities.append(game)
        else:
            print(f"Warning: Skipping a row due to insufficient data: {row}")

        with progress_lock:
            increment_progress(f"Scraping Host Cities ({season})", progress_data)

    return host_cities

def scrape_host_cities():
    """Scrape host cities and save them to a CSV file."""
    base_url = "https://www.olympedia.org/editions"
    print(f"Fetching data from {base_url}")

    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching {base_url}: {e}")
        return

    soup = BeautifulSoup(response.content, "lxml")
    tables = soup.find_all("table")
    if len(tables) < 2:
        print("Error: Expected at least two tables for Summer and Winter Olympics.")
        return

    summer_table = tables[0]
    winter_table = tables[1]

    all_host_cities = []

    print("Scraping Summer Olympics host cities...")
    summer_host_cities = get_host_cities(summer_table, "Summer")
    all_host_cities.extend(summer_host_cities)

    print("Scraping Winter Olympics host cities...")
    winter_host_cities = get_host_cities(winter_table, "Winter")
    all_host_cities.extend(winter_host_cities)

    if all_host_cities:
        # Ensure the data directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        host_cities_df = pd.DataFrame(all_host_cities)
        host_cities_df.to_csv(HOST_CITIES_CSV, index=False)
        print(f"Host cities data saved to {HOST_CITIES_CSV}")
    else:
        print("No host cities data found.")

