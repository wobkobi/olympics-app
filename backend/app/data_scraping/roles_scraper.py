import os
import gzip
import json
import pandas as pd
from app.utils import init_progress, increment_progress, progress_lock

# Directory setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw_data")
ATHLETES_ROLES_CSV = os.path.join(DATA_DIR, "athletes_roles.csv")
ATHLETES_CONTENT_JSON_GZ = os.path.join(RAW_DATA_DIR, "athletes_content.json.gz")

# Initialize progress data
progress_data = {
    "total": 0,
    "current": 0,
    "start_time": None,
    "last_logged_percentage": 0.0,
    "last_print_time": 0,
}

def extract_roles():
    """Extract roles from athlete content and save them to a CSV file."""
    if not os.path.exists(ATHLETES_CONTENT_JSON_GZ):
        print(f"Error: {ATHLETES_CONTENT_JSON_GZ} file not found. Cannot extract athlete roles.")
        return

    print(f"Reading data from {ATHLETES_CONTENT_JSON_GZ}")

    # Read the GZIP JSON file
    try:
        with gzip.open(ATHLETES_CONTENT_JSON_GZ, "rt", encoding="utf-8") as file:
            # Read JSON array from gzipped file
            athletes_content = json.load(file)
    except (OSError, EOFError, json.JSONDecodeError) as e:
        print(f"Error occurred: {e}")
        print("The GZIP file might be corrupted or improperly formatted. Please verify the file and try again.")
        return

    total_athletes = len(athletes_content)
    init_progress(total_athletes, progress_data)

    all_athletes_roles = []

    for athlete_data in athletes_content:
        roles = athlete_data.get('roles')
        if roles:
            athlete_roles = {
                'id': athlete_data.get('id'),
                'name': athlete_data.get('name'),
                'roles': roles
            }
            all_athletes_roles.append(athlete_roles)

        with progress_lock:
            increment_progress("Extracting Athlete Roles", progress_data)

    if all_athletes_roles:
        # Ensure the data directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        athletes_roles_df = pd.DataFrame(all_athletes_roles)
        athletes_roles_df.to_csv(ATHLETES_ROLES_CSV, index=False)
        print(f"Athletes roles data saved to {ATHLETES_ROLES_CSV}")
    else:
        print("No athletes with roles found.")