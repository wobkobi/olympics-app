from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
import os
from url_scraping.countries import fetch_and_save_countries
from url_scraping.events import fetch_and_save_events, progress_data as event_progress_data
from url_scraping.athletes import fetch_and_save_athletes, progress_data as athlete_progress_data
from data_scraper import (
    scrape_athlete_data, 
    scrape_host_cities, 
    scrape_noc_countries, 
    scrape_athletes_roles
)
from threading import Lock

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

# Paths for data storage
DATA_DIR = os.path.join(os.getcwd(), "data")
RAW_DATA_DIR = os.path.join(os.getcwd(), "raw_data")

# Define file paths
COUNTRIES_URLS_JSON = os.path.join(RAW_DATA_DIR, "countries_urls.json")
EVENTS_URLS_JSON = os.path.join(RAW_DATA_DIR, "events_urls.json")
ATHLETES_URLS_JSON = os.path.join(RAW_DATA_DIR, "athletes_urls.json")

# Global variables to track status and progress
status_message = "Idle"
progress_percentage = 0.0
progress_lock = Lock()  # Lock for thread safety

def update_status(message):
    global status_message
    with progress_lock:
        status_message = message
        print(f"Status updated: {status_message}")

def update_progress():
    """Update the progress using progress data from all stages."""
    global progress_percentage
    with progress_lock:
        # Use the highest progress percentage from all sources
        event_progress = (event_progress_data["current"] / event_progress_data["total"]) * 100 if event_progress_data["total"] > 0 else 0
        athlete_progress = (athlete_progress_data["current"] / athlete_progress_data["total"]) * 100 if athlete_progress_data["total"] > 0 else 0
        
        # Take the higher of the three percentages
        progress_percentage = max(event_progress, athlete_progress)
        print(f"Progress: {progress_percentage:.2f}%")

def ensure_directories():
    """Ensure that the required directories and initial files exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

def check_and_run_data_pipeline():
    global status_message, progress_data
    progress_data = {
        "total": 0,
        "current": 0,
        "start_time": None,
        "ema_time_per_url": None,
        "historical_avg_time_per_url": None,
        "last_logged_percentage": 0.0,
    }

    try:
        print("Starting pipeline...")
        update_status("Checking if data exists...")

        # Ensure that the directories exist
        ensure_directories()

        # Step 1: Fetch country URLs if missing
        if not os.path.exists(COUNTRIES_URLS_JSON):
            print("Fetching country URLs...")
            update_status("Fetching country URLs...")
            fetch_and_save_countries()
            update_progress()
        else:
            print(f"Country URLs already exist at {os.path.abspath(COUNTRIES_URLS_JSON)}. Skipping country URL collection.")

        # Step 2: Fetch event URLs if missing
        if not os.path.exists(EVENTS_URLS_JSON):
            print("Fetching event URLs...")
            update_status("Fetching event URLs...")
            fetch_and_save_events()
            update_progress()
        else:
            print(f"Event URLs already exist at {os.path.abspath(EVENTS_URLS_JSON)}. Skipping event URL collection.")

        # Step 3: Fetch athlete URLs if missing
        if not os.path.exists(ATHLETES_URLS_JSON):
            print("Fetching athlete URLs...")
            update_status("Fetching athlete URLs...")
            fetch_and_save_athletes()
            update_progress()
        else:
            print(f"Athlete URLs already exist at {os.path.abspath(ATHLETES_URLS_JSON)}. Skipping athlete URL collection.")

        # Step 4: Proceed with data scraping
        print("Scraping athlete data...")
        update_status("Scraping athlete data...")
        scrape_athlete_data()
        update_progress()

        print("Scraping host cities...")
        scrape_host_cities()

        print("Scraping NOC countries...")
        scrape_noc_countries()

        print("Scraping athletes' roles...")
        scrape_athletes_roles()

        print("Data scraping completed.")
        update_status("Data scraping completed.")

    except Exception as e:
        print(f"Error occurred: {e}")
        update_status(f"Pipeline failed: {str(e)}")

# APScheduler setup to run the scraping process weekly
scheduler = BackgroundScheduler()
scheduler.add_job(check_and_run_data_pipeline, 'interval', weeks=1)
scheduler.start()

@app.on_event("startup")
def set_idle_status_on_startup():
    update_status("Idle")

@app.post("/run-data-pipeline")
def run_data_pipeline():
    print("Received manual trigger.")
    update_status("Manual trigger: Running data pipeline...")
    check_and_run_data_pipeline()
    return {"message": "Data collection and scraping pipeline triggered."}

@app.get("/status")
def get_status():
    global progress_percentage
    with progress_lock:
        return {
            "status": status_message,
            "progress": f"{progress_percentage:.2f}%"
        }

@app.on_event("shutdown")
def shutdown_event():
    update_status("Shutting down scheduler...")
    scheduler.shutdown()
    update_status("Scheduler shutdown complete.")
