import csv
import os
import threading
from fastapi import FastAPI, HTTPException, BackgroundTasks, Path, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pandas as pd
import numpy as np
import json
from typing import Iterator, List, Optional
from fastapi.responses import JSONResponse, StreamingResponse
import logging
from functools import lru_cache
from app.url_scraping.countries import fetch_and_save_countries
from app.url_scraping.events import fetch_and_save_events
from app.url_scraping.athletes import fetch_and_save_athletes
from app.data_scraping.athletes_scraper import scrape_athlete_data
from app.data_scraping.host_cities_scraper import scrape_host_cities
from app.data_scraping.noc_countries_scraper import scrape_noc_countries
from app.data_scraping.roles_scraper import extract_roles

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CORS setup - Adjusted for security
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://aws.harrisonraynes.com"],  # Allowed origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Directory setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw_data")

# Define file paths
COUNTRIES_URLS_JSON = os.path.join(RAW_DATA_DIR, "countries_urls.json")
EVENTS_URLS_JSON = os.path.join(RAW_DATA_DIR, "events_urls.json")
ATHLETES_URLS_JSON = os.path.join(RAW_DATA_DIR, "athletes_urls.json")
ATHLETES_CSV = os.path.join(DATA_DIR, "athletes.csv")
HOST_CITIES_CSV = os.path.join(DATA_DIR, "host_cities.csv")
NOC_COUNTRIES_CSV = os.path.join(DATA_DIR, "noc_countries.csv")
ATHLETES_ROLES_CSV = os.path.join(DATA_DIR, "athletes_roles.csv")

# Global variables with thread safety
status_message_lock = threading.Lock()
status_message = "Idle"

def update_status(message: str):
    global status_message
    with status_message_lock:
        status_message = message
    logger.info(f"Status updated: {status_message}")

def get_status_message():
    with status_message_lock:
        return status_message

def ensure_directories():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

def check_and_run_data_pipeline():
    try:
        logger.info("Starting pipeline...")
        update_status("Checking if data exists...")
        ensure_directories()

        if not os.path.exists(COUNTRIES_URLS_JSON):
            update_status("Fetching country URLs...")
            fetch_and_save_countries()
        else:
            logger.info(f"Skipping country URL collection. File exists: {COUNTRIES_URLS_JSON}")

        if not os.path.exists(EVENTS_URLS_JSON):
            update_status("Fetching event URLs...")
            fetch_and_save_events()
        else:
            logger.info(f"Skipping event URL collection. File exists: {EVENTS_URLS_JSON}")

        if not os.path.exists(ATHLETES_URLS_JSON):
            update_status("Fetching athlete URLs...")
            fetch_and_save_athletes()
        else:
            logger.info(f"Skipping athlete URL collection. File exists: {ATHLETES_URLS_JSON}")

        if not os.path.exists(ATHLETES_CSV):
            update_status("Scraping athlete data...")
            scrape_athlete_data()
        else:
            logger.info(f"Skipping athlete data scraping. File exists: {ATHLETES_CSV}")
        
        if not os.path.exists(HOST_CITIES_CSV):
            update_status("Scraping host cities...")
            scrape_host_cities()
        else:
            logger.info(f"Skipping host cities scraping. File exists: {HOST_CITIES_CSV}")

        if not os.path.exists(NOC_COUNTRIES_CSV):
            update_status("Scraping NOC countries...")
            scrape_noc_countries()
        else:
            logger.info(f"Skipping NOC countries scraping. File exists: {NOC_COUNTRIES_CSV}")
            
        if not os.path.exists(ATHLETES_ROLES_CSV):
            update_status("Extracting athlete roles...")
            extract_roles()
        else:
            logger.info(f"Skipping athlete roles extraction. File exists: {ATHLETES_ROLES_CSV}")
        
        update_status("Data scraping completed.")
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        update_status(f"Pipeline failed: {str(e)}")

# APScheduler setup
scheduler = AsyncIOScheduler()
scheduler.add_job(check_and_run_data_pipeline, 'interval', weeks=1)
scheduler.start()

@app.on_event("startup")
async def on_startup():
    update_status("Idle")

@app.on_event("shutdown")
async def on_shutdown():
    update_status("Shutting down scheduler...")
    scheduler.shutdown()
    update_status("Scheduler shutdown complete.")

@app.post("/run-data-pipeline")
async def run_data_pipeline(background_tasks: BackgroundTasks):
    update_status("Manual trigger: Running data pipeline...")
    background_tasks.add_task(check_and_run_data_pipeline)
    return {"message": "Data collection and scraping pipeline triggered."}

@app.get("/status")
def get_status():
    return {"status": get_status_message()}

# Caching CSV Data

@lru_cache(maxsize=10)
def load_csv_as_dataframe(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="CSV file not found")
    
    df = pd.read_csv(file_path)
    df.replace([np.inf, -np.inf], None, inplace=True)
    df = df.where(pd.notnull(df), None)
    
    return df

@app.get("/athletes")
def get_athletes(
    skip: int = Query(0, ge=0, description="Number of records to skip."),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return (max 1000)."),
    game: Optional[str] = Query(None, description="Filter by Olympic game (e.g., '2020 Summer Olympics')."),
    sport: Optional[str] = Query(None, description="Filter by sport."),
    role: Optional[str] = Query(None, description="Filter by role."),
    name: Optional[str] = Query(None, description="Filter by athlete name (partial match).")
):
    """
    Retrieve athletes data with pagination and optional filtering.
    """
    try:
        df = load_csv_as_dataframe(ATHLETES_CSV)

        # Apply filters
        if game:
            df = df[df['game'].str.lower() == game.lower()]
        if sport:
            df = df[df['sport'].str.lower().str.contains(sport.lower())]
        if role:
            df = df[df['roles'].str.lower().str.contains(role.lower())]
        if name:
            df = df[df['name'].str.lower().str.contains(name.lower())]

        total_records = len(df)
        
        # Apply pagination
        df = df.iloc[skip: skip + limit]

        return JSONResponse(content={"athletes": df.to_dict(orient="records"), "total_records": total_records})

    except Exception as e:
        logger.error(f"Error retrieving athletes data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving athletes data: {e}")

@app.get("/athletes/count")
def get_athletes_count(
    game: Optional[str] = Query(None, description="Filter by Olympic game (e.g., '2020 Summer Olympics')."),
    sport: Optional[str] = Query(None, description="Filter by sport."),
    role: Optional[str] = Query(None, description="Filter by role."),
    name: Optional[str] = Query(None, description="Filter by athlete name (partial match).")
):
    """
    Retrieve the total count of athletes based on applied filters.
    """
    try:
        df = load_csv_as_dataframe(ATHLETES_CSV)

        # Apply filters
        if game:
            df = df[df['game'].str.lower() == game.lower()]
        if sport:
            df = df[df['sport'].str.lower().str.contains(sport.lower())]
        if role:
            df = df[df['roles'].str.lower().str.contains(role.lower())]
        if name:
            df = df[df['name'].str.lower().str.contains(name.lower())]
        
        return {"total_records": len(df)}
    except Exception as e:
        logger.error(f"Error counting athletes data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error counting athletes data: {e}")

@app.get("/athletes/{athlete_id}")
def get_athlete_details(athlete_id: int = Path(..., description="The ID of the athlete to retrieve")):
    """
    Retrieve a single athlete by their ID with all associated events.
    """
    try:
        df = load_csv_as_dataframe(ATHLETES_CSV)
        athlete_df = df[df['id'] == athlete_id]

        if athlete_df.empty:
            raise HTTPException(status_code=404, detail="Athlete not found or no events available")

        # Extract event details
        athlete_record = athlete_df.iloc[0].to_dict()
        event_details = athlete_df[['game', 'sport', 'event', 'team', 'position']].to_dict(orient='records')
        
        response = {
            "athlete": {
                "id": athlete_record.get("id"),
                "name": athlete_record.get("name"),
                "gender": athlete_record.get("gender"),
                "born": athlete_record.get("born"),
                "died": athlete_record.get("died"),
                "height": athlete_record.get("height"),
                "weight": athlete_record.get("weight"),
                "noc": athlete_record.get("noc"),
                "roles": athlete_record.get("roles"),
                "image_url": athlete_record.get("image_url"),
            },
            "events": event_details
        }

        return JSONResponse(content=response)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error retrieving events for athlete {athlete_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve athlete events")

@app.get("/host-cities")
def get_host_cities():
    """
    Retrieve host cities data as a JSON array.
    """
    def stream_host_cities_as_json_array(file_path: str) -> Iterator[str]:
        try:
            df = load_csv_as_dataframe(file_path)
            records = df.to_dict(orient='records')
            yield json.dumps(records)
        except Exception as e:
            logger.error(f"Error streaming CSV file {file_path}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error streaming CSV file: {e}")

    return StreamingResponse(
        stream_host_cities_as_json_array(HOST_CITIES_CSV),
        media_type="application/json"
    )

@app.get("/noc-countries")
def get_noc_countries():
    """
    Retrieve NOC countries data as a JSON array.
    """
    def stream_noc_countries_as_json_array(file_path: str) -> Iterator[str]:
        try:
            df = load_csv_as_dataframe(file_path)
            records = df.to_dict(orient='records')
            yield json.dumps(records)
        except Exception as e:
            logger.error(f"Error streaming CSV file {file_path}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error streaming CSV file: {e}")

    return StreamingResponse(
        stream_noc_countries_as_json_array(NOC_COUNTRIES_CSV),
        media_type="application/json"
    )

@app.get("/")
def read_root():
    return {"message": "Welcome to the Olympic Data API!"}
