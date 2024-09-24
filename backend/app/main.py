# main.py

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
from app.url_scraping.countries import fetch_and_save_countries
from app.url_scraping.events import fetch_and_save_events
from app.url_scraping.athletes import fetch_and_save_athletes
from app.data_scraping.athletes_scraper import scrape_athlete_data
from app.data_scraping.host_cities_scraper import scrape_host_cities
from app.data_scraping.noc_countries_scraper import scrape_noc_countries

import logging

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CORS setup - Adjusted for security
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Consider specifying allowed origins in production
    allow_credentials=True,
    allow_methods=["GET", "POST"],  # Restrict methods if possible
    allow_headers=["*"],
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

    - **skip**: Number of records to skip.
    - **limit**: Number of records to retrieve (max 1000).
    - **game**: Filter by Olympic game (e.g., '2020 Summer Olympics').
    - **sport**: Filter by sport.
    - **role**: Filter by role.
    - **name**: Filter by athlete name (partial match).
    """
    if not os.path.exists(ATHLETES_CSV):
        raise HTTPException(status_code=404, detail="CSV file not found")

    try:
        # Define a generator to stream records
        def record_generator():
            df_iter = pd.read_csv(ATHLETES_CSV, chunksize=1000)
            for df_chunk in df_iter:
                # Clean the data
                df_chunk = df_chunk.where(pd.notnull(df_chunk), None)
                df_chunk.replace([np.inf, -np.inf], None, inplace=True)
                
                # Apply filters
                if game:
                    df_chunk = df_chunk[df_chunk['game'].str.lower() == game.lower()]
                if sport:
                    df_chunk = df_chunk[df_chunk['sport'].str.lower().str.contains(sport.lower())]
                if role:
                    df_chunk = df_chunk[df_chunk['roles'].str.lower().str.contains(role.lower())]
                if name:
                    df_chunk = df_chunk[df_chunk['name'].str.lower().str.contains(name.lower())]
                
                records = df_chunk.to_dict(orient='records')
                for record in records:
                    yield record

        # Create a generator with filters applied
        filtered_records = record_generator()

        # Skip the first 'skip' records
        for _ in range(skip):
            try:
                next(filtered_records)
            except StopIteration:
                break  # Less records than skip

        # Collect the next 'limit' records
        athletes_list = []
        for _ in range(limit):
            try:
                record = next(filtered_records)
                athletes_list.append(record)
            except StopIteration:
                break  # No more records

        return JSONResponse(content={"athletes": athletes_list})

    except Exception as e:
        logger.error(f"Error streaming filtered CSV file {ATHLETES_CSV}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error streaming CSV file: {e}")

@app.get("/athletes/count")
def get_athletes_count(
    game: Optional[str] = Query(None, description="Filter by Olympic game (e.g., '2020 Summer Olympics')."),
    sport: Optional[str] = Query(None, description="Filter by sport."),
    role: Optional[str] = Query(None, description="Filter by role."),
    name: Optional[str] = Query(None, description="Filter by athlete name (partial match).")
):
    """
    Retrieve the total count of athletes based on applied filters.

    - **game**: Filter by Olympic game (e.g., '2020 Summer Olympics').
    - **sport**: Filter by sport.
    - **role**: Filter by role.
    - **name**: Filter by athlete name (partial match).
    """
    if not os.path.exists(ATHLETES_CSV):
        raise HTTPException(status_code=404, detail="CSV file not found")
    try:
        count = 0
        df_iter = pd.read_csv(ATHLETES_CSV, chunksize=1000)
        for df_chunk in df_iter:
            # Clean the data
            df_chunk = df_chunk.where(pd.notnull(df_chunk), None)
            df_chunk.replace([np.inf, -np.inf], None, inplace=True)
            
            # Apply filters
            if game:
                df_chunk = df_chunk[df_chunk['game'].str.lower() == game.lower()]
            if sport:
                df_chunk = df_chunk[df_chunk['sport'].str.lower().str.contains(sport.lower())]
            if role:
                df_chunk = df_chunk[df_chunk['roles'].str.lower().str.contains(role.lower())]
            if name:
                df_chunk = df_chunk[df_chunk['name'].str.lower().str.contains(name.lower())]
            
            count += len(df_chunk)
        
        return {"total_records": count}
    except Exception as e:
        logger.error(f"Error counting filtered athletes: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to count athletes")

@app.get("/athletes/{athlete_id}")
def get_athlete_details(athlete_id: int = Path(..., description="The ID of the athlete to retrieve")):
    """
    Retrieve a single athlete by their ID with all associated events.

    - **athlete_id**: The unique ID of the athlete.
    """
    if not os.path.exists(ATHLETES_CSV):
        raise HTTPException(status_code=404, detail="CSV file not found")

    try:
        events = []
        athlete_record = None
        df_iter = pd.read_csv(ATHLETES_CSV, chunksize=1000)
        for df_chunk in df_iter:
            # Clean the data
            df_chunk = df_chunk.where(pd.notnull(df_chunk), None)
            df_chunk.replace([np.inf, -np.inf], None, inplace=True)
            
            # Filter for the athlete
            filtered = df_chunk[df_chunk['id'] == athlete_id]
            if not filtered.empty:
                if not athlete_record:
                    # Assuming all records have the same athlete details
                    athlete_record = filtered.iloc[0].to_dict()
                # Extract event details
                event_details = filtered[['game', 'sport', 'event', 'team', 'position']].to_dict(orient='records')
                events.extend(event_details)
        
        if not athlete_record:
            raise HTTPException(status_code=404, detail="Athlete not found or no events available")
        
        # Structure the response
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
            "events": events
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
    if not os.path.exists(HOST_CITIES_CSV):
        raise HTTPException(status_code=404, detail="CSV file not found")
    
    def stream_host_cities_as_json_array(file_path: str) -> Iterator[str]:
        try:
            yield "["  # Start of JSON array
            first = True
            with open(file_path, mode='r', encoding='utf-8') as csvfile:
                reader = pd.read_csv(csvfile, chunksize=1000)
                for df_chunk in reader:
                    # Replace NaN with None and infinite values with None
                    df_chunk = df_chunk.where(pd.notnull(df_chunk), None)
                    df_chunk.replace([np.inf, -np.inf], None, inplace=True)
                    records = df_chunk.to_dict(orient='records')
                    for record in records:
                        if not first:
                            yield ","  # Separator between JSON objects
                        else:
                            first = False
                        yield json.dumps(record)
            yield "]"  # End of JSON array
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
    if not os.path.exists(NOC_COUNTRIES_CSV):
        raise HTTPException(status_code=404, detail="CSV file not found")
    
    def stream_noc_countries_as_json_array(file_path: str) -> Iterator[str]:
        try:
            yield "["  # Start of JSON array
            first = True
            with open(file_path, mode='r', encoding='utf-8') as csvfile:
                reader = pd.read_csv(csvfile, chunksize=1000)
                for df_chunk in reader:
                    # Replace NaN with None and infinite values with None
                    df_chunk = df_chunk.where(pd.notnull(df_chunk), None)
                    df_chunk.replace([np.inf, -np.inf], None, inplace=True)
                    records = df_chunk.to_dict(orient='records')
                    for record in records:
                        if not first:
                            yield ","  # Separator between JSON objects
                        else:
                            first = False
                        yield json.dumps(record)
            yield "]"  # End of JSON array
        except Exception as e:
            logger.error(f"Error streaming CSV file {file_path}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error streaming CSV file: {e}")

    return StreamingResponse(
        stream_noc_countries_as_json_array(NOC_COUNTRIES_CSV),
        media_type="application/json"
    )
