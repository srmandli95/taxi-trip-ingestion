import pandas as pd
import requests
import json
import os
from datetime import datetime

from utils.helpers import get_project_bucket, upload_parquet_to_gcs, upload_string_to_gcs, normalize_date, get_airflow_var
from utils.logger import log



def ingest_trips(ds: str, **kwargs):

    """Ingest NYC taxi trip data for the given date string (ds).
    Args:
        ds (str): Date string in YYYY-MM-DD format.
    """
    
    project, bucket = get_project_bucket()
    base_url = get_airflow_var("AIRFLOW_VAR_TRIPS_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")
    
    # Check for override date in dag_run.conf
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf and 'date' in dag_run.conf:
        ds = dag_run.conf['date']
        log(f"Using overridden date from UI: {ds}")

    # Parse date from ds
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    year = date_obj.year
    month = date_obj.month
    ym_label = date_obj.strftime("%Y-%m")
    
    url = f"{base_url}/yellow_tripdata_{ym_label}.parquet"
    log(f"Downloading data from {url}")
    df = pd.read_parquet(url, engine="pyarrow")

    # Filter for the specific day
    log(f"Filtering data for date: {ds}")
    df = df[df['tpep_pickup_datetime'].dt.date == date_obj.date()]
    log(f"Rows for {ds}: {len(df)}")

    day = date_obj.day
    gcs_path = (
        f"raw/trips/year={year}/"
        f"month={month:02d}/"
        f"day={day:02d}/"
        f"yellow_tripdata_{ds}.parquet"
    )

    upload_parquet_to_gcs(project, bucket, df, gcs_path)

def ingest_weather(ds: str, **kwargs):
    """Ingest weather data for the given date."""
    project, bucket = get_project_bucket()
    
    # Check for override date in dag_run.conf
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf and 'date' in dag_run.conf:
        ds = dag_run.conf['date']
        log(f"Using overridden date from UI: {ds}")
    
    # Parse date
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    
    # Open-Meteo API
    lat = 40.71
    lon = -74.01
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": ds,
        "end_date": ds,
        "hourly": "temperature_2m,precipitation",
        "timezone": "UTC",
    }
    
    log(f"Fetching weather for {ds}...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Upload to GCS
    gcs_path = f"raw/weather/year={year}/month={month:02d}/day={day:02d}/weather_{ds}.json"
    upload_string_to_gcs(project, bucket, json.dumps(data), gcs_path, mime_type="application/json")

def ingest_events(ds: str, **kwargs):
    """Ingest NYC permitted events for the given date."""
    project, bucket = get_project_bucket()
    
    # Check for override date in dag_run.conf
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf and 'date' in dag_run.conf:
        ds = dag_run.conf['date']
        log(f"Using overridden date from UI: {ds}")
    
    # Parse date
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    
    base = "https://data.cityofnewyork.us/resource/tvpp-9vvx.json"
    token = get_airflow_var("NYC_APP_TOKEN") # Optional
    headers = {"X-App-Token": token} if token else {}
    
    # Query for events starting on this day
    where = f"start_date_time between '{ds}T00:00:00.000' and '{ds}T23:59:59.999'"
    params = {
        "$select": "*",
        "$where": where,
        "$order": "start_date_time DESC",
        "$limit": 50000
    }
    
    log(f"Fetching events for {ds}...")
    response = requests.get(base, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    
    gcs_path = f"raw/events/year={year}/month={month:02d}/day={day:02d}/events_{ds}.json"
    upload_string_to_gcs(project, bucket, json.dumps(data), gcs_path, mime_type="application/json")

def ingest_zones(**kwargs):
    """Ingest taxi zone lookup table (static)."""
    project, bucket = get_project_bucket()
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    
    log(f"Fetching taxi zones from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    content = response.text
    
    gcs_path = "raw/geo/taxi_zone_lookup.csv"
    upload_string_to_gcs(project, bucket, content, gcs_path, mime_type="text/csv")
