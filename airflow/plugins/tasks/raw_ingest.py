import pandas as pd
import requests
import json
import os
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

from utils.helpers import get_project_bucket, get_airflow_var, get_credentials, get_execution_date
from utils.logger import log
from pandas_gbq import to_gbq

def ingest_trips(**kwargs):
    """Ingest NYC taxi trip data for the given date.
    
    Args:
        **kwargs: Airflow context (ds, dag_run, execution_date, etc.)
    """
    project, _ = get_project_bucket()
    base_url = get_airflow_var("AIRFLOW_VAR_TRIPS_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")
    
    
    ds = get_execution_date(**kwargs)
    log(f"Processing trips for date: {ds}")

 
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    ym_label = date_obj.strftime("%Y-%m")
    
    url = f"{base_url}/yellow_tripdata_{ym_label}.parquet"
    log(f"Downloading data from {url}")
    df = pd.read_parquet(url, engine="pyarrow")

    log(f"Filtering data for date: {ds}")
    df = df[df['tpep_pickup_datetime'].dt.date == date_obj.date()]
    log(f"Rows for {ds}: {len(df)}")

    df['partition_date'] = date_obj.date()
    
    df['ingestion_date'] = datetime.now()

    
    table_id = f"{project}.raw.yellow_trips"
    log(f"Loading {len(df)} rows to {table_id}")
    to_gbq(df, table_id, project_id=project, if_exists='append', credentials=get_credentials())

def ingest_weather(**kwargs):
    """Ingest weather data for the given date.
    
    Args:
        **kwargs: Airflow context (ds, dag_run, execution_date, etc.)
    """
    project, _ = get_project_bucket()
    
    
    ds = get_execution_date(**kwargs)
    log(f"Processing weather for date: {ds}")
    
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    
    
    lat = float(get_airflow_var("AIRFLOW_VAR_NYC_LAT", "40.71"))
    lon = float(get_airflow_var("AIRFLOW_VAR_NYC_LON", "-74.01"))
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
    
    
    row = {
        "latitude": data.get("latitude"),
        "longitude": data.get("longitude"),
        "generationtime_ms": data.get("generationtime_ms"),
        "utc_offset_seconds": data.get("utc_offset_seconds"),
        "timezone": data.get("timezone"),
        "timezone_abbreviation": data.get("timezone_abbreviation"),
        "elevation": data.get("elevation"),
        "hourly_units": data.get("hourly_units"),
        "hourly": data.get("hourly"),
        "partition_date": date_obj.strftime("%Y-%m-%d"),
        "ingestion_date": datetime.now().isoformat()
    }
    
    
    table_id = f"{project}.raw.weather"
    log(f"Loading weather data to {table_id}")
    
    credentials = get_credentials()
    client = bigquery.Client(project=project, credentials=credentials)
    
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")


def ingest_zones(**kwargs):
    """Ingest taxi zone lookup table (static)."""
    project, _ = get_project_bucket()
    url = "hta/misc/taxi_zone_lookup.csv"
    
    log(f"Fetching taxi zones from {url}...")
    df = pd.read_csv(url)
    
    df['ingestion_date'] = datetime.now()
    
    table_id = f"{project}.raw.taxi_zones"
    log(f"Loading {len(df)} zones to {table_id}")
    to_gbq(df, table_id, project_id=project, if_exists='replace', credentials=get_credentials())
