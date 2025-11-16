from datetime import datetime, timedelta
import os

import pandas as pd
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.utils import upload_parquet_to_gcs


# ---------------- Helper ----------------
def _get_var(key: str, default: str | None = None) -> str | None:
    """Prefer Airflow Variable. If not found, fall back to environment variable."""
    try:
        val = Variable.get(key, default_var=None)
    except Exception:
        val = None
    if val is None:
        return os.environ.get(key, default)
    return val


def _get_project_bucket() -> tuple[str, str]:
    project = _get_var("AIRFLOW_VAR_GCP_PROJECT")
    bucket = _get_var("AIRFLOW_VAR_DATA_BUCKET")
    if not project or not bucket:
        raise RuntimeError(
            "Set AIRFLOW_VAR_GCP_PROJECT and AIRFLOW_VAR_DATA_BUCKET "
            "(as Airflow Variables or env vars)."
        )
    return project, bucket


# ---------------- Trips Ingest ----------------
def ingest_trips(ds: str, **kwargs):
    """Fetch NYC taxi trip data for the given date and upload to GCS."""
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    ds_final = date_obj.strftime("%Y-%m-%d")

    project, bucket = _get_project_bucket()

    url = "https://data.cityofnewyork.us/resource/gkne-dk5s.csv?$limit=5000"
    df = pd.read_csv(url)
    print(f"[trips] Fetched {len(df)} records for {ds_final}")

    gcs_path = (
        f"raw/trips/year={ds_final[:4]}/"
        f"month={ds_final[5:7]}/"
        f"day={ds_final[8:10]}/trips_{ds_final}.parquet"
    )

    upload_parquet_to_gcs(project, bucket, df, gcs_path)
    print(f"[trips] Uploaded to {gcs_path}")


# ---------------- Geo Ingest ----------------
def ingest_geo(ds: str, **kwargs):
    """Fetch NYC taxi zone lookup table and upload to GCS."""
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    ds_final = date_obj.strftime("%Y-%m-%d")

    project, bucket = _get_project_bucket()

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    df = pd.read_csv(url)
    print(f"[geo] Fetched {len(df)} taxi zone rows")

    gcs_path = f"raw/geo/{ds_final}/zones_lookup.parquet"
    upload_parquet_to_gcs(project, bucket, df, gcs_path)
    print(f"[geo] Uploaded to {gcs_path}")


# ---------------- Weather Ingest (Open-Meteo Only) ----------------
def ingest_weather_openmeteo(ds: str, **kwargs):
    """Fetch hourly temperature + precipitation for NYC from Open-Meteo."""
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    ds_final = date_obj.strftime("%Y-%m-%d")

    project, bucket = _get_project_bucket()

    start_date = ds_final
    end_date = ds_final

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.71,
        "longitude": -74.01,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation",
        "timezone": "UTC"
    }

    print(f"[weather-openmeteo] Fetching hourly: {params}")
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    hourly = data.get("hourly", {})
    if not hourly:
        print(f"[weather-openmeteo] No hourly data returned for {ds_final}")
        return

    df = pd.DataFrame(hourly)
    df["date"] = ds_final

    print(f"[weather-openmeteo] Fetched {len(df)} hourly rows")

    gcs_path = f"raw/weather_openmeteo/{ds_final}/hourly.parquet"
    upload_parquet_to_gcs(project, bucket, df, gcs_path)
    print(f"[weather-openmeteo] Uploaded to {gcs_path}")


# ---------------- Events Ingest ----------------
def ingest_events(ds: str, **kwargs):
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    ds_final = date_obj.strftime("%Y-%m-%d")

    project, bucket = _get_project_bucket()

    url = "https://data.cityofnewyork.us/resource/tvpp-9vvx.json"
    params = {"$limit": 5000}

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    events = resp.json()

    df = pd.json_normalize(events)
    print(f"[events] Fetched {len(df)} event rows")

    gcs_path = f"raw/events/{ds_final}/events.parquet"
    upload_parquet_to_gcs(project, bucket, df, gcs_path)
    print(f"[events] Uploaded to {gcs_path}")


# ---------------- POIs Ingest ----------------
def ingest_pois(ds: str, **kwargs):
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    ds_final = date_obj.strftime("%Y-%m-%d")

    project, bucket = _get_project_bucket()

    url = "https://data.cityofnewyork.us/resource/rxuy-2muj.json"
    params = {"$limit": 5000}

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    pois = resp.json()

    df = pd.json_normalize(pois)
    print(f"[pois] Fetched {len(df)} POI rows")

    gcs_path = f"raw/pois/{ds_final}/pois.parquet"
    upload_parquet_to_gcs(project, bucket, df, gcs_path)
    print(f"[pois] Uploaded to {gcs_path}")


# ---------------- DAG ----------------
dag = DAG(
    dag_id="nyc_trips_pipeline",
    description="Ingest NYC taxi trips + geo + weather (Open-Meteo) + events + POIs",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2025, 1, 2),
    catchup=True,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "date": None,
        "day_offset": 0,
    },
)

with dag:
    with TaskGroup(group_id="ingest_raw") as ingest_raw:
        ingest_trips_day = PythonOperator(
            task_id="ingest_trips_day",
            python_callable=ingest_trips,
            op_kwargs={"ds": "{{ params.date or ds }}"},
        )

        ingest_geo_day = PythonOperator(
            task_id="ingest_geo_day",
            python_callable=ingest_geo,
            op_kwargs={"ds": "{{ params.date or ds }}"},
        )

        ingest_weather_openmeteo_day = PythonOperator(
            task_id="ingest_weather_openmeteo_day",
            python_callable=ingest_weather_openmeteo,
            op_kwargs={"ds": "{{ params.date or ds }}"},
        )

        ingest_events_day = PythonOperator(
            task_id="ingest_events_day",
            python_callable=ingest_events,
            op_kwargs={"ds": "{{ params.date or ds }}"},
        )

        ingest_pois_day = PythonOperator(
            task_id="ingest_pois_day",
            python_callable=ingest_pois,
            op_kwargs={"ds": "{{ params.date or ds }}"},
        )

    ingest_raw
