from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.utils import upload_parquet_to_gcs
import pandas as pd
import requests
import os

def _get_var(key: str, default: str | None = None) -> str | None:
    """Prefer Airflow Variable, fall back to environment variable."""
    val = None
    try:
        val = Variable.get(key, default_var=None)
    except Exception:
        val = None
    if val is None:
        return os.environ.get(key, default)
    return val


def ingest_trips(ds: str, **kwargs):
    """Fetch NYC taxi trip data for the given date and upload to GCS."""
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    ds_final = date_obj.strftime("%Y-%m-%d")

    # Config
    project = _get_var("AIRFLOW_VAR_GCP_PROJECT")
    bucket = _get_var("AIRFLOW_VAR_DATA_BUCKET")
    if not project or not bucket:
        raise RuntimeError("Set AIRFLOW_VAR_GCP_PROJECT and AIRFLOW_VAR_DATA_BUCKET (Variable or env).")

    # NYC TLC sample endpoint (limit for testing)
    url = "https://data.cityofnewyork.us/resource/gkne-dk5s.csv?$limit=5000"

    # Read CSV directly from the public API
    df = pd.read_csv(url)
    print(f"Fetched {len(df)} records for {ds_final}")

    # Build partitioned path
    gcs_path = f"raw/trips/year={ds_final[:4]}/month={ds_final[5:7]}/day={ds_final[8:10]}/trips_{ds_final}.parquet"

    # Upload to GCS
    upload_parquet_to_gcs(project, bucket, df, gcs_path)
    print(f"Uploaded to {gcs_path}")


# ---- DAG ----
from datetime import datetime, timedelta
dag = DAG(
    dag_id="nyc_trips_pipeline",
    description="Ingest NYC taxi trips and weather data into GCS and BigQuery",
    schedule_interval="0 6 2 * *",   # 06:00 on the 2nd of every month
    start_date=datetime(2025, 1, 2),
    catchup=True,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    # Default params; can be overridden on trigger
    params={
        "date": None,        # e.g. "2024-01-15"; if None, uses Airflow's ds
        "city": "New York",
        "day_offset": 0
    },
)

with dag:
    ingest_trips_day = PythonOperator(
        task_id="ingest_trips_day",
        python_callable=ingest_trips,
        op_kwargs={"ds": "{{ params.date or ds }}"},
    )

