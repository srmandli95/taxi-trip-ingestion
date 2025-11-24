import pandas as pd
import requests
from datetime import datetime

from utils.helpers import get_project_bucket, upload_parquet_to_gcs, normalize_date, get_airflow_var
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

    gcs_path = (
        f"raw/trips/year={year}/"
        f"month={month:02d}/"
        f"yellow_tripdata_{ym_label}.parquet"
    )

    upload_parquet_to_gcs(project, bucket, df, gcs_path)
