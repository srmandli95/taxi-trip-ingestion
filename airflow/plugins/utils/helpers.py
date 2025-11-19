import io
import os
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import logging 
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
 

def get_airflow_var(key: str, default:str | None = None) -> str | None:
    """Prefer Airflow Variable. If not found, fall back to environment variable.
    
    Args:
        key (str): The key of the variable to retrieve.
        default (str | None): The default value to return if the variable is not found.
    """
    try:
        val = Variable.get(key, default_var=None)
    except Exception:
        val = None
    if val is None:
        return os.environ.get(key, default)
    return val

def get_project_bucket() -> tuple[str, str]:
    """Retrieve GCP project and bucket from Airflow Variables or env vars."""

    project = get_airflow_var("AIRFLOW_VAR_GCP_PROJECT")
    bucket = get_airflow_var("AIRFLOW_VAR_DATA_BUCKET")
    if not project or not bucket:
        raise RuntimeError(
            "Set AIRFLOW_VAR_GCP_PROJECT and AIRFLOW_VAR_DATA_BUCKET "
            "(as Airflow Variables or env vars)."
        )
    return project, bucket

def normalize_date(ds: str) -> str:
    """Convert date string to YYYY-MM-DD format.
    Args:
        ds (str): Date string in any parsable format.
    """
    date_obj = datetime.strptime(ds, "%Y-%m-%d")
    return date_obj.strftime("%Y-%m-%d")

def upload_parquet_to_gcs(project: str, bucket: str, df, path: str, gcp_conn_id: str = "google_cloud_default") -> None:
    """
    Upload a pandas DataFrame to GCS as Parquet using Airflow's GCSHook.
    Auth comes from the Airflow connection (no ADC needed).
    
    Args:
        project (str): GCP project ID.
        bucket (str): GCS bucket name.
        df: pandas DataFrame to upload.
        path (str): GCS object path.
        gcp_conn_id (str): Airflow GCP connection ID.
    """

    if df is None:
        raise ValueError("DataFrame is None")
    
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        object_name=path,
        data=buf.getvalue(),
        mime_type="application/octet-stream",
    )




