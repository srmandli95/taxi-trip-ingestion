import io
import os
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.oauth2 import service_account
from google.cloud import bigquery
from utils.logger import log
 
def get_credentials():
    key_path = "/usr/local/airflow/keys/gcp-sa.json"
    if os.path.exists(key_path):
        return service_account.Credentials.from_service_account_file(key_path)
    return None

def get_execution_date(**kwargs) -> str:
    """Get execution date, checking for override in dag_run.conf.
    
    Args:
        **kwargs: Airflow context containing ds, dag_run, execution_date, etc.
    
    Returns:
        str: Date in YYYY-MM-DD format
    """
    
    dag_run = kwargs.get('dag_run')
    if dag_run and hasattr(dag_run, 'conf') and dag_run.conf:
        override_date = dag_run.conf.get('date')
        if override_date:
            log(f"Using overridden date from DAG config: {override_date}")
            return override_date
    
    ds = kwargs.get('ds')
    if ds:
        log(f"Using Airflow execution date (ds): {ds}")
        return ds
    
    execution_date = kwargs.get('execution_date')
    if execution_date:
        result = execution_date.strftime('%Y-%m-%d')
        log(f"Using execution_date datetime: {result}")
        return result
    
    result = "2024-01-01"
    log(f"No date provided, using default historical date: {result}")
    return result

def get_airflow_var(key: str, default:str | None = None) -> str | None:
    """Prefer Airflow Variable. If not found, fall back to environment variable.
    
    Args:
        key (str): The key of the variable to retrieve.
        default (str | None): The default value to return if the variable is not found.
    Returns:
        str | None: The value of the variable, or default if not found.
    """
    try:
        val = Variable.get(key, default_var=None)
    except Exception:
        val = None
    if val is None:
        return os.environ.get(key, default)
    return val

def get_project_bucket() -> tuple[str, str]:
    """Retrieve GCP project and bucket from Airflow Variables or env vars.
    Returns:
        tuple[str, str]: (project, bucket)
    """

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
    Returns:
        str: Date string in YYYY-MM-DD format.
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
    
    Returns:
        None
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

def upload_string_to_gcs(project: str, bucket: str, content: str, path: str, mime_type: str = "text/plain", gcp_conn_id: str = "google_cloud_default") -> None:
    """
    Upload a string (JSON, CSV, etc.) to GCS using Airflow's GCSHook.
    
    Args:
        project (str): GCP project ID.
        bucket (str): GCS bucket name.
        content (str): String content to upload.
        path (str): GCS object path.
        mime_type (str): MIME type of the content.
        gcp_conn_id (str): Airflow GCP connection ID.
    """
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        object_name=path,
        data=content,
        mime_type=mime_type,
    )


def check_and_reset_partition(
    project_id: str,
    dataset:str,
    table:str,
    partition_date: str,
    expected_count: int,
) -> bool:
    """Check if a partition has expected row count; if not, delete the partition.
    
    Args:
        project_id (str): GCP project ID.
        dataset (str): BigQuery dataset name.
        table (str): BigQuery table name.
        partition_date (str): Partition date in YYYY-MM-DD format.
        expected_count (int): Expected number of rows in the partition.
    
    Returns:
        bool: True if partition was reset (deleted), False otherwise.
    """
    credentials = get_credentials()
    if not credentials:
        raise RuntimeError("GCP credentials not found. Check that /usr/local/airflow/keys/gcp-sa.json exists.")
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_id = f"{project_id}.{dataset}.{table}"
    
    
    # 1) Count existing
    count_query = f"""
        SELECT COUNT(*) AS c
        FROM {table_id}
        WHERE partition_date = DATE(@partition_date)
    """
    count_job = client.query(
        count_query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("partition_date", "DATE", partition_date)
            ]
        ),
    )
    existing_count = list(count_job.result())[0]["c"]

    if existing_count == expected_count and existing_count > 0:
        # Data is already fully loaded; skip ingest
        return False

    if existing_count > 0:
        # 2) Delete existing rows for that day
        delete_query = f"""
            DELETE FROM {table_id}
            WHERE partition_date = DATE(@partition_date)
        """
        try:
            client.query(
                delete_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("partition_date", "DATE", partition_date)
                    ]
                ),
            ).result()
        except Exception as e:
            # Handle streaming buffer error (common with insert_rows_json)
            if "streaming buffer" in str(e).lower():
                log(f"Warning: Cannot delete from streaming buffer for {table_id}. "
                    f"Skipping delete, will append (may create duplicates if rerun within 90 min).")
                return True
            raise

    # Need to (re)ingest
    return True