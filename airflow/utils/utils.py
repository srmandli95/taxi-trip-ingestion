# utils/utils.py
import io
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def upload_parquet_to_gcs(project: str, bucket: str, df, path: str, gcp_conn_id: str = "google_cloud_default"):
    """
    Upload a pandas DataFrame to GCS as Parquet using Airflow's GCSHook.
    Auth comes from the Airflow connection (no ADC needed).
    """
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
