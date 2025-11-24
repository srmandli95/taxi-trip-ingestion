from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from plugins.tasks import raw_ingest
from airflow import DAG
from datetime import datetime, timedelta



with DAG(
    dag_id="nyc_trips_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    params={
        "date": Param(None, type=["null", "string"], description="Override date (YYYY-MM-DD). If empty, uses execution date.")
    }
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_nyc_trips",
        python_callable=raw_ingest.ingest_trips,
    )


