from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from plugins.tasks import raw_ingest
from airflow import DAG
from datetime import datetime, timedelta
from plugins.utils.helpers import get_project_bucket, get_execution_date, get_airflow_var
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

project_id, bucket = get_project_bucket()

with DAG(
    dag_id="nyc_trips_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    params={
        "date": Param(None, type=["null", "string"], description="Override date (YYYY-MM-DD). If empty, uses execution date.")
    }
) as dag:

    with TaskGroup("ingest_raw_data") as ingest_group:
        ingest_task = PythonOperator(
            task_id="ingest_nyc_trips",
            python_callable=raw_ingest.ingest_trips,
        )

        ingest_weather_task = PythonOperator(
            task_id="ingest_weather",
            python_callable=raw_ingest.ingest_weather,
        )

        ingest_zones_task = PythonOperator(
            task_id="ingest_zones",
            python_callable=raw_ingest.ingest_zones,
        )

    with TaskGroup("data_quality_validation") as dq_group:

        validate_trips_task = DataprocCreateBatchOperator(
            task_id="validate_trips_dq",
            project_id=project_id,
            region="us-central1",
            batch_id="validate-trips-dq-{{ ts_nodash | lower }}-{{ task_instance.try_number }}",
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": f"gs://{bucket}/code/tasks/dq_validation.py",
                    #"python_file_uris": [f"gs://{bucket}/code/deps/dq_utils.zip"],
                    "args": [
                        project_id,
                        "{{ ds }}",
                    ],
                },
                "environment_config": {
                    "execution_config": {
                        "service_account": get_airflow_var("AIRFLOW_VAR_DATAPROC_RUNTIME_SA"),
                    }
                },
            },
        )




    ingest_group >> dq_group

