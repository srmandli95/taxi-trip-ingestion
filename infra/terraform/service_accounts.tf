resource "google_service_account" "airflow" {
  account_id   = "airflow-orchestrator"
  display_name = "Airflow Orchestrator (Astronomer)"
}

resource "google_service_account" "dataproc" {
  account_id   = "dataproc-runtime"
  display_name = "Dataproc Serverless Runtime"
}
