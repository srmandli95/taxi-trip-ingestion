# Single data bucket name
output "data_bucket" {
  value       = google_storage_bucket.data.name
  description = "Unified GCS bucket used with prefixes: raw/, stg/, curated/, gold/, code/"
}

# Dataset ids (handy for wiring Variables/Connections)
output "datasets" {
  value = {
    raw     = google_bigquery_dataset.raw.dataset_id
    stg     = google_bigquery_dataset.stg.dataset_id
    curated = google_bigquery_dataset.curated.dataset_id
    mart    = google_bigquery_dataset.mart.dataset_id
    ops     = google_bigquery_dataset.ops.dataset_id
  }
  description = "BigQuery dataset ids for each zone"
}

# (Optional but useful) Service account emails for Airflow & Dataproc runtime
# Requires service_accounts.tf to define these resources.
output "airflow_service_account_email" {
  value       = google_service_account.airflow.email
  description = "Airflow Orchestrator (Astronomer) service account email"
}

output "dataproc_service_account_email" {
  value       = google_service_account.dataproc.email
  description = "Dataproc Serverless runtime service account email"
}
