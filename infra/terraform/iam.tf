resource "google_storage_bucket_iam_member" "bucket_admin_all" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_storage_bucket_iam_member" "bucket_admin_dataproc" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc.email}"
}

# OPTIONAL: If you want to restrict by prefix using IAM Conditions,
# grant Viewer for raw/ only to Dataproc, and Writer only to stg/curated/gold/.
# (Adjust as needed; this is an example showing conditions.)

# Read raw/* only
resource "google_storage_bucket_iam_member" "dataproc_view_raw_only" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dataproc.email}"

  condition {
    title       = "read_raw_prefix"
    description = "Allow reads under raw/"
    expression  = "resource.name.startsWith('projects/_/buckets/${var.bucket_name}/objects/raw/')"
  }
}

# Dataproc: write access to stg/, curated/, and gold/ using ONE binding
resource "google_storage_bucket_iam_member" "dataproc_write_stg_curated_gold" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc.email}"

  condition {
    title       = "write_stg_curated_gold_prefixes"
    description = "Allow writes under stg/, curated/, and gold/ only"
    expression  = "resource.name.startsWith('projects/_/buckets/${var.bucket_name}/objects/stg/') || resource.name.startsWith('projects/_/buckets/${var.bucket_name}/objects/curated/') || resource.name.startsWith('projects/_/buckets/${var.bucket_name}/objects/gold/')"
  }
}

# Grant BigQuery Data Editor role to Airflow service account
resource "google_project_iam_member" "airflow_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# Grant BigQuery Job User role to Airflow service account
resource "google_project_iam_member" "airflow_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_service_account_iam_member" "airflow_can_use_dataproc_runtime" {
  service_account_id = google_service_account.dataproc.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.airflow.email}"
}

# Dataproc runtime service account needs Dataproc Worker role
resource "google_project_iam_member" "dataproc_runtime_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}


# Dataproc: BigQuery data editor
resource "google_project_iam_member" "dataproc_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}

# Dataproc: BigQuery job user
resource "google_project_iam_member" "dataproc_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}

# Dataproc: allow creating BigQuery read sessions (for Storage Read API)
resource "google_project_iam_member" "dataproc_bq_read_session_user" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}