# BigQuery datasets for the NYC pipeline (single project, multi-region by var.bq_location)

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "stg" {
  dataset_id = "stg"
  location   = var.bq_location
}

resource "google_bigquery_dataset" "curated" {
  dataset_id = "curated"
  location   = var.bq_location
}

resource "google_bigquery_dataset" "mart" {
  dataset_id = "mart"
  location   = var.bq_location
}

# Optional ops dataset for pipeline metrics/observability
resource "google_bigquery_dataset" "ops" {
  dataset_id = "ops"
  location   = var.bq_location
}