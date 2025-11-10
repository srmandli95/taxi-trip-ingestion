

locals {
  required_apis = [
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "logging.googleapis.com",
    "artifactregistry.googleapis.com",
    "compute.googleapis.com"
  ]
}

resource "google_project_service" "enabled" {
  for_each           = toset(local.required_apis)
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}
