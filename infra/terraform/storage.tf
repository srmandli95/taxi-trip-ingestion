resource "google_storage_bucket" "data" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  versioning { enabled = true }

 
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age            = 30
      matches_prefix = ["raw/"]        # raw -> NEARLINE after 30 days
    }
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition {
      age            = 365
      matches_prefix = ["raw/"]        # delete raw after 365 days
    }
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition {
      age            = 90
      matches_prefix = ["stg/"]        # stg is ephemeral
    }
  }

  
}
resource "google_storage_bucket_object" "folders" {
  for_each = toset(["raw/", "stg/", "curated/", "gold/", "code/"])
  name     = "${each.value}_README.txt"
  content  = "Prefix ${each.value} for NYC pipeline."
  bucket   = google_storage_bucket.data.name
}