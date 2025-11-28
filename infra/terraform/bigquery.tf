# BigQuery datasets for the NYC pipeline (single project, multi-region by var.bq_location)

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "raw_yellow_trips" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "yellow_trips"
  
  time_partitioning {
    type  = "DAY"
    field = "partition_date"
  }

  schema = <<EOF
[
  {"name": "VendorID", "type": "INTEGER"},
  {"name": "tpep_pickup_datetime", "type": "TIMESTAMP"},
  {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP"},
  {"name": "passenger_count", "type": "INTEGER"},
  {"name": "trip_distance", "type": "FLOAT"},
  {"name": "RatecodeID", "type": "INTEGER"},
  {"name": "store_and_fwd_flag", "type": "STRING"},
  {"name": "PULocationID", "type": "INTEGER"},
  {"name": "DOLocationID", "type": "INTEGER"},
  {"name": "payment_type", "type": "INTEGER"},
  {"name": "fare_amount", "type": "FLOAT"},
  {"name": "extra", "type": "FLOAT"},
  {"name": "mta_tax", "type": "FLOAT"},
  {"name": "tip_amount", "type": "FLOAT"},
  {"name": "tolls_amount", "type": "FLOAT"},
  {"name": "improvement_surcharge", "type": "FLOAT"},
  {"name": "total_amount", "type": "FLOAT"},
  {"name": "congestion_surcharge", "type": "FLOAT"},
  {"name": "airport_fee", "type": "FLOAT"},
  {"name": "cbd_congestion_fee", "type": "FLOAT"},
  {"name": "partition_date", "type": "DATE"},
  {"name": "ingestion_date", "type": "TIMESTAMP"}
]
EOF
}

resource "google_bigquery_table" "raw_weather" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "weather"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "partition_date"
  }

  schema = <<EOF
[
  {"name": "latitude", "type": "FLOAT"},
  {"name": "longitude", "type": "FLOAT"},
  {"name": "generationtime_ms", "type": "FLOAT"},
  {"name": "utc_offset_seconds", "type": "INTEGER"},
  {"name": "timezone", "type": "STRING"},
  {"name": "timezone_abbreviation", "type": "STRING"},
  {"name": "elevation", "type": "FLOAT"},
  {
    "name": "hourly_units",
    "type": "RECORD",
    "fields": [
      {"name": "time", "type": "STRING"},
      {"name": "temperature_2m", "type": "STRING"},
      {"name": "precipitation", "type": "STRING"}
    ]
  },
  {
    "name": "hourly",
    "type": "RECORD",
    "fields": [
      {"name": "time", "type": "STRING", "mode": "REPEATED"},
      {"name": "temperature_2m", "type": "FLOAT", "mode": "REPEATED"},
      {"name": "precipitation", "type": "FLOAT", "mode": "REPEATED"}
    ]
  },
  {"name": "partition_date", "type": "DATE"},
  {"name": "ingestion_date", "type": "TIMESTAMP"}
]
EOF
}

resource "google_bigquery_table" "raw_taxi_zones" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "taxi_zones"

  schema = <<EOF
[
  {"name": "LocationID", "type": "INTEGER"},
  {"name": "Borough", "type": "STRING"},
  {"name": "Zone", "type": "STRING"},
  {"name": "service_zone", "type": "STRING"},
  {"name": "ingestion_date", "type": "TIMESTAMP"}
]
EOF
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

resource "google_bigquery_dataset" "quarantine" {
  dataset_id                 = "quarantine"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "dq_failures" {
  dataset_id = google_bigquery_dataset.quarantine.dataset_id
  table_id   = "dq_failures"

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingestion_date"
  }

  schema = <<EOF
[
  {"name": "source_table", "type": "STRING"},
  {"name": "failure_reason", "type": "STRING"},
  {"name": "record_content", "type": "STRING"},
  {"name": "ingestion_date", "type": "DATE"},
  {"name": "ingestion_timestamp", "type": "TIMESTAMP"}
]
EOF
}