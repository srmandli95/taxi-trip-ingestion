variable "project_id" { type = string }
variable "region" { type = string default = "us-central1" }
variable "bq_location" { type = string default = "US"}
variable "name_prefix" { type= string default = "nyc" }
variable "env" { type= string default = "dev" }

variable "sa_airflow_id" { type = string default = "airflow-orchestrator" }
variable "sa_dataproc_id" { type = string default = "dataproc-runtime" }
