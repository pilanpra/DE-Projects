locals {
  data_lake_bucket = "zoomcamp-api-to-gcs"
}

variable "project" {
  description = "dm-project-407810"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "US-east-2"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset"
  type = string
  default = "BART_DailyRiders"
}

variable "TABLE_NAME_2020" {
  description = "BigQuery Table that raw data (from GCS) will be written to"
  type = string
  default = "dm-project-407810.BART_DailyRiders.BART_2020DailyRiders"
}
variable "TABLE_NAME_2021" {
  description = "BigQuery Table that raw data (from GCS) will be written to"
  type = string
  default = "dm-project-407810.BART_DailyRiders.BART_2021DailyRiders"
}
variable "TABLE_NAME_2022" {
  description = "BigQuery Table that raw data (from GCS) will be written to"
  type = string
  default = "dm-project-407810.BART_DailyRiders.BART_2022DailyRiders"
}
variable "TABLE_NAME_2023" {
  description = "BigQuery Table that raw data (from GCS) will be written to"
  type = string
  default = "dm-project-407810.BART_DailyRiders.BART_2023DailyRiders"
}
