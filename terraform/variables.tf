locals {
  data_lake_bucket = "tfl_data_lake"
}

variable "project" {
  description = "Please Enter Your GCP Project ID"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "zone" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6-a"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
  type = string
}

variable "stg_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "tfl_data"
}

variable "prd_dataset" {
  description = "BigQuery Dataset for storing dbt production models"
  type = string
  default = "tfl_bike_dwh"
}

variable "vm_image" {
  description = "Base image for your Virtual Machine."
  type = string
  default = "ubuntu-os-cloud/ubuntu-2004-lts"
}
