variable "project_name" {
  description = "GCP Project ID"
  type        = string
}

variable "bucket_name" {
  description = "GCS bucket name for data lake"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "storage_class" {
  description = "Storage class for GCS bucket"
  type        = string
  default     = "STANDARD"
}

variable "dataset_name" {
  description = "BigQuery dataset name"
  type        = string
  default     = "stock_market_etl"
}

variable "credentials_file" {
  description = "Path to GCP credentials JSON file"
  type        = string
  default     = "../gcp_credentials.json"
}