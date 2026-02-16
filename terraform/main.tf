terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_name
  region      = var.region
  credentials = file(var.credentials_file)
}

resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  storage_class = var.storage_class
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = false
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
  
  force_destroy = true
}

resource "google_bigquery_dataset" "financial_data" {
  dataset_id    = var.dataset_name
  location      = var.region
  description   = "Dataset for stock market fundamental and pricing data"
  
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "stock_fundamentals_external" {
  dataset_id = google_bigquery_dataset.financial_data.dataset_id
  table_id   = "stock_fundamentals_external"
  
  deletion_protection = false
  
  external_data_configuration {
    source_format = "PARQUET"
    autodetect    = true
    source_uris = [
      "gs://${var.bucket_name}/transformed/fundamentals/*.parquet"
    ]
    
    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${var.bucket_name}/transformed/fundamentals/"
    }
  }
}

resource "google_bigquery_table" "stock_prices_external" {
  dataset_id = google_bigquery_dataset.financial_data.dataset_id
  table_id   = "stock_prices_external"
  
  deletion_protection = false
  
  external_data_configuration {
    source_format = "PARQUET"
    autodetect    = true
    source_uris = [
      "gs://${var.bucket_name}/transformed/prices/*.parquet"
    ]
    
    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${var.bucket_name}/transformed/prices/"
    }
  }
}