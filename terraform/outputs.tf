output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "bucket_url" {
  description = "GCS bucket URL"
  value       = google_storage_bucket.data_lake.url
}

output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.financial_data.dataset_id
}

output "dataset_location" {
  description = "BigQuery dataset location"
  value       = google_bigquery_dataset.financial_data.location
}

output "external_tables" {
  description = "External tables created"
  value = {
    fundamentals = google_bigquery_table.stock_fundamentals_external.table_id
    prices       = google_bigquery_table.stock_prices_external.table_id
  }
}