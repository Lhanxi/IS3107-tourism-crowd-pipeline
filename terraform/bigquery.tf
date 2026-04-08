resource "google_bigquery_dataset" "raw" {
  project                    = var.project_id
  dataset_id                 = "raw"
  location                   = "asia-southeast1"
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "staging" {
  project                    = var.project_id
  dataset_id                 = "staging"
  location                   = "asia-southeast1"
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "mart" {
  project                    = var.project_id
  dataset_id                 = "mart"
  location                   = "asia-southeast1"
  delete_contents_on_destroy = false
}