resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = "asia-southeast1"
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "staging"
  location                   = "asia-southeast1"
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "mart" {
  dataset_id                 = "mart"
  location                   = "asia-southeast1"
  delete_contents_on_destroy = false
}