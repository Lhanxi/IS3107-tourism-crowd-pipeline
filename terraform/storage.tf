resource "google_storage_bucket" "data_distance_singapore" {
  name     = "${var.project_id}-distance-sg"
  location = "ASIA-SOUTHEAST1"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_exchangerate_tourism" {
  name     = "${var.project_id}-exchange-rate"
  location = "ASIA-SOUTHEAST1"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_gov_sg_tourism" {
  name     = "${var.project_id}-gov-sg"
  location = "ASIA-SOUTHEAST1"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_kaggle_tourism" {
  name     = "${var.project_id}-kaggle"
  location = "ASIA-SOUTHEAST1"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_public_holiday" {
  name     = "${var.project_id}-public-holiday"
  location = "ASIA-SOUTHEAST1"
  uniform_bucket_level_access = true
}