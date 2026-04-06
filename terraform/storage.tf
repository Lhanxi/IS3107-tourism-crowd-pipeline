resource "google_storage_bucket" "data_distance_singapore" {
  name     = "data_distance_singapore"
  location = "ASIA"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_exchangerate_tourism" {
  name     = "data_exchangerate_tourism"
  location = "ASIA"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_gov_sg_tourism" {
  name     = "data_gov_sg_tourism"
  location = "ASIA-SOUTHEAST1"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_kaggle_tourism" {
  name     = "data_kaggle_tourism"
  location = "ASIA"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_monthly_visitor_data" {
  name     = "data_monthly_visitor_data"
  location = "ASIA"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "data_public_holiday" {
  name     = "data_public_holiday"
  location = "ASIA"
  uniform_bucket_level_access = true
}