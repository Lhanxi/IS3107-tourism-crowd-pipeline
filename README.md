# IS3107 Tourism Crowd Pipeline

## Project Overview
This project is developed for IS3107 Data Engineering and focuses on building an end-to-end data pipeline to analyze tourism-related datasets and generate insights for crowd prediction in Singapore.

The pipeline uses:
- Airflow → workflow orchestration
- Google Cloud Storage (GCS) → data lake
- BigQuery → data warehouse
- Python (Pandas, APIs) → data processing

### Team Members
- Leung Han Xi
- Josiah Praeman Yang En
- Leow Kang You

---

## Project Architecture

The pipeline follows a standard data engineering flow:

Data Sources → GCS (Raw Layer) → BigQuery (Raw Tables)
→ Transform (Airflow) → BigQuery (Staging Layer)
→ Analytics / ML / Visualization

---

## Project Structure

```text
projectroot/
│
├── dags/
│   ├── upload_data_to_datalake.py
│   ├── load_gcs_to_bigquery.py
│   ├── transform_raw_to_staging.py
│   └── create_final_features_dag.py
│
├── datasets/
│   └── datasets.py
│
├── notebooks/
│   ├── data_loading.ipynb
│   ├── eda_features.ipynb
│   ├── feature_engineering.ipynb
│   ├── hotel_aircraft_traffic.ipynb
│   ├── lag_regression.ipynb
│   └── time_series_analysis.ipynb
│
├── config/
│   └── airflow.cfg
│
├── keys/
│   └── gcp-key.json
│
├── logs/
├── plugins/
│
├── .env
├── docker-compose.yaml
├── airflow-setup.md
├── README.md
└── .gitignore
```

---

## Pipeline Breakdown (DAGs)

### 1. upload_data_to_datalake.py
Purpose: Data Ingestion
- Extracts data from APIs and Kaggle
- Uploads raw files to GCS

Output:
GCS (raw files)

---

### 2. load_gcs_to_bigquery.py
Purpose: Raw Layer Loading
- Reads raw files from GCS
- Loads into BigQuery raw tables
- One table per dataset

Output:
BigQuery (raw dataset)

---

### 3. transform_raw_to_staging.py
Purpose: Data Transformation
- Cleans and standardizes data
- Handles missing values, formatting, and dates
- Produces structured tables for analysis

Output:
BigQuery (staging dataset)

---

## Setup Instructions

### Step 1: Add GCP Key
Unzip the keys folder and place:
projectroot/keys/gcp-key.json

---

### Step 2: Add Environment File

Create:
projectroot/.env

Example:
GCP_PROJECT_ID=your_project_id
BQ_RAW_DATASET=raw
BQ_STAGING_DATASET=staging

API_BUCKET_NAME=your_api_bucket
KAGGLE_BUCKET_NAME=your_kaggle_bucket
EXCHANGE_RATE_BUCKET_NAME=your_exchange_bucket

---

### Step 3: Start Airflow

docker compose up -d

---

### Step 4: Access Airflow UI

URL: http://localhost:8080
Username: airflow
Password: airflow

---

### Step 5: Run DAGs (Recommended Order)

1. upload_data_to_datalake
2. load_gcs_to_bigquery
3. transform_raw_to_staging

---

### Step 6: Shut Down

docker compose down

---

## Key Notes

- Each dataset follows:
  1 GCS file → 1 BigQuery raw table
- Transformations handled in Airflow
- Modular and scalable pipeline
- Supports analytics and ML use cases

---

## Terraform (Optional)
```bash
terraform init
terraform plan
terraform apply
```