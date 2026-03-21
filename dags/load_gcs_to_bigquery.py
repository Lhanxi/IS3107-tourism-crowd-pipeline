import os
import json
import pandas as pd

from airflow.sdk import dag, task
from datetime import datetime

from google.cloud import storage
from google.cloud import bigquery

import sys
sys.path.append('/opt/airflow/datasets')

from datasets import DATASETS

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")

API_BUCKET_NAME = os.getenv("API_BUCKET_NAME")
KAGGLE_BUCKET_NAME = os.getenv("KAGGLE_BUCKET_NAME")
EXCHANGE_RATE_BUCKET_NAME = os.getenv("EXCHANGE_RATE_BUCKET_NAME")
DISTANCE_BUCKET_NAME = os.getenv("DISTANCE_BUCKET_NAME")

LOCAL_DIR = "/tmp/airflow_bq_load"

@dag(
    dag_id="load_gcs_to_bigquery",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "gcs", "raw"],
)
def load_gcs_to_bigquery_pipeline():
    @task
    def ensure_dataset():
        client = bigquery.Client()
        print(PROJECT_ID)
        dataset_id = f"{PROJECT_ID}.{RAW_DATASET}"

        try:
            client.get_dataset(dataset_id)
            print(f"Dataset {dataset_id} already exists")
        except:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "asia-southeast1"
            client.create_dataset(dataset)
            print(f"Created dataset {dataset_id}")
    
    @task
    def load_api_dataset(dataset_name, bucket_path):
        os.makedirs(LOCAL_DIR, exist_ok=True)

        storage_client = storage.Client()
        bucket = storage_client.bucket(API_BUCKET_NAME)

        local_file = os.path.join(LOCAL_DIR, f"{dataset_name}.json")
        blob = bucket.blob(bucket_path)
        blob.download_to_filename(local_file)

        print(f"Downloaded {bucket_path}")

        # Load JSON
        with open(local_file, "r") as f:
            data = json.load(f)

        # Extract records
        records = data.get("result", {}).get("records", [])

        if not records:
            raise Exception(f"No records found for {dataset_name}")

        df = pd.DataFrame(records)

        # Load into BigQuery
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{RAW_DATASET}.{dataset_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f"Loaded {dataset_name} into {table_id}")
    
    @task
    def load_kaggle_dataset():
        storage_client = storage.Client()
        bucket = storage_client.bucket(KAGGLE_BUCKET_NAME)

        client = bigquery.Client()

        blobs = list(bucket.list_blobs(prefix="raw/gdp_1975_2025/"))

        for blob in blobs:
            if not blob.name.endswith(".csv"):
                continue

            table_name = os.path.basename(blob.name).replace(".csv", "")
            table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"

            uri = f"gs://{KAGGLE_BUCKET_NAME}/{blob.name}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE"
            )

            job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            job.result()

            print(f"Loaded {blob.name} into {table_id}")
    
    @task
    def load_exchange_rates():
        storage_client = storage.Client()
        bucket = storage_client.bucket(EXCHANGE_RATE_BUCKET_NAME)

        client = bigquery.Client()

        blobs = list(bucket.list_blobs(prefix="raw/exchange_rates/"))

        for blob in blobs:
            if not blob.name.endswith(".csv"):
                continue

            # skip metadata file if you want separate handling
            if "metadata" in blob.name:
                continue

            table_name = os.path.basename(blob.name).replace(".csv", "")
            table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"

            uri = f"gs://{EXCHANGE_RATE_BUCKET_NAME}/{blob.name}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE"
            )

            job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            job.result()

            print(f"Loaded {blob.name} into {table_id}")
        
    @task
    def load_distance_dataset():
        storage_client = storage.Client()
        bucket = storage_client.bucket(DISTANCE_BUCKET_NAME)

        client = bigquery.Client()

        blobs = list(bucket.list_blobs(prefix="raw/distance_singapore/"))

        for blob in blobs:
            if not blob.name.endswith(".csv"):
                continue

            table_name = os.path.basename(blob.name).replace(".csv", "")
            table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"

            uri = f"gs://{DISTANCE_BUCKET_NAME}/{blob.name}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE"
            )

            job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            job.result()

            print(f"Loaded {blob.name} into {table_id}")

    dataset_task = ensure_dataset()
    api_tasks = []
    for dataset_name, dataset_info in DATASETS.items():
        t = load_api_dataset(dataset_name, dataset_info["bucket_path"])
        api_tasks.append(t)
    
    kaggle_task = load_kaggle_dataset()
    exchange_task = load_exchange_rates()
    distance_task = load_distance_dataset()
    
    dataset_task >> api_tasks >> kaggle_task >> exchange_task >> distance_task

load_gcs_to_bigquery_dag = load_gcs_to_bigquery_pipeline()
