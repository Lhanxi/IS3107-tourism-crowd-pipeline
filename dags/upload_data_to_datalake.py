import pandas as pd

from airflow.sdk import dag, task
from datetime import datetime
import os
import requests
import time
from google.cloud import storage
import sys
sys.path.append('/opt/airflow/datasets')

import kagglehub
import yfinance as yf

from datasets import DATASETS
from tickers import TICKERS

# Google Cloud Storage Bucket name
API_BUCKET_NAME = os.getenv("API_BUCKET_NAME")
KAGGLE_BUCKET_NAME = os.getenv("KAGGLE_BUCKET_NAME")
EXCHANGE_RATE_BUCKET_NAME = os.getenv("EXCHANGE_RATE_BUCKET_NAME")
LOCAL_DIR = "/tmp/airflow_downloads"

HEADERS = {
    "User-Agent": "IS3107-Project/1.0"
}

@dag(
    dag_id="ingest_datasets_to_gcs",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "gcs", "hawkers", "datasets"],
)
def data_ingestion_pipeline():
        
    @task
    def download_api_dataset_and_upload(dataset_name, url, bucket_path, max_retries=10, wait_time=30):
        os.makedirs(LOCAL_DIR, exist_ok=True)
        local_file = os.path.join(LOCAL_DIR, f"{dataset_name}.json")
        print(f"{local_file} will be used for downloading {dataset_name} dataset.")

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=HEADERS, timeout=60)
                
                if response.status_code == 429:
                    print(f"Rate limited (429). Retry {attempt}/{max_retries} after {wait_time}s")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()

                with open(local_file, "wb") as f:
                    f.write(response.content)

                print(f"Downloaded file to {local_file}")
                
                if not os.path.exists(local_file):
                    raise Exception(f"Downloaded file not found: {local_file}")

                client = storage.Client()
                bucket = client.bucket(API_BUCKET_NAME)

                blob = bucket.blob(bucket_path)
                blob.upload_from_filename(local_file)

                print(f"Uploaded {local_file} to gs://{API_BUCKET_NAME}/{bucket_path}")
                return bucket_path

            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    raise
                wait_time = 2 ** attempt
                print(f"Request failed: {e}. Retry {attempt}/{max_retries} after {wait_time}s")
                time.sleep(wait_time)

        raise Exception("Failed to download file after retries")
    
    @task
    def download_kaggle_dataset_and_upload():
        os.makedirs(LOCAL_DIR, exist_ok=True)
        print("Downloading Kaggle GDP dataset...")
        kaggle_dataset_path = kagglehub.dataset_download("codebynadiia/gdp-1975-2025")
        print(f"Kaggle dataset downloaded to: {kaggle_dataset_path}")

        client = storage.Client()
        bucket = client.bucket(KAGGLE_BUCKET_NAME)
        uploaded_files = []

        for file_name in os.listdir(kaggle_dataset_path):
            local_file_path = os.path.join(kaggle_dataset_path, file_name)

            if os.path.isfile(local_file_path):
                bucket_path = f"raw/gdp_1975_2025/{file_name}"
                blob = bucket.blob(bucket_path)
                blob.upload_from_filename(local_file_path)
                uploaded_files.append(bucket_path)
                print(f"Uploaded {local_file_path} to gs://{KAGGLE_BUCKET_NAME}/{bucket_path}")

        return uploaded_files
    
    @task
    def download_exchange_rates_and_upload():
        metadata = []
        client = storage.Client()
        bucket = client.bucket(EXCHANGE_RATE_BUCKET_NAME)

        for dataset_name, ticker in TICKERS.items():
            print(f"Downloading {ticker}...")

            df = yf.download(
                ticker,
                start="1975-01-01",
                end="2026-12-31",
                progress=False,
                auto_adjust=False
            )

            if df.empty:
                print(f"No data found for {ticker}, skipping.")
                continue

            df.reset_index(inplace=True)

            local_file_path = os.path.join(LOCAL_DIR, f"{dataset_name}.csv")
            df.to_csv(local_file_path, index=False)

            bucket_path = f"raw/exchange_rates/{dataset_name}.csv"
            blob = bucket.blob(bucket_path)
            blob.upload_from_filename(local_file_path)

            needs_normalization = dataset_name.startswith("sgd_")

            metadata.append({
                "dataset_name": dataset_name,
                "ticker": ticker,
                "bucket_path": bucket_path,
                "needs_normalization": needs_normalization
            })

            print(f"Uploaded {local_file_path} to gs://{EXCHANGE_RATE_BUCKET_NAME}/{bucket_path}")

        metadata_file = os.path.join(LOCAL_DIR, "exchange_rate_metadata.csv")
        pd.DataFrame(metadata).to_csv(metadata_file, index=False)

        metadata_bucket_path = "raw/exchange_rates/exchange_rate_metadata.csv"
        bucket.blob(metadata_bucket_path).upload_from_filename(metadata_file)

        print(f"Uploaded metadata file to gs://{EXCHANGE_RATE_BUCKET_NAME}/{metadata_bucket_path}")

        return metadata


    api_tasks = []
    for dataset_name, dataset_info in DATASETS.items():
        task_instance = download_api_dataset_and_upload(dataset_name, dataset_info["url"], dataset_info["bucket_path"])
        api_tasks.append(task_instance)
    
    kaggle_task = download_kaggle_dataset_and_upload()
    exchange_rate_task = download_exchange_rates_and_upload()
    api_tasks >> kaggle_task >> exchange_rate_task

data_ingestion_dag = data_ingestion_pipeline()