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
        client = storage.Client()
        bucket = client.bucket(EXCHANGE_RATE_BUCKET_NAME)

        # 1. Read currency reference file from GCS
        reference_blob_path = "raw/exchange_rates/currency_code.csv"
        local_reference_path = os.path.join(LOCAL_DIR, "currency_code.csv")

        bucket.blob(reference_blob_path).download_to_filename(local_reference_path)
        print(f"Downloaded reference file from gs://{EXCHANGE_RATE_BUCKET_NAME}/{reference_blob_path}")

        ref_df = pd.read_csv(local_reference_path)
        ref_df.columns = [c.strip() for c in ref_df.columns]

        print("Reference columns:", ref_df.columns.tolist())
        print("Reference shape:", ref_df.shape)

        country_col = "Country"
        currency_col = "Currency"
        code_col = "Code"

        if country_col not in ref_df.columns or currency_col not in ref_df.columns or code_col not in ref_df.columns:
            raise ValueError(
                f"Expected columns {country_col}, {currency_col}, {code_col}, "
                f"but got {ref_df.columns.tolist()}"
            )

        ref_df[country_col] = ref_df[country_col].astype(str).str.strip()
        ref_df[currency_col] = ref_df[currency_col].astype(str).str.strip()
        ref_df[code_col] = ref_df[code_col].astype(str).str.strip().str.upper()

        # 2. Clean invalid / unwanted codes
        invalid_codes = {
            "", "NAN", "NONE",
            "BOV", "CHE", "CHW", "COU", "MXV", "USN", "UYI", "XSU", "XUA"
        }

        ref_df = ref_df[~ref_df[code_col].isin(invalid_codes)].copy()

        ref_df = ref_df[
            ~ref_df[currency_col].str.lower().str.contains("no universal currency", na=False)
        ].copy()

        # Singapore itself does not need SGD->SGD
        ref_df = ref_df[ref_df[code_col] != "SGD"].copy()

        print("Cleaned reference shape:", ref_df.shape)

        # 3. Unique currency codes only
        codes_df = (
            ref_df[[code_col, currency_col]]
            .drop_duplicates(subset=[code_col])
            .sort_values(code_col)
            .reset_index(drop=True)
        )

        codes_df["primary_ticker"] = codes_df[code_col] + "SGD=X"
        codes_df["inverse_ticker"] = "SGD" + codes_df[code_col] + "=X"

        print("Unique currency codes to test:", len(codes_df))

        # 4. Helper: test ticker
        def ticker_has_data(ticker: str) -> bool:
            try:
                test_df = yf.download(
                    ticker,
                    start="2024-01-01",
                    end="2024-02-01",
                    progress=False,
                    auto_adjust=False,
                    threads=False
                )
                return not test_df.empty
            except Exception as e:
                print(f"Ticker test failed for {ticker}: {e}")
                return False

        # 5. Validate available tickers
        availability_records = []

        for _, row in codes_df.iterrows():
            code = row[code_col]
            currency = row[currency_col]
            primary_ticker = row["primary_ticker"]
            inverse_ticker = row["inverse_ticker"]

            print(f"Testing currency {code}...")

            primary_available = ticker_has_data(primary_ticker)
            inverse_available = False
            selected_ticker = None
            needs_inversion = False
            availability_status = "unavailable"

            if primary_available:
                selected_ticker = primary_ticker
                needs_inversion = False
                availability_status = "primary"
            else:
                inverse_available = ticker_has_data(inverse_ticker)
                if inverse_available:
                    selected_ticker = inverse_ticker
                    needs_inversion = True
                    availability_status = "inverse"

            availability_records.append({
                "code": code,
                "currency": currency,
                "primary_ticker": primary_ticker,
                "inverse_ticker": inverse_ticker,
                "primary_available": primary_available,
                "inverse_available": inverse_available,
                "selected_ticker": selected_ticker,
                "needs_inversion": needs_inversion,
                "availability_status": availability_status,
            })

        availability_df = pd.DataFrame(availability_records)
        print("Ticker availability summary:")
        print(availability_df["availability_status"].value_counts(dropna=False))

        # 6. Upload ticker availability CSV
        availability_local_path = os.path.join(LOCAL_DIR, "fx_ticker_availability.csv")
        availability_blob_path = "raw/exchange_rates/fx_ticker_availability.csv"

        availability_df.to_csv(availability_local_path, index=False)
        bucket.blob(availability_blob_path).upload_from_filename(availability_local_path)

        print(f"Uploaded gs://{EXCHANGE_RATE_BUCKET_NAME}/{availability_blob_path}")

        # 7. Download full history for available tickers
        downloaded_metadata = []

        available_fx_df = availability_df[availability_df["selected_ticker"].notna()].copy()

        for _, row in available_fx_df.iterrows():
            code = row["code"].lower()
            ticker = row["selected_ticker"]
            needs_inversion = bool(row["needs_inversion"])
            currency = row["currency"]

            dataset_name = f"{code}_sgd"

            print(f"Downloading historical FX for {ticker}...")

            fx_df = yf.download(
                ticker,
                start="1975-01-01",
                end="2026-12-31",
                progress=False,
                auto_adjust=False,
                threads=False
            )

            if fx_df.empty:
                print(f"No historical data found for {ticker}, skipping.")
                continue

            fx_df.reset_index(inplace=True)

            local_file_path = os.path.join(LOCAL_DIR, f"{dataset_name}.csv")
            fx_df.to_csv(local_file_path, index=False)

            bucket_path = f"raw/exchange_rates/{dataset_name}.csv"
            bucket.blob(bucket_path).upload_from_filename(local_file_path)

            downloaded_metadata.append({
                "dataset_name": dataset_name,
                "code": row["code"],
                "currency": currency,
                "ticker": ticker,
                "bucket_path": bucket_path,
                "needs_inversion": needs_inversion
            })

            print(f"Uploaded {local_file_path} to gs://{EXCHANGE_RATE_BUCKET_NAME}/{bucket_path}")

        # 8. Upload exchange rate metadata
        metadata_df = pd.DataFrame(downloaded_metadata)
        metadata_local_path = os.path.join(LOCAL_DIR, "exchange_rate_metadata.csv")
        metadata_blob_path = "raw/exchange_rates/exchange_rate_metadata.csv"

        metadata_df.to_csv(metadata_local_path, index=False)
        bucket.blob(metadata_blob_path).upload_from_filename(metadata_local_path)

        print(f"Uploaded gs://{EXCHANGE_RATE_BUCKET_NAME}/{metadata_blob_path}")

        return downloaded_metadata


    api_tasks = []
    for dataset_name, dataset_info in DATASETS.items():
        task_instance = download_api_dataset_and_upload(dataset_name, dataset_info["url"], dataset_info["bucket_path"])
        api_tasks.append(task_instance)
    
    kaggle_task = download_kaggle_dataset_and_upload()
    exchange_rate_task = download_exchange_rates_and_upload()
    api_tasks >> kaggle_task >> exchange_rate_task

data_ingestion_dag = data_ingestion_pipeline()