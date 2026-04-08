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
PUBLIC_HOLIDAY_BUCKET_NAME = os.getenv("PUBLIC_HOLIDAY_BUCKET_NAME")

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

        special_files = {
            "raw/exchange_rates/currency_code.csv": "currency_code",
            "raw/exchange_rates/fx_ticker_availability.csv": "fx_ticker_availability",
            "raw/exchange_rates/exchange_rate_metadata.csv": "exchange_rate_metadata",
        }

        for blob_path, table_name in special_files.items():
            blob = bucket.blob(blob_path)

            if not blob.exists():
                print(f"File not found, skipping: gs://{EXCHANGE_RATE_BUCKET_NAME}/{blob_path}")
                continue

            table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"
            uri = f"gs://{EXCHANGE_RATE_BUCKET_NAME}/{blob_path}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
            )

            job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            job.result()

            print(f"Loaded {blob_path} into {table_id}")

        blobs = list(bucket.list_blobs(prefix="raw/exchange_rates/"))

        skip_files = {
            "currency_code.csv",
            "fx_ticker_availability.csv",
            "exchange_rate_metadata.csv",
        }

        for blob in blobs:
            if not blob.name.endswith(".csv"):
                continue

            file_name = os.path.basename(blob.name)

            if file_name in skip_files:
                continue

            table_name = file_name.replace(".csv", "")
            table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"
            uri = f"gs://{EXCHANGE_RATE_BUCKET_NAME}/{blob.name}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
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
    
    @task
    def load_public_holidays():
        storage_client = storage.Client()
        bucket = storage_client.bucket(PUBLIC_HOLIDAY_BUCKET_NAME)
        client = bigquery.Client()

        mapping_blob_path = "raw/public_holidays/country_name_to_code.csv"
        mapping_table_id = f"{PROJECT_ID}.{RAW_DATASET}.country_name_to_code"

        mapping_blob = bucket.blob(mapping_blob_path)
        if not mapping_blob.exists():
            raise FileNotFoundError(
                f"Mapping file not found: gs://{PUBLIC_HOLIDAY_BUCKET_NAME}/{mapping_blob_path}"
            )

        mapping_uri = f"gs://{PUBLIC_HOLIDAY_BUCKET_NAME}/{mapping_blob_path}"

        mapping_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )

        mapping_job = client.load_table_from_uri(
            mapping_uri,
            mapping_table_id,
            job_config=mapping_job_config
        )
        mapping_job.result()

        print(f"Loaded mapping file into {mapping_table_id}")

        # Read mapping locally for enrichment
        local_mapping_path = os.path.join(LOCAL_DIR, "country_name_to_code.csv")
        os.makedirs(LOCAL_DIR, exist_ok=True)
        mapping_blob.download_to_filename(local_mapping_path)

        mapping_df = pd.read_csv(local_mapping_path)
        mapping_df.columns = [str(c).strip() for c in mapping_df.columns]

        if {"country_name", "country_code"}.issubset(mapping_df.columns):
            pass
        elif {"Country", "Code"}.issubset(mapping_df.columns):
            mapping_df = mapping_df.rename(columns={"Country": "country_name", "Code": "country_code"})
        elif {"name", "code"}.issubset(mapping_df.columns):
            mapping_df = mapping_df.rename(columns={"name": "country_name", "code": "country_code"})
        else:
            raise ValueError(f"Unexpected mapping columns: {mapping_df.columns.tolist()}")

        mapping_df["country_name"] = mapping_df["country_name"].astype(str).str.strip()
        mapping_df["country_code"] = mapping_df["country_code"].astype(str).str.strip().str.upper()

        code_to_name = dict(
            zip(mapping_df["country_code"], mapping_df["country_name"])
        )

        blobs = list(bucket.list_blobs(prefix="raw/public_holidays/"))
        holiday_records = []

        for blob in blobs:
            if not blob.name.endswith(".json"):
                continue

            # skip any other json file sitting at root if needed
            # expected format: raw/public_holidays/{country_code}/{year}.json
            parts = blob.name.split("/")
            if len(parts) != 4:
                print(f"Skipping unexpected JSON path: {blob.name}")
                continue

            _, _, country_code, filename = parts
            year_str = filename.replace(".json", "")

            if not year_str.isdigit():
                print(f"Skipping non-year JSON file: {blob.name}")
                continue

            local_json_path = os.path.join(LOCAL_DIR, f"{country_code}_{year_str}.json")
            blob.download_to_filename(local_json_path)

            with open(local_json_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except Exception as e:
                    print(f"Failed to parse {blob.name}: {e}")
                    continue

            if not isinstance(data, list):
                print(f"Skipping non-list JSON file: {blob.name}")
                continue

            for item in data:
                holiday_records.append({
                    "country_code": country_code,
                    "country_name": code_to_name.get(country_code),
                    "year": int(year_str),
                    "date": item.get("date"),
                    "local_name": item.get("localName"),
                    "name": item.get("name"),
                    "global_holiday": item.get("global"),
                    "counties": json.dumps(item.get("counties")) if item.get("counties") is not None else None,
                    "launch_year": item.get("launchYear"),
                    "types": json.dumps(item.get("types")) if item.get("types") is not None else None,
                })

        if not holiday_records:
            raise ValueError("No public holiday JSON records found to load.")

        holiday_df = pd.DataFrame(holiday_records)

        holiday_df["country_code"] = holiday_df["country_code"].astype(str).str.strip().str.upper()
        holiday_df["country_name"] = holiday_df["country_name"].astype(str).str.strip()
        holiday_df["year"] = pd.to_numeric(holiday_df["year"], errors="coerce").astype("Int64")
        holiday_df["date"] = pd.to_datetime(holiday_df["date"], errors="coerce").dt.date
        holiday_df["launch_year"] = pd.to_numeric(holiday_df["launch_year"], errors="coerce").astype("Int64")

        holiday_table_id = f"{PROJECT_ID}.{RAW_DATASET}.public_holidays"

        holiday_job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("country_code", "STRING"),
                bigquery.SchemaField("country_name", "STRING"),
                bigquery.SchemaField("year", "INTEGER"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("local_name", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("global_holiday", "BOOL"),
                bigquery.SchemaField("counties", "STRING"),
                bigquery.SchemaField("launch_year", "INTEGER"),
                bigquery.SchemaField("types", "STRING"),
            ],
        )

        holiday_job = client.load_table_from_dataframe(
            holiday_df,
            holiday_table_id,
            job_config=holiday_job_config
        )
        holiday_job.result()

        print(f"Loaded {len(holiday_df)} holiday records into {holiday_table_id}")

    dataset_task = ensure_dataset()
    api_tasks = []
    for dataset_name, dataset_info in DATASETS.items():
        t = load_api_dataset(dataset_name, dataset_info["bucket_path"])
        api_tasks.append(t)
    
    kaggle_task = load_kaggle_dataset()
    exchange_task = load_exchange_rates()
    distance_task = load_distance_dataset()
    public_holiday_task = load_public_holidays()
    
    dataset_task >> api_tasks >> kaggle_task >> exchange_task >> distance_task >> public_holiday_task

load_gcs_to_bigquery_dag = load_gcs_to_bigquery_pipeline()
