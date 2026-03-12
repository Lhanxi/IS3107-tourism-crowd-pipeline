from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import time
import requests
from google.cloud import storage

DATASET_NAME = "hawker_locations"
SOURCE_URL = "https://data.gov.sg/api/action/datastore_search?resource_id=d_68a42f09f350881996d83f9cd73ab02f"
BUCKET_NAME = os.getenv("BUCKET_NAME")

HEADERS = {
    "User-Agent": "IS3107-Project/1.0"
}


def download_file_with_retry(url, local_file, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
            
            if response.status_code == 429:
                wait_time = 2 ** attempt
                print(f"Rate limited (429). Retry {attempt}/{max_retries} after {wait_time}s")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            with open(local_file, "wb") as f:
                f.write(response.content)

            print(f"Downloaded file to {local_file}")
            return

        except requests.exceptions.RequestException as e:
            if attempt == max_retries:
                raise
            wait_time = 2 ** attempt
            print(f"Request failed: {e}. Retry {attempt}/{max_retries} after {wait_time}s")
            time.sleep(wait_time)

    raise Exception("Failed to download file after retries")


def download_and_upload():
    local_dir = "/tmp/airflow_downloads"
    os.makedirs(local_dir, exist_ok=True)

    local_file = os.path.join(local_dir, f"{DATASET_NAME}.json")

    download_file_with_retry(SOURCE_URL, local_file)

    if not os.path.exists(local_file):
        raise Exception(f"Downloaded file not found: {local_file}")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    gcs_path = f"raw/{DATASET_NAME}/{DATASET_NAME}.json"

    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file)

    print(f"Uploaded {local_file} to gs://{BUCKET_NAME}/{gcs_path}")


with DAG(
    dag_id="one_time_ingest_hawkers_to_gcs",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "gcs", "hawkers"],
) as dag:

    ingest_task = PythonOperator(
        task_id="download_and_upload_hawkers_to_gcs",
        python_callable=download_and_upload,
    )

    ingest_task