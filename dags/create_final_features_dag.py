import os
from datetime import datetime

from airflow.sdk import dag, task
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
STAGING_DATASET = os.getenv("BQ_STAGING_DATASET", "staging")
MART_DATASET = os.getenv("BQ_MART_DATASET", "mart")
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-southeast1")


@dag(
    dag_id="create_final_visitor_features",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "final", "feature-engineering"],
)
def create_final_features_pipeline():

    @task
    def ensure_mart_dataset():
        client = bigquery.Client(project=PROJECT_ID)
        dataset_id = f"{PROJECT_ID}.{MART_DATASET}"

        try:
            client.get_dataset(dataset_id)
            print(f"Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = BQ_LOCATION
            client.create_dataset(dataset)
            print(f"Created dataset {dataset_id} in {BQ_LOCATION}")

    @task
    def create_final_table():
        client = bigquery.Client(project=PROJECT_ID)

        mapping_table = f"{PROJECT_ID}.{STAGING_DATASET}.final_country_mapping"
        visitor_table = f"{PROJECT_ID}.{STAGING_DATASET}.visitor_arrivals_monthly"
        gdp_table = f"{PROJECT_ID}.{STAGING_DATASET}.gdp_monthly"
        fx_table = f"{PROJECT_ID}.{STAGING_DATASET}.exchange_rates_monthly"
        holiday_table = f"{PROJECT_ID}.{STAGING_DATASET}.public_holidays_monthly"
        aircraft_table = f"{PROJECT_ID}.{STAGING_DATASET}.aircraft_monthly"
        traffic_table = f"{PROJECT_ID}.{STAGING_DATASET}.traffic_monthly"
        hotel_table = f"{PROJECT_ID}.{STAGING_DATASET}.hotel_monthly"
        final_table = f"{PROJECT_ID}.{MART_DATASET}.visitor_prediction_features"

        query = f"""
        CREATE OR REPLACE TABLE `{final_table}` AS

        WITH country_mapping AS (
          SELECT DISTINCT
            TRIM(country) AS raw_country,
            TRIM(country_mapping) AS standard_country
          FROM `{mapping_table}`
          WHERE country_mapping IS NOT NULL
            AND TRIM(country_mapping) != ''
        ),

        visitor_arrivals AS (
          SELECT
            m.standard_country AS country,
            v.month,
            SUM(v.visitor_arrivals) AS visitor_arrivals
          FROM `{visitor_table}` v
          JOIN country_mapping m
            ON TRIM(v.country) = m.raw_country
          GROUP BY 1, 2
        ),

        gdp AS (
          SELECT
            m.standard_country AS country,
            g.month,
            AVG(g.gdp) AS gdp
          FROM `{gdp_table}` g
          JOIN country_mapping m
            ON TRIM(g.country) = m.raw_country
          GROUP BY 1, 2
        ),

        exchange_rates AS (
          SELECT
            m.standard_country AS country,
            fx.month,
            AVG(fx.exchange_rate) AS exchange_rate
          FROM `{fx_table}` fx
          JOIN country_mapping m
            ON TRIM(fx.country) = m.raw_country
          GROUP BY 1, 2
        ),

        public_holidays AS (
          SELECT
            m.standard_country AS country,
            ph.month,
            SUM(ph.public_holiday_count) AS public_holiday_count
          FROM `{holiday_table}` ph
          JOIN country_mapping m
            ON TRIM(ph.country) = m.raw_country
          GROUP BY 1, 2
        )

        SELECT
          v.country,
          v.month,
          v.visitor_arrivals,
          g.gdp,
          fx.exchange_rate,
          COALESCE(ph.public_holiday_count, 0) AS public_holiday_count,
          a.aircraft_passengers,
          t.traffic_volume,
          h.hotel_rate,
          h.hotel_occupancy
        FROM visitor_arrivals v
        LEFT JOIN gdp g
          ON v.country = g.country
         AND v.month = g.month
        LEFT JOIN exchange_rates fx
          ON v.country = fx.country
         AND v.month = fx.month
        LEFT JOIN public_holidays ph
          ON v.country = ph.country
         AND v.month = ph.month
        LEFT JOIN `{aircraft_table}` a
          ON v.month = a.month
        LEFT JOIN `{traffic_table}` t
          ON v.month = t.month
        LEFT JOIN `{hotel_table}` h
          ON v.month = h.month
        ORDER BY v.country, v.month
        """

        job_config = bigquery.QueryJobConfig()
        job = client.query(query, job_config=job_config, location=BQ_LOCATION)
        job.result()

        print(f"Final table created: {final_table}")

    mart_task = ensure_mart_dataset()
    final_task = create_final_table()

    mart_task >> final_task


create_final_features_dag = create_final_features_pipeline()