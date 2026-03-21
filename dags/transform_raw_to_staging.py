import os
import pandas as pd

from airflow.sdk import dag, task
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")
STAGING_DATASET = os.getenv("BQ_STAGING_DATASET", "staging")

REGION_ROWS = {
    "Southeast Asia",
    "Greater China",
    "North Asia",
    "South Asia",
    "West Asia",
    "Americas",
    "Europe",
    "Oceania",
    "Africa",
    "Others",
}

TOTAL_ROW = "Total International Visitor Arrivals By Place Of Residence"

@dag(
    dag_id="transform_raw_to_staging",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "staging", "transform"],
)
def transform_raw_to_staging_pipeline():
    @task
    def ensure_staging_dataset():
        client = bigquery.Client()
        dataset_id = f"{PROJECT_ID}.{STAGING_DATASET}"

        try:
            client.get_dataset(dataset_id)
            print(f"Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "asia-southeast1"
            client.create_dataset(dataset)
            print(f"Created dataset {dataset_id}")

    @task
    def transform_visitor_arrivals_monthly():
        client = bigquery.Client()

        source_table = f"{PROJECT_ID}.{RAW_DATASET}.monthly_visitor_arrivals_markets"
        country_target_table = f"{PROJECT_ID}.{STAGING_DATASET}.visitor_arrivals_monthly"
        total_target_table = f"{PROJECT_ID}.{STAGING_DATASET}.total_visitor_arrivals_monthly"

        query = f"SELECT * FROM `{source_table}`"
        df = client.query(query).to_dataframe()

        print("Raw shape:", df.shape)
        print("Raw columns before cleaning:", df.columns.tolist()[:10])

        # Clean column names
        df.columns = [col.strip() for col in df.columns]

        print("Raw columns after cleaning:", df.columns.tolist()[:10])

        # Drop auto id if present
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])

        # The first column is the series/market name
        series_col = "Data Series"

        df = df[df[series_col].notna()].copy()
        month_columns = [col for col in df.columns if col != series_col]

        df_long = df.melt(
            id_vars=[series_col],
            value_vars=month_columns,
            var_name="month_str",
            value_name="visitor_arrivals",
        )

        df_long["visitor_arrivals"] = pd.to_numeric(
            df_long["visitor_arrivals"],
            errors="coerce"
        )

        # Convert "2026 Jan" -> datetime
        df_long["month"] = pd.to_datetime(
            df_long["month_str"],
            format="%Y %b",
            errors="coerce"
        )

        df_long = df_long.rename(columns={series_col: "market"})
        df_long = df_long[["market", "month", "visitor_arrivals"]]
        df_long = df_long.dropna(subset=["market", "month", "visitor_arrivals"])

        print("Long table shape:", df_long.shape)
        print(df_long.head())

        # Total table
        df_total = df_long[df_long["market"] == TOTAL_ROW].copy()
        df_total = df_total.rename(columns={"visitor_arrivals": "total_visitor_arrivals"})
        df_total = df_total[["month", "total_visitor_arrivals"]].sort_values("month")

        print("Total table shape:", df_total.shape)
        print(df_total.head())

        # Country table
        df_country = df_long[df_long["market"] != TOTAL_ROW].copy()
        df_country = df_country[~df_country["market"].isin(REGION_ROWS)]
        df_country = df_country[~df_country["market"].str.startswith("Other Markets In", na=False)]
        df_country = df_country.rename(columns={"market": "country"})
        df_country = df_country[["country", "month", "visitor_arrivals"]].sort_values(["country", "month"])

        print("Country table shape:", df_country.shape)
        print(df_country.head())

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )

        country_job = client.load_table_from_dataframe(
            df_country,
            country_target_table,
            job_config=job_config
        )
        country_job.result()

        total_job = client.load_table_from_dataframe(
            df_total,
            total_target_table,
            job_config=job_config
        )
        total_job.result()

        print(f"Loaded country table into {country_target_table}")
        print(f"Loaded total table into {total_target_table}")
    
    @task
    def transform_gdp_dataset():
        client = bigquery.Client(project=PROJECT_ID)
        source_table = f"{PROJECT_ID}.{RAW_DATASET}.GDP_1975_2025_uploaded"
        target_table = f"{PROJECT_ID}.{STAGING_DATASET}.gdp_monthly"

        df = client.query(f"SELECT * FROM `{source_table}`").to_dataframe()
        print("Raw GDP shape:", df.shape)
        print("Raw GDP columns:", df.columns.tolist())

        if df.empty:
            raise ValueError("GDP raw table is empty.")
        
        rename_map = {"string_field_0": "country"}
        
        start_year = 1975
        for i in range(1, len(df.columns)):
            rename_map[f"double_field_{i}"] = str(start_year + i - 1)
        df = df.rename(columns=rename_map)

        print("Renamed GDP columns:", df.columns.tolist())

        year_cols = [str(year) for year in range(1975, 2026)]
        keep_cols = ["country"] + [c for c in year_cols if c in df.columns]
        gdp_df = df[keep_cols].copy()

        gdp_df["country"] = gdp_df["country"].astype(str).str.strip()
        gdp_long = gdp_df.melt(
            id_vars="country",
            value_vars=[c for c in year_cols if c in gdp_df.columns],
            var_name="year",
            value_name="gdp"
        )

        gdp_long["year"] = pd.to_numeric(gdp_long["year"], errors="coerce").astype("Int64")
        gdp_long["gdp"] = pd.to_numeric(gdp_long["gdp"], errors="coerce")

        gdp_long = gdp_long.dropna(subset=["country", "year", "gdp"]).copy()

        months = pd.DataFrame({"month_num": range(1, 13)})
        gdp_monthly = gdp_long.merge(months, how="cross")

        gdp_monthly["month"] = pd.to_datetime(
            dict(
                year=gdp_monthly["year"].astype(int),
                month=gdp_monthly["month_num"],
                day=1
            )
        )

        gdp_monthly = gdp_monthly[["country", "month", "gdp"]].sort_values(
            ["country", "month"]
        )

        print("GDP monthly shape:", gdp_monthly.shape)
        print(gdp_monthly.head())

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("month", "DATE"),
                bigquery.SchemaField("gdp", "FLOAT"),
            ],
        )

        load_job = client.load_table_from_dataframe(
            gdp_monthly,
            target_table,
            job_config=job_config,
        )
        load_job.result()

        print(f"Loaded {len(gdp_monthly)} rows into {target_table}")
    
    @task
    def transform_exchange_rates_monthly():
        client = bigquery.Client(project=PROJECT_ID)

        metadata_table = f"{PROJECT_ID}.{RAW_DATASET}.exchange_rate_metadata"
        currency_code_table = f"{PROJECT_ID}.{RAW_DATASET}.currency_code"
        target_table = f"{PROJECT_ID}.{STAGING_DATASET}.exchange_rates_monthly"

        metadata_df = client.query(f"SELECT * FROM `{metadata_table}`").to_dataframe()
        currency_df = client.query(f"SELECT * FROM `{currency_code_table}`").to_dataframe()

        print("Metadata shape:", metadata_df.shape)
        print("Currency code shape:", currency_df.shape)

        if metadata_df.empty:
            raise ValueError("exchange_rate_metadata table is empty.")

        if currency_df.empty:
            raise ValueError("currency_code table is empty.")

        # Clean metadata columns
        metadata_df.columns = [col.strip() for col in metadata_df.columns]
        currency_df.columns = [col.strip() for col in currency_df.columns]

        metadata_df["dataset_name"] = metadata_df["dataset_name"].astype(str).str.strip()
        metadata_df["code"] = metadata_df["code"].astype(str).str.strip().str.upper()

        # Normalize currency_code column names
        if {"Country", "Currency", "Code"}.issubset(currency_df.columns):
            currency_df = currency_df.rename(columns={
                "Country": "country",
                "Currency": "currency",
                "Code": "code"
            })
        elif {"country", "currency", "code"}.issubset(currency_df.columns):
            pass
        elif len(currency_df.columns) >= 3:
            # fallback if BigQuery used generic names like string_field_0, string_field_1, string_field_2
            currency_df = currency_df.rename(columns={
                currency_df.columns[0]: "country",
                currency_df.columns[1]: "currency",
                currency_df.columns[2]: "code"
            })
        else:
            raise ValueError(f"Unexpected currency_code columns: {currency_df.columns.tolist()}")

        currency_df["country"] = currency_df["country"].astype(str).str.strip()
        currency_df["currency"] = currency_df["currency"].astype(str).str.strip()
        currency_df["code"] = currency_df["code"].astype(str).str.strip().str.upper()

        monthly_frames = []

        for _, row in metadata_df.iterrows():
            dataset_name = row["dataset_name"]
            code = row["code"]
            needs_inversion = row["needs_inversion"]

            source_table = f"{PROJECT_ID}.{RAW_DATASET}.{dataset_name}"
            print(f"Processing FX table: {source_table}")

            fx_df = client.query(f"SELECT * FROM `{source_table}`").to_dataframe()
            fx_df.columns = [col.strip() for col in fx_df.columns]

            if fx_df.empty:
                print(f"Skipping empty FX table: {source_table}")
                continue

            # Detect date column
            possible_date_cols = ["Date", "date"]
            date_col = next((c for c in possible_date_cols if c in fx_df.columns), None)

            if date_col is None:
                raise ValueError(f"No date column found in {source_table}. Columns: {fx_df.columns.tolist()}")

            # Detect Adj Close column
            possible_adj_close_cols = ["Adj Close", "Adj_Close", "adj_close", "adj close"]
            adj_close_col = next((c for c in possible_adj_close_cols if c in fx_df.columns), None)

            if adj_close_col is None:
                raise ValueError(f"No Adj Close column found in {source_table}. Columns: {fx_df.columns.tolist()}")

            fx_df["date"] = pd.to_datetime(fx_df[date_col], errors="coerce")
            fx_df["exchange_rate"] = pd.to_numeric(fx_df[adj_close_col], errors="coerce")

            fx_df = fx_df.dropna(subset=["date", "exchange_rate"]).copy()

            # Protect against division by zero
            fx_df["exchange_rate"] = fx_df["exchange_rate"].replace(0, pd.NA)

            # Normalize all rates to: 1 unit of foreign currency = X SGD
            if str(needs_inversion).lower() in ("true", "1", "yes"):
                fx_df["exchange_rate"] = 1 / fx_df["exchange_rate"]

            fx_df = fx_df.dropna(subset=["exchange_rate"]).copy()

            # Aggregate daily -> monthly average
            fx_df["month"] = fx_df["date"].dt.to_period("M").dt.to_timestamp()
            fx_monthly = (
                fx_df.groupby("month", as_index=False)["exchange_rate"]
                .mean()
            )

            fx_monthly["code"] = code
            monthly_frames.append(fx_monthly)

        if not monthly_frames:
            raise ValueError("No exchange rate tables were successfully transformed.")

        all_fx_monthly = pd.concat(monthly_frames, ignore_index=True)

        print("Combined FX monthly shape:", all_fx_monthly.shape)
        print(all_fx_monthly.head())

        final_df = all_fx_monthly.merge(
            currency_df[["country", "code"]],
            on="code",
            how="left"
        )

        final_df = final_df[["country", "code", "month", "exchange_rate"]].copy()
        final_df = final_df.dropna(subset=["country", "code", "month", "exchange_rate"])
        final_df = final_df.sort_values(["country", "month"])

        print("Final FX monthly shape:", final_df.shape)
        print(final_df.head())

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("month", "DATE"),
                bigquery.SchemaField("exchange_rate", "FLOAT"),
            ],
        )

        load_job = client.load_table_from_dataframe(
            final_df,
            target_table,
            job_config=job_config,
        )
        load_job.result()

        print(f"Loaded {len(final_df)} rows into {target_table}")


    dataset_task = ensure_staging_dataset()
    transform_task = transform_visitor_arrivals_monthly()
    gdp_task = transform_gdp_dataset()
    fx_task = transform_exchange_rates_monthly()

    dataset_task >> transform_task >> gdp_task >> fx_task


transform_raw_to_staging_dag = transform_raw_to_staging_pipeline()