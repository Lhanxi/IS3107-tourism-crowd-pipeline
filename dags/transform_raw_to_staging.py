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
    
    @task
    def transform_aircraft_monthly():
        client = bigquery.Client(project=PROJECT_ID)

        source_table = f"{PROJECT_ID}.{RAW_DATASET}.monthly_aircraft_passengers"
        target_table = f"{PROJECT_ID}.{STAGING_DATASET}.aircraft_monthly"

        df = client.query(f"SELECT * FROM `{source_table}`").to_dataframe()

        print("Aircraft raw shape:", df.shape)
        print("Aircraft raw columns before cleaning:", df.columns.tolist()[:10])

        if df.empty:
            raise ValueError("monthly_aircraft_passengers raw table is empty.")

        # Clean column names
        df.columns = [str(col).strip() for col in df.columns]

        print("Aircraft raw columns after cleaning:", df.columns.tolist()[:10])

        # Drop auto id if present
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])

        # Detect series column
        possible_series_cols = ["Data Series", "DataSeries", "data_series"]
        series_col = next((c for c in possible_series_cols if c in df.columns), None)

        if series_col is None:
            raise ValueError(f"No series column found. Columns: {df.columns.tolist()}")

        df[series_col] = df[series_col].astype(str).str.strip()
        df = df[df[series_col].notna()].copy()

        # Keep only the metric you want
        # Change this if later you want a different aircraft metric
        target_metric = "Total Passengers"

        df = df[df[series_col] == target_metric].copy()

        if df.empty:
            raise ValueError(
                f"No rows found for metric '{target_metric}' in {source_table}. "
                f"Available sample values: {client.query(f'SELECT DISTINCT `{series_col}` FROM `{source_table}` LIMIT 20').to_dataframe().iloc[:,0].tolist()}"
            )

        # All other columns should be month columns like 2026Jan / 2026 Jan / 2026 Jan 
        month_columns = [col for col in df.columns if col != series_col]

        print("Aircraft month columns sample:", month_columns[:10])

        df_long = df.melt(
            id_vars=[series_col],
            value_vars=month_columns,
            var_name="month_str",
            value_name="aircraft_passengers",
        )

        # Clean month strings
        df_long["month_str"] = (
            df_long["month_str"]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )

        # Convert values to numeric
        df_long["aircraft_passengers"] = pd.to_numeric(
            df_long["aircraft_passengers"],
            errors="coerce"
        )

        # Handle formats like:
        # 2026Jan
        # 2026 Jan
        # 2026 Jan 
        # Convert 2026Jan -> 2026 Jan first
        df_long["month_str_clean"] = df_long["month_str"].str.replace(
            r"^(\d{4})([A-Za-z]{3})$",
            r"\1 \2",
            regex=True
        )

        df_long["month"] = pd.to_datetime(
            df_long["month_str_clean"],
            format="%Y %b",
            errors="coerce"
        )

        # Keep only valid rows
        df_long = df_long.dropna(subset=["month", "aircraft_passengers"]).copy()

        # Final shape
        final_df = (
            df_long[["month", "aircraft_passengers"]]
            .sort_values("month")
            .drop_duplicates(subset=["month"])
            .reset_index(drop=True)
        )

        print("Aircraft final shape:", final_df.shape)
        print(final_df.head())

        if final_df.empty:
            raise ValueError("Aircraft transformed dataframe is empty after cleaning.")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("month", "DATE"),
                bigquery.SchemaField("aircraft_passengers", "FLOAT"),
            ],
        )

        load_job = client.load_table_from_dataframe(
            final_df,
            target_table,
            job_config=job_config,
        )
        load_job.result()

        print(f"Loaded {len(final_df)} rows into {target_table}")

    @task
    def transform_traffic_monthly():
        client = bigquery.Client(project=PROJECT_ID)

        source_table = f"{PROJECT_ID}.{RAW_DATASET}.average_traffic_volume_entering"
        target_table = f"{PROJECT_ID}.{STAGING_DATASET}.traffic_monthly"

        df = client.query(f"SELECT * FROM `{source_table}`").to_dataframe()

        print("Traffic raw shape:", df.shape)
        print(df.head())

        if df.empty:
            raise ValueError("Traffic raw table is empty.")

        # Clean columns
        df.columns = [col.strip() for col in df.columns]

        # Drop _id if exists
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])

        # Rename for consistency
        df = df.rename(columns={
            "year": "year",
            "ave_daily_traffic_volume_entering_city": "traffic_volume"
        })

        # Convert types
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df["traffic_volume"] = pd.to_numeric(df["traffic_volume"], errors="coerce")

        df = df.dropna(subset=["year", "traffic_volume"])

        # Expand year → 12 months
        records = []

        for _, row in df.iterrows():
            year = int(row["year"])
            value = row["traffic_volume"]

            for month in range(1, 13):
                records.append({
                    "month": f"{year}-{month:02d}-01",
                    "traffic_volume": value
                })

        final_df = pd.DataFrame(records)

        # Convert to datetime
        final_df["month"] = pd.to_datetime(final_df["month"])

        final_df = final_df.sort_values("month").reset_index(drop=True)

        print("Traffic final shape:", final_df.shape)
        print(final_df.head())

        if final_df.empty:
            raise ValueError("Traffic transformed dataframe is empty.")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("month", "DATE"),
                bigquery.SchemaField("traffic_volume", "FLOAT"),
            ],
        )

        load_job = client.load_table_from_dataframe(
            final_df,
            target_table,
            job_config=job_config,
        )
        load_job.result()

        print(f"Loaded {len(final_df)} rows into {target_table}")

    @task
    def transform_hotel_monthly():
        client = bigquery.Client(project=PROJECT_ID)

        source_table = f"{PROJECT_ID}.{RAW_DATASET}.monthly_average_hotel_rates"
        target_table = f"{PROJECT_ID}.{STAGING_DATASET}.hotel_monthly"

        df = client.query(f"SELECT * FROM `{source_table}`").to_dataframe()

        print("Hotel raw shape:", df.shape)

        if df.empty:
            raise ValueError("Hotel raw table is empty.")

        # Clean columns
        df.columns = [str(col).strip() for col in df.columns]

        if "_id" in df.columns:
            df = df.drop(columns=["_id"])

        # Clean DataSeries
        df["DataSeries"] = df["DataSeries"].astype(str).str.strip()

        print("Available DataSeries:", df["DataSeries"].unique())

        rate_row = df[df["DataSeries"] == "Average Room Rate"].copy()
        occupancy_row = df[df["DataSeries"] == "Average Hotel Occupancy Rate"].copy()

        if rate_row.empty or occupancy_row.empty:
            raise ValueError("Required hotel metrics not found. Check DataSeries names.")

        # Month columns
        month_cols = [col for col in df.columns if col != "DataSeries"]

        # Melt both
        rate_long = rate_row.melt(
            id_vars=["DataSeries"],
            value_vars=month_cols,
            var_name="month_str",
            value_name="hotel_rate",
        )

        occ_long = occupancy_row.melt(
            id_vars=["DataSeries"],
            value_vars=month_cols,
            var_name="month_str",
            value_name="hotel_occupancy",
        )

        # Clean month format
        def clean_month(col):
            return (
                col.astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.replace(r"^(\d{4})([A-Za-z]{3})$", r"\1 \2", regex=True)
            )

        rate_long["month_str"] = clean_month(rate_long["month_str"])
        occ_long["month_str"] = clean_month(occ_long["month_str"])

        # Convert to datetime
        rate_long["month"] = pd.to_datetime(rate_long["month_str"], format="%Y %b", errors="coerce")
        occ_long["month"] = pd.to_datetime(occ_long["month_str"], format="%Y %b", errors="coerce")

        # Convert values
        rate_long["hotel_rate"] = pd.to_numeric(rate_long["hotel_rate"], errors="coerce")
        occ_long["hotel_occupancy"] = pd.to_numeric(occ_long["hotel_occupancy"], errors="coerce")

        # Drop invalid
        rate_long = rate_long.dropna(subset=["month", "hotel_rate"])
        occ_long = occ_long.dropna(subset=["month", "hotel_occupancy"])

        # Merge
        final_df = pd.merge(
            rate_long[["month", "hotel_rate"]],
            occ_long[["month", "hotel_occupancy"]],
            on="month",
            how="inner"
        )

        final_df = final_df.sort_values("month").reset_index(drop=True)

        print("Hotel final shape:", final_df.shape)
        print(final_df.head())

        if final_df.empty:
            raise ValueError("Hotel transformed dataframe is empty.")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("month", "DATE"),
                bigquery.SchemaField("hotel_rate", "FLOAT"),
                bigquery.SchemaField("hotel_occupancy", "FLOAT"),
            ],
        )

        load_job = client.load_table_from_dataframe(
            final_df,
            target_table,
            job_config=job_config,
        )
        load_job.result()

        print(f"Loaded {len(final_df)} rows into {target_table}")

    @task
    def transform_public_holidays_monthly():
        client = bigquery.Client(project=PROJECT_ID)

        source_table = f"{PROJECT_ID}.{RAW_DATASET}.public_holidays"
        target_table = f"{PROJECT_ID}.{STAGING_DATASET}.public_holidays_monthly"

        df = client.query(f"SELECT * FROM `{source_table}`").to_dataframe()

        print("Public holidays raw shape:", df.shape)
        print("Public holidays raw columns:", df.columns.tolist())

        if df.empty:
            raise ValueError("public_holidays raw table is empty.")

        # Clean columns
        df.columns = [str(col).strip() for col in df.columns]

        if "_id" in df.columns:
            df = df.drop(columns=["_id"])

        required_cols = ["country_name", "date"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in public_holidays table: {missing_cols}")

        # Use country_name as final country field
        df["country"] = df["country_name"].astype(str).str.strip()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        df = df.dropna(subset=["country", "date"]).copy()

        # Remove empty / invalid country strings
        df = df[df["country"] != ""].copy()
        df = df[df["country"].str.lower() != "nan"].copy()

        # Convert date -> month
        df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

        # Count number of public holidays in each country-month
        final_df = (
            df.groupby(["country", "month"], as_index=False)
              .size()
              .rename(columns={"size": "public_holiday_count"})
              .sort_values(["country", "month"])
              .reset_index(drop=True)
        )

        print("Public holidays monthly shape:", final_df.shape)
        print(final_df.head())

        if final_df.empty:
            raise ValueError("Public holidays transformed dataframe is empty.")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("month", "DATE"),
                bigquery.SchemaField("public_holiday_count", "INTEGER"),
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
    aircraft_task = transform_aircraft_monthly()
    traffic_task = transform_traffic_monthly()
    hotel_task = transform_hotel_monthly()
    public_holiday_task = transform_public_holidays_monthly()

    dataset_task >> transform_task >> gdp_task >> fx_task >> aircraft_task >> traffic_task >> hotel_task >> public_holiday_task


transform_raw_to_staging_dag = transform_raw_to_staging_pipeline()