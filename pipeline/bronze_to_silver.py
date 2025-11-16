import pandas as pd
import os
import pipeline.utils as utils

RAW_PATH = "data/raw/"
BRONZE_PATH = "data/bronze/"
SILVER_PATH = "data/silver/"


def filter_to_correct_month(df, year, month):

    # Convert expected year-month into Period representation
    expected = pd.Period(f"{year}-{month:02d}", freq="M")

    df["pickup_month"] = df["tpep_pickup_datetime"].dt.to_period("M")

    # Filter ONLY rows that match the file's month
    df = df[df["pickup_month"] == expected]

    # Drop helper column
    df = df.drop(columns=["pickup_month"])

    return df

def convert_column_names(column_list):
    # convert col names to snake_case
    new_columns = {
        "VendorID":"vendor_id",
        "tpep_pickup_datetime":"tpep_pickup_datetime",
        "tpep_dropoff_datetime":"tpep_dropoff_datetime",
        "passenger_count":"passenger_count",
        "trip_distance":"trip_distance",
        "RatecodeID":"ratecode_id",
        "store_and_fwd_flag":"store_and_fwd_flag",
        "PULocationID":"pickup_location_id",
        "DOLocationID":"dropoff_location_id",
        "payment_type":"payment_type",
        "fare_amount":"fare_amount",
        "extra":"extra",
        "mta_tax":"mta_tax",
        "tip_amount":"tip_amount",
        "tolls_amount":"tolls_amount",
        "improvement_surcharge":"improvement_surcharge",
        "total_amount":"total_amount",
        "congestion_surcharge":"congestion_surcharge",
        "Airport_fee":"airport_fee",
        "cbd_congestion_fee":"cbd_congestion_fee"
    }
    
    # check if both lists has the same items
    assert sorted(column_list) == sorted(new_columns.keys()), "New df has different set of columns!"

    return [new_columns[c] for c in column_list]

def process_bronze_to_silver(filename):

    # ---- BRONZE LAYER ---- Not needed anymore because the data comes in .parquet
    '''
    raw_file = os.path.join(RAW_PATH, filename)
    bronze_file = os.path.join(BRONZE_PATH, filename.replace(".csv", ".parquet"))

    print(f"[BRONZE] Reading raw file: {raw_file}")

    df_bronze = pd.read_csv(raw_file, low_memory=False)
    df_bronze.to_parquet(bronze_file)
    print(f"[BRONZE] Saved to: {bronze_file}")
    '''

    # ---- SILVER LAYER ----
    print("[SILVER] Cleaning & transforming...")
    
    raw_file = os.path.join(RAW_PATH, filename)
    df_silver = pd.read_parquet(raw_file)
    
    year, month = utils.extract_year_month(filename)

    # column names -> snake_case and check if columns are consistent
    df_silver.columns = convert_column_names(df_silver.columns)


    # Parse timestamps safely
    df_silver["tpep_pickup_datetime"] = pd.to_datetime(df_silver["tpep_pickup_datetime"], errors="coerce")
    df_silver["tpep_dropoff_datetime"] = pd.to_datetime(df_silver["tpep_dropoff_datetime"], errors="coerce")

    # Drop rows with corrupted dates
    df_silver = df_silver.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    # Filter out data that is NOT from this month
    df_silver = filter_to_correct_month(df_silver, year, month)

    # Compute duration (minutes)
    df_silver["trip_minutes"] = (
        (df_silver["tpep_dropoff_datetime"] - df_silver["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    )

    # Filter meaningless data
    df_silver = df_silver[
        (df_silver["trip_distance"] > 0) &
        (df_silver["trip_minutes"] > 0)
    ]

    # Save cleaned version
    silver_file = os.path.join(SILVER_PATH, filename)
    df_silver.to_parquet(silver_file, index=False)

    print(f"[SILVER] Saved clean month {year}-{month:02d}: {len(df_silver)} rows")

    return silver_file
