
def build_gold_layer(db_con, silver_filename):

    print("[STAGING] Loading Silver Data into duckdb staging")
    # Parse year-month into date format
    db_con.execute(f"""
        CREATE OR REPLACE TABLE staging_trips AS 
        SELECT *,
            DATE_TRUNC('month', tpep_pickup_datetime) AS pickup_month
        FROM read_parquet('{silver_filename}');
    """)

    # load into silver_trips
    print("[SILVER] Loading into silver_trips table")
    db_con.execute(open("sql/10_load_silver_trips.sql").read())

    print("[GOLD] Creating Dimensions")
    db_con.execute(open("sql/20_init_dimensions.sql").read())

    print("[GOLD] Creating Fact Table")
    db_con.execute(open("sql/30_create_fact_trips.sql").read())

    print("[GOLD] Creating Fraud / Anomaly Mart")
    db_con.execute(open("sql/40_create_fraud_mart.sql").read())

    print("[GOLD] Done! Database ready at nyc_taxi.duckdb")

