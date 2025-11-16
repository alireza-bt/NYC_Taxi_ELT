from pipeline.bronze_to_silver import process_bronze_to_silver
from pipeline.load_gold_to_duckdb import build_gold_layer
from pipeline.data_ingestion import resume_ingestion
from pipeline.init_duckdb import init_db
import pipeline.utils as utils
from datetime import datetime


if __name__ == "__main__":
    print("=== NYC Taxi Pipeline Start ===")

    db_conn = init_db()

    while True:
        # 1. Resume ingestion using the latest existing file or start over from 2025-01
        # raw_file_path = resume_ingestion()

        # 1.1. Extract filename from path
        # filename = raw_file_path.split("/")[-1]
        filename = "yellow_tripdata_2025-09.parquet"
        # 2. Bronze -> Silver for one or multiple files
        # silver_filename = process_bronze_to_silver(filename)
        silver_filename = process_bronze_to_silver(filename)

        # check if silver_filename exists

        # 2. Build the Gold modeling layer in DuckDB
        build_gold_layer(db_conn, silver_filename)

        # break if next loading month = current_month
        year, month = utils.extract_year_month(filename)
        month += 2
        if month == 13:
            month = 1
            year += 1
        if int(f"{year}{month:02d}") == int(f"{datetime.now().year}{datetime.now().month:02d}"):
            break

    print("=== Pipeline Complete ===")
