import os
import glob
import requests
from datetime import datetime
import pipeline.utils as utils

RAW_PATH = "data/raw/"


def download_tlc_file(year: int, month: int):
    """
    Download NYC TLC Yellow Taxi Parquet file for a given year and month.
    Saves to data/raw/
    """

    # Ensure directory exists
    os.makedirs(RAW_PATH, exist_ok=True)

    # Format month with leading zero
    month_str = f"{month:02d}"

    filename = f"yellow_tripdata_{year}-{month_str}.parquet"
    
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    local_path = os.path.join(RAW_PATH, filename)

    # Skip download if file already exists
    if os.path.exists(local_path):
        print(f"[INGEST] File already exists: {local_path}")
        return local_path

    print(f"[INGEST] Downloading {filename} ...")
    response = requests.get(url, stream=True)

    if response.status_code != 200:
        raise Exception(f"Failed to download {url}. Status: {response.status_code}")

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    print(f"[INGEST] Saved to: {local_path}")
    return local_path


def download_latest_month():
    """
    Automatically fetch the most recent full month.
    Example: If today is 2025-11-15 -> downloads 2025-10.
    """
    now = datetime.now()
    year = now.year
    month = now.month - 1

    if month == 0:
        year -= 1
        month = 12

    return download_tlc_file(year, month)


def ingest_zone_lookup():
    # download if not exists
    if os.path.exists(os.path.join(RAW_PATH, "taxi_zone_lookup.csv")):
        print(f"[INGEST] File already exists: taxi_zone_lookup.csv")
    else:
        resp = requests.get('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv')
        with open('./data/raw/taxi_zone_lookup.csv', "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def resume_ingestion():
    """
    It check automatically the oldest month existing in raw folder and Resumes ingestion
    based on that, or if the folder is empty starts from 2025!
    """

    # Ensure directory exists
    os.makedirs(RAW_PATH, exist_ok=True)

    # check if taxi_zone_lookup exists
    ingest_zone_lookup()

    existing_parquets = glob.glob(f"{RAW_PATH}yellow_tripdata_*.parquet")
    existing_parquets.sort(reverse=True)

    year, month = 2025, 9
    # if there's any file then get the last
    if existing_parquets:
        oldest_file = existing_parquets[0]
        print(f"[INGEST] Oldest file found: {oldest_file}")
        year, month = utils.extract_year_month(oldest_file)
        
        if year <= 2025 and month > 0 and month <= 12:
            if month == 12: # goes to next year
                year += 1
                month = 1
            else:
                month += 1
    
    return download_tlc_file(year, month)
    