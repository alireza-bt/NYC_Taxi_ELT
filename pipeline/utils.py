def extract_year_month(filename: str):
    try:
        ym = filename.split("_")[-1].replace(".parquet", "")  # "2024-01"
        year, month = ym.split("-")
        return int(year), int(month)
    except Exception as e:
        raise Exception(f"Failed to extract year and month from filename {filename}.")
