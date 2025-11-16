# **NYC Taxi Data Engineering Project (Python + Pandas + DuckDB)**

This project implements an end-to-end **data pipeline** using the **NYC Yellow Taxi dataset**, with:

* **Bronze & Silver layers in Python using Pandas**
* **Gold modeling layer in DuckDB (Star Schema + Fraud/Anomaly Mart)**
* **Parquet files as the Lakehouse storage format**

---

# **Pipeline Overview**

```
Bronze (ingesting raw parquet files)  ->  Silver (clean parquet)  ->  Gold (data modeling using SQL and DuckDB)
```

### **Technologies Used**

| Layer   | Technology      | Purpose                                          |
| ------- | --------------- | ------------------------------------------------ |
| Bronze  | Python + Pandas | Raw ingestion → Parquet                          |
| Silver  | Python + Pandas | Cleaning, typing, feature creation               |
| Gold    | DuckDB          | Fact/dimension modeling, fraud/anomaly detection |
| Storage | Parquet         | Lakehouse file format                            |

---

# **Project Structure**

```
nyc-taxi-project/
│
├── data/
│   ├── raw/                        # Original Parquets from NYC TLC
│   ├── silver/                     # Cleaned -> Parquet
│   └── gold/                       # final modeled tables in DuckDB
│
├── sql/
│   ├── 00_init_duckdb.sql          # initialize fact_trips table
│   ├── 10_load_silver_trips.sql    # Delete, insert into silver_trips table
│   ├── 20_init_dimensions.sql      # location, vendor, payment dimensions
│   ├── 30_create_fact_trips.sql    # Fact table for taxi trips
│   ├── 40_create_fraud_mart.sql    # Fraud / anomaly mart
│   └── analysis/                   # Contaians final analytical SQLs
│
├── pipeline/
│   ├── bronze_to_silver.py         # Bronze -> Silver transform (using Pandas)
│   ├── data_ingestion.py           # Methods for ingesting data from the API
│   ├── init_duckdb.py              
│   ├── load_gold_duckdb.py         # Load Silver layer into DuckDB and create Gold layer
│   └── utils.py                    # util functions used in the different steps
│
├── main.py                         # Runs entire pipeline
├── README.md                       
└── requirements.txt
```

---

# 🥉 **Bronze Layer (Raw Parquet)**

The Bronze layer stores the original raw data **as-is** but in efficient Parquet format.

Handled in:
📄 `pipeline/bronze_to_silver.py`

---

# 🥈 **Silver Layer (Clean, Typed, Validated)**

Still in **Python + Pandas**, we:

* Convert timestamps
* Compute trip duration
* Ensure valid distances/times
* Filter corrupted trips
* Output clean Parquet

---

# 🥇 **Gold Layer (Star Schema + Anomaly Detection)**

Once Silver Parquet is created, the **Gold layer is fully modeled in DuckDB**.

Handled in:
📄 `pipeline/load_gold_to_duckdb.py`

### **Dimension Tables**

* `dim_location`
* `dim_vendor`
* `dim_payment`

### **Fact Table**

* `fact_trips`

### **Fraud/Anomaly Detection Mart**

Includes features like:

* `speed_mph`
* `fare_per_mile`
* `tip_ratio`
* `flag_anomaly`

All SQL definitions live under `sql/`.

---

# ▶️ **Running the Pipeline**

0. create a venv and install requirements.txt
    0.0. python -m venv .env
    0.1. source into venv and pip install -r requirement.txt

1. Run the pipeline:

```bash
python main.py
```

This will:

1. Ingest data from API
2. Clean data and create Silver Parquet
3. Build Gold tables inside `nyc_taxi.duckdb`

---

# 📊 **Querying Results**

After running the pipeline, you can open DuckDB:

```python
import duckdb
con = duckdb.connect("nyc_taxi.duckdb")
con.execute("SELECT * FROM fact_trips LIMIT 10").df()
```

---

# 📬 **Next Steps / Optional Enhancements**

* Adding **DBT** for Gold transformation
* Using **Airflow** for orchestration
* Adding **unit tests** (pytest)
* Creating a BI dashboard (DuckDB, Metabase, PowerBI)
* Adding incremental loads (monthly partitions)

