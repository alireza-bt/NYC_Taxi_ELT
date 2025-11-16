-- Datetime dimension
-- CREATE OR REPLACE TABLE dim_datetime AS
-- SELECT DISTINCT
--     tpep_pickup_datetime AS datetime_id,
--     DATE_TRUNC('day', tpep_pickup_datetime) AS date,
--     EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
--     EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
--     EXTRACT(DAY FROM tpep_pickup_datetime) AS day,
--     EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
--     EXTRACT(DOW FROM tpep_pickup_datetime) AS weekday
-- FROM silver_trips;

-- Location dimension
CREATE TABLE IF NOT EXISTS dim_location AS
SELECT
    locationid as location_id,
    borough,
    zone,
    service_zone
FROM read_csv_auto('./data/raw/taxi_zone_lookup.csv');

-- Vendor dimension
CREATE OR REPLACE TABLE dim_vendor AS
SELECT DISTINCT vendor_id
FROM silver_trips;

-- Payment type dimension
CREATE OR REPLACE TABLE dim_payment AS
SELECT DISTINCT payment_type AS payment_id
FROM silver_trips;

-- Ratecode dimension
CREATE OR REPLACE TABLE dim_ratecode AS
SELECT DISTINCT ratecode_id
FROM silver_trips;
