---- DELETE existing month from FACT table
DELETE FROM fact_trips
WHERE pickup_month = (SELECT DISTINCT pickup_month FROM staging_trips);

---- INSERT new fact rows
INSERT INTO fact_trips
SELECT
    CONCAT(vendor_id, '_', CAST(tpep_pickup_datetime AS VARCHAR)) AS trip_id,
    s.* exclude(payment_type), 
    payment_type as payment_id
FROM staging_trips s;
