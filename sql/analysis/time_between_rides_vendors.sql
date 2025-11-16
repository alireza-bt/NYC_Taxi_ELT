-- time between rides for each vendor
SELECT
    vendor_id,
    tpep_pickup_datetime,
    LAG(tpep_pickup_datetime) OVER (PARTITION BY vendor_id ORDER BY tpep_pickup_datetime) AS previous_pickup,
    EXTRACT(epoch FROM (tpep_pickup_datetime - 
        LAG(tpep_pickup_datetime) OVER (PARTITION BY vendor_id ORDER BY tpep_pickup_datetime))) / 60 AS minutes_since_last_trip
FROM fact_trips
ORDER BY vendor_id, tpep_pickup_datetime;
