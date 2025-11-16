CREATE OR REPLACE TABLE trip_anomalies_materialized AS
SELECT
    trip_id,
    trip_distance,
    trip_minutes,
    total_amount,
    fare_amount,
    tip_amount,
    (trip_distance / NULLIF(trip_minutes/60, 0)) AS speed_mph,
    fare_amount / NULLIF(trip_distance, 0) AS fare_per_mile,
    fare_amount / NULLIF(trip_minutes, 0) AS fare_per_minute,
    tip_amount / NULLIF(fare_amount, 0) AS tip_percentage,
    CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END AS is_zero_distance_fare,
    CASE
        WHEN fare_amount / NULLIF(trip_distance, 0) > 15 THEN 1
        WHEN (trip_distance / NULLIF(trip_minutes/60, 0)) > 80 THEN 1
        WHEN tip_amount / NULLIF(fare_amount, 0) > 1 THEN 1
        ELSE 0
    END AS flag_anomaly
FROM fact_trips;
