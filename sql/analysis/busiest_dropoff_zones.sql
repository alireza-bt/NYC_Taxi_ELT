SELECT
    l.zone AS dropoff_zone,
    COUNT(*) AS num_dropoffs
FROM fact_trips t
JOIN dim_zones z ON t.dropoff_location_id = l.location_id
WHERE EXTRACT('year' from t.tpep_pickup_datetime) = 2025
GROUP BY l.zone
ORDER BY num_dropoffs DESC
LIMIT 20;
