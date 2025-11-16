SELECT
    l.zone AS pickup_zone,
    COUNT(*) AS num_pickups
FROM fact_trips t
JOIN dim_location l ON t.pickup_location_id = l.location_id
WHERE EXTRACT('year' from t.tpep_pickup_datetime) = 2025
GROUP BY l.zone
ORDER BY num_pickups DESC
LIMIT 20;
