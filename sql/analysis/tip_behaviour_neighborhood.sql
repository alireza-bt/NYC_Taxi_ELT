SELECT
    l.zone,
    AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount END) AS avg_tip_pct
FROM fact_trips t
JOIN dim_location l ON t.pickup_location_id = l.location_id
WHERE EXTRACT('year' from t.tpep_pickup_datetime) = 2025
GROUP BY l.zone
ORDER BY avg_tip_pct DESC
LIMIT 20;
