WITH hourly AS (
    SELECT
        EXTRACT(hour FROM pickup_datetime_id) AS hour,
        total_amount,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_amount) AS median_fare
    FROM fact_trips
    GROUP BY hour
)
SELECT *
FROM hourly
WHERE total_amount > median_fare * 2  -- 2x spike
ORDER BY total_amount DESC;
