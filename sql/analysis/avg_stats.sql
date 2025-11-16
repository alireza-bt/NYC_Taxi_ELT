SELECT 
    AVG(trip_distance) AS avg_distance,
    AVG(trip_minutes) AS avg_minutes,
    AVG(total_amount) AS avg_total_amount
FROM fact_trips

------
-- hourly
SELECT EXTRACT(hour from tpep_pickup_datetime) hour,
    AVG(trip_distance) AS avg_distance,
    AVG(trip_minutes) AS avg_minutes,
    AVG(total_amount) AS avg_total_amount
FROM fact_trips
WHERE EXTRACT(year from tpep_pickup_datetime) = 2025
group by 1
order by avg_total_amount desc