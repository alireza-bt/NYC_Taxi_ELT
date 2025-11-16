---- DELETE existing month from SILVER table
-- print("[IDEMPOTENT] Deleting newly loaded month from silver table, if any", ym)
DELETE FROM silver_trips
WHERE DATE_TRUNC('month', tpep_pickup_datetime) = (SELECT DISTINCT pickup_month FROM staging_trips);

---- INSERT fresh data into SILVER table
-- print("[INSERT] Adding fresh silver rows")
INSERT INTO silver_trips
SELECT * FROM staging_trips;
