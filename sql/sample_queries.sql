-- Useful queries for checking ingestion and exploring historical data.
-- Run inside the container:  docker exec -it hb-postgres psql -U heartbeat -d heartbeat

-- 1. How many rows have been ingested?
SELECT COUNT(*) AS total_readings FROM heartbeat_readings;

-- 2. The 20 most recent readings
SELECT customer_id, reading_time, heart_rate, status
FROM heartbeat_readings
ORDER BY reading_time DESC
LIMIT 20;

-- 3. Latest reading per customer (uses the view)
SELECT * FROM latest_heartbeat ORDER BY customer_id;

-- 4. Per-customer statistics for the last 10 minutes
SELECT customer_id,
       COUNT(*)                  AS readings,
       ROUND(AVG(heart_rate), 1) AS avg_bpm,
       MIN(heart_rate)           AS min_bpm,
       MAX(heart_rate)           AS max_bpm
FROM heartbeat_readings
WHERE reading_time > NOW() - INTERVAL '10 minutes'
GROUP BY customer_id
ORDER BY customer_id;

-- 5. Recent anomalies (uses the partial index)
SELECT customer_id, reading_time, heart_rate, status
FROM heartbeat_readings
WHERE status <> 'normal'
ORDER BY reading_time DESC
LIMIT 20;

-- 6. Status breakdown
SELECT status, COUNT(*) FROM heartbeat_readings GROUP BY status;

-- 7. Rejected messages and why
SELECT error_reason, COUNT(*) FROM invalid_readings GROUP BY error_reason ORDER BY 2 DESC;
SELECT raw_payload, error_reason, received_at FROM invalid_readings ORDER BY received_at DESC LIMIT 10;

-- 8. Average heart rate per minute (time-series rollup)
SELECT date_trunc('minute', reading_time) AS minute,
       ROUND(AVG(heart_rate), 1)          AS avg_bpm,
       COUNT(*)                           AS readings
FROM heartbeat_readings
GROUP BY 1
ORDER BY 1 DESC
LIMIT 15;

-- 9. Show that the timestamp index is used
EXPLAIN ANALYZE
SELECT * FROM heartbeat_readings
WHERE reading_time > NOW() - INTERVAL '5 minutes';
