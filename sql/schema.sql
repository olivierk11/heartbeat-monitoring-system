-- =====================================================================
-- Real-Time Customer Heart Beat Monitoring System - database schema
-- Runs automatically the first time the PostgreSQL container starts.
-- Can also be run by hand:  psql -U heartbeat -d heartbeat -f sql/schema.sql
-- =====================================================================

-- Valid, processed readings (the main time-series table)
CREATE TABLE IF NOT EXISTS heartbeat_readings (
    id            BIGSERIAL    PRIMARY KEY,
    customer_id   VARCHAR(20)  NOT NULL,
    reading_time  TIMESTAMPTZ  NOT NULL,                 -- when the sensor took the reading
    heart_rate    SMALLINT     NOT NULL CHECK (heart_rate BETWEEN 30 AND 220),
    status        VARCHAR(10)  NOT NULL CHECK (status IN ('normal', 'low', 'high')),
    ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),   -- when the consumer stored it
    -- Prevents duplicates if Kafka redelivers a message (at-least-once delivery)
    CONSTRAINT uq_customer_reading UNIQUE (customer_id, reading_time)
);

-- Time-based queries: "everything in the last 15 minutes"
CREATE INDEX IF NOT EXISTS idx_hb_reading_time
    ON heartbeat_readings (reading_time DESC);

-- Per-customer history: "customer X over the last hour"
CREATE INDEX IF NOT EXISTS idx_hb_customer_time
    ON heartbeat_readings (customer_id, reading_time DESC);

-- Fast lookup of anomalies only (partial index)
CREATE INDEX IF NOT EXISTS idx_hb_anomalies
    ON heartbeat_readings (reading_time DESC)
    WHERE status <> 'normal';

-- Messages that failed validation (kept for auditing / debugging, never silently dropped)
CREATE TABLE IF NOT EXISTS invalid_readings (
    id               BIGSERIAL    PRIMARY KEY,
    raw_payload      TEXT         NOT NULL,
    error_reason     TEXT         NOT NULL,
    kafka_partition  INTEGER,
    kafka_offset     BIGINT,
    received_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_invalid_received_at
    ON invalid_readings (received_at DESC);

-- Convenience view: most recent reading for every customer
CREATE OR REPLACE VIEW latest_heartbeat AS
SELECT DISTINCT ON (customer_id)
       customer_id, reading_time, heart_rate, status
FROM heartbeat_readings
ORDER BY customer_id, reading_time DESC;
