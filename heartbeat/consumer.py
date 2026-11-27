"""Kafka consumer: validates heart-rate messages and writes them to PostgreSQL.

    python -m heartbeat.consumer
    python -m heartbeat.consumer --batch-size 50 --flush-interval 1

Processing per message:
  1. parse JSON           -> malformed messages go to invalid_readings
  2. validate fields      -> missing/impossible values go to invalid_readings
  3. classify heart rate  -> status = normal | low | high (anomalies are logged as ALERT)
  4. buffer, then bulk-insert into heartbeat_readings

Offsets are committed to Kafka only AFTER the database transaction succeeds
(at-least-once delivery). Redelivered duplicates are ignored by the
UNIQUE (customer_id, reading_time) constraint.
"""
from __future__ import annotations

import argparse
import logging
import signal
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError
from psycopg2.extras import execute_values

from heartbeat import config
from heartbeat.validation import ValidationError, parse_message, validate

log = logging.getLogger("consumer")

INSERT_VALID = """
    INSERT INTO heartbeat_readings (customer_id, reading_time, heart_rate, status)
    VALUES %s
    ON CONFLICT (customer_id, reading_time) DO NOTHING
"""
INSERT_INVALID = """
    INSERT INTO invalid_readings (raw_payload, error_reason, kafka_partition, kafka_offset)
    VALUES %s
"""


def connect_db(retries: int = 10, delay: float = 3.0):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**config.pg_params())
            log.info("Connected to PostgreSQL at %s:%s/%s",
                     config.PG_HOST, config.PG_PORT, config.PG_DB)
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("DB not ready (attempt %d/%d): %s", attempt, retries,
                        str(exc).strip())
            time.sleep(delay)
    raise SystemExit("Could not connect to PostgreSQL - is the container running?")


def write_batch(conn, valid: list[tuple], invalid: list[tuple]) -> int:
    """Insert both lists in ONE transaction. Returns number of new valid rows."""
    with conn:                       # commits on success, rolls back on error
        with conn.cursor() as cur:
            inserted = 0
            if valid:
                # one page for the whole batch so rowcount covers every row
                execute_values(cur, INSERT_VALID, valid, page_size=len(valid))
                inserted = cur.rowcount
            if invalid:
                execute_values(cur, INSERT_INVALID, invalid, page_size=len(invalid))
    return inserted


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",   # new group starts from the beginning
        "enable.auto.commit": False,       # we commit manually after the DB write
    })


def run(batch_size: int, flush_interval: float) -> None:
    consumer = build_consumer()
    consumer.subscribe([config.KAFKA_TOPIC])
    conn = connect_db()

    running = True

    def stop(*_):
        nonlocal running
        running = False
        log.info("Shutdown requested...")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    valid: list[tuple] = []
    invalid: list[tuple] = []
    totals = {"stored": 0, "rejected": 0, "alerts": 0, "duplicates": 0}
    last_flush = time.monotonic()

    def flush():
        nonlocal conn, last_flush
        if not valid and not invalid:
            return
        try:
            inserted = write_batch(conn, valid, invalid)
        except psycopg2.OperationalError as exc:      # connection lost: retry once
            log.error("DB connection lost (%s); reconnecting", exc)
            conn = connect_db()
            inserted = write_batch(conn, valid, invalid)
        consumer.commit(asynchronous=False)           # only after DB commit
        totals["stored"] += inserted
        totals["duplicates"] += len(valid) - inserted
        totals["rejected"] += len(invalid)
        log.info("Flushed batch: %d stored, %d rejected | totals: stored=%d rejected=%d alerts=%d",
                 inserted, len(invalid), totals["stored"], totals["rejected"], totals["alerts"])
        valid.clear()
        invalid.clear()
        last_flush = time.monotonic()

    log.info("Listening on topic '%s' (group '%s') at %s",
             config.KAFKA_TOPIC, config.KAFKA_CONSUMER_GROUP, config.KAFKA_BOOTSTRAP_SERVERS)
    try:
        while running:
            msg = consumer.poll(0.5)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka error: %s", msg.error())
                else:
                    raw = msg.value() or b""
                    try:
                        reading = validate(parse_message(raw))
                        valid.append(reading.as_row())
                        if reading.status != "normal":
                            totals["alerts"] += 1
                            log.warning("ALERT %-6s customer=%s heart_rate=%d at %s",
                                        reading.status.upper(), reading.customer_id,
                                        reading.heart_rate, reading.timestamp.isoformat())
                        else:
                            log.info("OK     customer=%s heart_rate=%d",
                                     reading.customer_id, reading.heart_rate)
                    except ValidationError as exc:
                        text = raw.decode("utf-8", errors="replace")
                        invalid.append((text, str(exc), msg.partition(), msg.offset()))
                        log.warning("REJECTED (%s): %s", exc, text)

            if (len(valid) + len(invalid) >= batch_size
                    or time.monotonic() - last_flush >= flush_interval):
                flush()
    finally:
        try:
            flush()
        finally:
            consumer.close()
            conn.close()
            log.info("Consumer stopped. %s", totals)


def main() -> None:
    p = argparse.ArgumentParser(description="Consume heart-rate data from Kafka into PostgreSQL")
    p.add_argument("--batch-size", type=int, default=100, help="max messages per DB transaction")
    p.add_argument("--flush-interval", type=float, default=2.0,
                   help="max seconds to hold a partial batch")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    run(a.batch_size, a.flush_interval)


if __name__ == "__main__":
    main()
