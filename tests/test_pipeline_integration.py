"""End-to-end test: Producer -> Kafka -> Consumer -> PostgreSQL.

Requirements:
  1. docker compose up -d           (Kafka + PostgreSQL running)
  2. python -m heartbeat.consumer   (running in another terminal)
Then:
  pytest -m integration -v

Each test run uses unique customer IDs, sends a known set of normal, low, high
and invalid readings, and checks that each one ends up in the right table with
the right status. Test rows are deleted afterwards.
"""
import json
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest

from heartbeat import config

pytestmark = pytest.mark.integration

TIMEOUT_S = 30


def _kafka_up() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        md = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS}).list_topics(timeout=5)
        return config.KAFKA_TOPIC in md.topics
    except Exception:
        return False


def _db_up() -> bool:
    try:
        psycopg2.connect(**config.pg_params(), connect_timeout=3).close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def infra():
    if not _kafka_up():
        pytest.skip("Kafka not reachable or topic missing - run `docker compose up -d`")
    if not _db_up():
        pytest.skip("PostgreSQL not reachable - run `docker compose up -d`")
    conn = psycopg2.connect(**config.pg_params())
    conn.autocommit = True
    yield conn
    conn.close()


def _send(messages):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    for m in messages:
        p.produce(config.KAFKA_TOPIC,
                  value=m if isinstance(m, bytes) else json.dumps(m).encode())
    assert p.flush(10) == 0, "not all test messages were delivered to Kafka"


def _wait_for(conn, sql, params, expected):
    deadline = time.time() + TIMEOUT_S
    count = -1
    while time.time() < deadline:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            count = cur.fetchone()[0]
        if count >= expected:
            return count
        time.sleep(1)
    return count


def test_full_pipeline(infra):
    conn = infra
    run = uuid.uuid4().hex[:6].upper()          # e.g. 'A1B2C3'
    now = lambda: datetime.now(timezone.utc).isoformat()

    scenarios = {
        f"T{run}-NORM": (72, "normal"),
        f"T{run}-LOW": (42, "low"),
        f"T{run}-HIGH": (165, "high"),
        f"T{run}-EDGE": (120, "normal"),
    }
    valid_msgs = [{"customer_id": cid, "timestamp": now(), "heart_rate": hr}
                  for cid, (hr, _) in scenarios.items()]
    invalid_msgs = [
        {"customer_id": f"T{run}-BAD1", "timestamp": now(), "heart_rate": 300},
        {"customer_id": f"T{run}-BAD2", "timestamp": now(), "heart_rate": -10},
        {"customer_id": f"T{run}-BAD3", "timestamp": now(), "heart_rate": "N/A"},
        {"customer_id": f"T{run}-BAD4", "heart_rate": 70},
        f'{{"customer_id": "T{run}-BAD5", broken json'.encode(),
    ]
    # duplicate of a valid message -> must be stored only once
    _send(valid_msgs + [valid_msgs[0]] + invalid_msgs)

    try:
        stored = _wait_for(conn, "SELECT COUNT(*) FROM heartbeat_readings WHERE customer_id LIKE %s",
                           (f"T{run}-%",), len(scenarios))
        rejected = _wait_for(conn, "SELECT COUNT(*) FROM invalid_readings WHERE raw_payload LIKE %s",
                             (f"%T{run}-%",), len(invalid_msgs))
        assert stored == len(scenarios), "valid rows missing - is the consumer running?"
        assert rejected == len(invalid_msgs)

        with conn.cursor() as cur:
            cur.execute("SELECT customer_id, heart_rate, status FROM heartbeat_readings "
                        "WHERE customer_id LIKE %s", (f"T{run}-%",))
            rows = {cid: (hr, status) for cid, hr, status in cur.fetchall()}
        assert rows == scenarios

        time.sleep(3)   # give the duplicate time to arrive; it must not add a row
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM heartbeat_readings WHERE customer_id LIKE %s",
                        (f"T{run}-%",))
            assert cur.fetchone()[0] == len(scenarios)
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM heartbeat_readings WHERE customer_id LIKE %s", (f"T{run}-%",))
            cur.execute("DELETE FROM invalid_readings WHERE raw_payload LIKE %s", (f"%T{run}-%",))
