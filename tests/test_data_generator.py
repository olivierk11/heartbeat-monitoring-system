"""Unit tests for the synthetic data generator (no Kafka/DB needed)."""
from datetime import datetime

import pytest

from heartbeat.data_generator import HeartbeatGenerator
from heartbeat.validation import ValidationError, validate


def test_reading_has_required_fields():
    r = HeartbeatGenerator(num_customers=3, seed=1).generate_reading()
    assert set(r) == {"customer_id", "timestamp", "heart_rate"}
    assert r["customer_id"].startswith("CUST-")
    assert isinstance(r["heart_rate"], int)
    datetime.fromisoformat(r["timestamp"])  # parses as ISO-8601


def test_batch_has_one_reading_per_customer():
    gen = HeartbeatGenerator(num_customers=7, seed=2)
    batch = gen.generate_batch()
    assert len(batch) == 7
    assert len({r["customer_id"] for r in batch}) == 7


def test_clean_data_always_passes_validation():
    gen = HeartbeatGenerator(num_customers=10, anomaly_rate=0.05, error_rate=0.0, seed=3)
    for _ in range(500):
        for r in gen.generate_batch():
            validate(r)   # must not raise


def test_anomalies_are_generated():
    gen = HeartbeatGenerator(num_customers=10, anomaly_rate=0.05, seed=4)
    statuses = {validate(r).status for _ in range(300) for r in gen.generate_batch()}
    assert {"normal", "low", "high"} <= statuses


def test_corrupted_data_is_always_rejected():
    gen = HeartbeatGenerator(num_customers=5, error_rate=1.0, seed=5)
    for _ in range(200):
        with pytest.raises(ValidationError):
            validate(gen.generate_reading())


def test_seed_makes_output_reproducible():
    a = HeartbeatGenerator(num_customers=4, seed=42).generate_batch()
    b = HeartbeatGenerator(num_customers=4, seed=42).generate_batch()
    assert [r["heart_rate"] for r in a] == [r["heart_rate"] for r in b]


def test_invalid_customer_count():
    with pytest.raises(ValueError):
        HeartbeatGenerator(num_customers=0)
