"""Unit tests for message validation and anomaly classification."""
from datetime import datetime, timedelta, timezone

import pytest

from heartbeat.validation import ValidationError, classify, parse_message, validate

NOW = datetime.now(timezone.utc).isoformat()


def msg(**overrides):
    base = {"customer_id": "CUST-0001", "timestamp": NOW, "heart_rate": 72}
    base.update(overrides)
    return base


@pytest.mark.parametrize("hr,expected", [
    (72, "normal"), (50, "normal"), (120, "normal"),
    (49, "low"), (30, "low"),
    (121, "high"), (220, "high"),
])
def test_classification(hr, expected):
    assert classify(hr) == expected
    assert validate(msg(heart_rate=hr)).status == expected


def test_valid_message_round_trip():
    r = validate(parse_message(b'{"customer_id":"CUST-0001","timestamp":"2026-01-01T10:00:00Z","heart_rate":80}'))
    assert r.customer_id == "CUST-0001"
    assert r.heart_rate == 80
    assert r.timestamp.tzinfo is not None


def test_float_heart_rate_is_rounded():
    assert validate(msg(heart_rate=71.6)).heart_rate == 72


def test_naive_timestamp_is_treated_as_utc():
    r = validate(msg(timestamp="2026-01-01T10:00:00"))
    assert r.timestamp.utcoffset() == timedelta(0)


@pytest.mark.parametrize("payload,reason", [
    ({"timestamp": NOW, "heart_rate": 70}, "missing"),
    ({"customer_id": "CUST-0001", "heart_rate": 70}, "missing"),
    ({"customer_id": "CUST-0001", "timestamp": NOW}, "missing"),
    (msg(heart_rate=-5), "outside valid range"),
    (msg(heart_rate=0), "outside valid range"),
    (msg(heart_rate=29), "outside valid range"),
    (msg(heart_rate=221), "outside valid range"),
    (msg(heart_rate=350), "outside valid range"),
    (msg(heart_rate="N/A"), "must be a number"),
    (msg(heart_rate=True), "must be a number"),
    (msg(timestamp="not-a-timestamp"), "ISO-8601"),
    (msg(timestamp=12345), "ISO-8601"),
    (msg(timestamp=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()), "future"),
    (msg(customer_id=123), "non-empty string"),
    (msg(customer_id="X" * 21), "longer than 20"),
])
def test_invalid_messages_are_rejected(payload, reason):
    with pytest.raises(ValidationError, match=reason):
        validate(payload)


@pytest.mark.parametrize("raw", [b"not json", b"[1,2,3]", b"\xff\xfe", b""])
def test_malformed_payloads(raw):
    with pytest.raises(ValidationError):
        parse_message(raw)
