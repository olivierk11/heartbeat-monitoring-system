"""Validation and classification of incoming heart-rate messages.

Kept separate from the consumer so it can be unit-tested without Kafka or a DB.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from heartbeat import config

REQUIRED_FIELDS = ("customer_id", "timestamp", "heart_rate")
MAX_CLOCK_SKEW = timedelta(minutes=5)   # readings this far in the future are rejected


class ValidationError(ValueError):
    """Raised when a message cannot be stored as a valid reading."""


@dataclass(frozen=True)
class Reading:
    customer_id: str
    timestamp: datetime
    heart_rate: int
    status: str   # 'normal' | 'low' | 'high'

    def as_row(self) -> tuple:
        return (self.customer_id, self.timestamp, self.heart_rate, self.status)


def classify(heart_rate: int) -> str:
    if heart_rate < config.HR_LOW_THRESHOLD:
        return "low"
    if heart_rate > config.HR_HIGH_THRESHOLD:
        return "high"
    return "normal"


def parse_message(raw: bytes | str) -> dict:
    """Decode a Kafka message value into a dict."""
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"malformed JSON: {exc.__class__.__name__}") from exc
    if not isinstance(payload, dict):
        raise ValidationError("payload is not a JSON object")
    return payload


def _parse_timestamp(value) -> datetime:
    if not isinstance(value, str):
        raise ValidationError("timestamp must be an ISO-8601 string")
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError("timestamp is not valid ISO-8601") from exc
    if ts.tzinfo is None:                       # assume UTC if no offset given
        ts = ts.replace(tzinfo=timezone.utc)
    if ts > datetime.now(timezone.utc) + MAX_CLOCK_SKEW:
        raise ValidationError("timestamp is in the future")
    return ts


def _parse_heart_rate(value) -> int:
    # bool is a subclass of int in Python, so exclude it explicitly
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("heart_rate must be a number")
    hr = int(round(value))
    if not (config.HR_MIN_VALID <= hr <= config.HR_MAX_VALID):
        raise ValidationError(
            f"heart_rate outside valid range "
            f"{config.HR_MIN_VALID}-{config.HR_MAX_VALID}"
        )
    return hr


def validate(payload: dict) -> Reading:
    """Return a clean Reading or raise ValidationError explaining why not."""
    missing = [f for f in REQUIRED_FIELDS if payload.get(f) in (None, "")]
    if missing:
        raise ValidationError(f"missing field(s): {', '.join(missing)}")

    customer_id = payload["customer_id"]
    if not isinstance(customer_id, str) or len(customer_id.strip()) == 0:
        raise ValidationError("customer_id must be a non-empty string")
    customer_id = customer_id.strip()
    if len(customer_id) > 20:
        raise ValidationError("customer_id longer than 20 characters")

    ts = _parse_timestamp(payload["timestamp"])
    hr = _parse_heart_rate(payload["heart_rate"])
    return Reading(customer_id, ts, hr, classify(hr))
