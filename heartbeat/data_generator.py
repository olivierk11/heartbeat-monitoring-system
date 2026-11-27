"""Synthetic heart-rate data generator.

Every simulated customer has a personal resting baseline (60-85 bpm). Each new
reading moves part of the way toward a target value plus random noise, so the
signal drifts smoothly the way a real heart rate does instead of jumping around.

Occasionally a customer enters an "episode" (tachycardia 130-180 bpm or
bradycardia 38-48 bpm) for a few readings, then drifts back to baseline. These
are the anomalies the consumer should flag.

With error_rate > 0 some records are deliberately corrupted (missing field,
negative value, impossible value, wrong type, bad timestamp) so the consumer's
validation can be tested.

Run on its own to preview the data:
    python -m heartbeat.data_generator --customers 5 --batches 3
"""
from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

CORRUPTION_KINDS = ("missing_field", "negative", "impossible", "bad_type", "bad_timestamp")


@dataclass
class CustomerState:
    customer_id: str
    baseline: int
    current: float
    episode_ticks: int = 0        # readings left in the current anomaly episode
    episode_target: float = 0.0   # heart rate the episode is pulling toward


class HeartbeatGenerator:
    def __init__(
        self,
        num_customers: int = 10,
        anomaly_rate: float = 0.01,
        error_rate: float = 0.0,
        seed: Optional[int] = None,
    ):
        if num_customers < 1:
            raise ValueError("num_customers must be at least 1")
        self.rng = random.Random(seed)
        self.anomaly_rate = anomaly_rate
        self.error_rate = error_rate
        self.customers: list[CustomerState] = []
        for i in range(1, num_customers + 1):
            baseline = self.rng.randint(60, 85)
            self.customers.append(CustomerState(f"CUST-{i:04d}", baseline, float(baseline)))

    # ------------------------------------------------------------------ #
    def _next_heart_rate(self, c: CustomerState) -> int:
        if c.episode_ticks > 0:
            c.episode_ticks -= 1
            target = c.episode_target
        elif self.rng.random() < self.anomaly_rate:
            c.episode_ticks = self.rng.randint(3, 10)
            c.episode_target = (
                self.rng.uniform(130, 180) if self.rng.random() < 0.5
                else self.rng.uniform(38, 48)
            )
            target = c.episode_target
        else:
            target = c.baseline
        # Move 30% of the way to the target, plus physiological noise
        c.current += (target - c.current) * 0.3 + self.rng.gauss(0, 2)
        return int(round(c.current))

    def _corrupt(self, reading: dict) -> dict:
        r = dict(reading)
        kind = self.rng.choice(CORRUPTION_KINDS)
        if kind == "missing_field":
            del r[self.rng.choice(["customer_id", "timestamp", "heart_rate"])]
        elif kind == "negative":
            r["heart_rate"] = -self.rng.randint(1, 50)
        elif kind == "impossible":
            r["heart_rate"] = self.rng.randint(260, 400)
        elif kind == "bad_type":
            r["heart_rate"] = "N/A"
        elif kind == "bad_timestamp":
            r["timestamp"] = "not-a-timestamp"
        return r

    # ------------------------------------------------------------------ #
    def generate_reading(self, customer: Optional[CustomerState] = None) -> dict:
        """One reading: {'customer_id', 'timestamp' (ISO-8601 UTC), 'heart_rate'}."""
        c = customer or self.rng.choice(self.customers)
        reading = {
            "customer_id": c.customer_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "heart_rate": self._next_heart_rate(c),
        }
        if self.error_rate and self.rng.random() < self.error_rate:
            reading = self._corrupt(reading)
        return reading

    def generate_batch(self) -> list[dict]:
        """One reading for every customer (one 'tick' of the simulation)."""
        return [self.generate_reading(c) for c in self.customers]


def main() -> None:
    p = argparse.ArgumentParser(description="Preview synthetic heart-rate data")
    p.add_argument("--customers", type=int, default=5)
    p.add_argument("--batches", type=int, default=3)
    p.add_argument("--anomaly-rate", type=float, default=0.01)
    p.add_argument("--error-rate", type=float, default=0.0)
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()
    gen = HeartbeatGenerator(a.customers, a.anomaly_rate, a.error_rate, a.seed)
    for _ in range(a.batches):
        for reading in gen.generate_batch():
            print(json.dumps(reading))


if __name__ == "__main__":
    main()
