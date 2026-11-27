"""Kafka producer: streams synthetic heart-rate readings to a Kafka topic.

    python -m heartbeat.producer                       # 10 customers, 1 batch/sec, forever
    python -m heartbeat.producer --customers 20 --interval 0.5
    python -m heartbeat.producer --error-rate 0.05     # 5% corrupted messages
    python -m heartbeat.producer --batches 30          # stop after 30 batches

Messages are keyed by customer_id, so all readings of one customer land in the
same partition and keep their order.
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import time

from confluent_kafka import KafkaException, Producer

from heartbeat import config
from heartbeat.data_generator import HeartbeatGenerator

log = logging.getLogger("producer")

stats = {"delivered": 0, "failed": 0}


def delivery_report(err, msg) -> None:
    """Called once per message when Kafka acknowledges (or rejects) it."""
    if err is not None:
        stats["failed"] += 1
        log.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        stats["delivered"] += 1
        log.debug("Delivered to %s [partition %d] @ offset %d",
                  msg.topic(), msg.partition(), msg.offset())


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "heartbeat-producer",
        "acks": "all",                 # wait for the broker to persist the message
        "enable.idempotence": True,    # no duplicates on producer retries
        "linger.ms": 20,               # small batching for throughput
    })


def run(customers: int, interval: float, anomaly_rate: float,
        error_rate: float, max_batches: int) -> None:
    producer = build_producer()
    generator = HeartbeatGenerator(customers, anomaly_rate, error_rate)

    running = True

    def stop(*_):
        nonlocal running
        running = False
        log.info("Shutdown requested, flushing remaining messages...")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    log.info("Producing to topic '%s' on %s | customers=%d interval=%.2fs error_rate=%.2f",
             config.KAFKA_TOPIC, config.KAFKA_BOOTSTRAP_SERVERS,
             customers, interval, error_rate)

    batches = sent = 0
    try:
        while running and (max_batches == 0 or batches < max_batches):
            for reading in generator.generate_batch():
                key = str(reading.get("customer_id", "unknown"))
                try:
                    producer.produce(
                        config.KAFKA_TOPIC,
                        key=key.encode(),
                        value=json.dumps(reading).encode(),
                        on_delivery=delivery_report,
                    )
                except BufferError:
                    # local queue full: serve callbacks then retry once
                    producer.poll(1)
                    producer.produce(config.KAFKA_TOPIC, key=key.encode(),
                                     value=json.dumps(reading).encode(),
                                     on_delivery=delivery_report)
                sent += 1
                log.info("Sent %s", json.dumps(reading))
            producer.poll(0)          # trigger delivery callbacks
            batches += 1
            time.sleep(interval)
    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
    finally:
        remaining = producer.flush(10)
        log.info("Stopped. batches=%d sent=%d delivered=%d failed=%d undelivered=%d",
                 batches, sent, stats["delivered"], stats["failed"], remaining)


def main() -> None:
    p = argparse.ArgumentParser(description="Stream synthetic heart-rate data to Kafka")
    p.add_argument("--customers", type=int, default=10, help="number of simulated customers")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between batches")
    p.add_argument("--anomaly-rate", type=float, default=0.01,
                   help="chance per reading that an anomaly episode starts")
    p.add_argument("--error-rate", type=float, default=0.0,
                   help="fraction of messages deliberately corrupted (0-1)")
    p.add_argument("--batches", type=int, default=0, help="stop after N batches (0 = forever)")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    run(a.customers, a.interval, a.anomaly_rate, a.error_rate, a.batches)


if __name__ == "__main__":
    main()
