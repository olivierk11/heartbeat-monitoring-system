# Real-Time Customer Heart Beat Monitoring System

A real-time data pipeline that simulates heart-rate monitors for multiple customers, streams the
readings through **Apache Kafka**, validates and classifies them in a **Python consumer**, and stores
them in **PostgreSQL** for historical querying and live dashboards.

![Data flow diagram](docs/architecture.png)

---

## Table of contents

1. [Project overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Dataset](#3-dataset)
4. [Database schema](#4-database-schema)
5. [Project structure](#5-project-structure)
6. [Prerequisites](#6-prerequisites)
7. [Setup](#7-setup)
8. [Running the pipeline](#8-running-the-pipeline)
9. [Querying the data](#9-querying-the-data)
10. [Testing](#10-testing)
11. [Dashboard](#11-dashboard)
12. [Results and screenshots](#12-results-and-screenshots)
13. [Design decisions](#13-design-decisions)
14. [Troubleshooting](#14-troubleshooting)
15. [Shutting down](#15-shutting-down)

---

## 1. Project overview

| Lab objective | How it is met |
|---|---|
| Simulate real-time sensor data | `heartbeat/data_generator.py` produces realistic readings for any number of customers |
| Use Kafka for streaming (producer + consumer) | `heartbeat/producer.py` publishes to topic `heartbeat-readings`; `heartbeat/consumer.py` reads it |
| Process and validate incoming data | `heartbeat/validation.py` rejects bad messages and flags anomalies (too low / too high) |
| Design a PostgreSQL schema for time-series data | `sql/schema.sql` with indexes on the timestamp column |
| Build and test the pipeline | Docker Compose environment, 36 unit tests and 1 end-to-end integration test |
| Optional dashboard | Streamlit app (`heartbeat/dashboard.py`) and a provisioned Grafana dashboard |

**Tech stack:** Python 3.12, Apache Kafka 7.6 (Confluent) with Zookeeper, PostgreSQL 16,
Docker Compose, confluent-kafka, psycopg2, pytest, Streamlit, Grafana 11.

---

## 2. Architecture

**Data flow:** data generator → Kafka producer → Kafka topic → Kafka consumer (parse → validate → classify) → PostgreSQL → dashboard

| Component | File | Role |
|---|---|---|
| Synthetic data generator | `heartbeat/data_generator.py` | Generates `customer_id`, `timestamp`, `heart_rate` readings |
| Kafka producer | `heartbeat/producer.py` | Sends readings continuously as JSON to topic `heartbeat-readings`, keyed by `customer_id` |
| Kafka + Zookeeper | `docker-compose.yml` | Message broker; the topic has 3 partitions |
| Kafka consumer | `heartbeat/consumer.py` | Reads messages, validates them, flags anomalies, bulk-inserts into PostgreSQL, then commits Kafka offsets |
| Validation rules | `heartbeat/validation.py` | Pure functions, unit-tested independently of Kafka and the database |
| PostgreSQL | `sql/schema.sql` | Valid readings, rejected messages, and a "latest reading" view |
| Dashboards | `heartbeat/dashboard.py`, `grafana/` | Live visualisation of the stored data |

The diagram above is also available as Mermaid source (`docs/architecture.mermaid`) and can be
regenerated with `python docs/make_diagram.py`.

### Processing rules

| Check | Result |
|---|---|
| Not valid JSON, or not a JSON object | rejected → `invalid_readings` |
| Missing `customer_id`, `timestamp` or `heart_rate` | rejected |
| `heart_rate` not a number, or outside 30–220 bpm (sensor error) | rejected |
| `timestamp` not ISO-8601, or more than 5 minutes in the future | rejected |
| Valid and `heart_rate` < 50 bpm | stored with `status = 'low'`, logged as `ALERT LOW` |
| Valid and `heart_rate` > 120 bpm | stored with `status = 'high'`, logged as `ALERT HIGH` |
| Otherwise | stored with `status = 'normal'` |

All thresholds are configurable in `.env`.

---

## 3. Dataset

There is no external dataset: **all data is synthetic and generated in real time** by
`heartbeat/data_generator.py`, as required by the lab ("simulate real-time sensor data").

Each message has exactly the three required fields:

```json
{"customer_id": "CUST-0003", "timestamp": "2026-11-25T22:12:28.276336+00:00", "heart_rate": 85}
```

How the data is made realistic:

- Every customer has a personal resting heart rate between 60 and 85 bpm.
- Each new reading moves part of the way towards a target value plus random noise, so the
  signal drifts smoothly like a real pulse instead of jumping randomly.
- Occasionally a customer enters an **anomaly episode** for 3–10 readings: tachycardia
  (130–180 bpm) or bradycardia (38–48 bpm), followed by a gradual return to baseline.
- With `--error-rate`, a fraction of messages is deliberately corrupted (missing field, negative
  value, impossible value, `"N/A"`, invalid timestamp) to exercise the validation.

Timestamps are generated and stored in **UTC**. A 1,000-row export of the stored data is included in
`docs/sample_data.csv`.

---

## 4. Database schema

Defined in `sql/schema.sql` and applied automatically the first time the PostgreSQL container starts.

**`heartbeat_readings`**: valid, processed readings (the time-series table)

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL | primary key |
| `customer_id` | VARCHAR(20) | not null |
| `reading_time` | TIMESTAMPTZ | when the sensor took the reading |
| `heart_rate` | SMALLINT | CHECK between 30 and 220 |
| `status` | VARCHAR(10) | CHECK in (`normal`, `low`, `high`) |
| `ingested_at` | TIMESTAMPTZ | when the consumer stored it, default `NOW()` |

Constraint `UNIQUE (customer_id, reading_time)` prevents duplicate rows when Kafka redelivers a message.

Indexes:

- `idx_hb_reading_time` on `(reading_time DESC)`: time-range queries such as "last 15 minutes"
- `idx_hb_customer_time` on `(customer_id, reading_time DESC)`: one customer's history
- `idx_hb_anomalies`: a partial index on `reading_time` covering only rows `WHERE status <> 'normal'`

**`invalid_readings`**: every rejected message, with the reason, Kafka partition and offset
(kept for auditing, never silently dropped).

**`latest_heartbeat`** (view): the most recent reading of each customer.

Example queries for checking ingestion and exploring history are in `sql/sample_queries.sql`.

---

## 5. Project structure

```
heartbeat-monitoring/
├── docker-compose.yml            # Zookeeper, Kafka, topic init, PostgreSQL, Grafana (profile)
├── requirements.txt
├── .env.example                  # copy to .env
├── pytest.ini
├── .streamlit/config.toml        # dashboard theme
├── sql/
│   ├── schema.sql                # tables, indexes, view
│   └── sample_queries.sql        # ingestion checks and historical queries
├── heartbeat/
│   ├── config.py                 # settings from environment variables
│   ├── data_generator.py         # synthetic data
│   ├── validation.py             # validation and anomaly rules
│   ├── producer.py               # Kafka producer
│   ├── consumer.py               # Kafka consumer → PostgreSQL
│   └── dashboard.py              # Streamlit dashboard
├── tests/
│   ├── test_data_generator.py    # unit tests
│   ├── test_validation.py        # unit tests
│   └── test_pipeline_integration.py   # end-to-end test
├── grafana/                      # data source and dashboard provisioning
└── docs/
    ├── architecture.png          # data flow diagram
    ├── architecture.mermaid
    ├── make_diagram.py
    ├── sample_data.csv           # sample of ingested data
    └── screenshots/              # evidence of the running system
```

---

## 6. Prerequisites

- **Docker Desktop** (Docker Engine with Compose v2), running
- **Python 3.10+** (developed and tested with Python 3.12 on Windows 11)
- Free host ports: 2181, 9092, the PostgreSQL port (5432 by default), 8501 (Streamlit), 3000 (Grafana)

---

## 7. Setup

Commands are shown for **Windows PowerShell**. On macOS/Linux, activate the environment with
`source .venv/bin/activate` and use `cp` instead of `copy`.

### 7.1 Clone the repository

```powershell
git clone https://github.com/<your-username>/heartbeat-monitoring.git
cd heartbeat-monitoring
```

### 7.2 Configure the environment

```powershell
copy .env.example .env
```

If port 5432 is already used on your machine (for example by a local PostgreSQL installation),
pick a free port and set it in `.env`. This is what was done during development:

```
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=15432
POSTGRES_HOST_PORT=15432
```

Docker Compose reads `POSTGRES_HOST_PORT` from `.env` to publish the container on that port;
the Python scripts read `POSTGRES_HOST` and `POSTGRES_PORT`.

### 7.3 Start the infrastructure

```powershell
docker compose up -d
docker compose ps -a
docker logs hb-kafka-init
```

Wait until `hb-kafka` and `hb-postgres` show **(healthy)**. `hb-kafka-init` is a one-time job: it
shows **Exited (0)** after creating the topic `heartbeat-readings` with 3 partitions.

### 7.4 Create the Python environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If PowerShell blocks the activation script, run this once and try again:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### 7.5 Check the database connection

```powershell
python -c "import psycopg2; from heartbeat import config; c=psycopg2.connect(**config.pg_params()); print('DB OK', c.server_version)"
```

---

## 8. Running the pipeline

Open **two terminals** in the project folder, with the virtual environment activated in each.

**Terminal 1: consumer** (start it first)

```powershell
python -m heartbeat.consumer
```

**Terminal 2: producer** (10 customers, one reading each per second, 5% corrupted messages)

```powershell
python -m heartbeat.producer --customers 10 --interval 1 --error-rate 0.05
```

| Producer option | Meaning | Default |
|---|---|---|
| `--customers N` | number of simulated customers | 10 |
| `--interval S` | seconds between batches | 1.0 |
| `--anomaly-rate P` | chance per reading that an anomaly episode starts | 0.01 |
| `--error-rate P` | fraction of deliberately corrupted messages | 0.0 |
| `--batches N` | stop after N batches (0 = run forever) | 0 |

Stop either process with **Ctrl+C**. Both shut down cleanly: the producer flushes pending messages,
and the consumer writes its last batch and commits offsets.

To preview generated data without Kafka:

```powershell
python -m heartbeat.data_generator --customers 5 --batches 3
```

---

## 9. Querying the data

Open an interactive SQL session inside the container:

```powershell
docker exec -it hb-postgres psql -U heartbeat -d heartbeat
```

```sql
SELECT COUNT(*) FROM heartbeat_readings;
SELECT customer_id, reading_time, heart_rate, status FROM heartbeat_readings ORDER BY reading_time DESC LIMIT 15;
SELECT status, COUNT(*) FROM heartbeat_readings GROUP BY status;
SELECT error_reason, COUNT(*) FROM invalid_readings GROUP BY error_reason ORDER BY 2 DESC;
\q
```

Run all prepared queries at once (PowerShell does not support `<` redirection):

```powershell
Get-Content sql\sample_queries.sql | docker exec -i hb-postgres psql -U heartbeat -d heartbeat
```

Export a sample to CSV:

```powershell
docker exec hb-postgres psql -U heartbeat -d heartbeat -c "\copy (SELECT customer_id, reading_time, heart_rate, status FROM heartbeat_readings ORDER BY reading_time DESC LIMIT 1000) TO STDOUT WITH CSV HEADER" | Out-File -Encoding utf8 docs\sample_data.csv
```

---

## 10. Testing

### Unit tests (no Docker needed)

```powershell
pytest -m "not integration" -v
```

36 tests cover the generator (fields, reproducibility, anomalies, corrupted data) and every
validation rule (classification boundaries, missing fields, wrong types, impossible values, bad and
future timestamps, malformed JSON).

### End-to-end integration test

Requires the containers **and the consumer** to be running.

```powershell
pytest -m integration -v
```

The test sends known messages through Kafka and checks the database:

| Message | Expected result |
|---|---|
| 72 bpm | stored as `normal` |
| 42 bpm | stored as `low` |
| 165 bpm | stored as `high` |
| 120 bpm (boundary) | stored as `normal` |
| duplicate of a valid message | stored only once |
| 300 bpm, −10 bpm, `"N/A"`, missing timestamp, broken JSON | all rejected into `invalid_readings` |

It uses unique customer IDs for each run and deletes its rows afterwards. If the consumer is not
running, the test fails with "valid rows missing - is the consumer running?", which confirms it
detects a broken pipeline.

### Manual test scenarios

| Scenario | Command |
|---|---|
| Normal traffic | `python -m heartbeat.producer --customers 10` |
| Many anomalies | `python -m heartbeat.producer --anomaly-rate 0.1` |
| Dirty data | `python -m heartbeat.producer --error-rate 0.3` |
| Higher load | `python -m heartbeat.producer --customers 200 --interval 0.2` |
| Fixed-length run | `python -m heartbeat.producer --batches 60` |

---

## 11. Dashboard

### Streamlit (used for the screenshots)

```powershell
streamlit run heartbeat/dashboard.py
```

Opens at http://localhost:8501 and shows:
- KPIs: readings, active customers, average bpm, anomalies, rejected messages
- a live heart-rate chart per customer
- the latest reading per customer
- recent anomalies
- rejected messages

The time window, refresh rate and selected customers can be changed in the sidebar.

### Grafana (optional, pre-provisioned)

```powershell
docker compose --profile grafana up -d
```

Open http://localhost:3000 and log in with `admin` / `admin`. The PostgreSQL data source
(`HeartbeatDB`) and the dashboard **Heartbeat → Customer Heart Beat Monitor** are created
automatically. Grafana connects to PostgreSQL over the Docker network, so the host port setting
does not affect it.

---

## 12. Results and screenshots

### Test results

| Test | Result |
|---|---|
| Unit tests | **36 passed** |
| Integration test | **passed in 6.4 s** |

### Ingestion run

Ingestion run with 10 customers (part of the run used `--anomaly-rate 0.1` to demonstrate alerts):

| Metric | Value |
|---|---|
| Readings stored | 10,796 |
| `normal` | 8,952 |
| `high` | 1,193 |
| `low` | 651 |
| Rejected messages | 598 |

The 598 rejected messages break down by reason as follows:

| Rejection reason | Count |
|---|---|
| heart rate outside 30–220 | 263 |
| invalid timestamp | 103 |
| heart rate not a number | 98 |
| missing timestamp | 50 |
| missing customer ID | 45 |
| missing heart rate | 39 |

`EXPLAIN` confirms that time-range queries use the timestamp index
(`Index Scan using idx_hb_reading_time`).

The anomaly share (about 17%) is high because the anomaly rate was raised from 0.01 to 0.1 for
the demonstration. With the default rate it is about 3–5%.

### Screenshots

**1. Docker containers healthy and Kafka topic created (3 partitions)**
![Docker containers](docs/screenshots/01_docker_containers.png)

**2. Unit tests: 36 passed**
![Unit tests](docs/screenshots/02_unit_tests.png)

**3. Synthetic data generator output**
![Generator](docs/screenshots/03_data_generator.png)

**4. Kafka producer streaming readings**
![Producer](docs/screenshots/04_producer.png)

**5. Kafka consumer: OK, ALERT and REJECTED messages, batches flushed to PostgreSQL**
![Consumer](docs/screenshots/05_consumer.png)

**6. PostgreSQL: row count, latest readings and status breakdown**
![Database ingestion](docs/screenshots/06_db_ingestion.png)

**7. PostgreSQL: recent anomalies and rejection reasons**
![Anomalies and rejections](docs/screenshots/07_db_anomalies_rejected.png)

**8. PostgreSQL: per-customer statistics and index usage**
![Statistics and index](docs/screenshots/08_db_stats_index.png)

**9. End-to-end integration test passed**
![Integration test](docs/screenshots/09_integration_test.png)

**10. Streamlit dashboard**
![Streamlit dashboard](docs/screenshots/10_streamlit_dashboard.png)

---

## 13. Design decisions

- **Messages are keyed by `customer_id`.** All readings of one customer go to the same partition,
  so they stay in order.
- **At-least-once delivery.** Kafka auto-commit is disabled; the consumer commits offsets only after
  the PostgreSQL transaction succeeds. If the consumer crashes, messages are re-read, not lost.
- **Idempotent storage.** `UNIQUE (customer_id, reading_time)` together with `ON CONFLICT DO NOTHING`
  makes redelivered messages harmless. The integration test verifies this with a duplicate.
- **Batch inserts.** Rows are written with `execute_values` in batches of up to 100 rows or every
  2 seconds, which is much faster than one `INSERT` per message.
- **Rejected data is kept.** Invalid messages are stored with their reason instead of being
  dropped, so data quality can be audited.
- **Time-series indexing.** B-tree indexes on `reading_time` and `(customer_id, reading_time)`, plus
  a partial index for anomalies, keep the dashboard's time-window queries fast as the table grows.
- **Validation is separate from I/O.** `validation.py` has no Kafka or database code, so all rules
  are covered by fast unit tests.
- **Producer reliability.** `acks=all` and `enable.idempotence=True` prevent message loss and
  duplicates on producer retries.

---

## 14. Troubleshooting

| Problem | Cause and fix |
|---|---|
| `password authentication failed for user "heartbeat"` | Another PostgreSQL is answering on that port. Choose a free port in `.env` (see 7.2) and run `docker compose up -d` again |
| `Bind for 0.0.0.0:<port> failed: port is already allocated` | Another container already uses that port. Check with `docker ps -a` and choose a different `POSTGRES_HOST_PORT` |
| Consumer shows `Disconnected ... POLLHUP` for `localhost:9092` | Kafka is still starting. Wait until `docker compose ps` shows `hb-kafka` as healthy |
| `UNKNOWN_TOPIC_OR_PART` | The topic was not created. Run `docker compose up -d` again and check `docker logs hb-kafka-init` |
| Tables do not exist | `schema.sql` only runs on a new volume. Run `docker compose down -v` then `docker compose up -d` |
| Integration test fails: "valid rows missing" | The consumer is not running. Start it and rerun the test |
| Dashboard shows "No readings in this window" | The producer or consumer is stopped, or the time window is too short |
| PowerShell: "running scripts is disabled" | `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser` |

### Notes from development on Windows

- Port 5432 was used by a local Windows PostgreSQL service (`postgresql-x64-16`), and port 5433
  by another project's Docker container. The database was therefore published on host port
  **15432** through `POSTGRES_HOST_PORT` in `.env`.
- `POSTGRES_HOST=127.0.0.1` is used instead of `localhost`, because Windows may resolve `localhost`
  to IPv6 (`::1`) and reach a different server.
- Integration test messages are processed even if the consumer starts after the test has finished.
  Leftover test rows can be removed with
  `DELETE FROM heartbeat_readings WHERE customer_id NOT LIKE 'CUST-%';`
- Newer Streamlit versions deprecate `use_container_width`; the dashboard uses `width="stretch"`.
- Log timestamps are local time (UTC+2), while `reading_time` in the database is UTC.

---

## 15. Shutting down

```powershell
docker compose --profile grafana down      # stop all containers, keep the data
docker compose --profile grafana down -v   # stop and delete all data
```

---
*Author: Olivier Kwizera, AmaliTech data engineering lab*