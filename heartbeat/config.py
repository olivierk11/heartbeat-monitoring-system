"""Central configuration, read from environment variables / a .env file."""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv is optional
    pass

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeat-readings")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "heartbeat-db-writer")

# PostgreSQL
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "heartbeat")
PG_USER = os.getenv("POSTGRES_USER", "heartbeat")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "heartbeat")

# Validation: outside [HR_MIN_VALID, HR_MAX_VALID] the reading is physically
# implausible (sensor error) and is rejected.
HR_MIN_VALID = int(os.getenv("HR_MIN_VALID", "30"))
HR_MAX_VALID = int(os.getenv("HR_MAX_VALID", "220"))

# Anomaly detection: a valid reading below/above these is flagged low/high.
HR_LOW_THRESHOLD = int(os.getenv("HR_LOW_THRESHOLD", "50"))
HR_HIGH_THRESHOLD = int(os.getenv("HR_HIGH_THRESHOLD", "120"))


def pg_params() -> dict:
    """Keyword arguments for psycopg2.connect()."""
    return {
        "host": PG_HOST,
        "port": PG_PORT,
        "dbname": PG_DB,
        "user": PG_USER,
        "password": PG_PASSWORD,
    }
