"""Streamlit dashboard for the heart-rate pipeline (optional extension).

    streamlit run heartbeat/dashboard.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # make 'heartbeat' importable

import pandas as pd
import psycopg2
import streamlit as st

from heartbeat import config

st.set_page_config(page_title="Heart beat monitor", page_icon="❤️", layout="wide")


@st.cache_resource
def get_connection():
    conn = psycopg2.connect(**config.pg_params())
    conn.autocommit = True
    return conn


def query(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    except psycopg2.Error:
        get_connection.clear()     # drop a broken cached connection
        raise


# ----------------------------- sidebar --------------------------------- #
st.sidebar.header("Settings")
window = st.sidebar.slider("Time window (minutes)", 1, 60, 10)
refresh = st.sidebar.slider("Refresh every (seconds)", 2, 30, 5)
auto = st.sidebar.toggle("Auto-refresh", value=True)

try:
    customers = query("SELECT DISTINCT customer_id FROM heartbeat_readings ORDER BY 1")
except psycopg2.Error as exc:
    st.error(f"Can't reach PostgreSQL at {config.PG_HOST}:{config.PG_PORT}. "
             f"Start it with `docker compose up -d`.\n\n{exc}")
    st.stop()

all_ids = customers["customer_id"].tolist()
selected = st.sidebar.multiselect("Customers", all_ids, default=all_ids[:5])

# ----------------------------- header ---------------------------------- #
st.title("Customer heart beat monitor")
st.caption(f"Live readings from Kafka → PostgreSQL, last {window} minutes. "
           f"Anomaly thresholds: below {config.HR_LOW_THRESHOLD} or above "
           f"{config.HR_HIGH_THRESHOLD} bpm.")

interval = f"{window} minutes"
kpi = query("""
    SELECT COUNT(*)                                   AS readings,
           COUNT(DISTINCT customer_id)                AS customers,
           ROUND(AVG(heart_rate)::numeric, 1)         AS avg_bpm,
           COUNT(*) FILTER (WHERE status <> 'normal') AS anomalies
    FROM heartbeat_readings
    WHERE reading_time > NOW() - %s::interval
""", (interval,)).iloc[0]
rejected = query("SELECT COUNT(*) AS n FROM invalid_readings "
                 "WHERE received_at > NOW() - %s::interval", (interval,)).iloc[0]["n"]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Readings", int(kpi["readings"]))
c2.metric("Active customers", int(kpi["customers"]))
c3.metric("Average bpm", kpi["avg_bpm"] if kpi["avg_bpm"] is not None else "–")
c4.metric("Anomalies", int(kpi["anomalies"]))
c5.metric("Rejected messages", int(rejected))

# ----------------------------- chart ----------------------------------- #
st.subheader("Heart rate over time")
if selected:
    df = query("""
        SELECT reading_time, customer_id, heart_rate
        FROM heartbeat_readings
        WHERE reading_time > NOW() - %s::interval AND customer_id = ANY(%s)
        ORDER BY reading_time
    """, (interval, selected))
    if df.empty:
        st.info("No readings in this window yet. Start the producer and consumer.")
    else:
        df["reading_time"] = pd.to_datetime(df["reading_time"])
        wide = (df.pivot_table(index=pd.Grouper(key="reading_time", freq="1s"),
                               columns="customer_id", values="heart_rate")
                  .ffill())
        st.line_chart(wide, height=360)
else:
    st.info("Pick at least one customer in the sidebar.")

# ----------------------------- tables ---------------------------------- #
left, right = st.columns(2)
with left:
    st.subheader("Latest reading per customer")
    st.dataframe(query("SELECT * FROM latest_heartbeat ORDER BY customer_id"),
                 hide_index=True, width="stretch")
with right:
    st.subheader("Recent anomalies")
    st.dataframe(query("""
        SELECT reading_time, customer_id, heart_rate, status
        FROM heartbeat_readings WHERE status <> 'normal'
        ORDER BY reading_time DESC LIMIT 20
    """), hide_index=True, width="stretch")

with st.expander("Rejected messages"):
    st.dataframe(query("""
        SELECT received_at, error_reason, raw_payload
        FROM invalid_readings ORDER BY received_at DESC LIMIT 20
    """), hide_index=True, width="stretch")

if auto:
    time.sleep(refresh)
    st.rerun()
