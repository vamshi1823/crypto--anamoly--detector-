import sqlite3

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Crypto Real-Time Anomaly Dashboard", layout="wide")

st.title("⚡ Real-Time Crypto Anomaly Detection Pipeline")
st.markdown("Monitoring live **BTC-USD** & **ETH-USD** ticks via Coinbase, Kafka, and River Online Machine Learning.")

# Auto-refresh the dashboard every 5 seconds
st.markdown(
    """
    <meta http-equiv="refresh" content="5">
    """,
    unsafe_allow_html=True
)

# Ensure the database table exists so the dashboard doesn't crash on startup
def init_db():
    conn = sqlite3.connect('crypto_anomalies.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            product_id TEXT,
            price REAL,
            sma REAL,
            score REAL
        )
    ''')
    conn.commit()
    conn.close()

init_db()

def load_data():
    conn = sqlite3.connect('crypto_anomalies.db')
    query = "SELECT * FROM anomalies ORDER BY id DESC LIMIT 50"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df_anomalies = load_data()

# Layout metrics
col1, col2 = st.columns(2)
with col1:
    st.metric(label="Total Logged Anomalies", value=len(df_anomalies))
with col2:
    latest_asset = df_anomalies.iloc[0]['product_id'] if not df_anomalies.empty else "N/A"
    st.metric(label="Last Flagged Asset", value=latest_asset)

st.subheader("🚨 Live Anomaly Alert Feed")
if df_anomalies.empty:
    st.info("No anomalies detected yet. The database table is ready—waiting for the consumer to log data!")
else:
    st.dataframe(df_anomalies, use_container_width=True)

    st.subheader("Anomaly Scores Chart")
    st.line_chart(df_anomalies.set_index('timestamp')['score'])