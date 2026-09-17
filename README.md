#  Real-Time Crypto Anomaly Detection Pipeline

A production-grade, event-driven streaming data pipeline that ingests live cryptocurrency ticker feeds, processes them through an Apache Kafka event broker, evaluates market ticks using incremental online machine learning, persists alerts to SQLite, and renders a live monitoring dashboard via Streamlit.

---

## 🏗️ Architecture & Data Flow

```text
[Coinbase WebSocket] 
       │ (Async Live Ticks)
       ▼
[Python Producer] 
       │ (JSON Serialization)
       ▼
[Apache Kafka (WSL2 / KRaft Mode)] 
       │ (Topic: crypto-ticks)
       ▼
[Python Consumer & River Online ML (Half-Space Trees)] 
       │ (Rolling Window: SMA, Price Delta, Anomaly Scoring)
       ├─ [Normal Ticks] ──► (Logged to terminal)
       └─ [Anomalies (>0.7)] ──► [SQLite Database (crypto_anomalies.db)]
                                       │
                                       ▼
                         [Streamlit Real-Time Dashboard]
