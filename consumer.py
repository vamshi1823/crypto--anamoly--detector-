import json
import sqlite3
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from river import anomaly


# Initialize SQLite Database for storing anomalies
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

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'crypto-anomaly-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
TOPIC = 'crypto-ticks'
consumer.subscribe([TOPIC])

# Initialize Half-Space Trees for online anomaly detection
anomaly_model = anomaly.HalfSpaceTrees(
    n_trees=5,
    height=3,
    window_size=50,
    seed=42
)

price_windows = {
    "BTC-USD": [],
    "ETH-USD": []
}
WINDOW_SIZE = 10

print("Anomaly Detection Consumer with DB Logging started...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        data = json.loads(msg.value().decode('utf-8'))
        product_id = data.get("product_id")
        price = float(data.get("price", 0))

        if product_id in price_windows:
            history = price_windows[product_id]
            price_delta = price - history[-1] if history else 0.0

            history.append(price)
            if len(history) > WINDOW_SIZE:
                history.pop(0)

            sma = sum(history) / len(history)

            features = {
                "price": price,
                "sma": sma,
                "price_delta": price_delta
            }

            score = anomaly_model.score_one(features)
            anomaly_model.learn_one(features)

            is_anomaly = score > 0.7
            status = "🚨 [ANOMALY DETECTED]" if is_anomaly else "[NORMAL]"

            print(f"{status} {product_id} | Price: ${price:.2f} | SMA: ${sma:.2f} | Score: {score:.3f}")

            # If it's an anomaly, save it to SQLite!
            if is_anomaly:
                conn = sqlite3.connect('crypto_anomalies.db')
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO anomalies (timestamp, product_id, price, sma, score)
                    VALUES (?, ?, ?, ?, ?)
                ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), product_id, price, sma, score))
                conn.commit()
                conn.close()

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
finally:
    consumer.close()