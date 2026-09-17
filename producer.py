import asyncio
import json

import websockets
from confluent_kafka import Producer

# Kafka configuration (WSL2 automatically forwards localhost to Windows)
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

TOPIC = 'crypto-ticks'

def delivery_report(err, msg):
    """Callback function to verify if a message was successfully delivered to Kafka."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Produced event to {msg.topic()} [Partition {msg.partition()}] at offset {msg.offset()}")

async def coinbase_stream():
    """Connects to Coinbase public WebSocket feed and streams ticker data into Kafka."""
    uri = "wss://ws-feed.exchange.coinbase.com"
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["ticker"]
    }

    async with websockets.connect(uri) as websocket:
        # Coinbase requires a subscription message within 5 seconds of connecting
        await websocket.send(json.dumps(subscribe_msg))
        print("Connected to Coinbase WebSocket and subscribed to BTC-USD and ETH-USD tickers.")

        async for message in websocket:
            data = json.loads(message)
            
            # Filter for ticker channel messages
            if data.get("type") == "ticker":
                product_id = data.get("product_id")
                price = data.get("price")
                
                print(f"Received Live Tick -> {product_id}: ${price}")

                # Produce message asynchronously into Kafka
                producer.produce(
                    TOPIC,
                    key=product_id.encode('utf-8'),
                    value=json.dumps(data).encode('utf-8'),
                    callback=delivery_report
                )
                # Trigger any pending delivery reports callbacks
                producer.poll(0)

if __name__ == "__main__":
    try:
        asyncio.run(coinbase_stream())
    except KeyboardInterrupt:
        print("\nProducer stopped by user. Flushing remaining messages...")
        producer.flush()