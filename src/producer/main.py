import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
# 1. Initialize Faker and configure the Kafka Producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    # Serialize the Python dictionary to a JSON byte string before sending
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
TOPIC_NAME = 'financial_trades'
SYMBOLS = ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA', 'BTC-USD', 'ETH-USD']

def generate_trade():
    """Simulates a single financial trade event."""
    return {
        "trade_id": fake.uuid4(),
        "symbol": random.choice(SYMBOLS),
        "price": round(random.uniform(10.0, 5000.0), 2),
        "volume": random.randint(1, 100),
        "timestamp": datetime.utcnow().isoformat()
    }
def main():
    print(f"Starting data generator... Streaming to Kafka topic: '{TOPIC_NAME}'")
    print("Press Ctrl+C to stop.\n")
    
    try:
        while True:
            trade = generate_trade()
            
            # 2. Send the generated trade to the Kafka topic
            producer.send(TOPIC_NAME, trade)
            print(f"Produced: {trade}")
            
            # 3. Sleep for a random fraction of a second to simulate real-world variability
            time.sleep(random.uniform(0.1, 0.8))
            
    except KeyboardInterrupt:
        print("\nGracefully shutting down producer...")
    finally:
        # Always close the producer to flush remaining messages and free resources
        producer.close()

if __name__ == "__main__":
    main()