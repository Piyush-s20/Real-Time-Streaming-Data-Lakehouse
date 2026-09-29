import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from faker import Faker
from kafka import KafkaProducer

# Make src/common importable when this file is run as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config  # noqa: E402

# 1. Initialize Faker
fake = Faker()
SYMBOLS = ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA', 'BTC-USD', 'ETH-USD']


def generate_trade():
    """Simulates a single financial trade event."""
    return {
        "trade_id": fake.uuid4(),
        "symbol": random.choice(SYMBOLS),
        "price": round(random.uniform(10.0, 5000.0), 2),
        "volume": random.randint(1, 100),
        # Explicit UTC offset, so the event time means the same thing on every machine
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def create_producer(burst=False):
    """Configures the Kafka producer. Burst mode batches records to maximise throughput."""
    # Wait up to 20 ms to fill batches of up to 256 KB instead of sending records one by one
    batching = {"linger_ms": 20, "batch_size": 256 * 1024} if burst else {}
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(','),
        # Serialize the Python dictionary to a JSON byte string before sending
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        **batching
    )


def produce(producer, rate=None, burst=False, count=None):
    """Sends trades until `count` is reached (or Ctrl+C) and returns (sent, failed, elapsed_seconds).

    burst: no sleep. rate: fixed events per second. Neither: random 0.1-0.8 s pause between events.
    """
    failed = []
    sent = 0
    start = next_send = last_report = time.perf_counter()
    try:
        while count is None or sent < count:
            trade = generate_trade()

            # 2. Send the generated trade to the Kafka topic
            producer.send(config.KAFKA_TOPIC, trade).add_errback(failed.append)
            sent += 1

            if burst or rate:
                now = time.perf_counter()
                if now - last_report >= 1:
                    print(f"Produced {sent} events ({sent / (now - start):,.0f} events/s)")
                    last_report = now
                if rate:
                    next_send += 1 / rate
                    if next_send > now:
                        time.sleep(next_send - now)
            else:
                print(f"Produced: {trade}")
                # 3. Sleep for a random fraction of a second to simulate real-world variability
                time.sleep(random.uniform(0.1, 0.8))

    except KeyboardInterrupt:
        print("\nGracefully shutting down producer...")
    finally:
        # Wait until every buffered record is acknowledged (or has failed)
        producer.flush()
    return sent, len(failed), time.perf_counter() - start


def parse_args():
    parser = argparse.ArgumentParser(description="Streams synthetic trades to Kafka.")
    pacing = parser.add_mutually_exclusive_group()
    pacing.add_argument("--rate", type=float,
                        help="events per second (default: a random 0.1-0.8 s pause between events)")
    pacing.add_argument("--burst", action="store_true",
                        help="no sleep: send as fast as possible, with producer batching")
    parser.add_argument("--count", type=int, help="stop after N events (default: run until Ctrl+C)")
    args = parser.parse_args()
    if args.rate is not None and args.rate <= 0:
        parser.error("--rate must be positive")
    if args.count is not None and args.count <= 0:
        parser.error("--count must be positive")
    return args


def main():
    args = parse_args()
    print(f"Starting data generator... Streaming to Kafka topic: '{config.KAFKA_TOPIC}'")
    print("Press Ctrl+C to stop.\n")

    producer = create_producer(burst=args.burst)
    try:
        sent, failed, elapsed = produce(producer, rate=args.rate, burst=args.burst, count=args.count)
    finally:
        # Always close the producer to free resources
        producer.close()
    print(f"Sent {sent} events in {elapsed:.2f} s ({sent / elapsed:,.0f} events/s), {failed} failed.")


if __name__ == "__main__":
    main()
