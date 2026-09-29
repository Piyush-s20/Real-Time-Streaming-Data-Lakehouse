"""Producer throughput benchmark.

Sends a fixed number of trades to Kafka in burst mode and prints the events/sec
acknowledged by the broker. Stop the regular producer first so the topic growth
check only counts benchmark events.

    python scripts/benchmark.py --count 200000 --runs 3
"""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from kafka import KafkaConsumer, TopicPartition  # noqa: E402

from common import config  # noqa: E402
from producer.main import create_producer, produce  # noqa: E402


def topic_size(consumer):
    """Total number of records ever written to the topic (sum of the partition end offsets)."""
    partitions = consumer.partitions_for_topic(config.KAFKA_TOPIC) or set()
    offsets = consumer.end_offsets([TopicPartition(config.KAFKA_TOPIC, p) for p in partitions])
    return sum(offsets.values())


def main():
    parser = argparse.ArgumentParser(description="Measures producer throughput in burst mode.")
    parser.add_argument("--count", type=int, default=100_000, help="events per run (default: 100000)")
    parser.add_argument("--runs", type=int, default=1, help="number of runs (default: 1)")
    args = parser.parse_args()

    consumer = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(','))
    print(f"Benchmark started at {datetime.now(timezone.utc).isoformat(timespec='seconds')} "
          f"({args.runs} x {args.count} events to '{config.KAFKA_TOPIC}')")

    rates = []
    for run in range(1, args.runs + 1):
        before = topic_size(consumer)
        producer = create_producer(burst=True)
        sent, failed, elapsed = produce(producer, burst=True, count=args.count)
        producer.close()
        acked = sent - failed
        rates.append(acked / elapsed)
        print(f"Run {run}: {acked} events acknowledged in {elapsed:.2f} s -> {rates[-1]:,.0f} events/sec "
              f"({failed} failed, topic grew by {topic_size(consumer) - before})")

    consumer.close()
    if len(rates) > 1:
        print(f"Min / mean / max: {min(rates):,.0f} / {sum(rates) / len(rates):,.0f} / {max(rates):,.0f} events/sec")


if __name__ == "__main__":
    main()
