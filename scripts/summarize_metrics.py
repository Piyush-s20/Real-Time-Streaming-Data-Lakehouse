"""Summarises the per-batch progress files written by the streaming jobs (metrics/<job>_progress.jsonl).

    python scripts/summarize_metrics.py --since 2026-09-28T17:00:00Z

Only batches that read at least one row are counted. "rows / batch time" is the total
number of rows divided by the summed batch durations.
"""
import argparse
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import config  # noqa: E402

JOBS = ["bronze", "silver", "gold"]


def parse_time(value):
    # Spark writes e.g. 2026-09-28T17:05:03.123Z; Python < 3.11 does not accept the "Z" suffix
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def main():
    parser = argparse.ArgumentParser(description="Summarises streaming job throughput.")
    parser.add_argument("--since", type=parse_time, help="only batches that started at/after this UTC time")
    parser.add_argument("--until", type=parse_time, help="only batches that started before this UTC time")
    args = parser.parse_args()

    print(f"{'job':<8}{'batches':>8}{'rows':>10}{'rows / batch time':>20}"
          f"{'median processedRowsPerSecond':>32}{'max':>10}{'avg batch ms':>14}")
    for job in JOBS:
        path = config.METRICS_DIR / f"{job}_progress.jsonl"
        if not path.exists():
            print(f"{job:<8}no metrics file at {path}")
            continue

        with path.open() as f:
            batches = [json.loads(line) for line in f if line.strip()]
        batches = [
            b for b in batches
            if b["num_input_rows"] > 0
            and (args.since is None or parse_time(b["timestamp"]) >= args.since)
            and (args.until is None or parse_time(b["timestamp"]) < args.until)
        ]
        if not batches:
            print(f"{job:<8}no batches with input rows in the selected period")
            continue

        rows = sum(b["num_input_rows"] for b in batches)
        busy_ms = sum(b["batch_duration_ms"] for b in batches)
        # Spark leaves the rate out when the batch duration rounds to zero
        rates = [b["processed_rows_per_second"] for b in batches if b["processed_rows_per_second"] is not None] or [0]
        print(f"{job:<8}{len(batches):>8}{rows:>10}{rows / max(busy_ms / 1000, 0.001):>20,.0f}"
              f"{statistics.median(rates):>32,.0f}{max(rates):>10,.0f}{busy_ms / len(batches):>14,.0f}")


if __name__ == "__main__":
    main()
