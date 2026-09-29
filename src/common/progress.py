"""Per-batch throughput logging for the streaming jobs."""
import json
import math

from pyspark.sql.streaming import StreamingQueryListener

from common import config


def _finite(value):
    # Spark reports NaN/Infinity for undefined rates; store those as null so the file stays valid JSON
    return value if value is not None and math.isfinite(value) else None


def _rate(value):
    return "n/a" if value is None else f"{value:,.1f}"


class ProgressLogger(StreamingQueryListener):
    """Prints Spark's progress report after every micro-batch and appends it to metrics/<job>_progress.jsonl."""

    def __init__(self, job_name):
        self.job_name = job_name
        config.METRICS_DIR.mkdir(parents=True, exist_ok=True)
        self.path = config.METRICS_DIR / f"{job_name}_progress.jsonl"

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        # Read the typed fields: Spark 3.5.0 leaves batchDuration out of progress.json
        progress = event.progress
        record = {
            "job": self.job_name,
            "run_id": str(progress.runId),
            "batch_id": progress.batchId,
            "timestamp": progress.timestamp,
            "num_input_rows": progress.numInputRows,
            "input_rows_per_second": _finite(progress.inputRowsPerSecond),
            "processed_rows_per_second": _finite(progress.processedRowsPerSecond),
            "batch_duration_ms": progress.batchDuration,
            "duration_ms": dict(progress.durationMs),
        }
        print(f"[{self.job_name}] batch {record['batch_id']}: {record['num_input_rows']} rows | "
              f"input {_rate(record['input_rows_per_second'])} rows/s | "
              f"processed {_rate(record['processed_rows_per_second'])} rows/s | "
              f"batch duration {record['batch_duration_ms']} ms")
        with self.path.open("a") as f:
            f.write(json.dumps(record) + "\n")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass
