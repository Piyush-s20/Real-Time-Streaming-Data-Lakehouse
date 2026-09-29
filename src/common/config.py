"""Settings shared by the producer and the Spark jobs.

Values come from environment variables (a .env file in the repo root is loaded
first, see .env.example) and fall back to the local docker-compose defaults.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Variables already set in the shell take precedence over the .env file
load_dotenv(PROJECT_ROOT / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "financial_trades"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

# Delta tables of the Medallion layers
BRONZE_TABLE = "s3a://bronze/financial_trades/"
SILVER_TABLE = "s3a://silver/financial_trades/"
GOLD_TABLE = "s3a://gold/financial_metrics/"

# Where the streaming jobs append their per-batch progress (<job>_progress.jsonl).
# Relative paths are resolved against the repo root.
METRICS_DIR = PROJECT_ROOT / os.getenv("METRICS_DIR", "metrics")
