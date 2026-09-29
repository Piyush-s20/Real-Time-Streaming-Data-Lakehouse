import sys
from pathlib import Path

# Make src/common importable when this file is run as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config  # noqa: E402
from common.progress import ProgressLogger  # noqa: E402
from common.spark_session import KAFKA_PACKAGE, create_spark_session  # noqa: E402


def main():
    print("Initializing Spark Session...")

    # 1. Configure Spark with Kafka, Delta Lake and AWS/S3 dependencies (see common/spark_session.py)
    spark = create_spark_session("BronzeIngestion", extra_packages=[KAFKA_PACKAGE])
    spark.streams.addListener(ProgressLogger("bronze"))

    print("Connecting to Kafka and reading stream...")

    # 2. Read the real-time stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    # Kafka stores data as binary by default. We cast it to a string so it's readable.
    raw_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_payload", "timestamp as kafka_ingest_time")

    print("Writing stream to the MinIO Bronze Delta table...")

    # 3. Write the stream to our S3 Bronze bucket as a Delta table
    # Every micro-batch becomes one atomic commit in the Delta transaction log.
    # We use Checkpointing to ensure fault tolerance. If this job crashes,
    # it knows exactly where it left off when it restarts.
    query = raw_df.writeStream \
        .format("delta") \
        .option("checkpointLocation", "s3a://bronze/checkpoints/financial_trades/") \
        .option("path", config.BRONZE_TABLE) \
        .outputMode("append") \
        .start()

    # Keep the streaming job running continuously
    query.awaitTermination()


if __name__ == "__main__":
    main()
