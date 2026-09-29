import sys
from pathlib import Path

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Make src/common importable when this file is run as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config  # noqa: E402
from common.progress import ProgressLogger  # noqa: E402
from common.spark_session import create_spark_session, wait_for_delta_table  # noqa: E402


def main():
    print("Initializing Spark Session for Silver Layer...")

    spark = create_spark_session("SilverProcessing")
    spark.streams.addListener(ProgressLogger("silver"))

    # 1. Define the exact schema we expect from our JSON payload
    # This enforces data quality. Anything that doesn't match this schema will be handled safely.
    json_schema = StructType([
        StructField("trade_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

    print("Reading real-time Delta stream from Bronze layer...")

    # 2. Stream the Bronze Delta table. Its schema comes from the Delta transaction log.
    wait_for_delta_table(spark, config.BRONZE_TABLE)
    bronze_df = spark.readStream \
        .format("delta") \
        .load(config.BRONZE_TABLE)

    # 3. Clean and transform the data
    # We parse the JSON string into a structured format, and pull the nested fields up into top-level columns
    silver_df = bronze_df \
        .withColumn("parsed_data", from_json(col("raw_payload"), json_schema)) \
        .select(
            col("parsed_data.trade_id").alias("trade_id"),
            col("parsed_data.symbol").alias("symbol"),
            col("parsed_data.price").alias("price"),
            col("parsed_data.volume").alias("volume"),
            col("parsed_data.timestamp").cast("timestamp").alias("trade_timestamp"),
            col("kafka_ingest_time").cast("timestamp").alias("ingest_timestamp")
        ) \
        .filter(col("price").isNotNull() & (col("price") > 0)) # Basic Data Quality Check: Drop invalid prices

    print("Writing cleansed data to the MinIO Silver Delta table...")

    # 4. Write the structured data to the Silver bucket as a Delta table
    query = silver_df.writeStream \
        .format("delta") \
        .option("checkpointLocation", "s3a://silver/checkpoints/financial_trades/") \
        .option("path", config.SILVER_TABLE) \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
