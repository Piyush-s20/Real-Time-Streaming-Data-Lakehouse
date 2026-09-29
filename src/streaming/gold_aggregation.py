import sys
from pathlib import Path

from pyspark.sql.functions import col, window, avg, sum

# Make src/common importable when this file is run as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config  # noqa: E402
from common.progress import ProgressLogger  # noqa: E402
from common.spark_session import create_spark_session, wait_for_delta_table  # noqa: E402


def main():
    print("Initializing Spark Session for Gold Layer...")

    spark = create_spark_session("GoldAggregation")
    spark.streams.addListener(ProgressLogger("gold"))

    print("Reading real-time structured stream from Silver layer...")

    # 1. Stream the Silver Delta table. Its schema comes from the Delta transaction log.
    wait_for_delta_table(spark, config.SILVER_TABLE)
    silver_df = spark.readStream \
        .format("delta") \
        .load(config.SILVER_TABLE)

    # 2. Business Logic: Real-time windowed aggregations
    # We group the data by 30-second time windows and the stock symbol.
    gold_df = silver_df \
        .withWatermark("trade_timestamp", "30 seconds") \
        .groupBy(
            window(col("trade_timestamp"), "30 seconds"),
            col("symbol")
        ) \
        .agg(
            avg("price").alias("moving_avg_price"),
            sum("volume").alias("total_volume")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("moving_avg_price"),
            col("total_volume")
        )

    print("Calculating metrics and writing to the MinIO Gold Delta table...")
    print("NOTE: Spark only commits a window to the Gold table after that 30-second window fully closes!")

    # 3. Write the aggregated metrics to the Gold bucket as a Delta table
    query = gold_df.writeStream \
        .format("delta") \
        .option("checkpointLocation", "s3a://gold/checkpoints/financial_metrics/") \
        .option("path", config.GOLD_TABLE) \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
