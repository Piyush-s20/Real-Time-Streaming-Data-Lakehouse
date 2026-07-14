from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def main():
    print("Initializing Spark Session for Gold Layer...")
    
    spark = SparkSession.builder \
        .appName("GoldAggregation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Define the Silver schema we are reading from
    silver_schema = StructType([
        StructField("trade_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", IntegerType(), True),
        StructField("trade_timestamp", TimestampType(), True),
        StructField("ingest_timestamp", TimestampType(), True)
    ])

    print("Reading real-time structured stream from Silver layer...")
    
    # 2. Read the Silver Parquet stream
    silver_df = spark.readStream \
        .format("parquet") \
        .schema(silver_schema) \
        .load("s3a://silver/financial_trades/")

    # 3. Business Logic: Real-time windowed aggregations
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

    print("Calculating metrics and writing to MinIO Gold layer...")
    print("NOTE: Spark will only write a file to S3 after a 30-second window fully closes!")
    
    # 4. Write the aggregated metrics to the Gold bucket
    query = gold_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "s3a://gold/checkpoints/financial_metrics/") \
        .option("path", "s3a://gold/financial_metrics/") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()