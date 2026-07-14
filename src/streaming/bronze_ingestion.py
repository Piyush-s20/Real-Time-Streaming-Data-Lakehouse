from pyspark.sql import SparkSession
def main():
    print("Initializing Spark Session...")
    
    # 1. Configure Spark with Kafka and AWS/S3 dependencies
    # We download these packages dynamically upon startup
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
# Reduce log spam in the console
    spark.sparkContext.setLogLevel("WARN")

    print("Connecting to Kafka and reading stream...")
    
    # 2. Read the real-time stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "financial_trades") \
        .option("startingOffsets", "earliest") \
        .load()
# Kafka stores data as binary by default. We cast it to a string so it's readable.
    raw_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_payload", "timestamp as kafka_ingest_time")

    print("Writing stream to MinIO Bronze layer...")
    
    # 3. Write the stream to our S3 Bronze bucket
    # We use Checkpointing to ensure fault tolerance. If this job crashes, 
    # it knows exactly where it left off when it restarts.
    query = raw_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "s3a://bronze/checkpoints/financial_trades/") \
        .option("path", "s3a://bronze/financial_trades/") \
        .outputMode("append") \
        .start()

    # Keep the streaming job running continuously
    query.awaitTermination()

if __name__ == "__main__":
    main()