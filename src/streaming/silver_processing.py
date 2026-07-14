from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
def main():
    print("Initializing Spark Session for Silver Layer...")
    
    spark = SparkSession.builder \
        .appName("SilverProcessing") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Define the exact schema we expect from our JSON payload
    # This enforces data quality. Anything that doesn't match this schema will be handled safely.
    json_schema = StructType([
        StructField("trade_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", IntegerType(), True),
        StructField("timestamp", StringType(), True) 
    ])

    # Because we are reading from Parquet directories, Spark needs to know the schema of the Bronze layer
    # Because we are reading from Parquet directories, Spark needs to know the schema of the Bronze layer
    bronze_schema = StructType([
        StructField("raw_payload", StringType(), True),
        StructField("kafka_ingest_time", TimestampType(), True) # <-- Changed from StringType
    ])

    print("Reading real-time Parquet stream from Bronze layer...")
    
    # 2. Read the streaming Parquet files from Bronze
    bronze_df = spark.readStream \
        .format("parquet") \
        .schema(bronze_schema) \
        .load("s3a://bronze/financial_trades/")

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

    print("Writing cleansed data to MinIO Silver layer...")
    
    # 4. Write the structured data to the Silver bucket
    query = silver_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "s3a://silver/checkpoints/financial_trades/") \
        .option("path", "s3a://silver/financial_trades/") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()