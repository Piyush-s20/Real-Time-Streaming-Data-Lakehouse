"""SparkSession setup shared by the Bronze, Silver and Gold streaming jobs."""
import os
import time

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from common import config

# Must match the delta-spark version pinned in requirements.txt
DELTA_VERSION = "3.2.0"

BASE_PACKAGES = [
    f"io.delta:delta-spark_2.12:{DELTA_VERSION}",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def create_spark_session(app_name, extra_packages=()):
    """Builds a SparkSession with Delta Lake enabled and S3A pointed at MinIO."""
    # On Windows, Hadoop finds winutils.exe through HADOOP_HOME, but the JVM only loads
    # hadoop.dll (needed for S3A's local upload buffer) from a folder on PATH
    if os.name == "nt" and os.getenv("HADOOP_HOME"):
        os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]

    # The packages are downloaded dynamically upon the first startup
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", ",".join([*extra_packages, *BASE_PACKAGES])) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Reduce log spam in the console
    spark.sparkContext.setLogLevel("WARN")
    return spark


def wait_for_delta_table(spark, path, poll_seconds=10):
    """Blocks until the upstream job has committed the first version of the Delta table at `path`.

    A Delta stream can only start once the table has a commit, so this lets the jobs be
    started in any order.
    """
    while True:
        try:
            # Resolving a batch read re-lists the transaction log each time. DeltaTable.isDeltaTable()
            # can keep answering from a cached, empty log if it looks before the first commit lands.
            spark.read.format("delta").load(path)
            return
        except AnalysisException:
            print(f"Waiting for the Delta table at {path} (is the upstream job running?)...")
            time.sleep(poll_seconds)
