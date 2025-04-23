import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, sum, date_format

# --- Configuration ---
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = os.getenv('MYSQL_PORT', 3306)
MYSQL_DB = os.getenv('MYSQL_DB', 'chat_app_db')
MYSQL_TABLE = os.getenv('MYSQL_METRICS_TABLE', 'stream_metrics_v2')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'm6a2n6a2s7')  # Handle securely in production
APP_NAME = "LiveStreamMetricsBatchProcessor"

# JDBC URL
jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true"
connection_properties = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}

print(f"Starting {APP_NAME}...")
print(f"Reading from MySQL table: {MYSQL_DB}.{MYSQL_TABLE}")

# --- Initialize Spark Session ---
spark = SparkSession.builder \
    .appName(APP_NAME) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("SparkSession Initialized.")

# --- Read from MySQL ---
try:
    metrics_df = spark.read.jdbc(
        url=jdbc_url,
        table=MYSQL_TABLE,
        properties=connection_properties
    )
    print(f"✅ Successfully read {metrics_df.count()} rows from {MYSQL_TABLE}.")
    metrics_df.printSchema()
except Exception as e:
    print(f"❌ ERROR reading from MySQL: {e}")
    spark.stop()
    exit(1)

# --- Aggregations ---
overall_aggregates_df = metrics_df.groupBy("stream_id").agg(
    avg("processing_fps").alias("overall_avg_fps"),
    max("viewer_count").alias("overall_max_viewers"),
    avg("average_latency_ms").alias("overall_avg_latency"),
    sum("frame_count").alias("total_frames_recorded")
)

print("\n--- Overall Aggregates per Stream ---")
overall_aggregates_df.show(truncate=False)

# --- Optional: Hourly Aggregates ---
try:
    hourly_aggregates_df = metrics_df \
        .withColumn("hour", date_format(col("metrics_timestamp"), "yyyy-MM-dd HH")) \
        .groupBy("hour", "stream_id") \
        .agg(
            avg("processing_fps").alias("hourly_avg_fps"),
            max("viewer_count").alias("hourly_max_viewers"),
            avg("average_latency_ms").alias("hourly_avg_latency")
        ) \
        .orderBy("hour", "stream_id")

    print("\n--- Hourly Aggregates per Stream ---")
    hourly_aggregates_df.show(truncate=False)

except Exception as e:
    print(f"\n⚠️ Warning: Could not calculate hourly aggregates: {e}")

print(f"{APP_NAME} finished.")
spark.stop()
