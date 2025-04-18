import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, sum, date_format # Import date_format for potential grouping

# --- Configuration ---
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = os.getenv('MYSQL_PORT', 3306)
MYSQL_DB = os.getenv('MYSQL_DB', 'chat_app_db')
MYSQL_TABLE = os.getenv('MYSQL_METRICS_TABLE', 'stream_metrics_v2')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'aneesh200') # Be careful with passwords in code/env vars
APP_NAME = "LiveStreamMetricsBatchProcessor"

# JDBC URL
jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true" # Adjust SSL params if needed
connection_properties = {
  "user": MYSQL_USER,
  "password": MYSQL_PASSWORD,
  "driver": "com.mysql.cj.jdbc.Driver"
}

print(f"Starting {APP_NAME}...")
print(f"Reading from MySQL table: {MYSQL_DB}.{MYSQL_TABLE}")

# --- Initialize Spark Session ---
# Make sure the path to the JDBC driver JAR is correct
# It's often better to specify this in spark-submit --jars
spark = SparkSession \
    .builder \
    .appName(APP_NAME) \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate() \
    .conf("spark.jars", "/Users/aneesh/Downloads/DatabricksJDBC42-2.7.1.1004/DatabricksJDBC42.jar") 
    # Example config if providing jar in code (less recommended):

spark.sparkContext.setLogLevel("WARN")
print("SparkSession Initialized.")

# --- Read from MySQL ---
try:
    metrics_df = spark.read.jdbc(
        url=jdbc_url,
        table=MYSQL_TABLE,
        properties=connection_properties
    )
    print(f"Successfully read {metrics_df.count()} rows from {MYSQL_TABLE}.")
    metrics_df.printSchema()
    # metrics_df.show(5, truncate=False) # Show sample data

except Exception as e:
    print(f"❌ ERROR reading from MySQL: {e}")
    spark.stop()
    exit(1)

# --- Define Aggregations (Example Workload - *Match Streaming Logic*) ---
# Perform *overall* aggregations on the entire dataset for comparison
overall_aggregates_df = metrics_df \
    .groupBy("stream_id") \
    .agg(
        avg("processing_fps").alias("overall_avg_fps"),
        max("viewer_count").alias("overall_max_viewers"),
        avg("average_latency_ms").alias("overall_avg_latency"),
        sum("frame_count").alias("total_frames_recorded")
        # Add more aggregations...
    )

print("\n--- Overall Aggregates per Stream ---")
overall_aggregates_df.show(truncate=False)

# --- Example: Mimic Streaming Window Aggregation (Optional) ---
# To compare directly with a specific streaming window output,
# you might filter the batch data for that time range.
# Requires the 'metrics_timestamp' column from MySQL.
try:
    # Example: Aggregate for a specific hour
    hourly_aggregates_df = metrics_df \
        .withColumn("hour", date_format(col("metrics_timestamp"), "yyyy-MM-dd HH")) \
        .groupBy("hour", "stream_id") \
        .agg(
            avg("processing_fps").alias("hourly_avg_fps"),
            max("viewer_count").alias("hourly_max_viewers"),
            avg("average_latency_ms").alias("hourly_avg_latency")
        ) \
        .orderBy("hour", "stream_id")

    print("\n--- Hourly Aggregates per Stream (Example) ---")
    hourly_aggregates_df.show(truncate=False)
except Exception as e:
    print(f"\n⚠️ Warning: Could not calculate hourly aggregates (maybe timestamp column issue?): {e}")


# --- Output Results (Show to console) ---
# You could also save results to files (Parquet, CSV) or another DB table
# overall_aggregates_df.write.parquet("/path/to/output/overall_aggregates.parquet")

print(f"{APP_NAME} finished.")
spark.stop()