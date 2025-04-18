import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, min, count, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_METRICS_TOPIC = os.getenv('KAFKA_METRICS_TOPIC', 'stream-metrics')
APP_NAME = "LiveStreamMetricsProcessor"
PROCESSING_INTERVAL = "30 seconds" # How often to trigger processing
WINDOW_DURATION = "1 minute"       # Tumbling or Sliding Window duration
SLIDE_DURATION = "30 seconds"      # Sliding interval (if using sliding window)
WATERMARK_DELAY = "10 seconds"     # How long to wait for late data

# --- Define Schema for Kafka JSON Data ---
# Must match the JSON structure sent by consumer.py's metrics publisher
metrics_schema = StructType([
    StructField("stream_id", StringType(), True),
    StructField("timestamp", DoubleType(), True), # UNIX timestamp from consumer
    StructField("interval_sec", DoubleType(), True),
    StructField("frame_count", IntegerType(), True),
    StructField("processing_fps", DoubleType(), True),
    StructField("average_latency_ms", DoubleType(), True),
    StructField("average_processing_time_ms", DoubleType(), True),
    StructField("average_frame_kbytes", DoubleType(), True),
    StructField("viewer_count", IntegerType(), True),
    StructField("latency_stddev_ms", DoubleType(), True), # Nullable
    StructField("kafka_last_offset", LongType(), True),
    StructField("kafka_partition", IntegerType(), True)
])

print(f"Starting {APP_NAME}...")
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Metrics Topic: {KAFKA_METRICS_TOPIC}")
print(f"Window: {WINDOW_DURATION}, Slide: {SLIDE_DURATION}, Watermark: {WATERMARK_DELAY}")

# --- Initialize Spark Session ---
spark = SparkSession \
    .builder \
    .appName(APP_NAME) \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity (optional)
spark.sparkContext.setLogLevel("WARN")
print("SparkSession Initialized.")

# --- Read from Kafka ---
raw_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_METRICS_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Kafka stream reader initialized.")

# --- Parse JSON and Select Data ---
# Kafka message value is binary, cast to string, then parse JSON
parsed_stream_df = raw_stream_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), metrics_schema).alias("data")) \
    .select("data.*") # Flatten the struct

# --- Add Timestamp and Watermark ---
# Convert UNIX timestamp (double) from JSON payload to Spark Timestamp
# Assuming the 'timestamp' field is the event time
processed_stream_df = parsed_stream_df \
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
    .withWatermark("event_timestamp", WATERMARK_DELAY) # Handle late data

print("JSON parsed, timestamp added, watermark set.")

# --- Define Aggregations (Example Workload) ---
# Calculate avg FPS, max viewers, avg latency per stream_id in windows
windowed_aggregates_df = processed_stream_df \
    .groupBy(
        window(col("event_timestamp"), WINDOW_DURATION, SLIDE_DURATION), # Sliding Window
        # window(col("event_timestamp"), WINDOW_DURATION), # Tumbling Window
        col("stream_id")
    ) \
    .agg(
        avg("processing_fps").alias("avg_fps"),
        max("viewer_count").alias("max_viewers"),
        avg("average_latency_ms").alias("avg_latency"),
        sum("frame_count").alias("total_frames_in_window")
        # Add more aggregations as needed (min, stddev, count, etc.)
    ) \
    .select(
         "window.start",
         "window.end",
         "stream_id",
         "avg_fps",
         "max_viewers",
         "avg_latency",
         "total_frames_in_window"
     ) # Select desired columns

print("Windowed aggregations defined.")

# --- Output Results (Console Sink for Testing) ---
# Use 'update' mode for aggregations with watermark
# Use 'complete' mode if not using watermark (can grow state indefinitely)
# Use 'append' mode for simple transformations (no aggregation)
query = windowed_aggregates_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 100) \
    .trigger(processingTime=PROCESSING_INTERVAL) \
    .start()

# You could also write to another Kafka topic, files, or a database sink
# Example: Kafka Sink
# query = windowed_aggregates_df \
#     .selectExpr("to_json(struct(*)) AS value") \ # Convert row to JSON string
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("topic", "aggregated-stream-metrics") \
#     .option("checkpointLocation", "/tmp/spark-checkpoints/aggregated-metrics") \ # MUST set checkpoint location for Kafka sink
#     .outputMode("update") \
#     .trigger(processingTime=PROCESSING_INTERVAL) \
#     .start()

print("Output query started. Waiting for termination...")
query.awaitTermination()
print("Streaming query terminated.")