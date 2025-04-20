import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    LongType
)

# === Configurations ===
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_METRICS_TOPIC = os.getenv('KAFKA_METRICS_TOPIC', 'stream-metrics')
APP_NAME = "LiveStreamMetricsProcessor"
PROCESSING_INTERVAL = "5 seconds"  # Faster micro-batches for low-latency
WINDOW_DURATION = "30 seconds"
SLIDE_DURATION = "10 seconds"
WATERMARK_DELAY = "10 seconds"

# === Define Schema ===
metrics_schema = StructType([
    StructField("stream_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("interval_sec", DoubleType(), True),
    StructField("frame_count", IntegerType(), True),
    StructField("processing_fps", DoubleType(), True),
    StructField("average_latency_ms", DoubleType(), True),
    StructField("average_processing_time_ms", DoubleType(), True),
    StructField("average_frame_kbytes", DoubleType(), True),
    StructField("viewer_count", IntegerType(), True),
    StructField("latency_stddev_ms", DoubleType(), True),
    StructField("kafka_last_offset", LongType(), True),
    StructField("kafka_partition", IntegerType(), True)
])

# === Initialize Spark Session ===
spark = SparkSession \
    .builder \
    .appName(APP_NAME) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ SparkSession Initialized.")

# === Read Kafka Stream ===
raw_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_METRICS_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("🔄 Kafka stream reader initialized.")

# === Parse JSON Payload ===
parsed_df = raw_stream_df \
    .selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), metrics_schema).alias("data")) \
    .select("data.*")

# === Convert Timestamps ===
processed_df = parsed_df \
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withWatermark("event_timestamp", WATERMARK_DELAY)

print("📦 Event timestamp + watermark applied.")

# === Windowed Aggregations ===
windowed_metrics_df = processed_df \
    .groupBy(
        window(col("event_timestamp"), WINDOW_DURATION, SLIDE_DURATION),
        col("stream_id")
    ) \
    .agg(
        F.avg("processing_fps").alias("avg_fps"),
        F.max("viewer_count").alias("max_viewers"),
        F.avg("average_latency_ms").alias("avg_latency"),
        F.sum("frame_count").alias("frames_in_window"),
        F.max("ingestion_time").alias("last_ingested_at")  # Track latest arrival
    ) \
    .select(
        "window.start", "window.end", "stream_id",
        "avg_fps", "max_viewers", "avg_latency", "frames_in_window",
        "last_ingested_at"
    )

print("📊 Aggregations defined.")

# === Output Stream to Console (Live Debug) ===
query = windowed_metrics_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 100) \
    .trigger(processingTime=PROCESSING_INTERVAL) \
    .start()

print("🚀 Streaming query started (console sink).")
query.awaitTermination()
