from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, max, min
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType

def create_spark_session(app_name="NodeHealthMonitor"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()
        
def define_schema():
    return StructType() \
        .add("node_id", IntegerType()) \
        .add("status", StringType()) \
        .add("timestamp", StringType()) 

def read_kafka_stream(spark, schema, topic, kafka_bootstrap):
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic) \
        .load()
    
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("timestamp").cast(TimestampType()))

    return parsed_df

def process_node_health(parsed_df):
    failure_df = parsed_df.filter(col("status") == "Failed")
    aggregated_df = failure_df.groupBy(
        window(col("event_time"), "5 seconds"), 
        col("node_id")
    ).agg(
        count("*").alias("failure_count"),
        avg("node_id").alias("avg_node_id"),
        max("node_id").alias("max_node_id"),
        min("node_id").alias("min_node_id")
    )

    return aggregated_df

def write_to_console(df):
    return df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

def write_to_mysql(df):
    flattened_df = df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window") 

    return flattened_df.writeStream \
        .foreachBatch(lambda batch_df, epochId: 
            batch_df.write
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/health_monitor")
            .option("dbtable", "node_health_aggregates")
            .option("user", "root")
            .option("password", "m6a2n6a2s7")
            .mode("append")
            .save()
        ).outputMode("update") \
        .start()
        
def main():
    spark = create_spark_session()
    schema = define_schema()
    kafka_topic = "node_status_updates"
    kafka_bootstrap = "localhost:9092"

    parsed_df = read_kafka_stream(spark, schema, kafka_topic, kafka_bootstrap)
    aggregated_df = process_node_health(parsed_df)

    write_to_console(aggregated_df)
    # TO-DO: fix error with MySQL connection
    # write_to_mysql(aggregated_df)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()