from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, max, min
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType

def create_spark_session(app_name="NodeHealthMonitor"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,mysql:mysql-connector-java:8.0.33") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark
        
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
    def process_batch(batch_df, epoch_id):
        try:
            jdbc_url = "jdbc:mysql://localhost:3306/health_monitor"
            connection_properties = {
                "user": "root",
                "password": "m6a2n6a2s7",
                "driver": "com.mysql.cj.jdbc.Driver"
            }
            
            if batch_df.count() > 0:
                flattened_df = batch_df \
                    .withColumn("window_start", col("window.start")) \
                    .withColumn("window_end", col("window.end")) \
                    .drop("window")
                    
                flattened_df.write \
                    .jdbc(url=jdbc_url, 
                        table="node_health_aggregates", 
                        mode="append", 
                        properties=connection_properties)
                print(f"Successfully wrote {flattened_df.count()} records to MySQL")
        except Exception as e:
            print(f"Error writing to MySQL: {e}")
    
    return df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()
        
def main():
    try:
        spark = create_spark_session()
        schema = define_schema()
        kafka_topic = "node_status_updates"
        kafka_bootstrap = "localhost:9092"

        parsed_df = read_kafka_stream(spark, schema, kafka_topic, kafka_bootstrap)
        aggregated_df = process_node_health(parsed_df)

        console_query = write_to_console(aggregated_df)
        
        try:
            mysql_query = write_to_mysql(aggregated_df)
            spark.streams.awaitAnyTermination()
        except Exception as e:
            print(f"Error with MySQL stream: {e}")
            console_query.awaitTermination()
    except Exception as e:
        print(f"Critical error in main: {e}")

if __name__ == "__main__":
    main()