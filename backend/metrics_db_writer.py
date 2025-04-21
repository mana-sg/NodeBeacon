import os
import json
import time
import signal
import sys
import traceback
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
import mysql.connector
from mysql.connector import Error as MySQLError, errorcode
from datetime import datetime

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_METRICS_TOPIC = os.getenv('KAFKA_METRICS_TOPIC', 'stream-metrics')
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '-')
MYSQL_DB = os.getenv('MYSQL_DB', 'chat_app_db')
MYSQL_METRICS_TABLE = 'stream_metrics_v2'

# --- Global State ---
db_connection = None
kafka_consumer = None
shutdown_flag = False

# --- Logging Prefix ---
LOG_PREFIX = "DEBUG [MetricsDBWriter]:"

def signal_handler(sig, frame):
    global shutdown_flag
    print(f"\n{LOG_PREFIX} Signal {sig} received, initiating shutdown...")
    shutdown_flag = True

def get_db_connection():
    global db_connection
    if db_connection and db_connection.is_connected():
        try:
            db_connection.ping(reconnect=True, attempts=1, delay=1)
            return db_connection
        except MySQLError as e:
            print(f"⚠️ {LOG_PREFIX} MySQL ping failed: {e}. Attempting reconnect.")
            try:
                db_connection.close()
            except Exception:
                pass
            db_connection = None

    if not db_connection:
        try:
            print(f"{LOG_PREFIX} Attempting NEW MySQL connection...")
            db_connection = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB,
                connection_timeout=10,
            )
            if db_connection.is_connected():
                print(f"{LOG_PREFIX} ✅ MySQL connection successful.")
                return db_connection
            else:
                print(f"{LOG_PREFIX} ❌ MySQL connection object created but not connected.")
                db_connection = None
        except MySQLError as e:
            print(f"❌ ERROR {LOG_PREFIX} MySQL Connection Error: {e}")
            db_connection = None
        except Exception as e:
            print(f"❌ ERROR {LOG_PREFIX} Unexpected error during MySQL connection: {e}")
            traceback.print_exc()
            db_connection = None
    return db_connection

def create_metrics_table():
    conn = get_db_connection()
    if not conn: return False
    cursor = None
    try:
        cursor = conn.cursor()
        sql = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_METRICS_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stream_id VARCHAR(255) NOT NULL,
                metrics_timestamp DATETIME(6) NOT NULL,
                interval_sec FLOAT,
                frame_count INT,
                processing_fps FLOAT,
                average_latency_ms FLOAT,
                average_processing_time_ms FLOAT,
                average_frame_kbytes FLOAT,
                viewer_count INT,
                latency_stddev_ms FLOAT NULL,
                kafka_last_offset BIGINT,
                kafka_partition INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX stream_id_timestamp_idx (stream_id, metrics_timestamp)
            ) ENGINE=InnoDB;
        """
        cursor.execute(sql)
        conn.commit()
        print(f"{LOG_PREFIX} ✅ MySQL table '{MYSQL_METRICS_TABLE}' checked/created.")
        return True
    except MySQLError as e:
        print(f"❌ ERROR {LOG_PREFIX} MySQL Error creating/checking metrics table: {e}")
        traceback.print_exc()
        if e.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST):
            global db_connection; db_connection = None
        try: conn.rollback()
        except: pass
        return False
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} Unexpected error during metrics table creation: {e}")
        traceback.print_exc()
        try: conn.rollback()
        except: pass
        return False
    finally:
        if cursor:
            try: cursor.close()
            except: pass

def store_stream_metrics(metrics_data):
    conn = get_db_connection()
    if not conn:
        return False

    required_fields = ['stream_id', 'timestamp', 'interval_sec', 'frame_count',
                       'processing_fps', 'average_latency_ms', 'average_processing_time_ms',
                       'average_frame_kbytes', 'viewer_count', 'kafka_last_offset', 'kafka_partition']
    missing_fields = [k for k in required_fields if k not in metrics_data or metrics_data[k] is None]
    if missing_fields:
        print(f"❌ ERROR {LOG_PREFIX} Invalid metrics structure. Missing fields: {missing_fields}")
        return False

    try:
        ts_datetime = datetime.fromtimestamp(float(metrics_data['timestamp']))
        val = (
            str(metrics_data['stream_id']),
            ts_datetime,
            float(metrics_data['interval_sec']),
            int(metrics_data['frame_count']),
            float(metrics_data['processing_fps']),
            float(metrics_data['average_latency_ms']),
            float(metrics_data['average_processing_time_ms']),
            float(metrics_data['average_frame_kbytes']),
            int(metrics_data['viewer_count']),
            float(metrics_data['latency_stddev_ms']) if metrics_data.get('latency_stddev_ms') is not None else None,
            int(metrics_data['kafka_last_offset']),
            int(metrics_data['kafka_partition'])
        )
    except (ValueError, TypeError, KeyError) as e:
        print(f"❌ ERROR {LOG_PREFIX} Invalid data types: {e}")
        return False

    sql = f"""
        INSERT INTO {MYSQL_METRICS_TABLE}
        (stream_id, metrics_timestamp, interval_sec, frame_count, processing_fps,
         average_latency_ms, average_processing_time_ms, average_frame_kbytes,
         viewer_count, latency_stddev_ms, kafka_last_offset, kafka_partition)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql, val)
        conn.commit()
        return True
    except MySQLError as e:
        print(f"❌ ERROR {LOG_PREFIX} MySQL Error storing metrics: {e}")
        traceback.print_exc()
        try: conn.rollback()
        except: pass
        if e.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST, errorcode.ER_LOCK_WAIT_TIMEOUT):
            global db_connection; db_connection = None
        return False
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} Unexpected error storing metrics: {e}")
        traceback.print_exc()
        try: conn.rollback()
        except: pass
        return False
    finally:
        if cursor:
            try: cursor.close()
            except: pass

def main():
    global kafka_consumer, db_connection, shutdown_flag
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    for i in range(3):
        if get_db_connection() and create_metrics_table():
            break
        if shutdown_flag:
            sys.exit(1)
        print(f"{LOG_PREFIX} Retry DB setup in 10s...")
        time.sleep(10)
    else:
        print(f"{LOG_PREFIX} DB setup failed. Exiting.")
        sys.exit(1)

    while not shutdown_flag:
        try:
            kafka_consumer = KafkaConsumer(
                KAFKA_METRICS_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode('utf-8', 'ignore')),
            )
            print(f"{LOG_PREFIX} ✅ Kafka consumer connected.")
            break
        except (NoBrokersAvailable, KafkaError, Exception) as e:
            print(f"{LOG_PREFIX} Kafka error: {e}")
            traceback.print_exc()
            if shutdown_flag:
                sys.exit(1)
            time.sleep(15)

    processed_count = 0
    error_count = 0
    last_log_time = time.time()

    try:
        while not shutdown_flag:
            try:
                msg_pack = kafka_consumer.poll(timeout_ms=1000)
                for tp, messages in msg_pack.items():
                    for message in messages:
                        if isinstance(message.value, dict) and store_stream_metrics(message.value):
                            processed_count += 1
                        else:
                            error_count += 1

                if time.time() - last_log_time >= 60:
                    print(f"{LOG_PREFIX} [Stats] Processed: {processed_count}, Errors: {error_count}")
                    processed_count = 0
                    error_count = 0
                    last_log_time = time.time()

            except MySQLError as db_err:
                global db_connection
                print(f"{LOG_PREFIX} MySQL error in loop: {db_err}")
                if db_err.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST):
                    db_connection = None
                error_count += 1
                time.sleep(5)
            except Exception as e:
                print(f"{LOG_PREFIX} Unexpected loop error: {e}")
                traceback.print_exc()
                error_count += 1
                time.sleep(5)

    finally:
        print(f"{LOG_PREFIX} Shutting down...")
        shutdown_flag = True
        if kafka_consumer:
            kafka_consumer.close()
        if db_connection and db_connection.is_connected():
            db_connection.close()
        print(f"{LOG_PREFIX} Shutdown complete.")

if __name__ == '__main__':
    main()
