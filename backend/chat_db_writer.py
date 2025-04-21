# chat_db_writer.py (Consumer & DB Writer - Updated for stream_id)

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

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
# Use the correct topic name if it changed
KAFKA_CHAT_TOPIC = os.getenv('KAFKA_CHAT_TOPIC', 'chat')
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '-') # Use your actual password
MYSQL_DB = os.getenv('MYSQL_DB', 'chat_app_db')
MYSQL_CHAT_TABLE = 'chat_messages'

# --- Global State ---
db_connection = None
kafka_consumer = None
shutdown_flag = False

# --- Logging Prefix ---
LOG_PREFIX = "DEBUG [DB Writer]:"

print(f"{LOG_PREFIX} Initializing script...")
print(f"{LOG_PREFIX} KAFKA_BROKER={KAFKA_BROKER}")
print(f"{LOG_PREFIX} KAFKA_CHAT_TOPIC={KAFKA_CHAT_TOPIC}")
print(f"{LOG_PREFIX} MYSQL_HOST={MYSQL_HOST}:{MYSQL_PORT}")
print(f"{LOG_PREFIX} MYSQL_DB={MYSQL_DB}")
print(f"{LOG_PREFIX} MYSQL_USER={MYSQL_USER}")

# --- Signal Handler ---
def signal_handler(sig, frame):
    global shutdown_flag
    print(f"\n{LOG_PREFIX} Signal {sig} received, initiating shutdown...")
    shutdown_flag = True

# --- MySQL Functions ---
def get_db_connection():
    # ... (no changes needed in get_db_connection itself) ...
    global db_connection
    if db_connection and db_connection.is_connected():
        return db_connection
    try:
        print(f"{LOG_PREFIX} Attempting NEW MySQL connection to {MYSQL_HOST}:{MYSQL_PORT} DB: {MYSQL_DB} User: {MYSQL_USER}")
        db_connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            connection_timeout=10
        )
        if db_connection.is_connected():
            print(f"{LOG_PREFIX} ✅ MySQL connection successful.")
            return db_connection
        else:
            print(f"{LOG_PREFIX} ❌ MySQL connection object created but not connected.")
            db_connection = None
            return None
    except MySQLError as e:
        print(f"❌ ERROR {LOG_PREFIX} MySQL Connection Error: {e}")
        db_connection = None
        return None
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} Unexpected error during MySQL connection: {e}")
        traceback.print_exc()
        db_connection = None
        return None

def create_chat_table():
    print(f"{LOG_PREFIX} Checking/Creating MySQL chat table...")
    conn = get_db_connection()
    if not conn:
        print(f"{LOG_PREFIX} Cannot check/create table, DB connection unavailable.")
        return False
    try:
        cursor = conn.cursor()
        # --- ADD stream_id COLUMN ---
        sql = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_CHAT_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stream_id VARCHAR(255) NOT NULL,  -- Added stream_id
                sender_sid VARCHAR(40) NOT NULL,
                message_text TEXT NOT NULL,
                timestamp DATETIME(6) NOT NULL,
                kafka_offset BIGINT,
                kafka_partition INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX stream_id_timestamp_idx (stream_id, timestamp) -- Add index
            ) ENGINE=InnoDB;
        """
        print(f"{LOG_PREFIX} Executing SQL for table check/create...")
        cursor.execute(sql)
        # You might need ALTER TABLE if the table already exists without stream_id
        # Consider adding this separately or handling the error if column exists
        try:
            cursor.execute(f"ALTER TABLE {MYSQL_CHAT_TABLE} ADD COLUMN stream_id VARCHAR(255) NOT NULL AFTER id")
            cursor.execute(f"ALTER TABLE {MYSQL_CHAT_TABLE} ADD INDEX stream_id_timestamp_idx (stream_id, timestamp)")
            print(f"{LOG_PREFIX} Ensured stream_id column and index exist.")
        except MySQLError as e:
            if e.errno == errorcode.ER_DUP_FIELDNAME or e.errno == errorcode.ER_DUP_KEYNAME:
                # Column or index already exists, ignore error
                print(f"{LOG_PREFIX} Column 'stream_id' or index 'stream_id_timestamp_idx' already exists.")
            else:
                # Re-raise other errors
                raise e

        conn.commit()
        cursor.close()
        print(f"{LOG_PREFIX} ✅ MySQL table '{MYSQL_CHAT_TABLE}' checked/created/altered.")
        return True
    except MySQLError as e:
        print(f"❌ ERROR {LOG_PREFIX} MySQL Error creating/altering table: {e}")
        traceback.print_exc()
        if e.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST):
            print(f"{LOG_PREFIX} MySQL connection lost during table check, will reconnect on next use.")
            global db_connection
            db_connection = None
        # Rollback if commit failed? Unlikely here but possible
        try: conn.rollback()
        except Exception: pass
        return False
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} Unexpected error during table creation/alteration: {e}")
        traceback.print_exc()
        try: conn.rollback()
        except Exception: pass
        return False

def store_chat_message(message_data, kafka_offset, kafka_partition):
    print(f"{LOG_PREFIX} Attempting to store message offset={kafka_offset}, partition={kafka_partition}")
    conn = get_db_connection()
    if not conn:
        print(f"❌ ERROR {LOG_PREFIX} Cannot store chat message offset={kafka_offset}, DB connection unavailable.")
        return False

    # --- VALIDATE presence of stream_id ---
    if not all(k in message_data for k in ('stream_id', 'sender', 'text', 'timestamp')):
        print(f"❌ ERROR {LOG_PREFIX} Invalid message structure (missing fields including stream_id) for DB storage offset={kafka_offset}: {message_data}")
        return False

    # Ensure stream_id is not empty
    stream_id = str(message_data.get('stream_id', '')).strip()
    if not stream_id:
         print(f"❌ ERROR {LOG_PREFIX} Missing or empty stream_id in message offset={kafka_offset}: {message_data}")
         return False # Or assign a default if appropriate

    # --- UPDATE SQL QUERY ---
    sql = f"""
        INSERT INTO {MYSQL_CHAT_TABLE}
        (stream_id, sender_sid, message_text, timestamp, kafka_offset, kafka_partition)
        VALUES (%s, %s, %s, FROM_UNIXTIME(%s), %s, %s)
    """
    try:
        ts_float = float(message_data['timestamp'])
    except (ValueError, TypeError):
        print(f"❌ ERROR {LOG_PREFIX} Invalid timestamp format offset={kafka_offset}: {message_data.get('timestamp')}")
        return False

    # --- UPDATE VALUES TUPLE ---
    val = (
        stream_id, # Added stream_id
        str(message_data.get('sender', 'Unknown'))[:40],
        str(message_data.get('text', '')),
        ts_float,
        kafka_offset,
        kafka_partition
    )

    cursor = None
    # ... (Rest of the try/except/finally for DB execution is the same) ...
    try:
        cursor = conn.cursor()
        cursor.execute(sql, val)
        conn.commit()
        insert_id = cursor.lastrowid
        print(f"{LOG_PREFIX} ✅ Chat message offset={kafka_offset} stored with DB ID: {insert_id} for stream: {stream_id}")
        cursor.close()
        return True
    except MySQLError as e:
        print(f"❌ ERROR {LOG_PREFIX} MySQL Error storing message offset={kafka_offset} for stream {stream_id}: {e}")
        traceback.print_exc()
        try: conn.rollback()
        except MySQLError as rb_err: print(f"   Additionally, rollback failed: {rb_err}")
        if cursor:
            try: cursor.close()
            except: pass
        if e.errno in (errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST):
            print(f"{LOG_PREFIX} MySQL connection lost during insert, will attempt reconnect on next use.")
            global db_connection
            db_connection = None
        return False
    except Exception as e:
         print(f"❌ ERROR {LOG_PREFIX} Unexpected error storing message offset={kafka_offset} for stream {stream_id}: {e}")
         traceback.print_exc()
         if cursor:
             try: cursor.close()
             except: pass
         try: conn.rollback()
         except: pass
         return False


# --- Main Execution ---
def main():
    global kafka_consumer, db_connection, shutdown_flag
    print(f"{LOG_PREFIX} --- Starting Chat DB Writer Service ---")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"{LOG_PREFIX} Performing initial DB connection and table setup...")
    if not get_db_connection() or not create_chat_table():
        print(f"❌ ERROR {LOG_PREFIX} Initial database setup failed. Exiting.")
        sys.exit(1)
    print(f"{LOG_PREFIX} Initial DB setup seems ok.")

    # --- Kafka Consumer Initialization ---
    consumer_group_id = 'chat-db-writers' # Keep dedicated group ID
    print(f"{LOG_PREFIX} Consumer Group ID: {consumer_group_id}")

    kafka_consumer = None
    try:
        print(f"{LOG_PREFIX} Attempting Kafka Consumer connection to {KAFKA_BROKER} for topic {KAFKA_CHAT_TOPIC}...")
        kafka_consumer = KafkaConsumer(
            KAFKA_CHAT_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
        )
        print(f"{LOG_PREFIX} ✅ Kafka Consumer connected.")
        print(f"{LOG_PREFIX} Initial partitions assigned: {kafka_consumer.partitions_for_topic(KAFKA_CHAT_TOPIC)}")

    # ... (Kafka connection error handling remains the same) ...
    except NoBrokersAvailable:
        print(f"❌ ERROR {LOG_PREFIX} Kafka Consumer Error: No brokers available at {KAFKA_BROKER}. Exiting.")
        sys.exit(1)
    except KafkaError as e:
         print(f"❌ ERROR {LOG_PREFIX} Kafka Consumer Setup Error (KafkaError): {e}. Exiting.")
         sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} Kafka Consumer Setup Error (General Exception): {e}. Exiting.")
        traceback.print_exc()
        sys.exit(1)

    print(f"{LOG_PREFIX} 🏁 DB Writer started. Waiting for chat messages...")
    processed_count = 0
    error_count = 0
    last_log_time = time.time()

    try:
        while not shutdown_flag:
            try:
                msg_pack = kafka_consumer.poll(timeout_ms=1000)
                if not msg_pack:
                    time.sleep(0.1)
                    continue

                for tp, messages in msg_pack.items():
                    if shutdown_flag: break
                    for message in messages:
                        if shutdown_flag: break
                        print(f"{LOG_PREFIX} --> Received message: Offset={message.offset}, Partition={message.partition}, Key={message.key}")

                        # Manual Deserialization & Processing
                        chat_data = None
                        try:
                            if not message.value:
                                print(f"⚠️ WARNING {LOG_PREFIX} Skipping empty message at Offset={message.offset}.")
                                error_count += 1
                                continue

                            decoded_value = message.value.decode('utf-8')
                            chat_data = json.loads(decoded_value)

                            # --- Check for stream_id after deserialization ---
                            if 'stream_id' not in chat_data or not chat_data['stream_id']:
                                print(f"⚠️ WARNING {LOG_PREFIX} Skipping message at Offset={message.offset} due to missing/empty 'stream_id'. Data: {chat_data}")
                                error_count += 1
                                continue
                            # --- End check ---

                        # ... (Deserialization error handling remains the same) ...
                        except UnicodeDecodeError as ude:
                            print(f"❌ ERROR {LOG_PREFIX} Cannot decode message value as UTF-8 at Offset={message.offset}. Error: {ude}")
                            try: print(f"   Raw bytes (Offset={message.offset}): {message.value!r}")
                            except Exception: pass
                            error_count += 1
                            continue
                        except json.JSONDecodeError as jde:
                            print(f"❌ ERROR {LOG_PREFIX} Failed to decode JSON at Offset={message.offset}. Error: {jde}")
                            try: print(f"   Invalid JSON string (Offset={message.offset}): '{message.value.decode('utf-8', errors='ignore')}'")
                            except Exception: pass
                            error_count += 1
                            continue
                        except Exception as e:
                            print(f"❌ ERROR {LOG_PREFIX} Unexpected error during deserialization at Offset={message.offset}: {e}")
                            traceback.print_exc()
                            error_count += 1
                            continue


                        # Store in DB if deserialization succeeded AND stream_id is present
                        if chat_data: # Already checked for stream_id inside try block
                            if store_chat_message(chat_data, message.offset, message.partition):
                                processed_count += 1
                            else:
                                error_count += 1
                                # Failed to store, already logged

                # ... (Periodic Stats Logging remains the same) ...
                now = time.time()
                if now - last_log_time >= 60.0:
                    print(f"{LOG_PREFIX} [Stats] Processed: {processed_count}, Errors: {error_count} in the last minute.")
                    processed_count = 0
                    error_count = 0
                    last_log_time = now

            # ... (Main loop error handling remains the same) ...
            except KafkaError as loop_err:
                 print(f"❌ ERROR {LOG_PREFIX} KafkaError in main processing loop: {loop_err}")
                 time.sleep(5)
            except Exception as loop_err:
                print(f"❌ ERROR {LOG_PREFIX} Unexpected error in main processing loop: {loop_err}")
                traceback.print_exc()
                time.sleep(2)

    # ... (Shutdown and finally block remains the same) ...
    except KeyboardInterrupt:
        print(f"\n{LOG_PREFIX} KeyboardInterrupt caught.")
    finally:
        print(f"{LOG_PREFIX} --- Shutting down Chat DB Writer ---")
        if kafka_consumer:
            print(f"{LOG_PREFIX} Closing Kafka consumer...")
            kafka_consumer.close()
            print(f"{LOG_PREFIX} Kafka consumer closed.")
        if db_connection and db_connection.is_connected():
            print(f"{LOG_PREFIX} Closing MySQL connection...")
            db_connection.close()
            print(f"{LOG_PREFIX} MySQL connection closed.")
        print(f"{LOG_PREFIX} Shutdown complete.")


if __name__ == '__main__':
    main()