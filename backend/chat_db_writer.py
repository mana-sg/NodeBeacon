import json
import time
from kafka import KafkaConsumer
import mysql.connector

# --- Config ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'chat'

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'aneesh200',  # use your actual password
    'database': 'chat_app_db'
}

# --- Connect to MySQL ---
def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)

def create_table_if_needed(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sender_sid VARCHAR(40) NOT NULL,
            message_text TEXT NOT NULL,
            timestamp DATETIME(6) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

def insert_message(cursor, data):
    sql = """
        INSERT INTO chat_messages (sender_sid, message_text, timestamp)
        VALUES (%s, %s, FROM_UNIXTIME(%s))
    """
    cursor.execute(sql, (
        str(data.get('sender', 'unknown'))[:40],
        str(data.get('text', '')),
        float(data.get('timestamp', time.time()))
    ))

# --- Kafka Consumer ---
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='simple-chat-writer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("[Consumer] ✅ Listening for messages...")

conn = get_mysql_connection()
cursor = conn.cursor()
create_table_if_needed(cursor)

try:
    for msg in consumer:
        data = msg.value
        print(f"[Consumer] → Received: {data}")
        try:
            insert_message(cursor, data)
            conn.commit()
            print("[Consumer] ✅ Stored in DB.")
        except Exception as db_err:
            print(f"[Consumer] ❌ DB Error: {db_err}")
except KeyboardInterrupt:
    print("\n[Consumer] ❌ Stopping...")
finally:
    cursor.close()
    conn.close()
    consumer.close()
