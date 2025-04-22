from kafka import KafkaConsumer
import mysql.connector
from datetime import datetime
import json
import logging
import os
import dotenv
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": "health_monitor"
}

KAFKA_CONFIG = {
    "bootstrap_servers": ["localhost:9092"],
    "topic": "node_status_updates",
    "group_id": "node_health_store"
}

def setup_database():
    """Set up the required database tables"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # Create main node health table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS node_health_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                node_id INT NOT NULL,
                status VARCHAR(20) NOT NULL,
                timestamp DATETIME NOT NULL,
                received_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_node_timestamp (node_id, timestamp)
            )
        """)
        
        # Create table for storing processing statistics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_stats (
                id INT AUTO_INCREMENT PRIMARY KEY,
                processing_mode VARCHAR(20) NOT NULL,
                start_time DATETIME NOT NULL,
                end_time DATETIME NOT NULL,
                messages_processed INT NOT NULL,
                avg_processing_time FLOAT NOT NULL
            )
        """)
        
        conn.commit()
        logger.info("Database tables created successfully")
        
    except mysql.connector.Error as err:
        logger.error(f"Error setting up database: {err}")
        raise
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def store_data(batch_size=100):
    """Store data from Kafka to MySQL"""
    try:
        consumer = KafkaConsumer(
            KAFKA_CONFIG["topic"],
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            group_id=KAFKA_CONFIG["group_id"],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        batch = []
        logger.info("Starting to consume messages...")
        
        for message in consumer:
            try:
                event = message.value
                batch.append((
                    event["node_id"],
                    event["status"],
                    datetime.fromisoformat(event["timestamp"])
                ))
                
                # Process batch when it reaches the specified size
                if len(batch) >= batch_size:
                    cursor.executemany("""
                        INSERT INTO node_health_data (node_id, status, timestamp)
                        VALUES (%s, %s, %s)
                    """, batch)
                    conn.commit()
                    logger.info(f"Stored batch of {len(batch)} records")
                    batch = []
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
            
    except KeyboardInterrupt:
        logger.info("Stopping data storage...")
    finally:
        # Store any remaining records in the batch
        if batch:
            cursor.executemany("""
                INSERT INTO node_health_data (node_id, status, timestamp)
                VALUES (%s, %s, %s)
            """, batch)
            conn.commit()
            logger.info(f"Stored final batch of {len(batch)} records")
        
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
        if 'consumer' in locals():
            consumer.close()

def main():
    setup_database()
    store_data()

if __name__ == "__main__":
    main()