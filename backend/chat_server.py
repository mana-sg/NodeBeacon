# chat_server.py (Producer + History Endpoint + Broadcast)

import os
import json
import time
import threading # For consumer thread
import traceback
from flask import Flask, request, jsonify # Added jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaProducer # Add Consumer back
from kafka.errors import KafkaError, NoBrokersAvailable

# --- DB Imports (for reading history) ---
import mysql.connector
from mysql.connector import Error as MySQLError, errorcode

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
# Use the correct topic name if it changed
KAFKA_CHAT_TOPIC = os.getenv('KAFKA_CHAT_TOPIC', 'chat')
CHAT_SERVER_HOST = os.getenv('CHAT_SERVER_HOST', '0.0.0.0')
CHAT_SERVER_PORT = int(os.getenv('CHAT_SERVER_PORT', 5003))
#ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', "http://localhost:3000")
HISTORY_LIMIT = int(os.getenv('CHAT_HISTORY_LIMIT', 100)) # Max messages to fetch

raw_origins = os.getenv('ALLOWED_ORIGINS', '["http://localhost:3000", "http://192.168.2.3:3000"]')

try:
    # Try to parse as JSON list
    ALLOWED_ORIGINS = json.loads(raw_origins)
    if isinstance(ALLOWED_ORIGINS, str):
        ALLOWED_ORIGINS = [ALLOWED_ORIGINS]
except json.JSONDecodeError:
    # Fallback: comma-separated string
    ALLOWED_ORIGINS = [origin.strip() for origin in raw_origins.split(',')]

# --- MySQL Configuration (for reading history) ---
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'm6a2n6a2s7') # Use your actual password
MYSQL_DB = os.getenv('MYSQL_DB', 'chat_app_db')
MYSQL_CHAT_TABLE = 'chat_messages'

# --- Logging Prefix ---
LOG_PREFIX = "LOG [ChatServer]:"

# --- Flask App & SocketIO ---
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}}) # Allow CORS for HTTP history endpoint
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS, async_mode='threading')

# --- Shared State ---
kafka_producer = None
kafka_consumer_thread = None # Thread for broadcasting live messages
stop_consumer_event = threading.Event()
# We don't need a persistent global DB connection here, connect per request/thread

print(f"{LOG_PREFIX} Initializing script...")
print(f"{LOG_PREFIX} KAFKA_BROKER={KAFKA_BROKER}")
print(f"{LOG_PREFIX} KAFKA_CHAT_TOPIC={KAFKA_CHAT_TOPIC}")
print(f"{LOG_PREFIX} CHAT_SERVER_HOST={CHAT_SERVER_HOST}")
print(f"{LOG_PREFIX} CHAT_SERVER_PORT={CHAT_SERVER_PORT}")
print(f"{LOG_PREFIX} ALLOWED_ORIGINS={ALLOWED_ORIGINS}")
print(f"{LOG_PREFIX} Reading history from MYSQL: {MYSQL_HOST}/{MYSQL_DB}")

# --- MySQL Function (for reading history) ---
def get_history_db_connection():
    """ Establishes a new MySQL connection for reading history. """
    # Separate function to avoid conflicts if a global connection was used elsewhere
    try:
        print(f"{LOG_PREFIX} DB-Hist: Attempting MySQL connection...")
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            connection_timeout=5 # Shorter timeout for read request
        )
        if conn.is_connected():
            print(f"{LOG_PREFIX} DB-Hist: ✅ Connection successful.")
            return conn
        else:
            print(f"{LOG_PREFIX} DB-Hist: ❌ Connection object created but not connected.")
            return None
    except MySQLError as e:
        print(f"❌ ERROR {LOG_PREFIX} DB-Hist: MySQL Connection Error: {e}")
        return None
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} DB-Hist: Unexpected error during MySQL connection: {e}")
        traceback.print_exc()
        return None

# --- Kafka Initialization ---
def init_kafka_producer():
    # ... (init_kafka function renamed to init_kafka_producer - code is the same) ...
    global kafka_producer
    if kafka_producer:
        return kafka_producer
    try:
        print(f"{LOG_PREFIX} Connecting Kafka producer to {KAFKA_BROKER}...")
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        print(f"{LOG_PREFIX} ✅ Kafka producer ready.")
        return kafka_producer
    except NoBrokersAvailable:
        print(f"{LOG_PREFIX} ❌ No Kafka brokers available at {KAFKA_BROKER}")
    except KafkaError as e:
        print(f"{LOG_PREFIX} ❌ KafkaError during producer init: {e}")
    except Exception as e:
        print(f"{LOG_PREFIX} ❌ General exception during producer init: {e}")
        traceback.print_exc()
    return None

# --- Kafka Producer Callbacks (Unchanged) ---
def on_send_success(record_metadata):
    # print(f"{LOG_PREFIX} ✅ Message delivered to {record_metadata.topic}...") # Less verbose
    pass

def on_send_error(ex):
    print(f"{LOG_PREFIX} ❌ Kafka send failed: {ex}")


# --- Kafka Consumer Thread (for Broadcasting Live Messages) ---
def kafka_broadcast_consumer_thread_func():
    """ Consumes chat messages from Kafka and broadcasts them via Socket.IO """
    print(f"{LOG_PREFIX} ConsumerThread: 🚀 Starting Kafka consumer thread for broadcasting...")
    print(f"{LOG_PREFIX} ConsumerThread:    Consuming Chat Topic: '{KAFKA_CHAT_TOPIC}'")

    consumer = None
    # Use a different group ID if other consumers exist, or unique one
    consumer_group_id = f'chat-server-broadcaster-{os.getpid()}' # Unique per instance
    print(f"{LOG_PREFIX} ConsumerThread: Consumer Group ID: {consumer_group_id}")

    while not stop_consumer_event.is_set(): # Reconnection loop
        try:
            print(f"{LOG_PREFIX} ConsumerThread: Attempting Kafka Consumer connection...")
            consumer = KafkaConsumer(
                KAFKA_CHAT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                consumer_timeout_ms=5000,
                auto_offset_reset='latest', # Only broadcast new messages
                group_id=consumer_group_id,
                # DO NOT auto-commit offsets if processing fails before broadcast
                enable_auto_commit=False
            )
            print(f"{LOG_PREFIX} ConsumerThread: ✅ Kafka Consumer connected.")

            while not stop_consumer_event.is_set():
                try:
                    msg_pack = consumer.poll(timeout_ms=1000)
                    if not msg_pack:
                        if stop_consumer_event.is_set(): break
                        continue

                    for tp, messages in msg_pack.items():
                        if stop_consumer_event.is_set(): break
                        # Process messages and commit offsets *after* successful broadcast
                        successfully_processed_offsets = {}
                        try:
                            for message in messages:
                                if stop_consumer_event.is_set(): break
                                print(f"{LOG_PREFIX} ConsumerThread: --> Received message for broadcast: Offset={message.offset}")
                                try:
                                    if not message.value: continue # Skip empty

                                    decoded_value = message.value.decode('utf-8')
                                    chat_data = json.loads(decoded_value)

                                    # Add unique ID for frontend key prop
                                    chat_data['id'] = f"{tp.partition}-{message.offset}"

                                    # Broadcast to all connected chat clients
                                    # Frontend will filter based on stream_id if necessary
                                    print(f"{LOG_PREFIX} ConsumerThread: Broadcasting message: {chat_data}")
                                    socketio.emit('new_chat_message', chat_data)

                                    # Track successful offset for commit
                                    if tp not in successfully_processed_offsets or successfully_processed_offsets[tp] < message.offset:
                                         successfully_processed_offsets[tp] = message.offset

                                except Exception as e:
                                    print(f"❌ ERROR {LOG_PREFIX} ConsumerThread: Error decoding/broadcasting message Offset={message.offset}: {e}")
                                    # Don't commit offset for failed messages
                                    traceback.print_exc()
                                    # Decide if you want to break or continue processing batch

                            # Commit offsets for successfully processed messages in this batch
                            if successfully_processed_offsets:
                                offsets_to_commit = {tp: offset + 1 for tp, offset in successfully_processed_offsets.items()}
                                print(f"{LOG_PREFIX} ConsumerThread: Committing offsets: {offsets_to_commit}")
                                consumer.commit(offsets=offsets_to_commit)

                        except Exception as batch_err:
                             print(f"❌ ERROR {LOG_PREFIX} ConsumerThread: Error processing message batch: {batch_err}")
                             traceback.print_exc() # Log error, but don't commit offsets for this batch


                except Exception as poll_err:
                    print(f"❌ ERROR {LOG_PREFIX} ConsumerThread: Kafka polling error: {poll_err}")
                    traceback.print_exc()
                    socketio.sleep(2)

            break # Exit outer loop

        except NoBrokersAvailable:
            print(f"❌ ERROR {LOG_PREFIX} ConsumerThread: Kafka Consumer Error: No brokers available. Retrying in 10s...")
        except KafkaError as e:
            print(f"❌ ERROR {LOG_PREFIX} ConsumerThread: Kafka Consumer Setup Error (KafkaError): {e}. Retrying in 10s...")
        except Exception as e:
            print(f"❌ ERROR {LOG_PREFIX} ConsumerThread: Kafka Consumer Setup Error (General Exception): {e}. Retrying in 10s...")
            traceback.print_exc()
        finally:
            if consumer:
                print(f"{LOG_PREFIX} ConsumerThread: Closing Kafka Consumer...")
                # Commit final offsets before closing (if any pending)
                try:
                    consumer.commit()
                except Exception as commit_err:
                    print(f"❌ ERROR {LOG_PREFIX} ConsumerThread: Error during final commit: {commit_err}")
                consumer.close()
                print(f"{LOG_PREFIX} ConsumerThread: Kafka Consumer closed.")
                consumer = None

        if not stop_consumer_event.is_set():
            socketio.sleep(10)

    print(f"{LOG_PREFIX} ConsumerThread: 🛑 Kafka Consumer thread stopped.")


# --- Socket.IO Events ---
@socketio.on('connect')
def on_connect():
    print(f"{LOG_PREFIX} ✅ Client connected: {request.sid}")
    # Ensure producer is ready
    if not kafka_producer:
        init_kafka_producer()

    # Start consumer thread if not already running
    global kafka_consumer_thread
    if kafka_consumer_thread is None or not kafka_consumer_thread.is_alive():
        print(f"{LOG_PREFIX} Client connected, starting Kafka consumer thread for broadcasting...")
        stop_consumer_event.clear()
        kafka_consumer_thread = threading.Thread(target=kafka_broadcast_consumer_thread_func, daemon=True)
        kafka_consumer_thread.start()
        if not kafka_consumer_thread.is_alive():
             print(f"❌ ERROR {LOG_PREFIX}: Failed to start consumer thread.")


@socketio.on('disconnect')
def on_disconnect():
    print(f"{LOG_PREFIX} ❌ Client disconnected: {request.sid}")

@socketio.on('send_chat_message')
def handle_message(data):
    """ Handle message from client, requiring stream_id """
    text = data.get('message', '').strip()
    stream_id = data.get('stream_id', '').strip() # Get stream_id from client

    print(f"{LOG_PREFIX} ↩️  Received message from {request.sid} for stream '{stream_id}': {text}")

    if not text:
        print(f"{LOG_PREFIX} ⚠️  Empty message text. Ignoring.")
        return
    if not stream_id:
        print(f"{LOG_PREFIX} ⚠️  Missing stream_id from client {request.sid}. Ignoring.")
        # Optionally send error back to client
        # emit('chat_error', {'error': 'Missing stream_id.'})
        return

    if not kafka_producer:
        print(f"{LOG_PREFIX} ❌ Kafka producer unavailable.")
        emit('chat_error', {'error': 'Chat service unavailable.'})
        return

    # Include stream_id in the message sent to Kafka
    msg = {
        'stream_id': stream_id, # Added stream_id
        'sender': f"User_{request.sid[:6]}",
        'text': text,
        'timestamp': time.time()
    }

    try:
        print(f"{LOG_PREFIX} Sending to Kafka topic '{KAFKA_CHAT_TOPIC}': {msg}")
        future = kafka_producer.send(KAFKA_CHAT_TOPIC, value=msg)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
    except KafkaError as e:
        print(f"{LOG_PREFIX} ❌ KafkaError during send: {e}")
        traceback.print_exc()
        emit('chat_error', {'error': 'Failed to send message to Kafka.'})
    except Exception as e:
        print(f"{LOG_PREFIX} ❌ Exception during Kafka send: {e}")
        traceback.print_exc()
        emit('chat_error', {'error': 'Internal server error.'})

# --- HTTP Endpoint for Chat History ---
@app.route('/chat/history/<stream_id>', methods=['GET'])
def get_chat_history(stream_id):
    print(f"{LOG_PREFIX} HTTP Request: /chat/history/{stream_id}")
    if not stream_id:
        return jsonify({"error": "stream_id is required"}), 400

    conn = None
    cursor = None
    try:
        conn = get_history_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor(dictionary=True) # Fetch as dictionaries

        # Query for recent messages for the specific stream_id
        query = f"""
            SELECT id, stream_id, sender_sid as sender, message_text as text, UNIX_TIMESTAMP(timestamp) as timestamp
            FROM {MYSQL_CHAT_TABLE}
            WHERE stream_id = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """
        # Fetch latest N messages
        cursor.execute(query, (stream_id, HISTORY_LIMIT))
        results = cursor.fetchall()

        # Results are newest first, reverse for chronological order in frontend
        results.reverse()

        print(f"{LOG_PREFIX} DB-Hist: Found {len(results)} messages for stream {stream_id}")
        return jsonify(results)

    except MySQLError as e:
        print(f"❌ ERROR {LOG_PREFIX} DB-Hist: MySQL Error fetching history for {stream_id}: {e}")
        traceback.print_exc()
        return jsonify({"error": "Failed to fetch chat history"}), 500
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} DB-Hist: Unexpected error fetching history for {stream_id}: {e}")
        traceback.print_exc()
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print(f"{LOG_PREFIX} DB-Hist: ✅ Connection closed.")


# --- Server Startup ---
def main():
    print(f"{LOG_PREFIX} 🚀 Starting Chat Server (Producer+History+Broadcast) on {CHAT_SERVER_HOST}:{CHAT_SERVER_PORT}")
    # ... (rest of startup logs) ...
    print(f"{LOG_PREFIX} 🔗 Kafka Broker: {KAFKA_BROKER}")
    print(f"{LOG_PREFIX} 📡 Chat Topic: {KAFKA_CHAT_TOPIC}")
    print(f"{LOG_PREFIX} 💾 History DB: {MYSQL_HOST}/{MYSQL_DB}")
    print(f"{LOG_PREFIX} 🌐 Allowed Origins: {ALLOWED_ORIGINS}")

    # Init Kafka Producer first
    if not init_kafka_producer():
        print(f"{LOG_PREFIX} ⚠️ Kafka producer could not be initialized. Sending messages will not work.")
        # Consider exiting

    # Consumer thread will be started by the first client connecting via Socket.IO

    try:
        # Run Flask App with SocketIO support
        # Needs host='0.0.0.0' to be accessible externally
        print(f"{LOG_PREFIX} Starting Flask/SocketIO server...")
        socketio.run(app, host=CHAT_SERVER_HOST, port=CHAT_SERVER_PORT, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print(f"\n{LOG_PREFIX} 👋 Server shutting down...")
    finally:
        print(f"{LOG_PREFIX} Requesting consumer thread stop...")
        stop_consumer_event.set()

        if kafka_consumer_thread and kafka_consumer_thread.is_alive():
            print(f"{LOG_PREFIX} Waiting for consumer thread to finish...")
            kafka_consumer_thread.join(timeout=5.0)
            if kafka_consumer_thread.is_alive():
                 print(f"{LOG_PREFIX} Consumer thread did not finish in time.")

        if kafka_producer:
            print(f"{LOG_PREFIX} 🔒 Closing Kafka producer...")
            kafka_producer.flush(timeout=10)
            kafka_producer.close(timeout=5)
            print(f"{LOG_PREFIX} ✅ Kafka producer closed.")

        # No global DB connection to close here

        print(f"{LOG_PREFIX} ✅ Chat server shutdown complete.")


if __name__ == '__main__':
    main()
