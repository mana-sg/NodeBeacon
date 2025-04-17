import os
import json
import time
import traceback
from flask import Flask, request
from flask_socketio import SocketIO, emit
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_CHAT_TOPIC = os.getenv('KAFKA_CHAT_TOPIC', 'chat')
CHAT_SERVER_HOST = os.getenv('CHAT_SERVER_HOST', '0.0.0.0')
CHAT_SERVER_PORT = int(os.getenv('CHAT_SERVER_PORT', 5003))
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', "http://localhost:3000")

# --- Logging Prefix ---
LOG_PREFIX = "LOG [ChatServer]:"

# --- Flask App & SocketIO ---
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS)
producer = None

# --- Kafka Initialization ---
def init_kafka():
    global producer
    if producer:
        return producer
    try:
        print(f"{LOG_PREFIX} Connecting to Kafka broker at {KAFKA_BROKER}...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retry_backoff_ms=300
        )
        print(f"{LOG_PREFIX} ✅ Kafka producer ready.")
        return producer
    except NoBrokersAvailable:
        print(f"{LOG_PREFIX} ❌ No Kafka brokers available at {KAFKA_BROKER}")
    except KafkaError as e:
        print(f"{LOG_PREFIX} ❌ KafkaError during init: {e}")
    except Exception as e:
        print(f"{LOG_PREFIX} ❌ General exception during Kafka init: {e}")
        traceback.print_exc()
    return None

# --- Kafka Callbacks ---
def on_send_success(record_metadata):
    print(f"{LOG_PREFIX} ✅ Message delivered to {record_metadata.topic} (partition {record_metadata.partition}, offset {record_metadata.offset})")

def on_send_error(ex):
    print(f"{LOG_PREFIX} ❌ Kafka send failed: {ex}")

# --- Socket.IO Events ---
@socketio.on('connect')
def on_connect():
    print(f"{LOG_PREFIX} ✅ Client connected: {request.sid}")
    if not producer:
        init_kafka()

@socketio.on('disconnect')
def on_disconnect():
    print(f"{LOG_PREFIX} ❌ Client disconnected: {request.sid}")

@socketio.on('send_chat_message')
def handle_message(data):
    text = data.get('message', '').strip()
    print(f"{LOG_PREFIX} ↩️  Received message from {request.sid}: {text}")

    if not text:
        print(f"{LOG_PREFIX} ⚠️  Empty message. Ignoring.")
        return

    if not producer:
        print(f"{LOG_PREFIX} ❌ Kafka producer unavailable.")
        emit('chat_error', {'error': 'Chat service unavailable.'})
        return

    msg = {
        'sender': f"User_{request.sid[:6]}",
        'text': text,
        'timestamp': time.time()
    }

    try:
        future = producer.send(KAFKA_CHAT_TOPIC, value=msg)
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

# --- Server Startup ---
def main():
    print(f"{LOG_PREFIX} 🚀 Starting Chat Server on {CHAT_SERVER_HOST}:{CHAT_SERVER_PORT}")
    print(f"{LOG_PREFIX} 🔗 Kafka Broker: {KAFKA_BROKER}")
    print(f"{LOG_PREFIX} 📡 Chat Topic: {KAFKA_CHAT_TOPIC}")
    print(f"{LOG_PREFIX} 🌐 Allowed Origins: {ALLOWED_ORIGINS}")

    if not init_kafka():
        print(f"{LOG_PREFIX} ⚠️ Kafka producer could not be initialized. Messages will not be sent.")

    try:
        socketio.run(app, host=CHAT_SERVER_HOST, port=CHAT_SERVER_PORT)
    except KeyboardInterrupt:
        print(f"\n{LOG_PREFIX} 👋 Server shutting down...")
    finally:
        if producer:
            print(f"{LOG_PREFIX} 🔒 Closing Kafka producer...")
            producer.flush(timeout=10)
            producer.close(timeout=5)
            print(f"{LOG_PREFIX} ✅ Kafka producer closed.")

if __name__ == '__main__':
    main()
