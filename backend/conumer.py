# consumer_server.py (MODIFIED - Video Only)

import zlib
import pickle
import time
import threading
import base64
import os
# import json # No longer needed here
import traceback
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from kafka import KafkaConsumer # KafkaProducer no longer needed here
from kafka.errors import NoBrokersAvailable, KafkaError
from engineio.payload import Payload

Payload.max_decode_packets = 500

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_VIDEO_TOPIC = os.getenv('KAFKA_TOPIC', 'video-stream')
# KAFKA_CHAT_TOPIC = os.getenv('KAFKA_CHAT_TOPIC', 'chat-messages') # REMOVED
CONSUMER_HOST = os.getenv('CONSUMER_HOST', '0.0.0.0')
CONSUMER_PORT = int(os.getenv('CONSUMER_PORT', 5002)) # Keep original port
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', "http://localhost:3000")
FRONTEND_BUILD_DIR = os.getenv('FRONTEND_BUILD_DIR', '../frontend/build')

# --- Flask App & SocketIO Setup ---
app = Flask(__name__, static_folder=FRONTEND_BUILD_DIR, static_url_path='/')
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}})
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS,
                    async_mode='threading',
                    max_http_buffer_size=10 * 1024 * 1024)

# --- Shared State & Thread Control ---
kafka_video_consumer_thread = None # Renamed for clarity
stop_consumer_event = threading.Event() # Renamed for clarity
# kafka_chat_producer = None # REMOVED

print(f"DEBUG [VideoServer]: Initializing script...")
print(f"DEBUG [VideoServer]: KAFKA_BROKER={KAFKA_BROKER}")
print(f"DEBUG [VideoServer]: KAFKA_VIDEO_TOPIC={KAFKA_VIDEO_TOPIC}")
# print(f"DEBUG [VideoServer]: KAFKA_CHAT_TOPIC={KAFKA_CHAT_TOPIC}") # REMOVED
print(f"DEBUG [VideoServer]: ALLOWED_ORIGINS={ALLOWED_ORIGINS}")

# --- Kafka Processing Thread (VIDEO ONLY) ---
def kafka_video_consumer_thread_func():
    """ Consumes from Kafka (Video ONLY), Emits Video via Socket.IO """
    print(f"DEBUG [VideoServer-Thread]: 🚀 Starting Kafka video consumer thread...")
    print(f"DEBUG [VideoServer-Thread]:    Consuming Video Topic: '{KAFKA_VIDEO_TOPIC}'")
    print(f"DEBUG [VideoServer-Thread]:    Broker: {KAFKA_BROKER}")

    consumer = None
    # Use a group ID specific to video consumers if desired, or keep the old one
    consumer_group_id = 'video-stream-consumers'
    print(f"DEBUG [VideoServer-Thread]: Consumer Group ID: {consumer_group_id}")

    while not stop_consumer_event.is_set(): # Reconnection loop
        try:
            print(f"DEBUG [VideoServer-Thread]: Attempting Kafka Consumer connection...")
            consumer = KafkaConsumer(
                KAFKA_VIDEO_TOPIC, # Subscribe ONLY to video topic
                bootstrap_servers=KAFKA_BROKER,
                consumer_timeout_ms=5000,
                group_id=consumer_group_id,
                auto_offset_reset='latest' # Usually appropriate for video
            )
            print("DEBUG [VideoServer-Thread]: ✅ Kafka Consumer (Video) connected.")

            frame_count = 0
            # chat_count = 0 # REMOVED
            start_time = time.time()
            active_viewers = True

            while not stop_consumer_event.is_set():
                if not socketio.server or not socketio.server.eio.sockets:
                    if active_viewers:
                        print("DEBUG [VideoServer-Thread]: 💤 No viewers connected, pausing Kafka consumption...")
                        active_viewers = False
                    time.sleep(1)
                    continue
                elif not active_viewers:
                    print("DEBUG [VideoServer-Thread]: ▶️ Viewers connected, resuming Kafka consumption...")
                    active_viewers = True

                try:
                    msg_pack = consumer.poll(timeout_ms=1000)
                    if not msg_pack:
                        if stop_consumer_event.is_set(): break
                        continue

                    # No need to iterate topics, we only subscribed to one
                    for tp, messages in msg_pack.items():
                        if stop_consumer_event.is_set(): break
                        # Assert topic is video topic if needed: assert tp.topic == KAFKA_VIDEO_TOPIC
                        for message in messages:
                            if stop_consumer_event.is_set(): break
                            process_video_message(message) # Process video frame
                            frame_count += 1

                    now = time.time()
                    if now - start_time >= 10.0:
                        print(f"DEBUG [VideoServer-Thread - Stats]: Processed {frame_count} video frames in ~10s.")
                        frame_count = 0
                        # chat_count = 0 # REMOVED
                        start_time = now

                except Exception as e:
                    print(f"❌ ERROR [VideoServer-Thread]: Kafka polling/processing error: {e}")
                    traceback.print_exc()
                    socketio.sleep(1)

            break # Exit outer loop

        except NoBrokersAvailable:
            print(f"❌ ERROR [VideoServer-Thread]: Kafka Consumer Error: No brokers available at {KAFKA_BROKER}. Retrying in 10s...")
        except KafkaError as e:
             print(f"❌ ERROR [VideoServer-Thread]: Kafka Consumer Setup Error (KafkaError): {e}. Retrying in 10s...")
        except Exception as e:
            print(f"❌ ERROR [VideoServer-Thread]: Kafka Consumer Setup Error (General Exception): {e}. Retrying in 10s...")
            traceback.print_exc()
        finally:
            if consumer:
                print("DEBUG [VideoServer-Thread]: Closing Kafka Consumer (Video)...")
                consumer.close()
                print("DEBUG [VideoServer-Thread]: ✅ Kafka Consumer (Video) closed.")
                consumer = None

        if not stop_consumer_event.is_set():
            socketio.sleep(10) # Wait before retrying connection

    print("DEBUG [VideoServer-Thread]: 🛑 Kafka Video Consumer thread stopped.")


def process_video_message(message):
    """ Decodes video frame and emits it via Socket.IO (Unchanged) """
    now = time.time()
    try:
        decompressed = zlib.decompress(message.value)
        frame_group = pickle.loads(decompressed)
        if not isinstance(frame_group, list) or not frame_group: return
        frame_data = frame_group[0]
        if not isinstance(frame_data, dict) or 'frame' not in frame_data: return

        jpeg_bytes = frame_data['frame']
        kafka_timestamp = frame_data.get('timestamp', now)
        latency_ms = (now - kafka_timestamp) * 1000
        base64_frame = base64.b64encode(jpeg_bytes).decode('utf-8')
        data_url = f"data:image/jpeg;base64,{base64_frame}"
        # Use the socketio instance from the main scope
        socketio.emit('video_frame', {'image': data_url, 'latency': latency_ms})
    except Exception as e:
        print(f"❌ ERROR [VideoServer - process_video_message]: Error processing offset {message.offset}: {e}")

# def process_chat_message_for_broadcast(message): # REMOVED
# def initialize_kafka_producer(): # REMOVED

# --- Flask Routes (Unchanged) ---
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return app.send_static_file(path)
    else:
        return app.send_static_file('index.html')

# --- Socket.IO Event Handlers (Only Connect/Disconnect needed for Video Server) ---
@socketio.on('connect')
def handle_connect():
    """ Handle new WebSocket connections (video viewers) """
    global kafka_video_consumer_thread
    print(f"DEBUG [VideoServer]: ✅ Viewer client connected: {request.sid}")

    # Start Kafka video consumer thread if not running
    if kafka_video_consumer_thread is None or not kafka_video_consumer_thread.is_alive():
        print("DEBUG [VideoServer]: Client connected, starting Kafka video consumer thread...")
        stop_consumer_event.clear()
        kafka_video_consumer_thread = threading.Thread(target=kafka_video_consumer_thread_func, daemon=True)
        kafka_video_consumer_thread.start()
        if not kafka_video_consumer_thread.is_alive():
             print("❌ ERROR [VideoServer]: Failed to start video consumer thread.")
    # else:
    #      print("DEBUG [VideoServer]: Video consumer thread already running.") # Less verbose

@socketio.on('disconnect')
def handle_disconnect():
    """ Handle WebSocket disconnections """
    print(f"DEBUG [VideoServer]: ❌ Viewer client disconnected: {request.sid}")
    # Consumer thread now handles pausing/resuming itself based on active viewers

# @socketio.on('send_chat_message') # REMOVED
# def handle_chat_message(data): # REMOVED


# --- Main Execution & Cleanup ---
def main():
    print(f"DEBUG [VideoServer]: 🖥️ Starting Video Server on {CONSUMER_HOST}:{CONSUMER_PORT}")
    print(f"DEBUG [VideoServer]: 🔧 Kafka Broker: {KAFKA_BROKER}")
    print(f"DEBUG [VideoServer]:    - Consuming Video Topic: {KAFKA_VIDEO_TOPIC}")
    print(f"DEBUG [VideoServer]: 🎨 Serving frontend from: {app.static_folder}")
    print(f"DEBUG [VideoServer]: 🔌 Allowing viewer connections from: {ALLOWED_ORIGINS}")

    # Note: Consumer thread starts on first client connection

    try:
        print("DEBUG [VideoServer]: Starting SocketIO server...")
        socketio.run(app, host=CONSUMER_HOST, port=CONSUMER_PORT, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print("\nDEBUG [VideoServer]: Ctrl+C received, shutting down...")
    finally:
        print("DEBUG [VideoServer]: Requesting video consumer thread stop...")
        stop_consumer_event.set()
        if kafka_video_consumer_thread and kafka_video_consumer_thread.is_alive():
            print("DEBUG [VideoServer]: Waiting for video consumer thread to finish...")
            kafka_video_consumer_thread.join(timeout=5.0)
            if kafka_video_consumer_thread.is_alive():
                print("DEBUG [VideoServer]: Video consumer thread did not finish in time.")

        # No producer to close here

        print("DEBUG [VideoServer]: Video server shutdown complete.")

if __name__ == '__main__':
    main()