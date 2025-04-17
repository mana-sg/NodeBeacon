import zlib
import pickle
import time
import threading
import base64
import os
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from engineio.payload import Payload

# Increase payload size limit for Engine.IO (underlying Socket.IO transport)
Payload.max_decode_packets = 500 # Adjust if needed

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'video-stream')
CONSUMER_HOST = os.getenv('CONSUMER_HOST', '0.0.0.0') # Listen on all interfaces
CONSUMER_PORT = int(os.getenv('CONSUMER_PORT', 5002)) # Specific port for consumer/viewer
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', "http://localhost:3000") # React dev server default
FRONTEND_BUILD_DIR = os.getenv('FRONTEND_BUILD_DIR', '../frontend/build') # Path to React build

# --- Flask App & SocketIO Setup ---
app = Flask(__name__, static_folder=FRONTEND_BUILD_DIR, static_url_path='/')
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}}) # Apply CORS broadly

socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS,
                    async_mode='threading',
                    max_http_buffer_size=10 * 1024 * 1024) # Increase buffer size

# --- Shared State & Thread Control ---
kafka_consumer_thread = None
stop_consumer_event = threading.Event()

# --- Kafka Consumer Thread ---
def kafka_consumer_thread_func():
    """ Function running in the background thread to consume Kafka messages. """
    print(f"🚀 Starting Kafka consumer thread for topic '{KAFKA_TOPIC}' on {KAFKA_BROKER}...")
    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            consumer_timeout_ms=5000,
            auto_offset_reset='latest', # Start from the latest messages
            # group_id='my-video-consumers' # Optional: Define a group ID
        )
        print("✅ Kafka Consumer connected.")
        frame_count = 0
        start_time = time.time()
        active_clients = True # Assume clients initially

        while not stop_consumer_event.is_set():
            # Check if any clients are connected. If not, pause consumption.
            # Use socketio.server.eio.sockets which is a dictionary of connected clients
            if not socketio.server or not socketio.server.eio.sockets:
                if active_clients:
                    print("💤 No clients connected, pausing Kafka consumption...")
                    active_clients = False
                time.sleep(1) # Check again in 1 second
                continue # Skip polling if no clients
            elif not active_clients:
                 print("▶️ Clients connected, resuming Kafka consumption...")
                 active_clients = True


            try:
                # Poll with a shorter timeout to be responsive
                msg_pack = consumer.poll(timeout_ms=100)

                if not msg_pack:
                    if stop_consumer_event.is_set(): break
                    continue

                for tp, messages in msg_pack.items():
                    for message in messages:
                        if stop_consumer_event.is_set(): break

                        now = time.time()
                        try:
                            # 1. Decompress
                            decompressed = zlib.decompress(message.value)
                            # 2. Unpickle - Expecting a list containing one dictionary
                            frame_group = pickle.loads(decompressed)

                            if not isinstance(frame_group, list) or not frame_group:
                                print(f"⚠️ Consumer: Received non-list or empty list: {type(frame_group)}")
                                continue
                            frame_data = frame_group[0]
                            if not isinstance(frame_data, dict) or 'frame' not in frame_data:
                                print(f"⚠️ Consumer: List item is not a valid frame dictionary: {type(frame_data)}")
                                continue

                            jpeg_bytes = frame_data['frame']
                            kafka_timestamp = frame_data.get('timestamp', now)

                            # 3. Base64 Encode for browser display
                            base64_frame = base64.b64encode(jpeg_bytes).decode('utf-8')
                            data_url = f"data:image/jpeg;base64,{base64_frame}"

                            # 4. Emit frame via Socket.IO to all connected viewers
                            socketio.emit('video_frame', {'image': data_url, 'timestamp': now})

                            frame_count += 1
                            if now - start_time >= 5.0:
                                latency_ms = (now - kafka_timestamp) * 1000
                                print(f" [Kafka Consumer]: Processed {frame_count} frames in ~5s. Last Kafka ts diff: {latency_ms:.1f} ms")
                                frame_count = 0
                                start_time = now

                        except pickle.UnpicklingError:
                            print("⚠️ Consumer: Could not unpickle message data.")
                        except zlib.error:
                            print("⚠️ Consumer: Could not decompress message data.")
                        except IndexError:
                             print("⚠️ Consumer: Received empty list after unpickling.")
                        except Exception as e:
                            print(f"❌ Consumer: Error processing Kafka message: {e}")

                    if stop_consumer_event.is_set(): break
                if stop_consumer_event.is_set(): break

            except Exception as e:
                 print(f"❌ Consumer: Kafka polling error: {e}")
                 socketio.sleep(1)

        print("🏁 Consumer loop finished.")

    except NoBrokersAvailable:
         print(f"❌ Kafka Consumer Error: No brokers available at {KAFKA_BROKER}")
    except Exception as e:
        print(f"❌ Kafka Consumer Thread Initialization Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if consumer:
            consumer.close()
            print("✅ Kafka Consumer closed.")
        print("🛑 Kafka Consumer thread stopped.")

# --- Flask Routes ---
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    """Serve the React frontend for all non-API routes."""
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        # Serve specific static files (like CSS, JS)
        return app.send_static_file(path)
    else:
        # Serve index.html for React Router routes
        print(f"Serving index.html for path: {path}")
        return app.send_static_file('index.html')

# --- Socket.IO Event Handlers (Viewer/Consumer Side) ---
@socketio.on('connect')
def handle_connect():
    """ Handle new WebSocket connections (viewers) """
    global kafka_consumer_thread
    print(f"✅ Viewer client connected: {request.sid}")

    # Start Kafka consumer thread if it's not already running
    if kafka_consumer_thread is None or not kafka_consumer_thread.is_alive():
        print("Attempting to start Kafka consumer thread...")
        stop_consumer_event.clear()
        kafka_consumer_thread = socketio.start_background_task(target=kafka_consumer_thread_func)
        if kafka_consumer_thread:
             print("Consumer thread started via background task.")
        else:
             print("Failed to start consumer thread.")
    else:
        # If thread exists, ensure it knows a client connected (wakes from potential pause)
        print("Consumer thread already running.")


@socketio.on('disconnect')
def handle_disconnect():
    """ Handle WebSocket disconnections """
    print(f"❌ Viewer client disconnected: {request.sid}")
    # The consumer thread now checks for active clients internally,
    # so no need to explicitly stop it here unless that's desired behavior.
    # Consider stopping if idle for a long time? More complex logic.

@socketio.on('send_chat_message')
def handle_chat_message(data):
    """ Handle incoming chat messages from clients """
    message_text = data.get('message', '').strip()
    if not message_text:
        return

    print(f"💬 Received chat message: {message_text}")
    chat_data = {
        'id': request.sid + str(time.time()),
        'sender': request.sid[:6], # Simple anonymous sender ID
        'text': message_text,
        'timestamp': time.time()
    }
    # Broadcast chat message to all connected viewers
    emit('new_chat_message', chat_data, broadcast=True)

# --- Main Execution & Cleanup ---
def main():
    print(f"🖥️ Starting Consumer/Viewer Server on {CONSUMER_HOST}:{CONSUMER_PORT}")
    print(f"🔧 Kafka Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    print(f"🎨 Serving frontend from: {app.static_folder}")
    print(f"🔌 Allowing viewer connections from: {ALLOWED_ORIGINS}")

    try:
        socketio.run(app, host=CONSUMER_HOST, port=CONSUMER_PORT, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print("\nCtrl+C received, shutting down consumer server...")
    finally:
        # Graceful shutdown
        print("Requesting consumer thread stop...")
        stop_consumer_event.set()
        if kafka_consumer_thread and kafka_consumer_thread.is_alive():
            print("Waiting for consumer thread to finish...")
            kafka_consumer_thread.join(timeout=5.0)
        print("Consumer server shutdown complete.")

if __name__ == '__main__':
    main()