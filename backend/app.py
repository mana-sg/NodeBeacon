# backend/app.py
import cv2
import zlib
import pickle
import numpy as np
import time
import threading
import base64
import os
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from kafka import KafkaConsumer
from engineio.payload import Payload

# Increase payload size limit for Engine.IO (underlying Socket.IO transport)
Payload.max_decode_packets = 500 # Adjust if needed

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'video-stream')
WEBSERVER_HOST = os.getenv('WEBSERVER_HOST', '0.0.0.0') # Listen on all interfaces
WEBSERVER_PORT = int(os.getenv('WEBSERVER_PORT', 5001)) # Port for the Flask web server
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', "http://localhost:3000") # React dev server default

# --- Flask App & SocketIO Setup ---
app = Flask(__name__, static_folder='../frontend/build', static_url_path='/')
# Apply CORS to the Flask app itself if serving other non-SocketIO routes
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}})

# Configure SocketIO with CORS
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS,
                    async_mode='threading', # Use threading for background tasks
                    max_http_buffer_size=10 * 1024 * 1024) # Increase buffer size (e.g., 10MB)

# --- Kafka Consumer Thread ---
kafka_thread = None
stop_event = threading.Event()

def kafka_consumer_thread_func():
    """ Function running in the background thread to consume Kafka messages. """
    print(f"🚀 Starting Kafka consumer thread for topic '{KAFKA_TOPIC}' on {KAFKA_BROKER}...")
    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            consumer_timeout_ms=5000
        )
        print("✅ Kafka Consumer connected.")
        frame_count = 0
        start_time = time.time()

        while not stop_event.is_set():
            try:
                msg_pack = consumer.poll(timeout_ms=100)

                if not msg_pack:
                    continue

                for tp, messages in msg_pack.items():
                    for message in messages:
                        now = time.time()
                        try:
                            # 1. Decompress
                            decompressed = zlib.decompress(message.value)
                            # 2. Unpickle - Expecting a list containing one dictionary
                            frame_group = pickle.loads(decompressed)

                            # --- FIX: Handle the list structure ---
                            # Check if it's a non-empty list
                            if not isinstance(frame_group, list) or not frame_group:
                                print(f"⚠️ Received non-list or empty list: {type(frame_group)}")
                                continue # Skip this message

                            # Extract the first item (which should be the dictionary)
                            frame_data = frame_group[0]

                            # Check if the extracted item is a dictionary with 'frame'
                            if not isinstance(frame_data, dict) or 'frame' not in frame_data:
                                print(f"⚠️ List item is not a valid frame dictionary: {type(frame_data)}")
                                continue # Skip this message
                            # --- END FIX ---

                            # Now frame_data is the dictionary we expect
                            jpeg_bytes = frame_data['frame']
                            kafka_timestamp = frame_data.get('timestamp', now) # Use .get for safety

                            # 3. Base64 Encode
                            base64_frame = base64.b64encode(jpeg_bytes).decode('utf-8')
                            data_url = f"data:image/jpeg;base64,{base64_frame}"

                            # 4. Emit frame via Socket.IO
                            socketio.emit('video_frame', {'image': data_url, 'timestamp': now})

                            frame_count += 1
                            if now - start_time >= 5.0:
                                print(f" [Kafka Consumer]: Processed {frame_count} frames in ~5s. Last Kafka ts diff: {(now - kafka_timestamp)*1000:.1f} ms")
                                frame_count = 0
                                start_time = now

                        except pickle.UnpicklingError:
                            print("⚠️ Error: Could not unpickle message data.")
                        except zlib.error:
                            print("⚠️ Error: Could not decompress message data.")
                        except IndexError:
                             print("⚠️ Error: Received empty list after unpickling.")
                        except Exception as e:
                            print(f"❌ Error processing Kafka message: {e}")
                            import traceback
                            traceback.print_exc()

            except Exception as e:
                 print(f"❌ Kafka polling error: {e}")
                 socketio.sleep(1)

    except Exception as e:
        print(f"❌ Kafka Consumer Thread Initialization Error: {e}")
    finally:
        if consumer:
            consumer.close()
            print("✅ Kafka Consumer closed.")
        print("🛑 Kafka Consumer thread stopped.")

# --- Flask Routes ---
@app.route('/')
def index():
    """Serve the React frontend."""
    print("Serving index.html")
    # Assumes React app is built into ../frontend/build
    return app.send_static_file('index.html')

# --- Socket.IO Event Handlers ---
@socketio.on('connect')
def handle_connect():
    """ Handle new WebSocket connections """
    print(f"✅ Client connected: {request.sid}")
    # Start Kafka consumer thread if it's not already running
    global kafka_thread
    if kafka_thread is None or not kafka_thread.is_alive():
        print("Starting Kafka consumer thread...")
        stop_event.clear()
        kafka_thread = socketio.start_background_task(target=kafka_consumer_thread_func)
    # You could potentially send the *very latest* frame on connect here if needed
    # but the stream will start sending new ones immediately anyway.

@socketio.on('disconnect')
def handle_disconnect():
    """ Handle WebSocket disconnections """
    print(f"❌ Client disconnected: {request.sid}")
    # Optional: Stop the Kafka thread if no clients are connected
    # if not socketio.server.eio.sockets: # Check if any clients remain
    #    print("No clients connected, requesting Kafka thread stop...")
    #    stop_event.set()
    #    global kafka_thread
    #    if kafka_thread:
    #        kafka_thread.join() # Wait for thread to finish
    #        kafka_thread = None


@socketio.on('send_chat_message')
def handle_chat_message(data):
    """ Handle incoming chat messages from clients """
    message_text = data.get('message', '').strip()
    if not message_text:
        return # Ignore empty messages

    print(f"💬 Received chat message: {message_text}")
    # Sanitize or validate message_text if necessary
    # Add sender info (e.g., session ID, or later a username) and timestamp
    chat_data = {
        'id': request.sid + str(time.time()), # Simple unique ID
        'sender': request.sid[:6], # Use partial SID as temporary name
        'text': message_text,
        'timestamp': time.time()
    }
    # Broadcast the message to ALL connected clients, including the sender
    emit('new_chat_message', chat_data, broadcast=True)


# --- Main Execution ---
if __name__ == '__main__':
    print(f"🌍 Starting Flask-SocketIO server on {WEBSERVER_HOST}:{WEBSERVER_PORT}")
    print(f"🔌 Allowing connections from: {ALLOWED_ORIGINS}")
    # Use socketio.run for development server with WebSocket support
    socketio.run(app, host=WEBSERVER_HOST, port=WEBSERVER_PORT, debug=True, use_reloader=False)
    # use_reloader=False is important to prevent the Kafka thread from starting twice in debug mode

    # Cleanup on shutdown (this might not always run cleanly on Ctrl+C)
    print("Requesting Kafka thread stop...")
    stop_event.set()
    if kafka_thread:
        kafka_thread.join()
    print("Server shutdown.")