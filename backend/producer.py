# producer.py
import cv2
import zlib
import pickle
import numpy as np
import time
import threading
import os
import json
from flask import Flask
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from engineio.payload import Payload
from flask import request
import uuid # For default stream ID

# Increase payload size limit if needed (less relevant here, more for consumer)
Payload.max_decode_packets = 500

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'video-stream')
PRODUCER_HOST = os.getenv('PRODUCER_HOST', '0.0.0.0') # Listen on all interfaces
PRODUCER_PORT = int(os.getenv('PRODUCER_PORT', 5001)) # Specific port for producer control
#ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', "http://localhost:3000") # React dev server default

raw_origins = os.getenv('ALLOWED_ORIGINS', '["http://localhost:3000", "http://192.168.2.3:3000"]')

try:
    # Try to parse as JSON list
    ALLOWED_ORIGINS = json.loads(raw_origins)
    if isinstance(ALLOWED_ORIGINS, str):
        ALLOWED_ORIGINS = [ALLOWED_ORIGINS]
except json.JSONDecodeError:
    # Fallback: comma-separated string
    ALLOWED_ORIGINS = [origin.strip() for origin in raw_origins.split(',')]

VIDEO_SOURCE = os.getenv('VIDEO_SOURCE', '0') # Keep as string for potential file paths
try:
    VIDEO_SOURCE_INT = int(VIDEO_SOURCE)
except ValueError:
    VIDEO_SOURCE_INT = VIDEO_SOURCE # Use as string if not an integer
PRODUCER_FPS_LIMIT = int(os.getenv('PRODUCER_FPS_LIMIT', 30))
# --- ADD STREAM ID ---
# Generate a default unique ID if not provided, or use the env var
DEFAULT_STREAM_ID = f"stream_{uuid.uuid4().hex[:8]}"
STREAM_ID = os.getenv('STREAM_ID', DEFAULT_STREAM_ID)
# --- END ADD STREAM ID ---

# --- Flask App & SocketIO Setup (for control only) ---
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}})
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS,
                    async_mode='threading')

# --- Shared State & Thread Control ---
kafka_producer_thread = None
stop_producer_event = threading.Event()
is_producer_active = False # Track producer status
current_stream_id = STREAM_ID # Store the active stream ID

# --- Logging ---
LOG_PREFIX = "DEBUG [Producer]:"

# --- Kafka Producer Thread ---
def kafka_producer_thread_func(stream_id_to_use):
    """ Function running in the background to capture video and send to Kafka. """
    global is_producer_active, current_stream_id
    print(f"{LOG_PREFIX} 🚀 Starting Kafka producer thread for STREAM_ID '{stream_id_to_use}'...")
    print(f"{LOG_PREFIX}    Topic '{KAFKA_TOPIC}' on {KAFKA_BROKER}...")
    print(f"{LOG_PREFIX} 📷 Trying to open video source: {VIDEO_SOURCE}")

    producer = None
    cap = None
    is_producer_active = True
    current_stream_id = stream_id_to_use # Update global state
    # Emit status via the producer's socketio instance
    socketio.emit('stream_status', {'active': True, 'error': None, 'stream_id': current_stream_id})

    frame_interval = 1.0 / PRODUCER_FPS_LIMIT if PRODUCER_FPS_LIMIT > 0 else 0
    last_emit_time = time.time()

    try:
        # Initialize Kafka Producer
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                # linger_ms=5, # Batch messages slightly
                # batch_size=16384 * 2, # Default 16KB, maybe increase
                # max_request_size=1048576 * 2, # Default 1MB, increase if needed
                # compression_type='gzip', # Optional - adds CPU overhead
                 key_serializer=lambda k: k.encode('utf-8') # Ensure key is bytes
            )
            print(f"{LOG_PREFIX} ✅ Kafka Producer connected.")
        except NoBrokersAvailable:
            print(f"{LOG_PREFIX} ❌ Kafka Producer Error: No brokers available at {KAFKA_BROKER}")
            raise
        except Exception as e:
            print(f"{LOG_PREFIX} ❌ Kafka Producer Error (Init): {e}")
            raise

        # Initialize Video Capture
        cap = cv2.VideoCapture(VIDEO_SOURCE_INT)
        if not cap.isOpened():
            error_msg = f"Could not open video source {VIDEO_SOURCE_INT}"
            print(f"{LOG_PREFIX} ❌ Error: {error_msg}")
            raise IOError(error_msg)
        print(f"{LOG_PREFIX} ✅ Video source {VIDEO_SOURCE_INT} opened successfully.")

        frame_count = 0
        start_time = time.time()

        while not stop_producer_event.is_set():
            loop_start_time = time.time()
            ret, frame = cap.read()
            if not ret:
                print(f"⚠️ {LOG_PREFIX} Producer: End of video source or cannot read frame.")
                break

            # --- Adjust Quality vs Size/CPU ---
            quality = 80 # Lower quality -> smaller size, less CPU encoding
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), quality]
            ret, buffer = cv2.imencode('.jpg', frame, encode_param)
            if not ret:
                print(f"⚠️ {LOG_PREFIX} Producer: Failed to encode frame to JPEG.")
                continue
            jpeg_bytes = buffer.tobytes()

            # 2. Prepare Data Package - INCLUDE STREAM_ID
            now = time.time()
            frame_data = {
                'frame': jpeg_bytes,
                'timestamp': now,
                'stream_id': stream_id_to_use, # Include the ID
                'frame_bytes': len(jpeg_bytes) # Add frame size metric
            }
            frame_group = [frame_data] # Keep the list structure

            # 3. Pickle and Compress
            pickled_data = pickle.dumps(frame_group)
            compressed_data = zlib.compress(pickled_data)

            # 4. Send to Kafka
            try:
                # Send with stream_id as key for potential partitioning
                producer.send(KAFKA_TOPIC, value=compressed_data, key=stream_id_to_use)
            except Exception as e:
                print(f"{LOG_PREFIX} ❌ Producer: Error sending message to Kafka: {e}")
                # Maybe add retry logic or check producer.metrics()
                time.sleep(1) # Avoid tight loop on persistent Kafka errors

            frame_count += 1
            processing_time = time.time() - loop_start_time

            # Optional: Frame rate limiting
            sleep_duration = frame_interval - processing_time
            if sleep_duration > 0:
                time.sleep(sleep_duration)

            # Print stats periodically
            current_time = time.time()
            if current_time - start_time >= 5.0:
                actual_fps = frame_count / (current_time - start_time)
                print(f" [{LOG_PREFIX} {stream_id_to_use}]: Sent {frame_count} frames (~{actual_fps:.1f} fps).")
                frame_count = 0
                start_time = current_time

        print(f"{LOG_PREFIX} 🏁 Producer loop finished for stream {stream_id_to_use}.")

    except Exception as e:
        error_message = f"{LOG_PREFIX} ❌ Kafka Producer Thread Error ({stream_id_to_use}): {e}"
        print(error_message)
        import traceback
        traceback.print_exc()
        socketio.emit('stream_status', {'active': False, 'error': str(e), 'stream_id': stream_id_to_use})
    finally:
        if cap:
            cap.release()
            print(f"{LOG_PREFIX} ✅ Video capture released.")
        if producer:
            try:
                producer.flush(timeout=5.0) # Ensure all messages are sent
                print(f"{LOG_PREFIX} ✅ Kafka Producer flushed.")
            except Exception as fe:
                 print(f"{LOG_PREFIX} ⚠️ Error flushing producer: {fe}")
            finally:
                 producer.close()
                 print(f"{LOG_PREFIX} ✅ Kafka Producer closed.")
        is_producer_active = False
        # Ensure final status update reflects reality
        final_error = None if stop_producer_event.is_set() else 'Stream ended unexpectedly'
        socketio.emit('stream_status', {
            'active': False,
            'error': final_error,
            'stream_id': stream_id_to_use if final_error else None # Send ID if stopped unexpectedly
        })
        print(f"{LOG_PREFIX} 🛑 Kafka Producer thread stopped for stream {stream_id_to_use}.")

# --- Socket.IO Event Handlers (Producer Control) ---
@socketio.on('connect')
def handle_connect():
    """ Handle new WebSocket connections (likely from control page) """
    print(f"{LOG_PREFIX} ✅ Control client connected: {request.sid}")
    # Inform the new client about the current producer status
    socketio.emit('stream_status', {
        'active': is_producer_active,
        'error': None, # Assuming no error if just connecting
        'stream_id': current_stream_id if is_producer_active else None
    }, room=request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    """ Handle WebSocket disconnections """
    print(f"{LOG_PREFIX} ❌ Control client disconnected: {request.sid}")
    # Optional: Policy to stop if no controllers are connected (implement if needed)

@socketio.on('start_stream')
def handle_start_stream(data=None): # Make data optional
    """ Client requested to start the Kafka producer stream. """
    global kafka_producer_thread, is_producer_active, current_stream_id
    # Handle the case where data is None or stream_id is missing safely
    requested_stream_id = STREAM_ID # Default stream ID
    if data and isinstance(data, dict) and 'stream_id' in data and data['stream_id']:
        requested_stream_id = data['stream_id']

    print(f"{LOG_PREFIX} Received start_stream request from {request.sid} for stream_id: {requested_stream_id}")

    if kafka_producer_thread is None or not kafka_producer_thread.is_alive():
        print(f"{LOG_PREFIX} Attempting to start Kafka producer thread for {requested_stream_id}...")
        stop_producer_event.clear()
        # Pass the stream_id to the thread function
        kafka_producer_thread = socketio.start_background_task(
            target=kafka_producer_thread_func,
            stream_id_to_use=requested_stream_id
        )
        if kafka_producer_thread:
            print(f"{LOG_PREFIX} Producer thread for {requested_stream_id} started via background task.")
            # Status is updated within the thread function
        else:
            print(f"{LOG_PREFIX} Failed to start producer thread.")
            socketio.emit('stream_status', {
                'active': False,
                'error': 'Failed to start thread',
                'stream_id': None
            }, room=request.sid)
    else:
        print(f"{LOG_PREFIX} Producer thread already running (Stream ID: {current_stream_id}). Cannot start another.")
        # Re-emit current status
        socketio.emit('stream_status', {
            'active': True,
            'error': 'Producer already active',
            'stream_id': current_stream_id
        }, room=request.sid)

@socketio.on('stop_stream')
def handle_stop_stream():
    """ Client requested to stop the Kafka producer stream. """
    print(f"{LOG_PREFIX} Received stop_stream request from {request.sid}")
    stop_producer() # Use helper function

def stop_producer():
    """ Helper function to stop the producer thread """
    global kafka_producer_thread # No need for is_producer_active here
    if kafka_producer_thread and kafka_producer_thread.is_alive():
        print(f"{LOG_PREFIX} Requesting Kafka producer thread stop (Stream ID: {current_stream_id})...")
        stop_producer_event.set()
        # Let the thread emit its final status
    else:
        print(f"{LOG_PREFIX} Producer thread not running or already stopped.")
        # Ensure status is correct if called when already stopped
        if is_producer_active: # Check the flag just in case
             socketio.emit('stream_status', {'active': False, 'error': None, 'stream_id': None})


# --- Main Execution & Cleanup ---
def main():
    print(f"{LOG_PREFIX} 🎬 Starting Producer Control Server on {PRODUCER_HOST}:{PRODUCER_PORT}")
    print(f"{LOG_PREFIX} 🔧 Kafka Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    print(f"{LOG_PREFIX} 📹 Video Source: {VIDEO_SOURCE}, Producer FPS Limit: {PRODUCER_FPS_LIMIT}")
    print(f"{LOG_PREFIX} 🆔 Default Stream ID: {STREAM_ID} (Set with STREAM_ID env var)")
    print(f"{LOG_PREFIX} 🔌 Allowing control connections from: {ALLOWED_ORIGINS}")

    try:
        # use_reloader=False is important for background threads
        socketio.run(app, host=PRODUCER_HOST, port=PRODUCER_PORT, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print(f"\n{LOG_PREFIX} Ctrl+C received, shutting down producer server...")
    finally:
        print(f"{LOG_PREFIX} Requesting producer thread stop...")
        stop_producer_event.set()
        if kafka_producer_thread and kafka_producer_thread.is_alive():
            print(f"{LOG_PREFIX} Waiting for producer thread to finish...")
            kafka_producer_thread.join(timeout=5.0)
            if kafka_producer_thread.is_alive():
                print(f"⚠️ {LOG_PREFIX} Producer thread did not exit cleanly.")
        print(f"{LOG_PREFIX} Producer server shutdown complete.")

if __name__ == '__main__':
    main()