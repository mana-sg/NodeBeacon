import cv2
import zlib
import pickle
import numpy as np
import time
import threading
import os
from flask import Flask
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from engineio.payload import Payload
from flask import request

# Increase payload size limit if needed (less relevant here, more for consumer)
Payload.max_decode_packets = 500

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'video-stream')
PRODUCER_HOST = os.getenv('PRODUCER_HOST', '0.0.0.0') # Listen on all interfaces
PRODUCER_PORT = int(os.getenv('PRODUCER_PORT', 5001)) # Specific port for producer control
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', "http://localhost:3000") # React dev server default
VIDEO_SOURCE = int(os.getenv('VIDEO_SOURCE', 0)) # 0 for default webcam, adjust as needed
PRODUCER_FPS_LIMIT = int(os.getenv('PRODUCER_FPS_LIMIT', 30))

# --- Flask App & SocketIO Setup (for control only) ---
app = Flask(__name__)
# Apply CORS *only* to the Socket.IO path if no other routes are needed
# Or apply broadly if you might add simple status endpoints later
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}})
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS,
                    async_mode='threading') # No large buffer needed here

# --- Shared State & Thread Control ---
kafka_producer_thread = None
stop_producer_event = threading.Event()
is_producer_active = False # Track producer status

# --- Kafka Producer Thread ---
def kafka_producer_thread_func():
    """ Function running in the background to capture video and send to Kafka. """
    global is_producer_active
    print(f"🚀 Starting Kafka producer thread for topic '{KAFKA_TOPIC}' on {KAFKA_BROKER}...")
    print(f"📷 Trying to open video source: {VIDEO_SOURCE}")
    producer = None
    cap = None
    is_producer_active = True
    # Emit status via the producer's socketio instance
    socketio.emit('stream_status', {'active': True, 'error': None})

    frame_interval = 1.0 / PRODUCER_FPS_LIMIT if PRODUCER_FPS_LIMIT > 0 else 0
    last_emit_time = time.time()

    try:
        # Initialize Kafka Producer
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                # compression_type='gzip', # Optional
                # acks='1' # default
            )
            print("✅ Kafka Producer connected.")
        except NoBrokersAvailable:
            print(f"❌ Kafka Producer Error: No brokers available at {KAFKA_BROKER}")
            raise

        # Initialize Video Capture
        cap = cv2.VideoCapture(VIDEO_SOURCE)
        if not cap.isOpened():
            error_msg = f"Could not open video source {VIDEO_SOURCE}"
            print(f"❌ Error: {error_msg}")
            raise IOError(error_msg)
        print(f"✅ Video source {VIDEO_SOURCE} opened successfully.")

        frame_count = 0
        start_time = time.time()

        while not stop_producer_event.is_set():
            loop_start_time = time.time()
            ret, frame = cap.read()
            if not ret:
                print("⚠️ Producer: End of video source or cannot read frame.")
                break

            # 1. Encode Frame (e.g., JPEG)
            ret, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
            if not ret:
                print("⚠️ Producer: Failed to encode frame to JPEG.")
                continue
            jpeg_bytes = buffer.tobytes()

            # 2. Prepare Data Package
            now = time.time()
            frame_data = {'frame': jpeg_bytes, 'timestamp': now}
            frame_group = [frame_data] # Keep the list structure

            # 3. Pickle and Compress
            pickled_data = pickle.dumps(frame_group)
            compressed_data = zlib.compress(pickled_data)

            # 4. Send to Kafka
            try:
                producer.send(KAFKA_TOPIC, value=compressed_data)
            except Exception as e:
                print(f"❌ Producer: Error sending message to Kafka: {e}")
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
                print(f" [Kafka Producer]: Sent {frame_count} frames in ~5s. Rate: ~{actual_fps:.1f} fps")
                frame_count = 0
                start_time = current_time


        print("🏁 Producer loop finished.")

    except Exception as e:
        error_message = f"❌ Kafka Producer Thread Error: {e}"
        print(error_message)
        # Emit status update with error
        socketio.emit('stream_status', {'active': False, 'error': str(e)})
    finally:
        if cap:
            cap.release()
            print("✅ Video capture released.")
        if producer:
            producer.close()
            print("✅ Kafka Producer closed.")
        is_producer_active = False
        # Ensure final status update is sent if thread exits cleanly or with error
        socketio.emit('stream_status', {'active': False, 'error': None if stop_producer_event.is_set() else 'Stream ended unexpectedly'})
        print("🛑 Kafka Producer thread stopped.")

# --- Socket.IO Event Handlers (Producer Control) ---
@socketio.on('connect')
def handle_connect():
    """ Handle new WebSocket connections (likely from control page) """
    print(f"✅ Control client connected: {request.sid}")
    # Inform the new client about the current producer status
    socketio.emit('stream_status', {'active': is_producer_active, 'error': None}, room=request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    """ Handle WebSocket disconnections """
    print(f"❌ Control client disconnected: {request.sid}")
    # Optional: Stop producer if *no* control clients are connected? Decide policy.
    # active_connections = len(socketio.server.eio.sockets)
    # print(f"Active connections: {active_connections}")
    # if active_connections == 0 and is_producer_active:
    #     print("No control clients left, stopping producer.")
    #     stop_producer()


@socketio.on('start_stream')
def handle_start_stream():
    """ Client requested to start the Kafka producer stream. """
    global kafka_producer_thread, is_producer_active
    print(f"Received start_stream request from {request.sid}")
    if kafka_producer_thread is None or not kafka_producer_thread.is_alive():
        print("Attempting to start Kafka producer thread...")
        stop_producer_event.clear()
        kafka_producer_thread = socketio.start_background_task(target=kafka_producer_thread_func)
        if kafka_producer_thread:
            print("Producer thread started via background task.")
            # Status is updated within the thread function
        else:
            print("Failed to start producer thread.")
            socketio.emit('stream_status', {'active': False, 'error': 'Failed to start thread'}, room=request.sid)
    else:
        print("Producer thread already running.")
        # Re-emit current status just in case
        socketio.emit('stream_status', {'active': True, 'error': None}, room=request.sid)

@socketio.on('stop_stream')
def handle_stop_stream():
    """ Client requested to stop the Kafka producer stream. """
    print(f"Received stop_stream request from {request.sid}")
    stop_producer() # Use helper function

def stop_producer():
    """ Helper function to stop the producer thread """
    global kafka_producer_thread # No need for is_producer_active here
    if kafka_producer_thread and kafka_producer_thread.is_alive():
        print("Requesting Kafka producer thread stop...")
        stop_producer_event.set()
        # Let the thread emit its final status
    else:
        print("Producer thread not running or already stopped.")
        # Ensure status is correct if called when already stopped
        if is_producer_active: # Check the flag just in case
             socketio.emit('stream_status', {'active': False, 'error': None})


# --- Main Execution & Cleanup ---
def main():
    print(f"🎬 Starting Producer Control Server on {PRODUCER_HOST}:{PRODUCER_PORT}")
    print(f"🔧 Kafka Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    print(f"📹 Video Source: {VIDEO_SOURCE}, Producer FPS Limit: {PRODUCER_FPS_LIMIT}")
    print(f"🔌 Allowing control connections from: {ALLOWED_ORIGINS}")

    try:
        socketio.run(app, host=PRODUCER_HOST, port=PRODUCER_PORT, debug=False, use_reloader=False) # Turn off debug/reloader for stability
    except KeyboardInterrupt:
        print("\nCtrl+C received, shutting down producer server...")
    finally:
        # Graceful shutdown
        print("Requesting producer thread stop...")
        stop_producer_event.set()
        if kafka_producer_thread and kafka_producer_thread.is_alive():
            print("Waiting for producer thread to finish...")
            kafka_producer_thread.join(timeout=5.0)
        print("Producer server shutdown complete.")

if __name__ == '__main__':
    main()