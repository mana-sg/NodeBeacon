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
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from engineio.payload import Payload

# Increase payload size limit for Engine.IO (underlying Socket.IO transport)
Payload.max_decode_packets = 500 # Adjust if needed

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'video-stream')
WEBSERVER_HOST = os.getenv('WEBSERVER_HOST', '0.0.0.0') # Listen on all interfaces
WEBSERVER_PORT = int(os.getenv('WEBSERVER_PORT', 5001)) # Port for the Flask web server
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', "http://localhost:3000") # React dev server default
VIDEO_SOURCE = int(os.getenv('VIDEO_SOURCE', 1)) # 0 for default webcam, or path to video file
PRODUCER_FPS_LIMIT = int(os.getenv('PRODUCER_FPS_LIMIT', 20)) # Limit FPS to reduce load

# --- Flask App & SocketIO Setup ---
app = Flask(__name__, static_folder='../frontend/build', static_url_path='/')
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}}) # Apply CORS to Flask app routes

socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS,
                    async_mode='threading',
                    max_http_buffer_size=10 * 1024 * 1024) # Increase buffer size

# --- Shared State & Thread Control ---
kafka_consumer_thread = None
stop_consumer_event = threading.Event()

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
    socketio.emit('stream_status', {'active': True, 'error': None}) # Notify clients

    frame_interval = 1.0 / PRODUCER_FPS_LIMIT if PRODUCER_FPS_LIMIT > 0 else 0

    try:
        # Initialize Kafka Producer
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                # Optional: Add compression, acks, etc. here if needed
                # compression_type='gzip',
                # acks='1' # default
            )
            print("✅ Kafka Producer connected.")
        except NoBrokersAvailable:
            print(f"❌ Kafka Producer Error: No brokers available at {KAFKA_BROKER}")
            raise # Re-raise to be caught by the outer try/except

        # Initialize Video Capture
        cap = cv2.VideoCapture(VIDEO_SOURCE)
        if not cap.isOpened():
            print(f"❌ Error: Could not open video source {VIDEO_SOURCE}")
            raise IOError(f"Cannot open video source {VIDEO_SOURCE}")
        print(f"✅ Video source {VIDEO_SOURCE} opened successfully.")

        frame_count = 0
        start_time = time.time()

        while not stop_producer_event.is_set():
            loop_start_time = time.time()
            ret, frame = cap.read()
            if not ret:
                print("⚠️ Producer: End of video source or cannot read frame.")
                break # Exit loop if no frame is read

            # 1. Encode Frame (e.g., JPEG)
            ret, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
            if not ret:
                print("⚠️ Producer: Failed to encode frame to JPEG.")
                continue

            jpeg_bytes = buffer.tobytes()

            # 2. Prepare Data Package (MATCHES CONSUMER EXPECTATION: list containing a dict)
            now = time.time()
            frame_data = {
                'frame': jpeg_bytes,
                'timestamp': now
            }
            frame_group = [frame_data] # Pack into a list

            # 3. Pickle and Compress
            pickled_data = pickle.dumps(frame_group)
            compressed_data = zlib.compress(pickled_data)

            # 4. Send to Kafka
            try:
                producer.send(KAFKA_TOPIC, value=compressed_data)
                # producer.flush() # Optional: Ensure message is sent immediately (lower throughput)
            except Exception as e:
                print(f"❌ Producer: Error sending message to Kafka: {e}")
                # Consider adding a delay or retry mechanism here
                time.sleep(1) # Avoid tight loop on persistent Kafka errors

            frame_count += 1
            processing_time = time.time() - loop_start_time

            # Optional: Frame rate limiting
            sleep_duration = frame_interval - processing_time
            if sleep_duration > 0:
                time.sleep(sleep_duration) # Use time.sleep for simple rate limiting

            # Print stats periodically
            if now - start_time >= 5.0:
                print(f" [Kafka Producer]: Sent {frame_count} frames in ~5s. Rate: ~{frame_count/5.0:.1f} fps")
                frame_count = 0
                start_time = now

        print("🏁 Producer loop finished.")

    except Exception as e:
        error_message = f"❌ Kafka Producer Thread Error: {e}"
        print(error_message)
        import traceback
        traceback.print_exc()
        socketio.emit('stream_status', {'active': False, 'error': str(e)}) # Notify clients of error
    finally:
        if cap:
            cap.release()
            print("✅ Video capture released.")
        if producer:
            producer.close()
            print("✅ Kafka Producer closed.")
        is_producer_active = False
        socketio.emit('stream_status', {'active': False, 'error': None}) # Notify clients stream stopped
        print("🛑 Kafka Producer thread stopped.")

# --- Kafka Consumer Thread (Mostly Unchanged, added stop_consumer_event) ---
def kafka_consumer_thread_func():
    """ Function running in the background thread to consume Kafka messages. """
    print(f"🚀 Starting Kafka consumer thread for topic '{KAFKA_TOPIC}' on {KAFKA_BROKER}...")
    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            consumer_timeout_ms=5000, # Wait up to 5s for messages
            # auto_offset_reset='latest' # Start from the latest messages
        )
        print("✅ Kafka Consumer connected.")
        frame_count = 0
        start_time = time.time()

        # Use stop_consumer_event here
        while not stop_consumer_event.is_set():
            try:
                # Poll with a shorter timeout to be more responsive to stop_event
                msg_pack = consumer.poll(timeout_ms=100) # Poll for 100ms

                if not msg_pack:
                    # No messages, check stop event again and continue polling
                    if stop_consumer_event.is_set():
                        break
                    continue

                for tp, messages in msg_pack.items():
                    for message in messages:
                        # Check stop event frequently within the inner loop too
                        if stop_consumer_event.is_set():
                            break # Break from inner message loop

                        now = time.time()
                        try:
                            # 1. Decompress
                            decompressed = zlib.decompress(message.value)
                            # 2. Unpickle - Expecting a list containing one dictionary
                            frame_group = pickle.loads(decompressed)

                            # --- FIX: Handle the list structure ---
                            if not isinstance(frame_group, list) or not frame_group:
                                print(f"⚠️ Consumer: Received non-list or empty list: {type(frame_group)}")
                                continue
                            frame_data = frame_group[0]
                            if not isinstance(frame_data, dict) or 'frame' not in frame_data:
                                print(f"⚠️ Consumer: List item is not a valid frame dictionary: {type(frame_data)}")
                                continue
                            # --- END FIX ---

                            jpeg_bytes = frame_data['frame']
                            kafka_timestamp = frame_data.get('timestamp', now)

                            # 3. Base64 Encode for browser display
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
                            print("⚠️ Consumer: Could not unpickle message data.")
                        except zlib.error:
                            print("⚠️ Consumer: Could not decompress message data.")
                        except IndexError:
                            print("⚠️ Consumer: Received empty list after unpickling.")
                        except Exception as e:
                            print(f"❌ Consumer: Error processing Kafka message: {e}")
                            # import traceback # Uncomment for debugging
                            # traceback.print_exc() # Uncomment for debugging
                    if stop_consumer_event.is_set(): # Check after processing partition messages
                        break
                if stop_consumer_event.is_set(): # Check after processing all partitions
                    break

            except Exception as e:
                 print(f"❌ Consumer: Kafka polling error: {e}")
                 socketio.sleep(1) # Wait a bit before retrying poll on error

        print("🏁 Consumer loop finished.")

    except NoBrokersAvailable:
         print(f"❌ Kafka Consumer Error: No brokers available at {KAFKA_BROKER}")
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
    return app.send_static_file('index.html')

# --- Socket.IO Event Handlers ---
@socketio.on('connect')
def handle_connect():
    """ Handle new WebSocket connections """
    global kafka_consumer_thread # Use the correct global variable name
    print(f"✅ Client connected: {request.sid}")

    # Start Kafka consumer thread if it's not already running
    if kafka_consumer_thread is None or not kafka_consumer_thread.is_alive():
        print("Attempting to start Kafka consumer thread...")
        stop_consumer_event.clear()
        kafka_consumer_thread = socketio.start_background_task(target=kafka_consumer_thread_func)
        if kafka_consumer_thread:
             print("Consumer thread started via background task.")
        else:
             print("Failed to start consumer thread.")


    # Inform the new client about the current producer status
    socketio.emit('stream_status', {'active': is_producer_active, 'error': None}, room=request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    """ Handle WebSocket disconnections """
    print(f"❌ Client disconnected: {request.sid}")
    # Optional: Stop threads if *no* clients are connected (consider carefully)
    # if not socketio.server.eio.sockets:
    #    print("No clients connected. Requesting thread stops...")
    #    stop_producer() # Use the helper function
    #    stop_consumer() # Use a similar helper if needed

# --- Producer Control Events ---
@socketio.on('start_stream')
def handle_start_stream():
    """ Client requested to start the Kafka producer stream. """
    global kafka_producer_thread, is_producer_active
    print(f"Received start_stream request from {request.sid}")
    if kafka_producer_thread is None or not kafka_producer_thread.is_alive():
        print("Attempting to start Kafka producer thread...")
        stop_producer_event.clear()
        # Use socketio.start_background_task to integrate with SocketIO's threading
        kafka_producer_thread = socketio.start_background_task(target=kafka_producer_thread_func)
        if kafka_producer_thread:
            print("Producer thread started via background task.")
            # Status is updated within the thread function now
        else:
            print("Failed to start producer thread.")
            socketio.emit('stream_status', {'active': False, 'error': 'Failed to start thread'}, room=request.sid)
    else:
        print("Producer thread already running.")
        # Optionally re-emit status if needed, though the thread does it on start/stop
        # socketio.emit('stream_status', {'active': True, 'error': None}, room=request.sid)


@socketio.on('stop_stream')
def handle_stop_stream():
    """ Client requested to stop the Kafka producer stream. """
    print(f"Received stop_stream request from {request.sid}")
    stop_producer() # Use helper function

def stop_producer():
    """ Helper function to stop the producer thread """
    global kafka_producer_thread, is_producer_active
    if kafka_producer_thread and kafka_producer_thread.is_alive():
        print("Requesting Kafka producer thread stop...")
        stop_producer_event.set()
        # Don't join here in a request handler, it can block.
        # The thread will clean up and emit status itself.
        # kafka_producer_thread.join(timeout=5.0) # Avoid blocking Flask/SocketIO thread
        # kafka_producer_thread = None # Thread function sets is_producer_active = False
        # is_producer_active = False # Thread function will set this
        # socketio.emit('stream_status', {'active': False, 'error': None}) # Thread function emits this
    else:
        print("Producer thread not running or already stopped.")
        # Ensure status is correct if called when already stopped
        if is_producer_active:
            is_producer_active = False
            socketio.emit('stream_status', {'active': False, 'error': None})


@socketio.on('send_chat_message')
def handle_chat_message(data):
    """ Handle incoming chat messages from clients """
    message_text = data.get('message', '').strip()
    if not message_text:
        return

    print(f"💬 Received chat message: {message_text}")
    chat_data = {
        'id': request.sid + str(time.time()),
        'sender': request.sid[:6],
        'text': message_text,
        'timestamp': time.time()
    }
    emit('new_chat_message', chat_data, broadcast=True)


# --- Main Execution & Cleanup ---
def main():
    print(f"🌍 Starting Flask-SocketIO server on {WEBSERVER_HOST}:{WEBSERVER_PORT}")
    print(f"🔧 Kafka Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    print(f"📹 Video Source: {VIDEO_SOURCE}, Producer FPS Limit: {PRODUCER_FPS_LIMIT}")
    print(f"🔌 Allowing connections from: {ALLOWED_ORIGINS}")

    # Use socketio.run for development server with WebSocket support
    try:
        socketio.run(app, host=WEBSERVER_HOST, port=WEBSERVER_PORT, debug=True, use_reloader=False)
    except KeyboardInterrupt:
        print("\nCtrl+C received, shutting down...")
    finally:
        # Graceful shutdown
        print("Requesting thread stops...")
        stop_producer_event.set()
        stop_consumer_event.set()

        if kafka_producer_thread and kafka_producer_thread.is_alive():
            print("Waiting for producer thread to finish...")
            kafka_producer_thread.join(timeout=5.0)
        if kafka_consumer_thread and kafka_consumer_thread.is_alive():
            print("Waiting for consumer thread to finish...")
            kafka_consumer_thread.join(timeout=5.0)

        print("Server shutdown complete.")


if __name__ == '__main__':
    main()