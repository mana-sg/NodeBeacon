# consumer.py (MODIFIED - Video, Metrics, Rooms - With Fixes)

import zlib
import pickle
import time
import threading
import base64
import os
import json # Needed for metrics
import traceback
import statistics # For stddev calculation
from collections import defaultdict, deque
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaProducer # ADD KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from engineio.payload import Payload

Payload.max_decode_packets = 500 # Adjust if needed

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', '192.168.2.3:9092')
KAFKA_VIDEO_TOPIC = os.getenv('KAFKA_TOPIC', 'video-stream')
KAFKA_METRICS_TOPIC = os.getenv('KAFKA_METRICS_TOPIC', 'stream-metrics') # NEW Metrics Topic
CONSUMER_HOST = os.getenv('CONSUMER_HOST', '0.0.0.0')
CONSUMER_PORT = int(os.getenv('CONSUMER_PORT', 5002))
#ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '["http://localhost:3000", "http://192.168.2.3:3000"]')
FRONTEND_BUILD_DIR = os.getenv('FRONTEND_BUILD_DIR', '../frontend/build')
METRICS_INTERVAL_SEC = int(os.getenv('METRICS_INTERVAL_SEC', 5)) # How often to calculate/send metrics
METRICS_LATENCY_WINDOW = int(os.getenv('METRICS_LATENCY_WINDOW', 30))  #Number of frames for rolling latency avg

raw_origins = os.getenv('ALLOWED_ORIGINS', '["http://localhost:3000", "http://192.168.2.3:3000"]')

try:
    # Try to parse as JSON list
    ALLOWED_ORIGINS = json.loads(raw_origins)
    if isinstance(ALLOWED_ORIGINS, str):
        ALLOWED_ORIGINS = [ALLOWED_ORIGINS]
except json.JSONDecodeError:
    # Fallback: comma-separated string
    ALLOWED_ORIGINS = [origin.strip() for origin in raw_origins.split(',')]

# --- Flask App & SocketIO Setup ---
app = Flask(__name__, static_folder=FRONTEND_BUILD_DIR, static_url_path='/')
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}})
# Increase buffer size for potentially larger frames or bursts
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS,
                    async_mode='threading',
                    max_http_buffer_size=20 * 1024 * 1024) # 20MB buffer

# --- Shared State & Thread Control ---
kafka_video_consumer_thread = None
kafka_metrics_publisher_thread = None # NEW thread for publishing metrics
stop_event = threading.Event() # Single event for all threads
kafka_metrics_producer = None # NEW Kafka producer instance for metrics - Initialize as None

# --- State per Stream (Thread-safe access needed) ---
# Use defaultdict for easier initialization
# Store: { stream_id: {'frame_count', 'latency_sum', 'start_time', 'last_emit_time',
#                      'processing_time_sum', 'byte_sum', 'last_offset', 'partition',
#                      'latency_samples'} }
stream_metrics_state = defaultdict(lambda: {
    'frame_count': 0,
    'latency_sum': 0.0,
    'processing_time_sum': 0.0,
    'byte_sum': 0,
    'start_time': time.time(),
    'last_emit_time': 0.0,
    'last_offset': -1,
    'partition': -1,
    'latency_samples': deque(maxlen=METRICS_LATENCY_WINDOW) # Store recent latencies
})
# Store: { stream_id: set(client_sids) }
stream_viewers = defaultdict(set)
state_lock = threading.Lock() # Lock for accessing shared stream_metrics_state and stream_viewers

LOG_PREFIX = "DEBUG [VideoServer]:" # Logging prefix

print(f"{LOG_PREFIX} Initializing script...")
print(f"{LOG_PREFIX} KAFKA_BROKER={KAFKA_BROKER}")
print(f"{LOG_PREFIX} KAFKA_VIDEO_TOPIC={KAFKA_VIDEO_TOPIC}")
print(f"{LOG_PREFIX} KAFKA_METRICS_TOPIC={KAFKA_METRICS_TOPIC}") # Log new topic
print(f"{LOG_PREFIX} ALLOWED_ORIGINS={ALLOWED_ORIGINS}")
print(f"{LOG_PREFIX} METRICS_INTERVAL_SEC={METRICS_INTERVAL_SEC}")

# --- Kafka Initialization ---
def initialize_kafka_connections():
    """Initialize Kafka Consumer and Producer."""
    global kafka_metrics_producer # Explicitly modify the global variable
    consumer = None
    producer = None # Local producer variable

    # Initialize Consumer (Video)
    consumer_group_id = 'video-stream-consumers-v3' # Use a distinct group ID
    print(f"{LOG_PREFIX} Consumer Group ID: {consumer_group_id}")
    try:
        print(f"{LOG_PREFIX} Attempting Kafka Consumer connection (Video)...")
        consumer = KafkaConsumer(
            KAFKA_VIDEO_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            consumer_timeout_ms=5000, # Shorter timeout, rely on poll loop
            group_id=consumer_group_id,
            auto_offset_reset='latest',
            # Consider manual commit if needed, but auto-commit is simpler here
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
             # Add deserializer for potential key if producer sends it (though value matters more here)
             # key_deserializer=lambda k: k.decode('utf-8') if k else None
        )
        print(f"{LOG_PREFIX} ✅ Kafka Consumer (Video) connected.")
    except NoBrokersAvailable:
        print(f"❌ ERROR {LOG_PREFIX} Kafka Consumer Error: No brokers available at {KAFKA_BROKER}.")
        return None, None # Indicate failure
    except KafkaError as e:
         print(f"❌ ERROR {LOG_PREFIX} Kafka Consumer Setup Error (KafkaError): {e}.")
         return None, None
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} Kafka Consumer Setup Error (General): {e}")
        traceback.print_exc()
        return None, None

    # Initialize Producer (Metrics)
    try:
        print(f"{LOG_PREFIX} Attempting Kafka Producer connection (Metrics)...")
        producer = KafkaProducer( # Assign to local first
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize metrics dict to JSON bytes
            key_serializer=lambda k: k.encode('utf-8'), # Add key serializer for metrics
            # linger_ms=100 # Allow slight batching for metrics
        )
        print(f"{LOG_PREFIX} ✅ Kafka Producer (Metrics) connected.")
        kafka_metrics_producer = producer # Assign to GLOBAL only on success
    except NoBrokersAvailable:
        print(f"❌ ERROR {LOG_PREFIX} Kafka Producer Error (Metrics): No brokers available.")
        if consumer: consumer.close() # Clean up consumer if producer fails
        kafka_metrics_producer = None # Ensure global is None on failure
        return consumer, None # Return consumer (if created), but None for producer status
    except KafkaError as e:
         print(f"❌ ERROR {LOG_PREFIX} Kafka Producer Setup Error (Metrics - KafkaError): {e}.")
         if consumer: consumer.close()
         kafka_metrics_producer = None # Ensure global is None on failure
         return consumer, None
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} Kafka Producer Setup Error (Metrics - General): {e}")
        traceback.print_exc()
        if consumer: consumer.close()
        kafka_metrics_producer = None # Ensure global is None on failure
        return consumer, None

    return consumer, producer # Return local vars (global producer is set on success)

# --- Kafka Processing Threads ---
def kafka_video_consumer_thread_func():
    """Consumes from Kafka (Video), Processes, Emits Video via Socket.IO to rooms."""
    print(f"{LOG_PREFIX}-Thread: 🚀 Starting Kafka video consumer thread...")
    global kafka_metrics_producer # Use global producer status check
    video_consumer = None

    while not stop_event.is_set():
        # Check if connections are established
        if not video_consumer or not kafka_metrics_producer:
             print(f"{LOG_PREFIX}-Thread: Kafka connections needed. Attempting initialization...")
             # Attempt to initialize both consumer and producer
             video_consumer, _ = initialize_kafka_connections() # We only need consumer handle here
             if not video_consumer or not kafka_metrics_producer: # Check global producer again AFTER init attempt
                 print(f"{LOG_PREFIX}-Thread: Kafka connection attempt failed, retrying in 10s...")
                 if video_consumer: # Close consumer if it opened but producer failed
                     video_consumer.close()
                 video_consumer = None # Ensure it's None for retry logic
                 kafka_metrics_producer = None # Ensure global producer is None
                 socketio.sleep(10)
                 continue # Retry connection
             else:
                 print(f"{LOG_PREFIX}-Thread: Kafka connections established.")

        # Check for viewers before polling (optimization)
        active_viewers_exist = False
        with state_lock:
            active_viewers_exist = any(stream_viewers.values()) # Check if any stream has viewers

        if not active_viewers_exist:
            # print(f"{LOG_PREFIX}-Thread: 💤 No viewers connected globally, pausing Kafka consumption...")
            socketio.sleep(1) # Check frequently if viewers connect
            continue
        # else: print(f"{LOG_PREFIX}-Thread: ▶️ Viewers present, consuming Kafka...") # Can be verbose

        try:
            # Poll for messages
            msg_pack = video_consumer.poll(timeout_ms=1000) # Poll longer if expect gaps
            if not msg_pack:
                if stop_event.is_set(): break
                continue # No messages, continue loop

            processing_start_time = time.time()
            frames_processed_in_poll = 0

            for tp, messages in msg_pack.items():
                if stop_event.is_set(): break
                for message in messages:
                    if stop_event.is_set(): break
                    # Process video frame expects stream_id to be in the message payload
                    process_video_message(message)
                    frames_processed_in_poll += 1

            # Optional: Log processing time for the poll batch
            # poll_processing_time = time.time() - processing_start_time
            # if frames_processed_in_poll > 0:
            #     print(f"{LOG_PREFIX}-Thread: Processed {frames_processed_in_poll} frames in {poll_processing_time:.3f}s")

        except KafkaError as e:
             print(f"❌ ERROR {LOG_PREFIX}-Thread: Kafka polling/processing error (KafkaError): {e}")
             # Attempt to re-initialize connection on persistent errors
             if video_consumer: video_consumer.close(); video_consumer = None
             if kafka_metrics_producer: kafka_metrics_producer.close(); kafka_metrics_producer = None # Reset global
             print(f"{LOG_PREFIX}-Thread: Resetting Kafka connections due to error. Will retry.")
             socketio.sleep(5) # Wait before retrying connection init
        except Exception as e:
            print(f"❌ ERROR {LOG_PREFIX}-Thread: Kafka polling/processing error (General): {e}")
            traceback.print_exc()
            socketio.sleep(1) # Shorter sleep for transient errors

    # --- Cleanup ---
    if video_consumer:
        print(f"{LOG_PREFIX}-Thread: Closing Kafka Consumer (Video)...")
        video_consumer.close()
        print(f"{LOG_PREFIX}-Thread: ✅ Kafka Consumer (Video) closed.")

    print(f"{LOG_PREFIX}-Thread: 🛑 Kafka Video Consumer thread stopped.")

def kafka_metrics_publisher_thread_func():
    """Periodically calculates and publishes metrics to Kafka."""
    print(f"{LOG_PREFIX}-Metrics: 📊 Starting Kafka metrics publisher thread (Interval: {METRICS_INTERVAL_SEC}s)...")
    global kafka_metrics_producer # Use global producer

    while not stop_event.is_set():
        socketio.sleep(METRICS_INTERVAL_SEC) # Wait for the interval

        if not kafka_metrics_producer:
            # print(f"{LOG_PREFIX}-Metrics: Kafka metrics producer not ready, skipping cycle.")
            continue # Producer might be getting reconnected by the other thread

        current_time = time.time()
        metrics_to_publish = []

        with state_lock:
            # Iterate over a copy of keys in case dict changes during iteration (unlikely here but safer)
            stream_ids = list(stream_metrics_state.keys())
            for stream_id in stream_ids:
                state = stream_metrics_state[stream_id]
                interval_sec = current_time - state['start_time']

                # Avoid division by zero if no frames or interval too short
                if state['frame_count'] > 0 and interval_sec > 0.1:
                    processing_fps = state['frame_count'] / interval_sec
                    avg_latency_ms = (state['latency_sum'] / state['frame_count']) * 1000
                    avg_processing_time_ms = (state['processing_time_sum'] / state['frame_count']) * 1000
                    avg_frame_kbytes = (state['byte_sum'] / state['frame_count']) / 1024

                    # Calculate latency standard deviation if enough samples
                    latency_stddev_ms = None
                    latency_samples_ms = [s * 1000 for s in state['latency_samples']] # Convert to ms for stats
                    if len(latency_samples_ms) >= 2:
                        try:
                             latency_stddev_ms = statistics.stdev(latency_samples_ms)
                        except statistics.StatisticsError as se:
                             print(f"⚠️ {LOG_PREFIX}-Metrics: Stats error calculating stdev for {stream_id}: {se}")


                    metrics_payload = {
                        "stream_id": stream_id,
                        "timestamp": current_time, # Timestamp of metrics calculation
                        "interval_sec": round(interval_sec, 3),
                        "frame_count": state['frame_count'],
                        "processing_fps": round(processing_fps, 2),
                        "average_latency_ms": round(avg_latency_ms, 2),
                        "average_processing_time_ms": round(avg_processing_time_ms, 3), # Time in consumer
                        "average_frame_kbytes": round(avg_frame_kbytes, 2),
                        "viewer_count": len(stream_viewers.get(stream_id, set())), # Get current viewer count
                        "kafka_last_offset": state['last_offset'],
                        "kafka_partition": state['partition'],
                        "latency_stddev_ms": round(latency_stddev_ms, 2) if latency_stddev_ms is not None else None # Optional: Std Dev
                    }
                    metrics_to_publish.append(metrics_payload)

                    # Reset state for the next interval
                    state['frame_count'] = 0
                    state['latency_sum'] = 0.0
                    state['processing_time_sum'] = 0.0
                    state['byte_sum'] = 0
                    state['start_time'] = current_time
                    # Don't reset last_offset, partition, latency_samples here

                # Clean up state for streams with no viewers and no recent activity (optional)
                # Add logic here if needed, e.g., based on last frame time or viewer count == 0 for a while

        # Publish metrics outside the lock
        if metrics_to_publish:
            print(f"{LOG_PREFIX}-Metrics: Publishing {len(metrics_to_publish)} metric records...")
            publish_error = False
            for payload in metrics_to_publish:
                if not kafka_metrics_producer: # Double check producer didn't disconnect
                     print(f"❌ ERROR {LOG_PREFIX}-Metrics: Producer unavailable mid-publish cycle.")
                     publish_error = True
                     break
                try:
                    # Use stream_id as key for metrics topic partitioning too
                    kafka_metrics_producer.send(KAFKA_METRICS_TOPIC,
                                                value=payload,
                                                key=payload['stream_id']) # Send key
                except Exception as e:
                    print(f"❌ ERROR {LOG_PREFIX}-Metrics: Failed to send metrics for {payload.get('stream_id')}: {e}")
                    publish_error = True # Mark error occurred
                    # Consider producer re-initialization logic if errors persist
                    # For now, we just log and continue

            if not publish_error and kafka_metrics_producer: # Try flushing if no errors and producer exists
                try:
                    kafka_metrics_producer.flush(timeout=1.0) # Attempt to send buffered messages
                except Exception as e:
                    print(f"❌ ERROR {LOG_PREFIX}-Metrics: Error flushing metrics producer: {e}")
                    # If flush fails, producer might be unhealthy, video thread will handle reconnect
            elif publish_error:
                 print(f"⚠️ {LOG_PREFIX}-Metrics: Skipped flush due to publishing errors.")


    # --- Cleanup ---
    if kafka_metrics_producer:
        print(f"{LOG_PREFIX}-Metrics: Closing Kafka Producer (Metrics)...")
        try:
             kafka_metrics_producer.flush(timeout=3.0)
             kafka_metrics_producer.close(timeout=3.0)
             print(f"{LOG_PREFIX}-Metrics: ✅ Kafka Producer (Metrics) closed.")
        except Exception as e:
             print(f"⚠️ {LOG_PREFIX}-Metrics: Error closing metrics producer: {e}")
        finally:
             kafka_metrics_producer = None # Ensure set to None after close attempt


    print(f"{LOG_PREFIX}-Metrics: 🛑 Kafka Metrics Publisher thread stopped.")


def process_video_message(message):
    """Decodes video frame, updates metrics, emits via Socket.IO to specific room."""
    # print(f"{LOG_PREFIX} Processing message offset={message.offset}, partition={message.partition}") # DEBUG
    process_start_time = time.time()
    try:
        # 1. Decompress and Unpickle
        decompressed = zlib.decompress(message.value)
        frame_group = pickle.loads(decompressed) # Expecting a list
        if not isinstance(frame_group, list) or not frame_group: return
        frame_data = frame_group[0] # Get the first (and likely only) frame dict
        if not isinstance(frame_data, dict) or 'frame' not in frame_data or 'stream_id' not in frame_data:
             print(f"⚠️ {LOG_PREFIX} Invalid frame data structure offset={message.offset}")
             return

        # 2. Extract Data
        jpeg_bytes = frame_data['frame']
        stream_id = frame_data['stream_id'] # <<< Get stream_id from payload
        # print(f"{LOG_PREFIX} Extracted stream_id='{stream_id}' from offset {message.offset}") # DEBUG
        kafka_timestamp = frame_data.get('timestamp', process_start_time) # Producer timestamp
        frame_byte_size = frame_data.get('frame_bytes', len(jpeg_bytes)) # Get size if available

        # 3. Calculate Latency
        receive_time = time.time() # Consumer receive/process time
        latency_sec = receive_time - kafka_timestamp
        latency_ms = latency_sec * 1000

        # 4. Encode for Web
        base64_frame = base64.b64encode(jpeg_bytes).decode('utf-8')
        data_url = f"data:image/jpeg;base64,{base64_frame}"

        # 5. Update Metrics (Thread-safe)
        processing_time_sec = time.time() - process_start_time
        with state_lock:
            state = stream_metrics_state[stream_id] # Access/create state for this stream
            state['frame_count'] += 1
            state['latency_sum'] += latency_sec
            state['processing_time_sum'] += processing_time_sec
            state['byte_sum'] += frame_byte_size
            state['last_offset'] = message.offset
            state['partition'] = message.partition
            state['latency_samples'].append(latency_sec) # Add to rolling window

        # 6. Emit to Specific Room
        # Check if there are actually viewers for this stream before emitting
        viewers_exist = False # Default to false
        with state_lock:
            # Check viewers specifically for THIS stream_id
            viewers_exist = stream_id in stream_viewers and len(stream_viewers[stream_id]) > 0

        if viewers_exist:
            # print(f"{LOG_PREFIX} Viewers exist for {stream_id}. Emitting frame to room.") # DEBUG
            socketio.emit('video_frame',
                          {'image': data_url, 'latency': latency_ms, 'stream_id': stream_id},
                          room=stream_id) # <<< Emit only to the stream's room
        # else:
            # print(f"[{stream_id}] No viewers, skipping frame emit.") # DEBUG


    except zlib.error as ze:
         print(f"❌ ERROR {LOG_PREFIX} zlib Decompression Error offset {message.offset}: {ze}")
    except pickle.UnpicklingError as pe:
         print(f"❌ ERROR {LOG_PREFIX} Pickle Error offset {message.offset}: {pe}")
    except KeyError as ke:
        print(f"❌ ERROR {LOG_PREFIX} Missing key in frame data offset {message.offset}: {ke}")
    except Exception as e:
        print(f"❌ ERROR {LOG_PREFIX} Error processing message offset {message.offset}: {e}")
        traceback.print_exc()

# --- Flask Routes ---
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return app.send_static_file(path)
    else:
        return app.send_static_file('index.html')

# --- Socket.IO Event Handlers ---
@socketio.on('connect')
def handle_connect():
    """ Handle new WebSocket connections (video viewers) """
    print(f"{LOG_PREFIX} ✅ Viewer client connected: {request.sid}")
    # Consumer/Metrics threads are started/managed globally now, no need to start here.
    # We wait for the client to tell us which stream they want via 'join_stream'

@socketio.on('disconnect')
def handle_disconnect():
    """ Handle WebSocket disconnections """
    sid = request.sid
    print(f"{LOG_PREFIX} ❌ Viewer client disconnected: {sid}")
    # Remove client from all streams they might have joined
    streams_left = []
    with state_lock:
        # Iterate through a copy of keys as we might modify the dict implicitly if a set becomes empty
        stream_ids_to_check = list(stream_viewers.keys())
        for stream_id in stream_ids_to_check:
            if sid in stream_viewers[stream_id]:
                print(f"{LOG_PREFIX} Removing {sid} from stream {stream_id}")
                stream_viewers[stream_id].remove(sid)
                leave_room(stream_id, sid=sid) # Flask-SocketIO leave
                streams_left.append(stream_id)
                # Optional: Clean up stream_metrics_state if viewers drop to 0 and no recent activity
                # if not stream_viewers[stream_id]:
                #     print(f"{LOG_PREFIX} No viewers left in {stream_id}. Clearing state.")
                #     # Potentially remove metrics state after a delay or based on last frame time
                #     # Be careful not to delete if producer might still send frames
                #     try:
                #          del stream_viewers[stream_id] # Remove stream key if set is empty
                #          # del stream_metrics_state[stream_id] # Maybe clear metrics state too?
                #     except KeyError:
                #          pass # Should not happen if sid was in the set

    # Broadcast updated viewer counts for streams the client left
    for stream_id in streams_left:
        with state_lock:
            # Get count, default to 0 if stream_id was removed above
            count = len(stream_viewers.get(stream_id, set()))
        print(f"{LOG_PREFIX} Broadcasting viewer count for {stream_id}: {count}")
        socketio.emit('viewer_count_update', {'stream_id': stream_id, 'count': count}, room=stream_id)


@socketio.on('join_stream')
def handle_join_stream(data):
    """ Client requests to join a specific stream's room """
    sid = request.sid
    if not data or not isinstance(data, dict) or 'stream_id' not in data or not data['stream_id']:
        print(f"⚠️ {LOG_PREFIX} Client {sid} sent invalid join_stream data: {data}")
        emit('error', {'message': 'stream_id is required and must be a non-empty string'}, room=sid)
        return

    stream_id = data['stream_id']
    print(f"{LOG_PREFIX} Client {sid} joining stream room: {stream_id}")

    join_room(stream_id, sid=sid) # Flask-SocketIO join

    with state_lock:
        # Ensure metrics state is initialized for this stream if it's the first viewer
        if stream_id not in stream_metrics_state:
             print(f"{LOG_PREFIX} First viewer for {stream_id}, initializing metrics state.")
             stream_metrics_state[stream_id]['start_time'] = time.time() # Initialize timer
        # Add viewer
        stream_viewers[stream_id].add(sid)
        count = len(stream_viewers[stream_id])

    # Send current count back to the joining client immediately
    print(f"{LOG_PREFIX} Sending initial viewer count {count} to {sid} for {stream_id}")
    emit('viewer_count_update', {'stream_id': stream_id, 'count': count}, room=sid)
    # Broadcast updated count to everyone else in the room (excluding the joiner)
    print(f"{LOG_PREFIX} Broadcasting viewer count update {count} to room {stream_id} (skip {sid})")
    socketio.emit('viewer_count_update', {'stream_id': stream_id, 'count': count}, room=stream_id, skip_sid=sid)

    print(f"{LOG_PREFIX} Updated viewer count for {stream_id}: {count}")


# --- Main Execution & Cleanup ---
def main():
    global kafka_video_consumer_thread, kafka_metrics_publisher_thread
    print(f"{LOG_PREFIX} 🖥️ Starting Video Server & Metrics Publisher on {CONSUMER_HOST}:{CONSUMER_PORT}")
    # Kafka connections are now attempted within the consumer thread loop

    # Start Kafka consumer thread
    print(f"{LOG_PREFIX} Starting Kafka video consumer thread...")
    stop_event.clear()
    kafka_video_consumer_thread = threading.Thread(target=kafka_video_consumer_thread_func, daemon=True)
    kafka_video_consumer_thread.start()

    # Start Kafka metrics publisher thread
    print(f"{LOG_PREFIX} Starting Kafka metrics publisher thread...")
    kafka_metrics_publisher_thread = threading.Thread(target=kafka_metrics_publisher_thread_func, daemon=True)
    kafka_metrics_publisher_thread.start()

    try:
        print(f"{LOG_PREFIX} Starting SocketIO server...")
        socketio.run(app, host=CONSUMER_HOST, port=CONSUMER_PORT, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print(f"\n{LOG_PREFIX} Ctrl+C received, shutting down...")
    except SystemExit:
         print(f"{LOG_PREFIX} SystemExit caught, shutting down...")
    except Exception as e:
         print(f"❌ ERROR {LOG_PREFIX} Unhandled exception in main execution: {e}")
         traceback.print_exc()
    finally:
        print(f"{LOG_PREFIX} Initiating shutdown sequence...")
        stop_event.set() # Signal threads to stop

        # Wait for threads to finish
        thread_timeout = METRICS_INTERVAL_SEC + 2 # Give metrics time for final publish/flush
        if kafka_video_consumer_thread and kafka_video_consumer_thread.is_alive():
            print(f"{LOG_PREFIX} Waiting for video consumer thread (timeout={thread_timeout}s)...")
            kafka_video_consumer_thread.join(timeout=thread_timeout)
            if kafka_video_consumer_thread.is_alive(): print(f"⚠️ {LOG_PREFIX} Video consumer thread did not finish.")

        if kafka_metrics_publisher_thread and kafka_metrics_publisher_thread.is_alive():
            print(f"{LOG_PREFIX} Waiting for metrics publisher thread (timeout={thread_timeout}s)...")
            kafka_metrics_publisher_thread.join(timeout=thread_timeout)
            if kafka_metrics_publisher_thread.is_alive(): print(f"⚠️ {LOG_PREFIX} Metrics publisher thread did not finish.")

        # Producer is closed within the metrics thread, consumer within its own thread.

        print(f"{LOG_PREFIX} Video server shutdown complete.")

if __name__ == '__main__':
    main()