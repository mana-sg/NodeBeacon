import cv2
import zlib
import pickle
import numpy as np
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'video-stream',
    bootstrap_servers='localhost:9092',
)

chunk_count = 0
frame_total = 0
start_time = time.time()
frame_times = []

print("🚀 Consumer started, waiting for video chunks...")

for msg in consumer:
    try:
        now = time.time()
        decompressed = zlib.decompress(msg.value)
        frame_group = pickle.loads(decompressed)
        print(f"📦 Received chunk with {len(frame_group)} frames")

        chunk_count += 1
        frame_total += len(frame_group)

        for i, item in enumerate(frame_group):
            timestamp = item['timestamp']
            frame_bytes = item['frame']

            latency_ms = (now - timestamp) * 1000
            frame_times.append(now)

            # FPS calculation over last second
            frame_times = [t for t in frame_times if now - t <= 1.0]
            fps = len(frame_times)

            np_arr = np.frombuffer(frame_bytes, dtype=np.uint8)
            frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

            if frame is None:
                print(f"⚠️ Frame {i+1}/{len(frame_group)} failed to decode.")
                continue

            # Overlay info
            cv2.putText(frame, "● LIVE", (10, 30),
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            cv2.putText(frame, f"Latency: {latency_ms:.1f} ms", (10, 60),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (100, 255, 255), 1)
            cv2.putText(frame, f"FPS: {fps}", (10, 85),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 1)
            cv2.putText(frame, f"Chunks: {chunk_count}", (10, 110),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (200, 200, 200), 1)
            cv2.putText(frame, f"Frames: {frame_total}", (10, 130),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (200, 200, 200), 1)

            cv2.namedWindow('Consumer - Video Stream', cv2.WINDOW_NORMAL)
            cv2.imshow('Consumer - Video Stream', frame)

            if chunk_count == 1 and i == 0:
                cv2.imwrite("debug_frame.jpg", frame)
                print("💾 Saved debug frame to debug_frame.jpg")

            if cv2.waitKey(1) & 0xFF == ord('q'):
                raise KeyboardInterrupt

    except Exception as e:
        print(f"❌ Error processing chunk: {e}")

cv2.destroyAllWindows()
print("🔚 Consumer stopped.")
