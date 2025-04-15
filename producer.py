import cv2
import zlib
import pickle
import time
from kafka import KafkaProducer

# Kafka setup
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'video-stream'

# Camera setup
cap = cv2.VideoCapture(0)
cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
cap.set(cv2.CAP_PROP_FPS, 30)  # Try increasing this if camera supports it

print("📸 Starting webcam stream...")

# Optional: JPEG encoding quality
encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 60]  # Lower quality = faster encode

# FPS logging
frame_count = 0
fps_timer = time.time()

while True:
    ret, frame = cap.read()
    if not ret:
        print("❌ Failed to read frame")
        break

    # Encode JPEG with reduced quality
    success, buffer = cv2.imencode('.jpg', frame, encode_param)
    if not success:
        continue

    timestamp = time.time()
    data = [{'timestamp': timestamp, 'frame': buffer.tobytes()}]
    serialized = pickle.dumps(data)
    compressed = zlib.compress(serialized)

    # Send to Kafka
    producer.send(topic, compressed)

    frame_count += 1
    if time.time() - fps_timer >= 1.0:
        print(f"📤 Sending at ~{frame_count} FPS")
        frame_count = 0
        fps_timer = time.time()

    # Display window (comment this out for max FPS)
    cv2.imshow('Producer - Webcam Feed', frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

# Cleanup
cap.release()
producer.flush()
producer.close()
cv2.destroyAllWindows()
print("🔚 Producer stopped.")
