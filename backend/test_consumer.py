from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'chat',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',       # <- important
    enable_auto_commit=True,
    group_id='chat-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print(f"Received: {message.value}")
