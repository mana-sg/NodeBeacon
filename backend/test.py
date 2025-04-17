from kafka import KafkaProducer
import json

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

message = {"sender": "User_p58", "text": "hi", "timestamp": 1744873112.3880}

producer.send('chat', value=message)

producer.flush()

print("Message sent!")
