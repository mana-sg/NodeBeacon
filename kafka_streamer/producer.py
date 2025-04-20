from kafka import KafkaProducer
import json
import random
from datetime import datetime
import time
import logging

class KafkaNodeProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.node_ids = [1, 2, 3, 4, 5]
        self.statuses = ["Active", "Failed", "Recovered"]

    def generate_node_event(self):
        return {
            "node_id": random.choice(self.node_ids),
            "status": random.choice(self.statuses),
            "timestamp": datetime.now().isoformat()
        }

    def send_event(self, event):
        try:
            self.producer.send(self.topic, event)
            logging.info(f"Produced: {event}")
            return True
        except Exception as e:
            logging.error(f"Failed to produce event: {e}")
            return False

    def start_producing(self, interval=0.5):
        while True:
            event = self.generate_node_event()
            self.send_event(event)
            time.sleep(interval)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    producer = KafkaNodeProducer(
        bootstrap_servers=['localhost:9092'],
        topic='node_status_updates'
    )
    producer.start_producing()