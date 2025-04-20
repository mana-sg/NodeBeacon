from kafka_streamer.producer import KafkaNodeProducer
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    producer = KafkaNodeProducer(
        bootstrap_servers=['localhost:9092'],
        topic='node_status_updates'
    )
    event = {
        "node_id": 123,
        "status": "Active",
        "timestamp": datetime.now().isoformat()
    }
    producer.send_event(event)

if __name__ == "__main__":
    main()