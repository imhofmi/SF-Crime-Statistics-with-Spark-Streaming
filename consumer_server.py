import confluent_kafka
from confluent_kafka import Consumer
import time
import json

BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "calls-consumer"
TOPIC_NAME = "calls"

def run_kafka_consumer():

    # Create Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        'default.topic.config': {'auto.offset.reset': 'earliest'}
    })

    # Subscribe to topic
    consumer.subscribe([TOPIC_NAME])

    # Consume calls
    while True:
        call = consumer.poll(1)
#         if call is None:
#             print("no call")
#         elif call.error() is not None:
#             print(f"call with error {call.error()}")
#         else:
        if call is not None:
            json_data = json.loads(call.value().decode('utf-8'))
            print(json_data)
            time.sleep(0.1)
    consumer.close()

if __name__ == "__main__":
    run_kafka_consumer()