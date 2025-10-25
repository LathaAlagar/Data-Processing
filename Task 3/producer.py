# src/cdc_producer.py
import json
import time
from kafka import KafkaProducer
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    # Simulate a database change
    data_change = {
        "id": random.randint(1, 10),
        "feature": random.random(),
        "target": random.random()
    }
    producer.send('cdc_topic', value=data_change)
    print(f"Sent: {data_change}")
    time.sleep(2)
