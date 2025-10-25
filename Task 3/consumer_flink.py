# src/consumer_flink.py
import json
from kafka import KafkaConsumer
import numpy as np
from incremental_model import IncrementalRegression

consumer = KafkaConsumer(
    'cdc_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

model = IncrementalRegression()

for msg in consumer:
    data = msg.value
    X = np.array([[data['feature']]])
    y = np.array([data['target']])
    model.update(X, y)
    print(f"Predicted: {model.predict(X)}, Actual: {y}")
