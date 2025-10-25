import json
import collections
from kafka import KafkaConsumer
import joblib
import numpy as np


TOPIC = 'sensor-data'
BOOTSTRAP_SERVERS = 'localhost:9092'
MODEL_PATH = 'model.joblib' # produced by model_training.py


# load model (if available)
model = None
try:
model = joblib.load(MODEL_PATH)
print('Loaded model from', MODEL_PATH)
except Exception as e:
print('No trained model found — proceed without ML. Error:', e)


consumer = KafkaConsumer(TOPIC,
bootstrap_servers=BOOTSTRAP_SERVERS,
auto_offset_reset='earliest',
enable_auto_commit=True,
value_deserializer=lambda v: json.loads(v.decode('utf-8')))


# maintain rolling window per sensor
WINDOW_SIZE = 10
windows = {}


print('Consumer started — listening to topic:', TOPIC)
for msg in consumer:
data = msg.value
sid = data['sensor_id']
val = float(data['value'])
ts = data['timestamp']


if sid not in windows:
windows[sid] = collections.deque(maxlen=WINDOW_SIZE)
windows[sid].append(val)


# compute rolling average
window = windows[sid]
rolling_avg = sum(window) / len(window)


# basic features for ML: last value, rolling avg
features = np.array([[val, rolling_avg]])


pred = None
if model is not None:
pred = model.predict(features)[0]


print(f"sensor={sid} ts={ts} value={val:.3f} rolling_avg={rolling_avg:.3f} prediction={pred}")