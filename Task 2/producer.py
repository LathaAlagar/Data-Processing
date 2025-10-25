import time
import random
from kafka import KafkaProducer
from utils import create_message


TOPIC = 'sensor-data'
BOOTSTRAP_SERVERS = 'localhost:9092'


producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
value_serializer=lambda v: v.encode('utf-8'))


print('Producer started — producing to topic:', TOPIC)


try:
sensor_ids = [1, 2, 3]
while True:
sid = random.choice(sensor_ids)
# simulate value with a small trend + noise
value = round(50 + sid * 2 + random.uniform(-5, 5), 3)
msg = create_message(sid, value)
producer.send(TOPIC, msg)
print('Sent:', msg)
producer.flush()
time.sleep(0.5) # 2 messages/sec
except KeyboardInterrupt:
print('Stopping producer')
finally:
producer.close()