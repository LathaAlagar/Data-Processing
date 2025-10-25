import json
from datetime import datetime




def create_message(sensor_id: int, value: float):
return json.dumps({
"sensor_id": sensor_id,
"value": value,
"timestamp": datetime.utcnow().isoformat() + "Z"
})