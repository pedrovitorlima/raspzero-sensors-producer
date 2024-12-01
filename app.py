import os 
from datetime import datetime
import psutil
import json
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import time

load_dotenv()

class Sensors:
  def __init__(self):
    self.mqtt_broker = os.getenv("BROKER_NAME")
    self.mqtt_port = 1883
    self.mqtt_topic = "prod/sensor_wormhole"
    self.mqtt_user = os.getenv("BROKER_USER")
    self.mqtt_password = os.getenv("BROKER_PASSWORD")

  def read_temperature(self):
    temp = os.popen('vcgencmd measure_temp').readline()
    return temp.strip()
  
  def read_cpu(self):
    return psutil.cpu_percent()
  
  def read_ram(self):
    return psutil.virtual_memory()
  
  def produce(self, messages):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(self.mqtt_user, self.mqtt_password)
    client.connect(self.mqtt_broker, self.mqtt_port, 60)

    for message in messages:
      print(message)
      client.publish(self.mqtt_topic, json.dumps(message))
    
    client.disconnect()

if __name__ == "__main__":
  sensors = Sensors()

while(True):
  messages = [
    {
      "device": "pi_zero",
      "sensor": "temperature",
      "reading": sensors.read_temperature(),
      "date": datetime.now(),
    },
    {
      "device": "pi_zero",
      "sensor": "cpu",
      "reading": sensors.read_cpu(),
      "date": datetime.now().isoformat(),
    },
    {
      "device": "pi_zero",
      "sensor": "ram",
      "reading": sensors.read_ram(),
      "date": datetime.now().isoformat(),
    },
  ]

  sensors.produce(messages)
  time.sleep(30)