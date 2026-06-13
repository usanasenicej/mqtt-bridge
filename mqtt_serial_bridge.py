import paho.mqtt.client as mqtt
import serial
import time

MQTT_BROKER = "157.178.101.159"
MQTT_PORT = 1883
TOPIC_SENSOR = "iot/sensor"
TOPIC_LED = "iot/led"

# Setup Serial (adjust COM port accordingly)
ser = serial.Serial('COM3', 9600, timeout=1)
time.sleep(2)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    client.subscribe(TOPIC_LED)

def on_message(client, userdata, msg):
    command = msg.payload.decode()
    print(f"Received command: {command}")
    ser.write((command + "\n").encode())

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_start()

try:
    while True:
        line = ser.readline().decode().strip()
        if line:
            print("From Arduino:", line)
            client.publish(TOPIC_SENSOR, line)
except KeyboardInterrupt:
    pass
finally:
    client.loop_stop()
    ser.close()
