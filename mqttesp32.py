

import paho.mqtt.client as mqtt
import csv

broker_address = "test.mosquitto.org"
topic = "esp32"

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))
    client.subscribe(topic)

def on_message(client, userdata, msg):
    data = str(msg.payload.decode("utf-8"))
    print("Received data: " + data)
    with open('/home/gharsallah/Downloads/es.csv', mode='a') as file:
        writer = csv.writer(file)
        writer.writerow([data])

client = mqtt.Client("")
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker_address)

client.loop_forever()










