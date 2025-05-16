import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import time

# MQTT Broker details
broker_address = "broker.emqx.io"  # Public test MQTT broker
port = 1883
mqtt_topic = "traffic/light"

# Create client
client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)

# Callback for when the client connects to the broker
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Successfully connected to MQTT broker!")
    else:
        print(f"Failed to connect, return code {reason_code}")

# Set the callback
client.on_connect = on_connect

# Connect to broker
def connect_mqtt():
    print(f"Connecting to MQTT broker at {broker_address}...")
    client.connect(broker_address, port, 60)
    client.loop_start()
    time.sleep(1)  # Give time for connection to establish
    print("Connected to MQTT broker")

# Publish message
def publish(message):
    result = client.publish(mqtt_topic, message)
    status = result[0]
    if status == 0:
        print(f"Sent '{message}' to topic '{mqtt_topic}'")
    else:
        print(f"Failed to send message to topic {mqtt_topic}")

# Main function with automatic sequence
def run_automatic():
    connect_mqtt()
    
    try:
        while True:
            # Traffic light sequence
            publish("red")
            time.sleep(5)  # Red light for 5 seconds
            
            publish("yellow")
            time.sleep(2)  # Yellow light for 2 seconds
            
            publish("green")
            time.sleep(5)  # Green light for 5 seconds
            
            publish("yellow")
            time.sleep(2)  # Yellow light again for 2 seconds
            
            # Loop will start again with red
            
    except KeyboardInterrupt:
        print("Exiting program")
        publish("off")  # Turn off all lights when exiting
    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected from MQTT broker")

if __name__ == '__main__':
    run_automatic()