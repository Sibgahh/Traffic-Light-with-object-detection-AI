import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import json
import time

# MQTT Broker details
broker_address = "broker.emqx.io"  # Public test MQTT broker
port = 1883
topic = "traffic/vehicle_count"

# Create client
client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)

# Callback for when the client connects to the broker
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Successfully connected to MQTT broker!")
        # Subscribe to the vehicle count topic
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")
    else:
        print(f"Failed to connect, return code {reason_code}")

# Callback for when a message is received
def on_message(client, userdata, message):
    try:
        # Parse the JSON message
        data = json.loads(message.payload.decode())
        
        # Print the vehicle counts
        print("\n=== Vehicle Count Update ===")
        print(f"Road Section ID: {data['road_section_id']}")
        print(f"Total Vehicles: {data['total_vehicles']}")
        print("Individual Counts:")
        for vehicle_type, count in data['vehicle_counts'].items():
            print(f"  {vehicle_type}: {count}")
        print(f"Timestamp: {data['timestamp']}")
        print("===========================\n")
    except Exception as e:
        print(f"Error processing message: {e}")

# Set the callbacks
client.on_connect = on_connect
client.on_message = on_message

# Connect to broker
def connect_mqtt():
    print(f"Connecting to MQTT broker at {broker_address}...")
    try:
        client.connect(broker_address, port, 60)
        client.loop_start()
        print("Connected to MQTT broker")
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")

def main():
    connect_mqtt()
    
    try:
        # Keep the program running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDisconnecting from MQTT broker...")
        client.loop_stop()
        client.disconnect()
        print("Disconnected from MQTT broker")

if __name__ == "__main__":
    main() 