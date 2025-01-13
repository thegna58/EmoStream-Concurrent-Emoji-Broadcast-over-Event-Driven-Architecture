#!/usr/bin/env python3 
from kafka import KafkaConsumer
import json
import websocket
import threading
import sys

# Define WebSocket server for broadcasting data to subscribers
subscribers = []

def broadcast_to_subscribers(emoji_data):
    """Broadcast emoji data to all registered subscribers."""
    for subscriber in subscribers:
        subscriber.send(json.dumps(emoji_data))

def on_message(ws, message):
    """Handle messages from the WebSocket client."""
    print(f"Received from subscriber: {message}")

def register_subscriber(ws):
    """Register a WebSocket client as a subscriber."""
    subscribers.append(ws)

def start_consumer(group_id):
    """Start the Kafka consumer and listen for messages."""
    consumer = KafkaConsumer(
        'processed_emoji_data',
        bootstrap_servers='localhost:9092',
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Poll for new messages and broadcast them
    for message in consumer:
        print(f"Cluster {group_id} received: {message.value}")
        broadcast_to_subscribers(message.value)

# Main function to start the consumer
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 cluster_publisher.py <cluster_id>")
        sys.exit(1)

    cluster_id = sys.argv[1]  # Get the cluster ID from command-line argument
    
    # Start the consumer with the specified cluster ID
    start_consumer(cluster_id)

