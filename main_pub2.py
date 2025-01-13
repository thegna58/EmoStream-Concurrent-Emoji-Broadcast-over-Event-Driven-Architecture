#!/usr/bin/env python3
from kafka import KafkaConsumer, KafkaProducer
import json
import time

# Configuration for Kafka Consumer and Producer
KAFKA_BROKER = 'localhost:9092'
CONSUMER_TOPIC = 'processed_emoji_data'  # Topic from which data is received

# Step 1: Initialize Kafka Consumer to receive data from Spark job
consumer = KafkaConsumer(
    CONSUMER_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='emoji_group'
)

# Step 2: Initialize Kafka Producer to send processed data to clusters
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Main Publisher started. Listening for data from Spark job...")

def process_and_forward_data(data):
    """Process the received data and forward it to another Kafka topic."""
    # Extract required fields from the received data
    processed_data = {
        "window_start": data["window_start"],   # Use 'window_start' key
        "window_end": data["window_end"],       # Use 'window_end' key
        "emoji_type": data["emoji_type"],
        "count": data["count"]
    }

    # Send the processed data to the 'cluster_output_data' topic
    producer.send('cluster_output_data', processed_data)
    print(f"Forwarded processed data: {processed_data}")


try:
    # Step 4: Continuously consume and forward data
    for message in consumer:
        received_data = message.value
        print(f"Received data from '{CONSUMER_TOPIC}': {received_data}")
        
        # Process and forward the received data
        process_and_forward_data(received_data)

except KeyboardInterrupt:
    print("Stopping Main Publisher.")

finally:
    # Step 5: Clean up resources
    consumer.close()
    producer.close()
    print("Kafka consumer and producer closed.")

