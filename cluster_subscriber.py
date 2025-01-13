#!/usr/bin/env python3
import asyncio
import websockets
import socket
import json
from websockets.exceptions import InvalidHandshake, ConnectionClosedError, ConnectionClosedOK


async def connect_with_retries(uri, max_retries=5, backoff_factor=2):
    """Attempt to connect to the WebSocket server with retries."""
    retries = 0
    while retries < max_retries:
        try:
            print(f"Attempting to connect to {uri} (attempt {retries + 1})...")
            connection = await asyncio.wait_for(websockets.connect(uri), timeout=10)
            print("Connected successfully!")
            return connection
        except (asyncio.TimeoutError, socket.timeout, InvalidHandshake, ConnectionClosedError) as e:
            print(f"Connection failed: {e}. Retrying in {backoff_factor ** retries} seconds...")
            await asyncio.sleep(backoff_factor ** retries)
            retries += 1
    raise ConnectionError(f"Failed to connect to {uri} after {max_retries} retries")


async def subscribe_to_cluster(cluster_id):
    """
    Connect to the WebSocket server and listen for messages.
    Reconnect automatically if the connection is lost.
    """
    uri = f"ws://localhost:900{cluster_id}"  # WebSocket port corresponds to the cluster ID
    print(f"Connecting to WebSocket server at {uri}...")

    while True:
        try:
            websocket = await connect_with_retries(uri)
            print(f"Connected to cluster {cluster_id}")

            async with websocket:
                while True:
                    try:
                        message = await websocket.recv()
                        #print(f"WebSocket connection open: {websocket.open}")

                        parsed_message = json.loads(message)  # Assuming the message is JSON
                        print(f"Received message from cluster {cluster_id}: {parsed_message}")
                    except json.JSONDecodeError as e:
                        print(f"Error decoding message: {e}")
                    except ConnectionClosedError as e:
                        print(f"Connection closed unexpectedly for cluster {cluster_id}: {e}")
                        break  # Reconnect
                    except ConnectionClosedOK:
                        print(f"Connection closed normally for cluster {cluster_id}")
                        return
        except Exception as e:
            print(f"Error in subscriber for cluster {cluster_id}: {e}")
            print("Retrying connection in 5 seconds...")
            await asyncio.sleep(5)  # Wait before attempting to reconnect


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python3 subscriber.py <cluster_id>")
        sys.exit(1)

    cluster_id = sys.argv[1]

    try:
        asyncio.run(subscribe_to_cluster(cluster_id))
    except KeyboardInterrupt:
        print("\nSubscriber stopped.")

