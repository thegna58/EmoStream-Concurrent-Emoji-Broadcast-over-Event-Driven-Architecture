const WebSocket = require('ws');

// Define the port based on the cluster ID
const args = process.argv.slice(2);
if (args.length === 0) {
    console.error("Usage: node websocket_server.js <port>");
    process.exit(1);
}
const port = parseInt(args[0]);

// Create a new WebSocket server
const wss = new WebSocket.Server({ host: 'localhost', port });

console.log(`WebSocket server is running on ws://localhost:${port}`);

// Maintain a list of connected clients
let clients = [];

// Function to send a ping to all clients to check their connection health
const heartbeat = () => {
    clients.forEach(client => {
        if (client.isAlive === false) {
            console.log('Client not responding, terminating connection');
            return client.terminate();
        }
        client.isAlive = false;
        client.ping();
    });
};

// Check for stale connections periodically
const interval = setInterval(heartbeat, 30000);

wss.on('connection', (ws) => {
    console.log('New client connected');
    ws.isAlive = true;

    clients.push(ws);   // Add new client to the list

    ws.on('message', (message) => {
        console.log(`Received from client (publisher): ${message}`);
        // Assuming you want to broadcast this message to all connected clients
        const data = JSON.parse(message);  // Adjust this if needed for your message format
        broadcast(data);
    });
    
    
        
    // Respond to pings
    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('close', () => {
        console.log('Client disconnected');
        clients = clients.filter(client => client !== ws);
    });

    ws.on('error', (error) => {
        console.error(`Client connection error: ${error}`);
    });
});

wss.on('close', () => {
    clearInterval(interval);
    console.log('WebSocket server shutting down');
});

// Broadcast function
function broadcast(data) {
    if (clients.length === 0) {
        console.log('No clients connected to broadcast to.');
    } else {
        console.log(`Broadcasting: ${JSON.stringify(data)}`);
        clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                try {
                    client.send(JSON.stringify(data));
                    console.log('Message sent to client');
                } catch (error) {
                    console.error(`Error sending message: ${error}`);
                }
            }
        });
    }
}

