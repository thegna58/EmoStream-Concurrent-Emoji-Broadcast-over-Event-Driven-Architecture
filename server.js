const express = require('express');
const bodyParser = require('body-parser');
const kafka = require('kafka-node');
const cors = require('cors');

const app = express();
const PORT = 5001;

// Middleware
app.use(cors()); // Enable CORS
app.use(bodyParser.json());

// Kafka setup
const kafkaClient = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafka.Producer(kafkaClient);

// Buffer to hold incoming messages
let messageBuffer = [];

// Function to send messages to Kafka
const sendMessagesToKafka = async () => {
    if (messageBuffer.length === 0) return; // Exit if there's nothing to send

    const payloads = [{ topic: 'emoji_topic', messages: JSON.stringify(messageBuffer) }];

    producer.send(payloads, (err, data) => {
        if (err) {
            console.error('Error sending message to Kafka:', err);
        } else {
            console.log('Messages sent to Kafka:', data);
            messageBuffer = []; // Clear the buffer after sending
        }
    });
};

// Set an interval to send messages every 500 milliseconds
setInterval(sendMessagesToKafka, 500);

// Handle producer errors
producer.on('error', (err) => {
    console.error('Kafka Producer Error:', err);
});

// API endpoint to receive emoji data
app.post('/emoji', (req, res) => {
    const { user_id, emoji_type, timestamp } = req.body;

    // Validate input
    if (!user_id || !emoji_type || !timestamp) {
        return res.status(400).json({ error: 'Missing required fields' });
    }

    // Add the message to the buffer
    const message = {
        user_id,
        emoji_type,
        timestamp
    };

    messageBuffer.push(message);
    res.status(200).json({ status: 'Emoji received', data: message });
});

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
