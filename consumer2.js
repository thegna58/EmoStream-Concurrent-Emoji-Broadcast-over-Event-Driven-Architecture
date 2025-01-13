const kafka = require('kafka-node');
// Kafka client setup
const kafkaClient = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
// Producer setup
const producer = new kafka.Producer(kafkaClient);
// List of text-based emoji types for testing
const emojiTypes = ['happy', 'neutral', 'pensive', 'heart', 'sad'];
// Function to generate test data
function generateTestData() {
    return JSON.stringify({
        user_id: Math.floor(Math.random() * 100) + 1, // Random user ID
        emoji_type: emojiTypes[Math.floor(Math.random() * emojiTypes.length)], // Random emoji type
        timestamp: new Date().toISOString() // Current timestamp
    });
}
// Function to send messages
function sendMessages(topic, numMessages = 1000, interval = 10) {
    let sentCount = 0;
    const intervalId = setInterval(() => {
        if (sentCount >= numMessages) {
            clearInterval(intervalId);
            producer.close(() => {
                console.log(`Finished sending ${numMessages} messages.`);
            });
            return;
        }
        const payloads = [
            {
                topic: topic,
                messages: generateTestData(),
                partition: 0,
            }
        ];
        producer.send(payloads, (err, data) => {
            if (err) {
                console.error('Error sending message:', err);
            } else {
                console.log('Message sent:', data);
            }
        });
        sentCount++;
    }, interval);
}
// Event handler for producer readiness
producer.on('ready', () => {
    console.log('Kafka producer is ready.');
    const topic = 'emoji_topic';
    const numMessages = 10000; // Total messages to send
    const interval = 5; // Interval between messages in milliseconds
    console.log(`Starting load test: Sending ${numMessages} messages to topic "${topic}"...`);
    sendMessages(topic, numMessages, interval);
});
// Handle producer errors
producer.on('error', (err) => {
    console.error('Error in Kafka producer:', err);
});