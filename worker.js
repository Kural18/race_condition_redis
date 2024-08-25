const axios = require('axios');
const amqp = require('amqplib');
const config = require('./config');

const processId = process.argv[2] || 'worker';
const serverUrl = 'http://localhost:3000/increment';
const queueServerUrl = 'http://localhost:3000/increment_queue';

let i = 0;
async function runWorker() {
    while (i<10000) {
        try {
            await axios.post(serverUrl);
            console.log(`[${processId}] Increment request sent`);
        } catch (error) {
            console.error(`[${processId}] Error sending request:`, error.message);
        }
        i++;
    }
}

async function runWorker2() {
    const rabbitmqConnection = await amqp.connect(config.rabbitmqUrl);
    const channel = await rabbitmqConnection.createChannel();
    
    const queue = await channel.assertQueue('', { exclusive: true });
    await channel.bindQueue(queue.queue, 'broadcast_exchange', '');

    channel.consume(queue.queue, (msg) => {
        if (msg !== null) {
            const { newValue, workerId, incrementNumber } = JSON.parse(msg.content.toString());
            console.log(`[${processId}] Counter updated to ${newValue} by ${workerId} (increment #${incrementNumber})`);
            channel.ack(msg);
        }
    });

    let i = 0;
    while (i < 10000) {
        try {
            await axios.post(queueServerUrl, { workerId: processId, incrementNumber: i + 1 });
            console.log(`[${processId}] Queue Increment request #${i + 1} sent`);
        } catch (error) {
            console.error(`[${processId}] Error sending request:`, error.message);
        }
        i++;
    }
}

// runWorker();
runWorker2().catch(console.error);
