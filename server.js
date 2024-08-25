const express = require('express');
const redis = require('redis');
const amqp = require('amqplib');
const config = require('./config');

const app = express();
const port = 3000;

let redisClient, rabbitmqChannel;

async function initializeServer() {
    redisClient = redis.createClient({ url: config.redisUrl });
    await redisClient.connect();
    await redisClient.set(config.counterKey, '0');
    console.log('Counter reset to 0');

    const rabbitmqConnection = await amqp.connect(config.rabbitmqUrl);
    rabbitmqChannel = await rabbitmqConnection.createChannel();
    await rabbitmqChannel.assertExchange('broadcast_exchange', 'fanout', { durable: false });

    processQueue();
}

async function processQueue() {
    while (true) {
        const nextIncrement = await redisClient.lPop('increment_queue');
        if (nextIncrement) {
            const [workerId, incrementNumber] = JSON.parse(nextIncrement);
            const currentValue = parseInt(await redisClient.get(config.counterKey));
            const newValue = currentValue + 1;
            await redisClient.set(config.counterKey, newValue.toString());
            // const newValue = await redisClient.incr(config.counterKey);
            console.log(`Queue: Counter incremented to ${newValue} by ${workerId}`);
            rabbitmqChannel.publish('broadcast_exchange', '', Buffer.from(JSON.stringify({
                newValue,
                workerId,
                incrementNumber
            })));
        }
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}

app.use(express.json());


app.post('/increment', async (req, res) => {
    const currentValue = parseInt(await redisClient.get(config.counterKey));
    const newValue = currentValue + 1;
    await redisClient.set(config.counterKey, newValue.toString());
    console.log(`Direct: Counter incremented to ${newValue}`);
    res.sendStatus(200);
});


app.post('/increment_queue', async (req, res) => {
    const { workerId, incrementNumber } = req.body;
    await redisClient.rPush('increment_queue', JSON.stringify([workerId, incrementNumber]));
    res.sendStatus(200);
});

initializeServer().then(() => {
    app.listen(port, () => {
        console.log(`Server running at http://localhost:${port}`);
    });
});