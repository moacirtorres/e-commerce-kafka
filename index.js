const { Kafka } = require('kafkajs')


const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const topicName = 'orderCreated'

const process = async () => {
    const admin = kafka.admin()
    await admin.connect()
    await admin.createTopics({
        topics: [{
            topic: topicName,
            numPartitions: 2,
            replicationFactor: 1
        }]
    })

    await admin.disconnect
}

process().then(() => console.log('done'))

const processConsumer = async () => {
    const ordersConsumer = kafka.consumer({groupId: 'orders'})
    const paymentsConsumer = kafka.consumer({groupId: 'payments'})
    const notificationsConsumer = kafka.consumer({groupId: 'notifications'})

    await Promise.all([
        ordersConsumer.connect(),
        paymentsConsumer.connect(),
        notificationsConsumer.connect()
    ])

    await Promise.all([
        await ordersConsumer.subscribe({topic: topicName}),
        await paymentsConsumer.subscribe({topic: topicName}),
        await notificationsConsumer.subscribe({topic: topicName})
    ])

    let orderCounter = 1
    let paymentCounter = 1
    let notificationCounter = 1

    await ordersConsumer.run({
        eachMessage: async({topic, partition, message}) => {
            logMessage(orderCounter, `ordersConsumer#${consumerNumber}`, topic, partition, message)
            orderCounter++
        }
    })
    await paymentsConsumer.run({
        eachMessage: async({topic, partition, message}) => {
            logMessage(paymentCounter, `ordersConsumer#${consumerNumber}`, topic, partition, message)
            paymentCounter++
        }
    })
    await notificationsConsumer.run({
        eachMessage: async({topic, partition, message}) => {
            logMessage(notificationCounter, `ordersConsumer#${consumerNumber}`, topic, partition, message)
            notificationCounter++
        }
    })
}

const logMessage = (counter, consumerName, topic, partition, message) => {
    console.log(`received a new message number: ${counter} on ${consumerName}: `, {
        topic,
        partition,
        message: {
            offset: message.offset,
            headers: message.headers,
            value: message.value.toString()
        }
    })
}

processConsumer();

const msg = JSON.stringify({customerId: 1, orderId: 1});
const processProducer  = async () => {
    const producer = kafka.producer();
    await producer.connect();
    for (let i = 0; i < 3; i++) {
        await producer.send({
            topic: topicName,
            messages: [
                { value: msg },
            ],
        });
    }
};

processProducer().then(() => {
    console.log('done');
});