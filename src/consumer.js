import { Kafka } from "kafkajs";
import dotenv from "dotenv";

dotenv.config();

const kafka = new Kafka({
    clientId: "consumer-app",
    brokers: [process.env.HOST_IP || "localhost:19092"]
});

const topic = process.env.TOPIC || "mon-super-topic";
const groupId = "consumer-group-1";

const consumer = kafka.consumer({ groupId });

const formatTimestamp = (timestamp) => {
    const date = new Date(Number(timestamp));
    return date.toLocaleString("fr-FR", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit"
    });
};

const run = async () => {
    await consumer.connect();
    console.log("Connecté au broker Kafka");
    await consumer.subscribe({ topic, fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(` Nouveau message sur ${topic} (partition ${partition}):`);
            console.log(` ${formatTimestamp(message.timestamp)}`);
            console.log(` User: ${message.key ? message.key.toString() : "N/A"}`);
            console.log(` Message: ${message.value.toString()}`);
            console.log("FIN DU MESSAGE TEST");
        }
    });
};

run().catch(console.error);
