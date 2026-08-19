import { Kafka } from "kafkajs";

export const kafkaClient = new Kafka({
    clientId: "backend",
    brokers: ["localhost:9092"],
});
