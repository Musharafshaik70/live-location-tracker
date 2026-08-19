import http from "node:http";
import path from "node:path";

import express from "express";
import { Server } from "socket.io";

import { kafkaClient } from "./kafka-client.js";

async function main() {
    const PORT = process.env.PORT ?? 9000;

    const app = express();
    const httpServer = http.createServer(app);
    const io = new Server();

    //connect kafka producer
    const producer = kafkaClient.producer();
    await producer.connect();

    //connect kafka consumer
    const consumer = kafkaClient.consumer({ groupId: `socket-server-${PORT}` });
    await consumer.connect();

    //consumer subscribes to a topic to recieve messages.
    await consumer.subscribe({
        topics: ["location-updates"],
        fromBeginning: true,
    });

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat }) => {
            const data = JSON.parse(message.value.toString());
            console.log("Kafka consumer data received", { data });
            const { id, latitude, longitude } = data;
            io.emit("server:location:update", { id, latitude, longitude });
            await heartbeat();
        },
    });

    io.attach(httpServer);
    //socket.io handlers
    io.on("connection", (socket) => {
        console.log(`Socket - ${socket.id} connected`);

        socket.on("client:location:update", async (locationData) => {
            console.log(`Socket - ${socket.id} is at ${locationData}`);

            const [latitude, longitude] = locationData;
            //producer pushes message to specific topic in kafka where key helps in partial routing based on its hash.
            await producer.send({
                topic: "location-updates",
                messages: [{ key: socket.id, value: JSON.stringify({ id: socket.id, latitude, longitude }) }],
            });
        });
    });

    //express handlers
    app.use(express.static(path.resolve("./public")));
    app.get("/health", (req, res) => {
        return res.status(200).json({ healthy: true });
    });

    httpServer.listen(PORT, () => {
        console.log(`Server is running on http://localhost:${PORT}`);
    });
}

main();
