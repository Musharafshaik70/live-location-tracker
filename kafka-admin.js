import { kafkaClient } from "./kafka-client.js";

async function setUp() {
    const admin = kafkaClient.admin();

    await admin.connect();
    console.log("kafka admin connected");

    await admin.createTopics({
        topics: [{ topic: "location-updates", numPartitions: 2 }],
    });
    console.log("kafka topics created");

    await admin.disconnect();
    console.log("kafka admin disconnected");
}

setUp();
