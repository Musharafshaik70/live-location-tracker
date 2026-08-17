import http from "node:http";
import path from "node:path";

import express from "express";
import { Server } from "socket.io";

async function main() {
    const PORT = process.env.PORT ?? 9000;

    const app = express();
    const httpServer = http.createServer(app);
    const io = new Server();

    io.attach(httpServer);
    //socket.io handlers
    io.on("connection", (socket) => {
        console.log(`Socket - ${socket.id} connected`);
        socket.on("client:location:update", (data) => {
            console.log(`Socket - ${socket.id} is at ${data}`);
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
