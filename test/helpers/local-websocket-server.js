import { createHash } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_FRAME_BYTES = 1024 * 1024;

function queue() {
    const items = [];
    const waiting = [];
    let failure;
    return {
        push(value) {
            const waiter = waiting.shift();
            if (waiter) waiter.resolve(value);
            else items.push(value);
        },
        fail(error) {
            failure ??= error;
            for (const waiter of waiting.splice(0)) waiter.reject(failure);
        },
        next() {
            if (items.length) return Promise.resolve(items.shift());
            if (failure) return Promise.reject(failure);
            return new Promise((resolve, reject) => waiting.push({ resolve, reject }));
        },
    };
}

function frame(opcode, payload) {
    const header = Buffer.alloc(payload.length < 126 ? 2 : 4);
    header[0] = 0x80 | opcode;
    header[1] = payload.length < 126 ? payload.length : 126;
    if (payload.length >= 126) header.writeUInt16BE(payload.length, 2);
    return Buffer.concat([header, payload]);
}

// A deliberately small test peer, not a general-purpose WebSocket server. The
// accumulator handles arbitrary TCP chunks and multiple frames in one chunk.
function peer(socket, request) {
    const messages = queue();
    const closed = once(socket, "close");
    let pending = Buffer.alloc(0);
    let fragments = [];
    let fragmentBytes = 0;
    let closeFrame;
    const acceptFrame = (opcode, final, payload) => {
        if (opcode === 8) {
            closeFrame = {
                code: payload.length >= 2 ? payload.readUInt16BE(0) : null,
                reason: payload.subarray(2).toString("utf8"),
            };
            socket.end(frame(8, payload));
        } else if (opcode === 9) {
            socket.write(frame(10, payload));
        } else if (opcode === 1 || opcode === 0) {
            if ((opcode === 1 && fragments.length) || (opcode === 0 && !fragments.length)) {
                throw new Error("Unexpected WebSocket continuation frame");
            }
            fragments.push(payload);
            fragmentBytes += payload.length;
            if (fragmentBytes > MAX_FRAME_BYTES) throw new Error("Test message exceeds byte limit");
            if (final) {
                messages.push(JSON.parse(Buffer.concat(fragments).toString("utf8")));
                fragments = [];
                fragmentBytes = 0;
            }
        } else if (opcode !== 10) {
            throw new Error("Unsupported test WebSocket opcode: " + opcode);
        }
    };
    const receive = chunk => {
        try {
            pending = Buffer.concat([pending, chunk]);
            while (pending.length >= 2) {
                const first = pending[0];
                const second = pending[1];
                if ((first & 0x70) || !(second & 0x80)) {
                    throw new Error("Expected an unextended, masked client frame");
                }
                let length = second & 0x7f;
                let offset = 2;
                if (length === 126) {
                    if (pending.length < 4) return;
                    length = pending.readUInt16BE(2);
                    offset = 4;
                } else if (length === 127) {
                    if (pending.length < 10) return;
                    const wideLength = pending.readBigUInt64BE(2);
                    if (wideLength > BigInt(MAX_FRAME_BYTES)) throw new Error("Test frame exceeds byte limit");
                    length = Number(wideLength);
                    offset = 10;
                }
                if (length > MAX_FRAME_BYTES) throw new Error("Test frame exceeds byte limit");
                if (pending.length < offset + 4 + length) return;
                const mask = pending.subarray(offset, offset + 4);
                const payload = Buffer.from(pending.subarray(offset + 4, offset + 4 + length));
                for (let index = 0; index < payload.length; index++) payload[index] ^= mask[index % 4];
                pending = pending.subarray(offset + 4 + length);
                acceptFrame(first & 0x0f, Boolean(first & 0x80), payload);
            }
        } catch (error) {
            messages.fail(error);
            socket.destroy();
        }
    };
    socket.on("data", receive);
    socket.on("error", error => messages.fail(error));
    socket.on("close", () => messages.fail(new Error("Test WebSocket connection closed")));
    return {
        request,
        closed,
        get closeFrame() { return closeFrame; },
        nextMessage: () => messages.next(),
        send(message) {
            const payload = Buffer.from(JSON.stringify(message));
            if (payload.length > 65535) throw new Error("Test server sends frames up to 65535 bytes");
            socket.write(frame(1, payload));
        },
        receive,
    };
}

export async function localWebSocketServer(context) {
    const connections = queue();
    const sockets = new Set();
    const server = createServer((_request, response) => response.writeHead(404).end());
    server.on("connection", socket => {
        sockets.add(socket);
        socket.on("close", () => sockets.delete(socket));
    });
    server.on("upgrade", (request, socket, head) => {
        const key = request.headers["sec-websocket-key"];
        if (typeof key !== "string" || request.headers["sec-websocket-version"] !== "13") {
            socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
            return;
        }
        const accept = createHash("sha1").update(key + GUID).digest("base64");
        socket.write([
            "HTTP/1.1 101 Switching Protocols", "Upgrade: websocket", "Connection: Upgrade",
            "Sec-WebSocket-Accept: " + accept, "", "",
        ].join("\r\n"));
        const connection = peer(socket, request);
        connections.push(connection);
        if (head.length) connection.receive(head);
    });
    server.on("error", error => connections.fail(error));
    context.after(async () => {
        for (const socket of sockets) socket.destroy();
        if (server.listening) await new Promise((resolve, reject) => {
            server.close(error => error ? reject(error) : resolve());
        });
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    return {
        url: `ws://127.0.0.1:${server.address().port}/realtime`,
        nextConnection: () => connections.next(),
    };
}
