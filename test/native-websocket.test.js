import assert from "node:assert/strict";
import { test } from "node:test";
import { RealtimeClient } from "@orionis-framework/realtime-client";
import { localWebSocketServer } from "./helpers/local-websocket-server.js";

async function connectedClient(context) {
    const server = await localWebSocketServer(context);
    const client = new RealtimeClient(server.url, {
        heartbeatIntervalMs: 0,
        connectTimeoutMs: 2000,
        invocationTimeoutMs: 2000,
        closeTimeoutMs: 500,
        reconnect: { enabled: false },
    });
    context.after(() => client.dispose());
    const connecting = client.connect();
    const peer = await server.nextConnection();
    peer.send({ type: "ready", version: 1, connection_id: "native-node-connection" });
    await connecting;
    return { client, peer };
}

test("native Node WebSocket exchanges calls, events and callbacks over a real TCP transport", {
    timeout: 5000,
}, async context => {
    const { client, peer } = await connectedClient(context);
    const errors = [];
    client.onError(error => errors.push(error));
    assert.equal(peer.request.url, "/realtime");
    assert.equal(client.state, "connected");
    assert.equal(client.connectionId, "native-node-connection");

    // Non-ASCII data and a frame larger than 125 bytes exercise UTF-8 and the
    // native client's masked 16-bit-length frames, not just a mocked transport.
    const payload = "¡Conexión real! 😀 ".repeat(30);
    const result = client.invoke("echo", [payload], { kwargs: { locale: "es" } });
    const request = await peer.nextMessage();
    assert.deepEqual(request, {
        type: "invoke", id: request.id, target: "echo", args: [payload], kwargs: { locale: "es" },
    });
    peer.send({ type: "completion", id: request.id, result: { echoed: payload } });
    assert.deepEqual(await result, { echoed: payload });

    const event = new Promise(resolve => client.on("notification", resolve));
    peer.send({ type: "send", target: "notification", args: [{ delivered: true }] });
    assert.deepEqual(await event, [{ delivered: true }]);

    client.handle("confirm", ([question], handlerContext) => {
        assert.equal(question, "Continue?");
        assert.equal(handlerContext.connectionId, "native-node-connection");
        assert.equal(handlerContext.signal.aborted, false);
        return { confirmed: true };
    });
    peer.send({ type: "invoke", id: "server-call", target: "confirm", args: ["Continue?"] });
    assert.deepEqual(await peer.nextMessage(), {
        type: "completion", id: "server-call", result: { confirmed: true },
    });

    await client.dispose();
    await peer.closed;
    assert.equal(client.state, "disposed");
    assert.equal(peer.closeFrame.code, 1000);
    assert.deepEqual(errors, []);
});

test("native Node WebSocket consumes a stream and sends cancellation on early return", {
    timeout: 5000,
}, async context => {
    const { client, peer } = await connectedClient(context);
    const stream = client.stream("updates", [2]);
    const request = await peer.nextMessage();
    assert.equal(request.type, "invoke");
    assert.equal(request.target, "updates");
    peer.send({ type: "stream_item", id: request.id, item: { index: 0 } });
    peer.send({ type: "stream_item", id: request.id, item: { index: 1 } });
    peer.send({ type: "stream_complete", id: request.id });
    const received = [];
    for await (const item of stream) received.push(item);
    assert.deepEqual(received, [{ index: 0 }, { index: 1 }]);

    const canceled = client.stream("updates");
    const second = await peer.nextMessage();
    peer.send({ type: "stream_item", id: second.id, item: "first" });
    for await (const item of canceled) {
        assert.equal(item, "first");
        break;
    }
    assert.deepEqual(await peer.nextMessage(), { type: "cancel", id: second.id });
    assert.deepEqual(await canceled.next(), { value: undefined, done: true });

    await client.stop();
    await peer.closed;
    assert.equal(client.state, "idle");
    assert.equal(peer.closeFrame.code, 1000);
});
