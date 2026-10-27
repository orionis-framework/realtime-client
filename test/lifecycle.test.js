import assert from "node:assert/strict";
import { setImmediate as flush } from "node:timers/promises";
import { test } from "node:test";
import { LimitError, TimeoutError } from "@orionis-framework/realtime-client";
import { deferred, FakeWebSocket, fixture, waitFor } from "./helpers/fake-websocket.js";

test("backpressure queues calls and resumes sending when the transport drains", async context => {
    const { client, connect } = fixture(context, { maxBufferedAmount: 1 });
    const socket = await connect();
    socket.bufferedAmount = 2;
    const pending = client.invoke("queued");
    assert.deepEqual(socket.sent, []);
    socket.bufferedAmount = 0;
    await waitFor(() => socket.sent.length === 1);
    socket.receive({ type: "completion", id: socket.sent[0].id, result: "delivered" });
    assert.equal(await pending, "delivered");
});

test("an exhausted outgoing queue rejects only the new operation", async context => {
    const { client, connect } = fixture(context, { maxBufferedAmount: 0, maxQueuedBytes: 128 });
    const socket = await connect();
    socket.bufferedAmount = 1;
    const first = client.invoke("first", ["x".repeat(32)]);
    await assert.rejects(client.invoke("second", ["x".repeat(32)]), LimitError);
    assert.equal(client.state, "connected");
    socket.bufferedAmount = 0;
    await waitFor(() => socket.sent.length === 1);
    socket.receive({ type: "completion", id: socket.sent[0].id, result: true });
    assert.equal(await first, true);
});

test("a blocked send deadline closes transport and rejects its queued calls", async context => {
    const { client, connect } = fixture(context, { maxBufferedAmount: 0, sendTimeoutMs: 10 });
    const socket = await connect();
    socket.bufferedAmount = 1;
    await assert.rejects(client.invoke("blocked"), { code: "send_timeout" });
    assert.equal(client.state, "idle");
    assert.equal(socket.closures.length, 1);
    assert.deepEqual(socket.sent, []);
});

test("priority control traffic cannot extend an older blocked send deadline", { timeout: 1500 }, async context => {
    const { client, connect } = fixture(context, { maxBufferedAmount: 0, sendTimeoutMs: 20 });
    const socket = await connect();
    socket.bufferedAmount = 1;
    const rejected = assert.rejects(client.invoke("blocked"), { code: "send_timeout" });
    const traffic = setInterval(() => socket.receive({ type: "ping" }), 1);
    context.after(() => clearInterval(traffic));
    await rejected;
    clearInterval(traffic);
    assert.equal(client.state, "idle");
    assert.deepEqual(socket.sent, []);
});

test("aborting a queued invocation removes it without sending cancellation", async context => {
    const { client, connect } = fixture(context, { maxBufferedAmount: 0, maxPendingInvocations: 1 });
    const socket = await connect();
    socket.bufferedAmount = 1;
    const controller = new AbortController();
    const rejected = assert.rejects(client.invoke("queued", [], { signal: controller.signal }), { code: "aborted" });
    controller.abort();
    await rejected;
    socket.bufferedAmount = 0;
    const next = client.invoke("replacement");
    assert.equal(socket.sent.length, 1);
    assert.equal(socket.sent[0].target, "replacement");
    socket.receive({ type: "completion", id: socket.sent[0].id, result: null });
    await next;
});

test("a synchronous transport send failure rejects calls and retires the socket", async context => {
    class BrokenSocket extends FakeWebSocket {
        send() { throw new Error("send failed"); }
    }
    const { client, connect } = fixture(context, { webSocketFactory: () => new BrokenSocket() });
    const socket = await connect();
    await assert.rejects(client.invoke("fails"), error => {
        assert.equal(error.code, "send_failed");
        assert.equal(error.cause.message, "send failed");
        return true;
    });
    assert.equal(client.state, "idle");
    assert.equal(socket.closures.length, 1);
});

test("timed-out callbacks retain admission until their application work settles", async context => {
    const { client, connect } = fixture(context, { maxConcurrentHandlers: 1, callbackTimeoutMs: 10 });
    const application = deferred();
    let firstSignal;
    let calls = 0;
    client.handle("slow", (_args, { signal }) => {
        calls++;
        if (calls === 1) {
            firstSignal = signal;
            return application.promise;
        }
        return "next";
    });
    const timedOut = deferred();
    client.onError(error => { if (error instanceof TimeoutError) timedOut.resolve(error); });
    const socket = await connect();
    socket.receive({ type: "invoke", id: "first", target: "slow", args: [] });
    await timedOut.promise;
    assert.equal(firstSignal.aborted, true);
    assert.equal(socket.sent[0].error.code, "timeout");
    socket.receive({ type: "invoke", id: "busy", target: "slow", args: [] });
    assert.equal(socket.sent[1].error.code, "busy");
    assert.equal(calls, 1);
    application.resolve("late result");
    await flush();
    socket.receive({ type: "invoke", id: "next", target: "slow", args: [] });
    await flush();
    assert.equal(calls, 2);
    assert.deepEqual(socket.sent[2], { type: "completion", id: "next", result: "next" });
    assert.equal(socket.sent.filter(message => message.id === "first").length, 1);
    assert.equal(client.state, "connected");
});

test("event callbacks share the method admission budget", async context => {
    const { client, connect } = fixture(context, { maxConcurrentHandlers: 1 });
    const application = deferred();
    let calls = 0;
    client.on("event", () => { calls++; return application.promise; });
    client.handle("method", () => true);
    const socket = await connect();
    socket.receive({ type: "send", target: "event", args: [] });
    socket.receive({ type: "invoke", id: "busy", target: "method", args: [] });
    assert.deepEqual(socket.sent[0], {
        type: "completion", id: "busy",
        error: { code: "busy", message: "Client method unavailable" },
    });
    await flush();
    assert.equal(calls, 1);
    application.resolve();
    await flush();
});

test("invalid callback results return an error while unrelated calls remain active", async context => {
    const { client, connect } = fixture(context);
    client.handle("invalid", () => ({ value: undefined }));
    const socket = await connect();
    const pending = client.invoke("independent");
    socket.receive({ type: "invoke", id: "invalid", target: "invalid", args: [] });
    await flush();
    assert.equal(socket.sent[1].error.code, "client_error");
    assert.equal(client.state, "connected");
    socket.receive({ type: "completion", id: socket.sent[0].id, result: true });
    assert.equal(await pending, true);
});

test("stop aborts an unfinished URL factory and ignores its late result", async context => {
    const address = deferred();
    let signal;
    const { client, sockets } = fixture(context, {}, factoryContext => {
        signal = factoryContext.signal;
        return address.promise;
    });
    const rejected = assert.rejects(client.connect(), { code: "stopped" });
    await flush();
    await client.stop();
    await rejected;
    assert.equal(signal.aborted, true);
    address.resolve("wss://example.com/obsolete");
    await flush();
    assert.equal(client.state, "idle");
    assert.deepEqual(sockets, []);
});

test("a late URL factory cannot replace a newer connection generation", async context => {
    const oldAddress = deferred();
    let calls = 0;
    const { client, sockets, nextSocket } = fixture(context, {}, () => {
        calls++;
        return calls === 1 ? oldAddress.promise : "wss://example.com/current";
    });
    const rejected = assert.rejects(client.connect(), { code: "stopped" });
    await flush();
    await client.stop();
    await rejected;
    const connecting = client.connect();
    const socket = await nextSocket();
    socket.open();
    socket.receive({ type: "ready", version: 1, connection_id: "current" });
    await connecting;
    oldAddress.resolve("wss://example.com/obsolete");
    await flush();
    assert.equal(client.connectionId, "current");
    assert.equal(socket.address, "wss://example.com/current");
    assert.equal(sockets.length, 1);
});

test("transport error followed by policy closure preserves the no-retry policy", async context => {
    const { client, connect, sockets } = fixture(context, {
        reconnect: { enabled: true, initialDelayMs: 0, maxDelayMs: 1 },
    });
    const socket = await connect();
    socket.fail();
    await assert.rejects(client.invoke("closing"), { code: "not_connected" });
    socket.close(4001);
    await flush();
    assert.equal(client.state, "idle");
    assert.equal(sockets.length, 1);
});

test("retired socket messages cannot complete operations on a new connection", async context => {
    const { client, connect } = fixture(context);
    const oldSocket = await connect();
    await client.stop();
    const currentSocket = await connect();
    const pending = client.invoke("current");
    const id = currentSocket.sent[0].id;
    oldSocket.receive({ type: "completion", id, result: "obsolete" });
    currentSocket.receive({ type: "completion", id, result: "current" });
    assert.equal(await pending, "current");
    assert.equal(client.connectionId, "connection-2");
});
