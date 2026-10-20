import assert from "node:assert/strict";
import { setImmediate as flush } from "node:timers/promises";
import { test } from "node:test";
import * as realtime from "orionis-realtime-client";

const {
    AbortError, ConnectionError, LimitError, ProtocolError, RealtimeClient,
    RealtimeError, RemoteError, TimeoutError,
} = realtime;

class TestSocket extends EventTarget {
    readyState = 0;
    bufferedAmount = 0;
    sent = [];
    closures = [];

    open() { this.readyState = 1; }

    receive(message) {
        const data = typeof message === "string" ? message : JSON.stringify(message);
        this.dispatchEvent(Object.assign(new Event("message"), { data }));
    }

    send(data) {
        assert.equal(this.readyState, 1);
        this.sent.push(JSON.parse(data));
    }

    close(code = 1000, reason = "") {
        if (this.readyState === 3) return;
        this.readyState = 3;
        this.closures.push({ code, reason });
        this.dispatchEvent(Object.assign(new Event("close"), { code, reason }));
    }
}

function fixture(context, options = {}, url = "https://example.com/realtime") {
    const sockets = [];
    const available = [];
    const waiting = [];
    const client = new RealtimeClient(url, {
        heartbeatIntervalMs: 0,
        reconnect: { enabled: false },
        ...options,
        webSocketFactory(address, protocols) {
            const socket = new TestSocket();
            socket.address = address;
            socket.protocols = protocols;
            sockets.push(socket);
            const resolve = waiting.shift();
            if (resolve) resolve(socket);
            else available.push(socket);
            return socket;
        },
    });
    context.after(() => client.dispose());
    const nextSocket = () => available.length ? Promise.resolve(available.shift()) :
        new Promise(resolve => waiting.push(resolve));
    const connect = async () => {
        const connecting = client.connect();
        const socket = await nextSocket();
        socket.open();
        socket.receive({ type: "ready", version: 1, connection_id: "connection-" + sockets.length });
        await connecting;
        return socket;
    };
    return { client, sockets, nextSocket, connect };
}

test("the package exports the documented client and error classes", () => {
    assert.deepEqual(Object.keys(realtime).sort(), [
        "AbortError", "ConnectionError", "LimitError", "ProtocolError", "RealtimeClient",
        "RealtimeError", "RemoteError", "TimeoutError",
    ]);
    for (const [ErrorType, code] of [
        [RealtimeError, "realtime_error"], [ConnectionError, "connection_lost"],
        [ProtocolError, "protocol_error"], [TimeoutError, "timeout"],
        [AbortError, "aborted"], [LimitError, "limit_exceeded"],
    ]) {
        const cause = new Error("original failure");
        const error = new ErrorType("message", { cause });
        assert.ok(error instanceof RealtimeError);
        assert.equal(error.name, ErrorType.name);
        assert.equal(error.code, code);
        assert.equal(error.cause, cause);
    }
});

test("construction is lazy and invalid options fail early", async context => {
    const { client, sockets } = fixture(context);
    assert.equal(client.state, "idle");
    assert.equal(client.connectionId, null);
    assert.equal(sockets.length, 0);
    for (const options of [
        { unknown: true }, { connectTimeoutMs: 0 }, { maxPendingInvocations: 1.5 },
        { protocols: ["duplicate", "duplicate"] }, { protocols: ["has spaces"] },
        { reconnect: { unknown: true } }, { reconnect: { jitter: 2 } },
        { connectTimeoutMs: 2147483648 },
    ]) {
        assert.throws(() => new RealtimeClient("ws://example.com", options), TypeError);
    }
    await assert.rejects(client.invoke("method"), { code: "not_connected" });
    assert.throws(() => client.stream("method"), { code: "not_connected" });
});

test("connect is idempotent and waits for ready rather than socket open", async context => {
    const { client, nextSocket } = fixture(context, { protocols: ["orionis"] });
    const connecting = client.connect();
    assert.equal(client.connect(), connecting);
    let resolved = false;
    connecting.then(() => { resolved = true; });
    const socket = await nextSocket();
    assert.equal(socket.address, "wss://example.com/realtime");
    assert.deepEqual(socket.protocols, ["orionis"]);
    socket.open();
    await flush();
    assert.equal(resolved, false);
    socket.receive({ type: "ready", version: 1, connection_id: "server-id" });
    await connecting;
    assert.equal(client.state, "connected");
    assert.equal(client.connectionId, "server-id");
});

test("invocations copy arguments and correlate out-of-order responses", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    const args = [{ text: "hello" }];
    const first = client.invoke("publish", args, { kwargs: { room: "lobby" }, serverTimeout: 5 });
    args[0].text = "changed";
    const second = client.invoke("second");
    const envelope = socket.sent[0];
    assert.deepEqual(envelope, {
        type: "invoke", id: envelope.id, target: "publish", args: [{ text: "hello" }],
        kwargs: { room: "lobby" }, timeout: 5,
    });
    assert.notEqual(envelope.id, socket.sent[1].id);
    socket.receive({ type: "completion", id: socket.sent[1].id, result: 2 });
    socket.receive({ type: "completion", id: envelope.id, result: { delivered: true } });
    assert.deepEqual(await Promise.all([first, second]), [{ delivered: true }, 2]);
});

test("remote errors preserve the server code and message", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    const result = client.invoke("denied");
    socket.receive({
        type: "completion", id: socket.sent[0].id,
        error: { code: "forbidden", message: "Access denied" },
    });
    await assert.rejects(result, error => {
        assert.ok(error instanceof RemoteError);
        assert.equal(error.code, "forbidden");
        assert.equal(error.message, "Access denied");
        return true;
    });
});

test("event handlers can unsubscribe without further delivery", async context => {
    const { client, connect } = fixture(context);
    const messages = [];
    const unsubscribe = client.on("message", args => { messages.push(args); });
    const socket = await connect();
    socket.receive({ type: "send", target: "message", args: ["hello"] });
    await flush();
    unsubscribe();
    unsubscribe();
    socket.receive({ type: "send", target: "message", args: ["ignored"] });
    await flush();
    assert.deepEqual(messages, [["hello"]]);
});

test("server callbacks receive context and return completions", async context => {
    const { client, connect } = fixture(context);
    client.handle("confirm", (args, handlerContext) => {
        assert.deepEqual(args, ["question"]);
        assert.equal(handlerContext.connectionId, "connection-1");
        assert.equal(handlerContext.signal.aborted, false);
        return true;
    });
    assert.throws(() => client.handle("confirm", () => false), TypeError);
    const socket = await connect();
    socket.receive({ type: "invoke", id: "server-call", target: "confirm", args: ["question"] });
    await flush();
    assert.deepEqual(socket.sent, [{ type: "completion", id: "server-call", result: true }]);
    socket.receive({ type: "invoke", id: "missing", target: "unknown", args: [] });
    assert.equal(socket.sent[1].error.code, "method_not_found");
});

test("callback failures are isolated from the transport", async context => {
    const { client, connect } = fixture(context);
    const errors = [];
    client.onError(error => { errors.push(error); });
    client.handle("fail", () => { throw new Error("application failure"); });
    const socket = await connect();
    socket.receive({ type: "invoke", id: "server-call", target: "fail", args: [] });
    await flush();
    assert.equal(client.state, "connected");
    assert.equal(errors[0].code, "callback_error");
    assert.equal(socket.sent[0].error.code, "client_error");
});

test("streams deliver buffered items and complete pending reads", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    const stream = client.stream("updates");
    const id = socket.sent[0].id;
    socket.receive({ type: "stream_item", id, item: 1 });
    socket.receive({ type: "stream_item", id, item: 2 });
    assert.deepEqual(await stream.next(), { value: 1, done: false });
    assert.deepEqual(await stream.next(), { value: 2, done: false });
    const pending = stream.next();
    await assert.rejects(stream.next(), { code: "concurrent_next" });
    socket.receive({ type: "stream_complete", id });
    assert.deepEqual(await pending, { value: undefined, done: true });
});

test("breaking stream consumption sends cancellation", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    const stream = client.stream("updates");
    const id = socket.sent[0].id;
    socket.receive({ type: "stream_item", id, item: "first" });
    for await (const item of stream) {
        assert.equal(item, "first");
        break;
    }
    assert.deepEqual(socket.sent[1], { type: "cancel", id });
    assert.deepEqual(await stream.next(), { value: undefined, done: true });
});

test("AbortSignal cancellation retains the reason locally", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    const controller = new AbortController();
    const result = client.invoke("wait", [], { signal: controller.signal });
    controller.abort("cancelled by caller");
    await assert.rejects(result, error => {
        assert.ok(error instanceof AbortError);
        assert.equal(error.cause, "cancelled by caller");
        return true;
    });
    assert.deepEqual(socket.sent[1], { type: "cancel", id: socket.sent[0].id });
});

test("invocation deadlines reject and cancel the remote operation", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    await assert.rejects(client.invoke("wait", [], { timeoutMs: 5 }), TimeoutError);
    assert.deepEqual(socket.sent[1], { type: "cancel", id: socket.sent[0].id });
    assert.equal(client.state, "connected");
});

test("invalid JSON values are rejected without running getters", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    const cyclic = {};
    cyclic.self = cyclic;
    let getterCalled = false;
    const accessor = Object.defineProperty({}, "value", {
        enumerable: true,
        get() { getterCalled = true; return 1; },
    });
    for (const value of [undefined, NaN, Infinity, 1n, new Date(), cyclic, accessor, Array(2)]) {
        await assert.rejects(client.invoke("invalid", [value]), TypeError);
    }
    assert.equal(getterCalled, false);
    assert.equal(socket.sent.length, 0);
});

test("special JSON keys remain inert data", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    const payload = JSON.parse('{"__proto__":{"polluted":true},"constructor":"data"}');
    const pending = client.invoke("echo", [payload]);
    assert.deepEqual(socket.sent[0].args[0], payload);
    socket.receive({ type: "completion", id: socket.sent[0].id, result: payload });
    assert.deepEqual(await pending, payload);
    assert.equal({}.polluted, undefined);
});

test("pending invocation budgets release slots after completion", async context => {
    const { client, connect } = fixture(context, { maxPendingInvocations: 1 });
    const socket = await connect();
    const first = client.invoke("first");
    await assert.rejects(client.invoke("second"), LimitError);
    socket.receive({ type: "completion", id: socket.sent[0].id, result: null });
    await first;
    const next = client.invoke("next");
    socket.receive({ type: "completion", id: socket.sent[1].id, result: null });
    await next;
});

test("stream buffer overflow cancels only the affected stream", async context => {
    const { client, connect } = fixture(context, { maxStreamBufferItems: 1 });
    const socket = await connect();
    const stream = client.stream("updates");
    const id = socket.sent[0].id;
    socket.receive({ type: "stream_item", id, item: 1 });
    socket.receive({ type: "stream_item", id, item: 2 });
    await assert.rejects(stream.next(), LimitError);
    assert.deepEqual(socket.sent[1], { type: "cancel", id });
    assert.equal(client.state, "connected");
});

test("malformed protocol data closes with 4002 and rejects pending calls", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    const rejected = assert.rejects(client.invoke("wait"), ProtocolError);
    socket.receive({ type: "completion", id: socket.sent[0].id, result: null, error: {} });
    await rejected;
    assert.equal(socket.closures[0].code, 4002);
    assert.equal(client.state, "idle");
});

test("incoming frame byte limits are enforced", async context => {
    const { client, connect } = fixture(context, { maxMessageBytes: 128 });
    const errors = [];
    client.onError(error => { errors.push(error); });
    const socket = await connect();
    socket.receive(JSON.stringify("x".repeat(129)));
    assert.ok(errors[0] instanceof LimitError);
    assert.equal(socket.closures[0].code, 4002);
});

test("server ping is answered with pong", async context => {
    const { client, connect } = fixture(context);
    const socket = await connect();
    socket.receive({ type: "ping" });
    socket.receive({ type: "pong" });
    assert.deepEqual(socket.sent, [{ type: "pong" }]);
    assert.equal(client.state, "connected");
});

test("missing heartbeat responses terminate the connection", async context => {
    const { client, connect } = fixture(context, { heartbeatIntervalMs: 1, heartbeatTimeoutMs: 5 });
    const failure = new Promise(resolve => client.onError(resolve));
    const socket = await connect();
    assert.equal((await failure).code, "heartbeat_timeout");
    assert.deepEqual(socket.sent, [{ type: "ping" }]);
});

test("handshake timeout applies after the socket opens", async context => {
    const { client, nextSocket } = fixture(context, { connectTimeoutMs: 5 });
    const rejected = assert.rejects(client.connect(), { code: "connect_timeout" });
    (await nextSocket()).open();
    await rejected;
    assert.equal(client.state, "idle");
});

test("invalid URLs fail before transport creation and retain their cause", async context => {
    const { client, sockets } = fixture(context, {}, "wss://user:password@example.com/hub");
    await assert.rejects(client.connect(), error => {
        assert.equal(error.code, "connect_failed");
        assert.ok(error.cause instanceof TypeError);
        return true;
    });
    assert.equal(sockets.length, 0);
});

test("reconnection restores transport without replaying failed operations", async context => {
    const { client, sockets, connect, nextSocket } = fixture(context, {
        reconnect: { enabled: true, maxAttempts: 1, initialDelayMs: 0, maxDelayMs: 1, jitter: 0 },
    });
    const first = await connect();
    const rejected = assert.rejects(client.invoke("mutation"), { code: "connection_lost" });
    first.close(1006);
    await rejected;
    assert.equal(client.state, "reconnecting");
    const reconnecting = client.connect();
    const second = await nextSocket();
    second.open();
    second.receive({ type: "ready", version: 1, connection_id: "replacement" });
    await reconnecting;
    assert.equal(client.connectionId, "replacement");
    assert.equal(sockets.length, 2);
    assert.deepEqual(second.sent, []);
});

test("normal closure does not reconnect automatically", async context => {
    const { client, sockets, connect } = fixture(context, { reconnect: { enabled: true } });
    const socket = await connect();
    socket.close(1000);
    await flush();
    assert.equal(client.state, "idle");
    assert.equal(sockets.length, 1);
});

test("stop is restartable while dispose is permanent", async context => {
    const { client, connect } = fixture(context);
    await connect();
    await client.stop();
    assert.equal(client.state, "idle");
    assert.equal(client.connectionId, null);
    await connect();
    await client.dispose();
    assert.equal(client.state, "disposed");
    await assert.rejects(client.connect(), { code: "disposed" });
    await assert.rejects(client.invoke("method"), { code: "disposed" });
    assert.throws(() => client.on("event", () => {}), { code: "disposed" });
});
