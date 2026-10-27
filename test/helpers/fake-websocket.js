import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { RealtimeClient } from "@orionis-framework/realtime-client";

export function deferred() {
    let resolve;
    let reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}

/** Controllable EventTarget transport; close is immediate unless overridden. */
export class FakeWebSocket extends EventTarget {
    readyState = 0;
    bufferedAmount = 0;
    sent = [];
    closures = [];

    open() { this.readyState = 1; }

    receive(message) {
        const data = typeof message === "string" ? message : JSON.stringify(message);
        this.dispatchEvent(Object.assign(new Event("message"), { data }));
    }

    fail() { this.dispatchEvent(new Event("error")); }

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

export function fixture(context, options = {}, url = "https://example.com/realtime") {
    const sockets = [];
    const available = [];
    const waiting = [];
    const createSocket = options.webSocketFactory ?? (() => new FakeWebSocket());
    const client = new RealtimeClient(url, {
        heartbeatIntervalMs: 0,
        reconnect: { enabled: false },
        ...options,
        webSocketFactory(address, protocols) {
            const socket = createSocket(address, protocols);
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

export async function waitFor(predicate) {
    const deadline = Date.now() + 1000;
    while (!predicate()) {
        assert.ok(Date.now() < deadline, "The expected transport activity did not occur");
        await delay(5);
    }
}
