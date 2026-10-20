/**
 * @license
 * # MIT License
 *
 * Copyright (c) 2023 Raul Mauricio Uñate Castro
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights to:
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * 1. The above copyright notice and this permission notice shall be included in all
 *     copies or substantial portions of the Software.
 *
 * 2. The Software is provided "as is", without warranty of any kind, express or implied,
 *     including but not limited to the warranties of merchantability, fitness for a
 *     particular purpose, and noninfringement. In no event shall the authors or copyright
 *     holders be liable for any claim, damages, or other liability, whether in an action
 *     of contract, tort, or otherwise, arising from, out of, or in connection with the
 *     Software or the use or other dealings in the Software.
 */

export class RealtimeError extends Error {
    constructor(message = "Realtime operation failed", options = {}) {
        super(message, options.cause === undefined ? undefined : { cause: options.cause });
        this.name = new.target.name;
        this.code = options.code ?? "realtime_error";
    }
}

export class ConnectionError extends RealtimeError {
    constructor(message = "Realtime connection lost", options = {}) {
        super(message, { ...options, code: options.code ?? "connection_lost" });
    }
}

export class ProtocolError extends RealtimeError {
    constructor(message = "Invalid Realtime protocol message", options = {}) {
        super(message, { ...options, code: options.code ?? "protocol_error" });
    }
}

export class RemoteError extends RealtimeError {
    constructor(code, message) {
        super(message, { code });
    }
}

export class TimeoutError extends RealtimeError {
    constructor(message = "Realtime operation timed out", options = {}) {
        super(message, { ...options, code: options.code ?? "timeout" });
    }
}

export class AbortError extends RealtimeError {
    constructor(message = "Realtime operation aborted", options = {}) {
        super(message, { ...options, code: options.code ?? "aborted" });
    }
}

export class LimitError extends RealtimeError {
    constructor(message = "Realtime resource limit exceeded", options = {}) {
        super(message, { ...options, code: options.code ?? "limit_exceeded" });
    }
}

const DEFAULTS = Object.freeze({
    protocols: [],
    connectTimeoutMs: 10000,
    closeTimeoutMs: 1000,
    invocationTimeoutMs: 30000,
    callbackTimeoutMs: 30000,
    heartbeatIntervalMs: 30000,
    heartbeatTimeoutMs: 10000,
    sendTimeoutMs: 10000,
    maxPendingInvocations: 128,
    maxConcurrentHandlers: 32,
    maxMessageBytes: 1048576,
    maxQueuedBytes: 2097152,
    maxBufferedAmount: 262144,
    maxStreamBufferItems: 64,
    maxStreamBufferBytes: 1048576,
    maxJsonDepth: 64,
    maxJsonNodes: 100000,
});
const RECONNECT_DEFAULTS = Object.freeze({
    enabled: true,
    maxAttempts: 10,
    initialDelayMs: 500,
    maxDelayMs: 30000,
    factor: 2,
    jitter: 0.2,
});
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const encoder = new TextEncoder();
const MAX_SUBSCRIPTIONS = 1024;
const POLL_MS = 10;
const MAX_TIMER_MS = 2147483647;
const RETRYABLE_CLOSE_CODES = new Set([1001, 1006, 1011, 1012, 1013, 1014]);
const MAX_URL_FACTORIES = 32;

function plain(value) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
}

function settingsObject(value, label) {
    if (!plain(value)) throw new TypeError(label + " must be a plain object");
    for (const key of Reflect.ownKeys(value)) {
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (typeof key !== "string" || !descriptor || !own(descriptor, "value")) {
            throw new TypeError(label + " must contain data properties only");
        }
    }
    return value;
}

function numberOption(value, label, minimum = 1, integer = true) {
    if (typeof value !== "number" || !Number.isFinite(value) || value < minimum ||
        (integer && !Number.isSafeInteger(value))) {
        throw new TypeError(label + " must be a finite " + (integer ? "integer" : "number") +
            " of at least " + minimum);
    }
    return value;
}

function optionsFor(input) {
    settingsObject(input, "options");
    for (const key of Object.keys(input)) {
        if (!own(DEFAULTS, key) && key !== "reconnect" && key !== "webSocketFactory") {
            throw new TypeError("Unknown Realtime option: " + key);
        }
    }
    const result = { ...DEFAULTS, ...input };
    for (const key of Object.keys(DEFAULTS)) {
        if (key === "protocols") continue;
        const allowZero = ["invocationTimeoutMs", "heartbeatIntervalMs", "maxBufferedAmount"].includes(key);
        numberOption(result[key], key, allowZero ? 0 : 1);
        if (key.endsWith("Ms") && result[key] > MAX_TIMER_MS) {
            throw new TypeError(key + " exceeds the native timer range");
        }
    }
    if (!Array.isArray(result.protocols) || result.protocols.length > 64 ||
        result.protocols.some(value => typeof value !== "string" || !value ||
            !/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(value)) ||
        new Set(result.protocols).size !== result.protocols.length) {
        throw new TypeError("protocols must be an array of unique WebSocket protocol tokens");
    }
    result.protocols = Object.freeze([...result.protocols]);
    const reconnect = input.reconnect ?? {};
    settingsObject(reconnect, "reconnect");
    for (const key of Object.keys(reconnect)) {
        if (!own(RECONNECT_DEFAULTS, key)) throw new TypeError("Unknown reconnect option: " + key);
    }
    result.reconnect = { ...RECONNECT_DEFAULTS, ...reconnect };
    if (typeof result.reconnect.enabled !== "boolean") {
        throw new TypeError("reconnect.enabled must be boolean");
    }
    for (const key of ["maxAttempts", "initialDelayMs", "maxDelayMs"]) {
        numberOption(result.reconnect[key], "reconnect." + key, key === "maxDelayMs" ? 1 : 0);
    }
    numberOption(result.reconnect.factor, "reconnect.factor", 1, false);
    numberOption(result.reconnect.jitter, "reconnect.jitter", 0, false);
    if (result.reconnect.jitter > 1 || result.reconnect.maxDelayMs < result.reconnect.initialDelayMs) {
        throw new TypeError("Invalid reconnect jitter or delay range");
    }
    if (result.reconnect.maxDelayMs > MAX_TIMER_MS) {
        throw new TypeError("reconnect.maxDelayMs exceeds the native timer range");
    }
    Object.freeze(result.reconnect);
    if (input.webSocketFactory !== undefined && typeof input.webSocketFactory !== "function") {
        throw new TypeError("webSocketFactory must be a function");
    }
    result.webSocketFactory ??= (url, protocols) => new globalThis.WebSocket(url, protocols);
    return Object.freeze(result);
}

function targetName(value) {
    if (typeof value !== "string" || !value || value.startsWith("_") ||
        [...value].length > 128) {
        throw new TypeError("Target must be a public nonempty name of at most 128 characters");
    }
    return value;
}

function invocationId(value) {
    return typeof value === "string" && value.length > 0 && value.length <= 128 &&
        /^[\x21-\x7e]+$/.test(value);
}

function deferred() {
    let resolve;
    let reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}

// Copy from descriptors so input getters and toJSON hooks are never executed.
// Define properties explicitly so "__proto__" remains ordinary JSON data.
function jsonCopy(value, limits) {
    const ancestors = new Set();
    let nodes = 0;
    let bytes = 0;
    const charge = amount => {
        bytes += amount;
        if (bytes > limits.maxMessageBytes) throw new LimitError("JSON message byte budget exceeded");
    };
    const chargeString = text => {
        if (text.length > limits.maxMessageBytes) throw new LimitError("JSON string is too large");
        charge(2);
        for (let index = 0; index < text.length; index++) {
            const code = text.charCodeAt(index);
            if (code === 34 || code === 92) charge(2);
            else if (code < 32) charge([8, 9, 10, 12, 13].includes(code) ? 2 : 6);
            else if (code < 128) charge(1);
            else if (code < 2048) charge(2);
            else if (code >= 0xd800 && code <= 0xdbff) {
                const low = text.charCodeAt(index + 1);
                if (low >= 0xdc00 && low <= 0xdfff) { charge(4); index++; }
                else charge(6);
            } else if (code >= 0xdc00 && code <= 0xdfff) charge(6);
            else charge(3);
        }
    };
    function visit(item, depth) {
        if (++nodes > limits.maxJsonNodes || depth > limits.maxJsonDepth) {
            throw new LimitError("JSON structural limit exceeded");
        }
        if (item === null) { charge(4); return item; }
        if (typeof item === "string") { chargeString(item); return item; }
        if (typeof item === "boolean") { charge(item ? 4 : 5); return item; }
        if (typeof item === "number") {
            if (!Number.isFinite(item)) throw new TypeError("JSON numbers must be finite");
            charge(JSON.stringify(item).length);
            return item;
        }
        if (typeof item !== "object") throw new TypeError("Only JSON data values are supported");
        if (ancestors.has(item)) throw new TypeError("Cyclic JSON data is not supported");
        ancestors.add(item);
        try {
            if (Array.isArray(item)) {
                if (item.length > limits.maxJsonNodes) throw new LimitError("JSON array is too large");
                const keys = Reflect.ownKeys(item);
                if (keys.length !== item.length + 1) {
                    throw new TypeError("JSON arrays must be dense and have no extra properties");
                }
                charge(2 + Math.max(0, item.length - 1));
                const output = [];
                for (let index = 0; index < item.length; index++) {
                    const descriptor = Object.getOwnPropertyDescriptor(item, String(index));
                    if (!descriptor || !own(descriptor, "value")) {
                        throw new TypeError("JSON arrays must contain data properties");
                    }
                    output.push(visit(descriptor.value, depth + 1));
                }
                return output;
            }
            if (!plain(item)) throw new TypeError("JSON objects must be plain objects");
            const keys = Reflect.ownKeys(item);
            if (keys.length > limits.maxJsonNodes) throw new LimitError("JSON object is too large");
            const output = {};
            charge(2);
            let first = true;
            for (const key of keys) {
                const descriptor = Object.getOwnPropertyDescriptor(item, key);
                if (typeof key !== "string" || !descriptor || !own(descriptor, "value") ||
                    !descriptor.enumerable) {
                    throw new TypeError("JSON objects must contain enumerable data properties");
                }
                if (!first) charge(1);
                first = false;
                chargeString(key);
                charge(1);
                Object.defineProperty(output, key, {
                    value: visit(descriptor.value, depth + 1), enumerable: true,
                    writable: true, configurable: true,
                });
            }
            return output;
        } finally {
            ancestors.delete(item);
        }
    }
    return visit(value, 0);
}

function exact(object, required, optional = []) {
    return plain(object) && required.every(key => own(object, key)) &&
        Object.keys(object).every(key => required.includes(key) || optional.includes(key));
}

function errorPayload(value) {
    return exact(value, ["code", "message"]) && typeof value.code === "string" &&
        value.code.length > 0 && [...value.code].length <= 64 &&
        typeof value.message === "string" && [...value.message].length <= 1024;
}

function validateEnvelope(message) {
    if (!plain(message) || typeof message.type !== "string") throw new ProtocolError();
    let valid = false;
    switch (message.type) {
        case "ready":
            valid = exact(message, ["type", "version", "connection_id"]) &&
                message.version === 1 && typeof message.connection_id === "string" &&
                message.connection_id.length > 0 && [...message.connection_id].length <= 128;
            break;
        case "send":
        case "invoke":
            valid = exact(message, message.type === "send" ?
                ["type", "target", "args"] : ["type", "id", "target", "args"]) &&
                Array.isArray(message.args) && message.args.length <= 64;
            if (valid) {
                try { targetName(message.target); } catch { valid = false; }
            }
            if (message.type === "invoke") valid &&= invocationId(message.id);
            break;
        case "completion":
            valid = exact(message, ["type", "id"], ["result", "error"]) &&
                invocationId(message.id) && own(message, "result") !== own(message, "error") &&
                (!own(message, "error") || errorPayload(message.error));
            break;
        case "stream_item":
            valid = exact(message, ["type", "id", "item"]) && invocationId(message.id);
            break;
        case "stream_complete":
            valid = exact(message, ["type", "id"], ["error"]) && invocationId(message.id) &&
                (!own(message, "error") || errorPayload(message.error));
            break;
        case "ping":
        case "pong":
            valid = exact(message, ["type"]);
            break;
        default:
            break;
    }
    if (!valid) throw new ProtocolError();
    return message;
}

function asError(error, message = "Application callback failed") {
    return error instanceof RealtimeError ? error :
        new RealtimeError(message, { code: "callback_error", cause: error });
}

/**
 * Browser ESM client for the JSON Orionis Realtime v1 wire protocol.
 * Connection recovery never replays application operations.
 */
export class RealtimeClient {
    #url;
    #options;
    #state = "idle";
    #connectionId = null;
    #generation = null;
    #generationNumber = 0;
    #counter = 0;
    #desired = false;
    #permanent = false;
    #attempt = 0;
    #connecting = null;
    #reconnectTimer = null;
    #stopping = null;
    #closing = new Set();
    #operations = new Map();
    #events = new Map();
    #methods = new Map();
    #stateListeners = new Set();
    #errorListeners = new Set();
    #subscriptions = 0;
    #activeCallbacks = 0;
    #observerWork = new Map();
    #urlFactories = 0;
    #retiring = 0;

    constructor(url, options = {}) {
        if (typeof url !== "function" && typeof url !== "string" && !(url instanceof URL)) {
            throw new TypeError("url must be a string, URL, or URL factory");
        }
        this.#url = url instanceof URL ? new URL(url.href) : url;
        this.#options = optionsFor(options);
    }

    get state() { return this.#state; }
    get connectionId() { return this.#state === "connected" ? this.#connectionId : null; }

    connect() {
        if (this.#permanent) return Promise.reject(new ConnectionError("Client is disposed", { code: "disposed" }));
        // Abort listeners run synchronously during retirement. Queue a fresh
        // connection decision until the old generation is fully detached.
        if (this.#retiring) return Promise.resolve().then(() => this.connect());
        if (this.#stopping) return this.#stopping.then(() => this.connect());
        if (this.#state === "connected" && this.#generation?.ready &&
            this.#current(this.#generation)) return Promise.resolve();
        if (this.#connecting) return this.#connecting.promise;
        this.#desired = true;
        this.#attempt = 0;
        this.#connecting = deferred();
        const promise = this.#connecting.promise;
        this.#changeState("connecting");
        if (this.#desired && !this.#generation) this.#open();
        return promise;
    }

    stop() { return this.#stop(false); }
    dispose() { return this.#stop(true); }

    async invoke(target, args = [], options = {}) {
        const operation = this.#operation("scalar", target, args, options);
        return operation.result.promise;
    }

    stream(target, args = [], options = {}) {
        const operation = this.#operation("stream", target, args, options);
        const client = this;
        const iterator = {
            [Symbol.asyncIterator]() { return this; },
            next() {
                if (operation.waiter) {
                    return Promise.reject(new RealtimeError(
                        "Only one stream next() may be outstanding", { code: "concurrent_next" },
                    ));
                }
                if (operation.buffer.length) {
                    const entry = operation.buffer.shift();
                    operation.bufferBytes -= entry.bytes;
                    return Promise.resolve({ value: entry.value, done: false });
                }
                if (operation.ended) {
                    return operation.error ? Promise.reject(operation.error) :
                        Promise.resolve({ value: undefined, done: true });
                }
                operation.waiter = deferred();
                return operation.waiter.promise;
            },
            return() {
                client.#cancel(operation, null, true);
                return Promise.resolve({ value: undefined, done: true });
            },
            throw(error) {
                const failure = error instanceof Error ? error :
                    new RealtimeError("Stream consumer failed", { cause: error });
                client.#cancel(operation, failure);
                return Promise.reject(failure);
            },
            cancel(reason) {
                client.#cancel(operation, new AbortError("Stream canceled", { cause: reason }));
            },
        };
        return iterator;
    }

    on(target, handler) {
        targetName(target);
        this.#checkHandler(handler);
        let listeners = this.#events.get(target);
        if (listeners?.has(handler)) return () => {};
        this.#reserveSubscription();
        if (!listeners) this.#events.set(target, listeners = new Set());
        listeners.add(handler);
        return this.#unsubscribe(() => {
            if (!listeners.delete(handler)) return false;
            if (!listeners.size) this.#events.delete(target);
            return true;
        });
    }

    handle(target, handler) {
        targetName(target);
        this.#checkHandler(handler);
        if (this.#methods.has(target)) throw new TypeError("Client method already has a handler");
        this.#reserveSubscription();
        this.#methods.set(target, handler);
        return this.#unsubscribe(() => {
            if (this.#methods.get(target) !== handler) return false;
            return this.#methods.delete(target);
        });
    }

    onState(handler) { return this.#observe(this.#stateListeners, handler); }
    onError(handler) { return this.#observe(this.#errorListeners, handler); }

    #checkHandler(handler) {
        if (this.#permanent) throw new ConnectionError("Client is disposed", { code: "disposed" });
        if (typeof handler !== "function") throw new TypeError("Handler must be a function");
    }

    #reserveSubscription() {
        if (this.#subscriptions >= MAX_SUBSCRIPTIONS) throw new LimitError("Too many registered handlers");
        this.#subscriptions++;
    }

    #unsubscribe(remove) {
        let removed = false;
        return () => {
            if (!removed) {
                removed = true;
                if (remove()) this.#subscriptions = Math.max(0, this.#subscriptions - 1);
            }
        };
    }

    #observe(set, handler) {
        this.#checkHandler(handler);
        if (set.has(handler)) return () => {};
        this.#reserveSubscription();
        set.add(handler);
        return this.#unsubscribe(() => set.delete(handler));
    }

    #changeState(state) {
        if (this.#state === state) return;
        const previous = this.#state;
        this.#state = state;
        const event = Object.freeze({ state, previous, attempt: this.#attempt });
        for (const listener of [...this.#stateListeners]) {
            if (this.#state !== state) break;
            if (this.#observerWork.has(listener) || this.#observerWork.size >= MAX_SUBSCRIPTIONS) continue;
            const token = {};
            this.#observerWork.set(listener, token);
            try {
                const result = listener(event);
                if (result && typeof result.then === "function") {
                    this.#observeWork(listener, result, true, token);
                } else if (this.#observerWork.get(listener) === token) this.#observerWork.delete(listener);
            } catch (error) {
                if (this.#observerWork.get(listener) === token) this.#observerWork.delete(listener);
                this.#report(asError(error));
            }
        }
    }

    #report(error) {
        const normalized = asError(error);
        for (const listener of [...this.#errorListeners]) {
            if (this.#observerWork.has(listener) || this.#observerWork.size >= MAX_SUBSCRIPTIONS) continue;
            const token = {};
            this.#observerWork.set(listener, token);
            try {
                const result = listener(normalized);
                if (result && typeof result.then === "function") this.#observeWork(listener, result, false, token);
                else if (this.#observerWork.get(listener) === token) this.#observerWork.delete(listener);
            } catch {
                if (this.#observerWork.get(listener) === token) this.#observerWork.delete(listener);
                /* Error observers must not cause recursive failure. */
            }
        }
    }

    #observeWork(listener, result, reportFailure, token) {
        Promise.resolve(result).then(() => {
            if (this.#observerWork.get(listener) === token) this.#observerWork.delete(listener);
        }, error => {
            if (this.#observerWork.get(listener) === token) this.#observerWork.delete(listener);
            if (reportFailure) this.#report(asError(error));
        });
    }

    #current(context) { return this.#generation === context && !context.dead; }

    #open() {
        if (!this.#desired || this.#permanent || this.#generation) return;
        const context = {
            number: ++this.#generationNumber,
            attempt: this.#attempt,
            controller: new AbortController(),
            socket: null,
            dead: false,
            ready: false,
            listeners: [],
            queue: [],
            queuedBytes: 0,
            connectTimer: null,
            sendTimer: null,
            heartbeatTimer: null,
            pongTimer: null,
            errorTimer: null,
            awaitingPong: false,
            callbacks: new Map(),
            callbackCount: 0,
        };
        this.#generation = context;
        context.connectTimer = setTimeout(() => {
            this.#lost(context, new ConnectionError("Realtime handshake timed out", { code: "connect_timeout" }));
        }, this.#options.connectTimeoutMs);
        // URL resolution is itself covered by the handshake deadline.
        Promise.resolve().then(async () => {
            if (!this.#current(context)) return;
            let candidate = this.#url;
            if (typeof this.#url === "function") {
                if (this.#urlFactories >= MAX_URL_FACTORIES) {
                    throw new LimitError("Too many unfinished URL factories");
                }
                this.#urlFactories++;
                try {
                    candidate = await this.#url({ attempt: context.attempt, signal: context.controller.signal });
                } finally {
                    this.#urlFactories--;
                }
            }
            if (!this.#current(context)) return;
            if (typeof candidate !== "string" && !(candidate instanceof URL)) {
                throw new TypeError("URL factory must return a string or URL");
            }
            const url = new URL(candidate, globalThis.location?.href);
            if (url.protocol === "https:") url.protocol = "wss:";
            if (url.protocol === "http:") url.protocol = "ws:";
            if (!["ws:", "wss:"].includes(url.protocol) || url.hash || url.username || url.password) {
                throw new TypeError("Realtime URL must use ws/wss without credentials or a fragment");
            }
            const socket = this.#options.webSocketFactory(url.href, [...this.#options.protocols]);
            if (!socket || typeof socket.addEventListener !== "function" ||
                typeof socket.removeEventListener !== "function" ||
                typeof socket.send !== "function" || typeof socket.close !== "function" ||
                typeof socket.readyState !== "number" || typeof socket.bufferedAmount !== "number") {
                throw new TypeError("webSocketFactory must return a WebSocket-compatible transport");
            }
            if (!this.#current(context)) {
                try { socket.close(1000, "Connection superseded"); } catch {}
                return;
            }
            context.socket = socket;
            const listen = (name, callback) => {
                socket.addEventListener(name, callback);
                context.listeners.push([name, callback]);
            };
            listen("message", event => {
                if (this.#current(context)) this.#receive(context, event.data);
            });
            listen("close", event => {
                const code = Number.isInteger(event.code) ? event.code : 1006;
                if (this.#current(context)) this.#lost(context, new ConnectionError(),
                    RETRYABLE_CLOSE_CODES.has(code));
            });
            listen("error", () => {
                if (!this.#current(context) || context.errorTimer !== null) return;
                // Native error normally precedes close. Preserve the close
                // code's retry policy instead of treating every error as 1006.
                context.errorTimer = setTimeout(() => {
                    this.#lost(context, new ConnectionError(
                        "WebSocket transport failed", { code: "connect_failed" },
                    ));
                }, this.#options.closeTimeoutMs);
            });
            // Native WebSocket construction is asynchronous. An already closed
            // custom transport cannot ever supply a valid ready envelope.
            if (socket.readyState >= 2) {
                this.#lost(context, new ConnectionError("Transport closed before ready", { code: "connect_failed" }));
            }
        }).catch(error => {
            if (this.#current(context)) this.#lost(context,
                new ConnectionError("Could not establish Realtime transport", {
                    code: "connect_failed", cause: error,
                }));
        });
    }

    #receive(context, data) {
        let message;
        try {
            if (typeof data !== "string") throw new ProtocolError("JSON protocol requires text frames");
            if (data.length > this.#options.maxMessageBytes ||
                encoder.encode(data).byteLength > this.#options.maxMessageBytes) {
                throw new LimitError("Incoming message exceeds maxMessageBytes");
            }
            message = validateEnvelope(jsonCopy(JSON.parse(data), this.#options));
            if (!context.ready) {
                if (message.type !== "ready") throw new ProtocolError("Expected ready before other messages");
                context.ready = true;
                clearTimeout(context.connectTimer);
                context.connectTimer = null;
                this.#connectionId = message.connection_id;
                this.#attempt = 0;
                const connecting = this.#connecting;
                this.#connecting = null;
                connecting?.resolve();
                this.#changeState("connected");
                if (this.#current(context)) this.#heartbeatLater(context);
                return;
            }
            if (message.type === "ready") throw new ProtocolError("Duplicate ready envelope");
        } catch (error) {
            const failure = error instanceof ProtocolError || error instanceof LimitError ? error :
                new ProtocolError("Invalid JSON protocol envelope", { cause: error });
            this.#lost(context, failure, false, 4002);
            return;
        }
        if (!this.#current(context)) return;
        if (message.type === "pong") {
            if (context.awaitingPong) {
                context.awaitingPong = false;
                clearTimeout(context.pongTimer);
                context.pongTimer = null;
                this.#heartbeatLater(context);
            }
            return;
        }
        if (message.type === "ping") {
            this.#control(context, { type: "pong" });
            return;
        }
        if (message.type === "send") {
            for (const handler of [...(this.#events.get(message.target) ?? [])]) {
                this.#eventCallback(context, handler, message.args);
            }
            return;
        }
        if (message.type === "invoke") {
            this.#clientInvocation(context, message);
            return;
        }
        const operation = this.#operations.get(message.id);
        if (!operation || operation.context !== context) return;
        if (message.type === "completion") {
            if (own(message, "error")) {
                this.#settle(operation, new RemoteError(message.error.code, message.error.message));
            } else if (operation.kind === "stream") {
                this.#cancel(operation, new ProtocolError("Stream received a scalar result"));
            } else this.#settle(operation, null, message.result);
        } else if (operation.kind !== "stream") {
            this.#cancel(operation, new ProtocolError("Scalar invocation received stream data"));
        } else if (message.type === "stream_complete") {
            this.#settle(operation, own(message, "error") ?
                new RemoteError(message.error.code, message.error.message) : null);
        } else {
            if (operation.waiter) {
                const waiter = operation.waiter;
                operation.waiter = null;
                waiter.resolve({ value: message.item, done: false });
            } else {
                const bytes = encoder.encode(JSON.stringify(message.item)).byteLength;
                if (operation.buffer.length >= this.#options.maxStreamBufferItems ||
                    operation.bufferBytes + bytes > this.#options.maxStreamBufferBytes) {
                    this.#cancel(operation, new LimitError("Stream consumer buffer is full"));
                } else {
                    operation.buffer.push({ value: message.item, bytes });
                    operation.bufferBytes += bytes;
                }
            }
        }
    }

    #operation(kind, target, args, input) {
        targetName(target);
        settingsObject(input, "invocation options");
        for (const key of Object.keys(input)) {
            if (!["kwargs", "timeoutMs", "serverTimeout", "signal"].includes(key)) {
                throw new TypeError("Unknown invocation option: " + key);
            }
        }
        if (!Array.isArray(args) || args.length > 64) throw new TypeError("args must be an array of at most 64 values");
        const kwargs = input.kwargs ?? {};
        if (!plain(kwargs) || Object.keys(kwargs).length > 64 ||
            Object.keys(kwargs).some(key => !key || [...key].length > 128)) {
            throw new TypeError("kwargs must contain at most 64 named arguments");
        }
        const timeoutMs = input.timeoutMs ?? this.#options.invocationTimeoutMs;
        numberOption(timeoutMs, "timeoutMs", 0);
        if (timeoutMs > MAX_TIMER_MS) throw new TypeError("timeoutMs exceeds the native timer range");
        if (input.serverTimeout !== undefined) numberOption(input.serverTimeout, "serverTimeout", Number.MIN_VALUE, false);
        const signal = input.signal;
        if (signal !== undefined && (!signal || typeof signal.aborted !== "boolean" ||
            typeof signal.addEventListener !== "function" || typeof signal.removeEventListener !== "function")) {
            throw new TypeError("signal must be an AbortSignal");
        }
        if (signal?.aborted) throw new AbortError("Invocation aborted", { cause: signal.reason });
        const context = this.#generation;
        if (this.#state !== "connected" || !context || !this.#current(context)) {
            throw new ConnectionError("Connect before invoking methods", {
                code: this.#permanent ? "disposed" : "not_connected",
            });
        }
        if (context.errorTimer !== null) {
            throw new ConnectionError("Transport is closing", { code: "not_connected" });
        }
        if (this.#operations.size >= this.#options.maxPendingInvocations) {
            throw new LimitError("Too many pending invocations");
        }
        if (this.#counter >= Number.MAX_SAFE_INTEGER) throw new LimitError("Invocation identifier budget exhausted");
        const id = "c" + context.number.toString(36) + "-" + (++this.#counter).toString(36);
        const envelope = { type: "invoke", id, target, args, kwargs };
        if (input.serverTimeout !== undefined) envelope.timeout = input.serverTimeout;
        const encoded = this.#encode(envelope);
        const operation = {
            id, kind, context, sent: false, ended: false, error: null,
            result: kind === "scalar" ? deferred() : null,
            timer: null, signal, abort: null, waiter: null,
            buffer: [], bufferBytes: 0,
        };
        this.#operations.set(id, operation);
        if (timeoutMs > 0) operation.timer = setTimeout(() => {
            this.#cancel(operation, new TimeoutError());
        }, timeoutMs);
        if (signal) {
            operation.abort = () => this.#cancel(operation,
                new AbortError("Invocation aborted", { cause: signal.reason }));
            signal.addEventListener("abort", operation.abort, { once: true });
            if (signal.aborted) operation.abort();
        }
        if (!operation.ended) {
            this.#enqueue(context, encoded, operation).catch(error => {
                if (!operation.ended) this.#settle(operation, error);
            });
        }
        return operation;
    }

    #settle(operation, error = null, value) {
        if (operation.ended) return;
        operation.ended = true;
        operation.error = error;
        this.#operations.delete(operation.id);
        clearTimeout(operation.timer);
        operation.timer = null;
        if (operation.signal && operation.abort) operation.signal.removeEventListener("abort", operation.abort);
        operation.signal = null;
        operation.abort = null;
        this.#removeQueued(operation);
        // A completed iterator must not retain its old socket and listeners.
        operation.context = null;
        if (operation.kind === "scalar") {
            if (error) operation.result.reject(error);
            else operation.result.resolve(value);
        } else {
            if (error) {
                operation.buffer.length = 0;
                operation.bufferBytes = 0;
            }
            if (operation.waiter) {
                const waiter = operation.waiter;
                operation.waiter = null;
                if (error) waiter.reject(error);
                else waiter.resolve({ value: undefined, done: true });
            }
        }
    }

    #cancel(operation, error, clean = false) {
        if (clean) {
            operation.buffer.length = 0;
            operation.bufferBytes = 0;
            if (operation.ended) {
                operation.error = null;
                return;
            }
        }
        if (operation.ended) return;
        const sent = operation.sent;
        const context = operation.context;
        this.#settle(operation, clean ? null : error);
        if (sent && this.#current(context)) {
            this.#control(context, { type: "cancel", id: operation.id });
        }
    }

    #encode(envelope) {
        const data = JSON.stringify(jsonCopy(envelope, this.#options));
        if (data.length > this.#options.maxMessageBytes) throw new LimitError("Outgoing message exceeds maxMessageBytes");
        const bytes = encoder.encode(data).byteLength;
        if (bytes > this.#options.maxMessageBytes) throw new LimitError("Outgoing message exceeds maxMessageBytes");
        return { data, bytes };
    }

    #enqueue(context, encoded, operation = null, control = false, onSent = null) {
        if (!this.#current(context) || !context.ready) return Promise.reject(new ConnectionError());
        const queueLimit = this.#options.maxPendingInvocations + this.#options.maxConcurrentHandlers + 16;
        if (context.queue.length >= queueLimit ||
            context.queuedBytes + encoded.bytes > this.#options.maxQueuedBytes) {
            return Promise.reject(new LimitError("Outgoing queue is full"));
        }
        const completion = deferred();
        const entry = {
            ...encoded, operation, onSent,
            deadline: Date.now() + this.#options.sendTimeoutMs,
            resolve: completion.resolve, reject: completion.reject,
        };
        if (control) context.queue.unshift(entry);
        else context.queue.push(entry);
        context.queuedBytes += entry.bytes;
        this.#drain(context);
        return completion.promise;
    }

    #drain(context) {
        if (!this.#current(context)) return;
        clearTimeout(context.sendTimer);
        context.sendTimer = null;
        if (context.errorTimer !== null) return;
        while (context.queue.length && this.#current(context)) {
            const entry = context.queue[0];
            // Priority pong/cancel traffic must not extend an older entry's
            // deadline. Every queued entry owns its original send budget.
            let earliestDeadline = entry.deadline;
            for (const queued of context.queue) earliestDeadline = Math.min(earliestDeadline, queued.deadline);
            if (Date.now() >= earliestDeadline) {
                this.#lost(context, new TimeoutError("WebSocket send buffer did not drain", { code: "send_timeout" }));
                return;
            }
            const socket = context.socket;
            if (!socket || socket.readyState !== 1) {
                this.#lost(context, new ConnectionError("Socket is not open", { code: "send_failed" }));
                return;
            }
            if (!Number.isFinite(socket.bufferedAmount) || socket.bufferedAmount < 0) {
                this.#lost(context, new ConnectionError("Invalid transport buffer", { code: "send_failed" }));
                return;
            }
            if (socket.bufferedAmount > this.#options.maxBufferedAmount) {
                const remaining = Math.max(1, earliestDeadline - Date.now());
                context.sendTimer = setTimeout(() => this.#drain(context), Math.min(POLL_MS, remaining));
                return;
            }
            context.queue.shift();
            context.queuedBytes -= entry.bytes;
            if (entry.operation?.ended) {
                entry.resolve(false);
                continue;
            }
            try {
                if (entry.operation) entry.operation.sent = true;
                entry.onSent?.();
                socket.send(entry.data);
                entry.resolve(true);
            } catch (error) {
                const failure = new ConnectionError("WebSocket send failed", { code: "send_failed", cause: error });
                entry.reject(failure);
                this.#lost(context, failure);
                return;
            }
        }
    }

    #removeQueued(operation) {
        const context = operation.context;
        for (let index = context.queue.length - 1; index >= 0; index--) {
            const entry = context.queue[index];
            if (entry.operation !== operation) continue;
            context.queue.splice(index, 1);
            context.queuedBytes -= entry.bytes;
            entry.resolve(false);
        }
        if (!context.queue.length) {
            clearTimeout(context.sendTimer);
            context.sendTimer = null;
        }
    }

    #control(context, envelope, onSent = null) {
        if (!this.#current(context)) return;
        try {
            this.#enqueue(context, this.#encode(envelope), null, true, onSent).catch(error => {
                if (this.#current(context)) this.#lost(context, asError(error));
            });
        } catch (error) {
            if (this.#current(context)) this.#lost(context, asError(error));
        }
    }

    #heartbeatLater(context) {
        if (!this.#current(context) || !this.#options.heartbeatIntervalMs) return;
        clearTimeout(context.heartbeatTimer);
        context.heartbeatTimer = setTimeout(() => {
            context.heartbeatTimer = null;
            if (!this.#current(context)) return;
            this.#control(context, { type: "ping" }, () => {
                context.awaitingPong = true;
                context.pongTimer = setTimeout(() => {
                    this.#lost(context, new ConnectionError("Realtime heartbeat timed out", { code: "heartbeat_timeout" }));
                }, this.#options.heartbeatTimeoutMs);
            });
        }, this.#options.heartbeatIntervalMs);
    }

    #eventCallback(context, handler, args) {
        if (!this.#current(context)) return;
        if (this.#activeCallbacks >= this.#options.maxConcurrentHandlers) {
            this.#report(new LimitError("Concurrent event handler limit exceeded"));
            return;
        }
        this.#runCallback(context, handler, args, false, null);
    }

    #clientInvocation(context, message) {
        if (context.callbacks.has(message.id)) {
            this.#lost(context, new ProtocolError("Duplicate active client invocation ID"), false, 4002);
            return;
        }
        const handler = this.#methods.get(message.target);
        if (!handler || this.#activeCallbacks >= this.#options.maxConcurrentHandlers) {
            this.#control(context, {
                type: "completion", id: message.id,
                error: { code: handler ? "busy" : "method_not_found", message: "Client method unavailable" },
            });
            return;
        }
        this.#runCallback(context, handler, message.args, true, message.id);
    }

    #runCallback(context, handler, args, rpc, id) {
        const token = id ?? Symbol("event");
        const controller = new AbortController();
        const canceled = deferred();
        // The race observes a handler's eventual rejection even after timeout.
        const record = { controller, cancel: error => canceled.reject(error), timer: null };
        context.callbacks.set(token, record);
        context.callbackCount++;
        this.#activeCallbacks++;
        record.timer = setTimeout(() => {
            const error = new TimeoutError("Client callback timed out");
            controller.abort(error);
            record.cancel(error);
        }, this.#options.callbackTimeoutMs);
        const identity = this.#connectionId;
        const produced = Promise.resolve().then(() => {
            if (!this.#current(context) || controller.signal.aborted) {
                throw new AbortError("Client callback aborted");
            }
            return rpc ? handler(args, { signal: controller.signal, connectionId: identity }) : handler(args);
        });
        const release = () => {
            this.#activeCallbacks--;
            if (context.callbacks.get(token) === record) {
                context.callbacks.delete(token);
                context.callbackCount--;
            }
        };
        // Admission follows actual application work, not only the timeout
        // race. A handler ignoring AbortSignal cannot accumulate new slots.
        produced.then(release, release);
        const run = async () => {
            try {
                const result = await Promise.race([produced, canceled.promise]);
                if (rpc && this.#current(context) && !controller.signal.aborted) {
                    // Encoding belongs to the application callback's error
                    // boundary: invalid results must not drop unrelated calls.
                    const encoded = this.#encode({
                        type: "completion", id, result: result === undefined ? null : result,
                    });
                    this.#enqueue(context, encoded, null, true).catch(error => {
                        if (this.#current(context)) this.#lost(context, asError(error));
                    });
                }
            } catch (error) {
                if (this.#current(context)) {
                    if (rpc) {
                        this.#control(context, {
                            type: "completion", id,
                            error: {
                                code: error instanceof TimeoutError ? "timeout" : "client_error",
                                message: "Client method failed",
                            },
                        });
                    }
                    this.#report(asError(error));
                }
            } finally {
                clearTimeout(record.timer);
            }
        };
        run().catch(error => this.#report(asError(error)));
    }

    #lost(context, error, retry = true, code = 1000) {
        if (!this.#current(context)) return;
        this.#retire(context, error, code);
        const policy = this.#options.reconnect;
        if (this.#desired && !this.#permanent && retry && policy.enabled &&
            this.#attempt < policy.maxAttempts) {
            this.#attempt++;
            if (!this.#connecting) {
                this.#connecting = deferred();
                // Recovery may run without an explicit connect() waiter.
                this.#connecting.promise.catch(() => {});
            }
            this.#changeState("reconnecting");
            if (this.#desired && !this.#permanent && this.#state === "reconnecting") {
                const base = policy.initialDelayMs === 0 ? 0 : Math.min(policy.maxDelayMs,
                    policy.initialDelayMs * Math.pow(policy.factor, Math.min(this.#attempt - 1, 1024)));
                const variation = 1 + (Math.random() * 2 - 1) * policy.jitter;
                const delay = Math.max(0, Math.min(policy.maxDelayMs, Math.round(base * variation)));
                this.#reconnectTimer = setTimeout(() => {
                    this.#reconnectTimer = null;
                    if (this.#desired && !this.#permanent) this.#open();
                }, delay);
            }
        } else {
            this.#desired = false;
            const connecting = this.#connecting;
            this.#connecting = null;
            connecting?.reject(error);
            this.#changeState(this.#permanent ? "disposed" : "idle");
        }
        this.#report(error);
    }

    #retire(context, error, code = 1000) {
        this.#retiring++;
        try {
            context.dead = true;
            if (this.#generation === context) this.#generation = null;
            this.#connectionId = null;
            context.controller.abort(error);
            for (const timer of ["connectTimer", "sendTimer", "heartbeatTimer", "pongTimer", "errorTimer"]) {
                clearTimeout(context[timer]);
                context[timer] = null;
            }
            for (const [name, listener] of context.listeners) {
                try { context.socket?.removeEventListener(name, listener); } catch {}
            }
            context.listeners.length = 0;
            for (const operation of [...this.#operations.values()]) {
                if (operation.context === context) this.#settle(operation, error);
            }
            for (const record of context.callbacks.values()) {
                clearTimeout(record.timer);
                record.controller.abort(error);
                record.cancel(error);
            }
            context.callbacks.clear();
            context.callbackCount = 0;
            for (const entry of context.queue) entry.reject(error);
            context.queue.length = 0;
            context.queuedBytes = 0;
            const closing = this.#closeSocket(context.socket, code);
            this.#closing.add(closing);
            closing.finally(() => this.#closing.delete(closing));
        } finally {
            this.#retiring--;
        }
    }

    #closeSocket(socket, code) {
        if (!socket || socket.readyState === 3) return Promise.resolve();
        return new Promise(resolve => {
            let timer;
            let finished = false;
            const finish = () => {
                if (finished) return;
                finished = true;
                clearTimeout(timer);
                try { socket.removeEventListener("close", finish); } catch {}
                resolve();
            };
            try {
                socket.addEventListener("close", finish);
                timer = setTimeout(finish, this.#options.closeTimeoutMs);
                if (socket.readyState < 2) socket.close(code, "Realtime client closed");
                if (socket.readyState === 3) finish();
            } catch { finish(); }
        });
    }

    #stop(permanent) {
        if (permanent) this.#permanent = true;
        if (this.#stopping) return this.#stopping;
        if (this.#state === "disposed") return Promise.resolve();
        const stopping = deferred();
        this.#stopping = stopping.promise;
        this.#desired = false;
        clearTimeout(this.#reconnectTimer);
        this.#reconnectTimer = null;
        const error = new ConnectionError(permanent ? "Client disposed" : "Client stopped", {
            code: permanent ? "disposed" : "stopped",
        });
        const connecting = this.#connecting;
        this.#connecting = null;
        connecting?.reject(error);
        if (this.#generation) this.#retire(this.#generation, error);
        this.#changeState("stopping");
        // A reentrant stop from an abort listener may precede the old
        // generation's close registration in the same synchronous stack.
        Promise.resolve().then(() => Promise.all([...this.#closing])).then(() => {
            if (this.#permanent) {
                this.#events.clear();
                this.#methods.clear();
                this.#subscriptions = 0;
            }
            this.#changeState(this.#permanent ? "disposed" : "idle");
            if (this.#permanent) {
                // An idle observer can dispose synchronously while this stop
                // is completing. Re-evaluate the final permanent state.
                this.#events.clear();
                this.#methods.clear();
                this.#subscriptions = 0;
                this.#changeState("disposed");
                this.#stateListeners.clear();
                this.#errorListeners.clear();
                this.#observerWork.clear();
            }
        }).then(() => {
            if (this.#stopping === stopping.promise) this.#stopping = null;
            stopping.resolve();
        }, failure => {
            if (this.#stopping === stopping.promise) this.#stopping = null;
            stopping.reject(failure);
        });
        return stopping.promise;
    }
}
