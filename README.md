# Orionis Realtime JavaScript client

A dependency-free ESM client for **Orionis Realtime v1 over JSON WebSocket**. It manages the ready handshake, correlated calls, server events and callbacks, asynchronous streams, cancellation, bounded buffers, heartbeat, and transport reconnection.

This client connects to `Route.hub(..., protocol="json")` endpoints. It does not implement raw WebSocket application protocols, SSE, MessagePack, or SignalR compatibility.

[Documentación en español](./README.es.md) · [Framework guides](https://docs.orionis-framework.com/en/realtime)

## Installation

### npm

```bash
npm install @orionis-framework/realtime-client
```

```javascript
import { RealtimeClient } from "@orionis-framework/realtime-client";
```

The package is ESM-only, includes TypeScript declarations, and has no runtime dependencies. Consumers need no build step. Use a modern browser or Node.js 22 or newer with native `WebSocket`, `AbortController`, and `TextEncoder` support. Importing the module creates no connection.

### Direct browser import

Copy `node_modules/@orionis-framework/realtime-client/orionis-realtime.js` into your application's served asset directory and import it from another module or `<script type="module">`. Adjust the import URL to match that location. A browser must load these files over HTTP(S).

The browser examples below place both JavaScript modules in the same directory:

```javascript
import { RealtimeClient } from "./orionis-realtime.js";
```

## Quick start with the documented chat

Configure the [chat Hub and route](https://docs.orionis-framework.com/en/realtime/chat) first. That example provides `/realtime/chat/{room:slug}`, the `publish` method, and the `message` event; joining the URL's room occurs in the server's `onConnect()`.

This browser module connects to `lobby`, prints incoming messages, publishes one message, then shuts down:

```javascript
import { RealtimeClient, RemoteError } from "./orionis-realtime.js";

const url = new URL("/realtime/chat/lobby", location.href);
url.protocol = location.protocol === "https:" ? "wss:" : "ws:";

const client = new RealtimeClient(url);
const removeMessage = client.on("message", ([message]) => {
    console.log("Chat message:", message);
});
const removeState = client.onState(({ state, previous, attempt }) => {
    console.log(previous, "→", state, "attempt", attempt);
});
const removeError = client.onError((error) => {
    console.error(error.code, error.message);
});

try {
    await client.connect();
    console.log("Connection:", client.connectionId);

    const delivery = await client.invoke(
        "publish",
        [{ author: "Ada", text: "Hello from the downloadable client" }],
        { timeoutMs: 10000, serverTimeout: 5 },
    );
    console.log("Delivery acknowledgement:", delivery);
} catch (error) {
    if (error instanceof RemoteError) {
        console.error("The Hub rejected the call:", error.code, error.message);
    } else {
        console.error(error);
    }
} finally {
    removeMessage();
    removeState();
    removeError();
    await client.dispose();
}
```

Keep the client connected for an interactive page and dispose it when that component is removed. An acknowledgement is not a persistence or display guarantee. A timeout or connection loss does not prove that a remote operation did not run; do not automatically resend mutations.

## Public API

| API | Behavior |
| --- | --- |
| `new RealtimeClient(url, options?)` | Accepts a URL string, a `URL`, or an asynchronous URL factory. |
| `connect(): Promise<void>` | Idempotent; resolves after a valid Realtime `ready` message, not merely WebSocket open. |
| `stop(): Promise<void>` | Stops transport and reconnect attempts; `connect()` can restart it. |
| `dispose(): Promise<void>` | Permanent shutdown. This instance cannot reconnect afterward. |
| `state` | `idle`, `connecting`, `connected`, `reconnecting`, `stopping`, or `disposed`. |
| `connectionId` | The server ID while connected; otherwise `null`. |
| `invoke(target, args?, options?)` | Returns a promise for a JSON result. Requires connected state. |
| `stream(target, args?, options?)` | Immediately starts a stream and returns its asynchronous iterator. |
| `on(target, handler)` | Receives an argument **array** for an event; returns an unsubscribe function. |
| `handle(target, handler)` | Registers one server-callable handler per target; returns an unsubscribe function. Duplicate registrations fail. |
| `onState(handler)` | Receives `{ state, previous, attempt }`; returns unsubscribe. |
| `onError(handler)` | Observes normalized `RealtimeError` instances; returns unsubscribe. |

`invoke` and `stream` accept positional `args` as an array and options `{ kwargs, timeoutMs, serverTimeout, signal }`. `kwargs` is a JSON object. Targets must be nonempty, at most 128 Unicode code points, and must not start with `_`; they must also match a method or event provided by your Hub.

Local `timeoutMs` is a total deadline in **milliseconds**, defaulting to `invocationTimeoutMs`; `0` disables it. Optional `serverTimeout` is sent as the protocol's `timeout` in **seconds** and must satisfy server policy. Omitting it leaves that policy in control. These are independent deadlines.

Invalid inputs produce `TypeError`. An invocation rejects its promise; eager stream setup can throw immediately. Subscribe to `onError` for transport/protocol and callback errors; this observer does not replace catching individual operation failures.

In a browser, relative URLs resolve against the current page. HTTP(S) URLs are normalized to WS(S). URL credentials and fragments are rejected. Connection and URL-factory failures reject `connect()` with `ConnectionError` and retain the original failure as its cause.

## Consuming a stream

Set up the [FeedHub example](https://docs.orionis-framework.com/en/realtime/protocol-and-streaming) providing `/realtime/feed` and `updates(count)`:

```javascript
import { RealtimeClient } from "./orionis-realtime.js";

const url = new URL("/realtime/feed", location.href);
url.protocol = location.protocol === "https:" ? "wss:" : "ws:";
const client = new RealtimeClient(url);

try {
    await client.connect();

    const updates = client.stream("updates", [3], {
        timeoutMs: 15000,
        serverTimeout: 10,
    });
    for await (const value of updates) {
        console.log("Update:", value); // 0, 1, 2.
    }
} finally {
    await client.dispose();
}
```

A stream is **eager**: creation registers/sends the invocation before the first `next()`. Consume it or cancel it. Use only one outstanding `next()`; concurrent reads are unsupported. A `break` in `for await` calls `return()` and cancels the remote invocation. Explicit `stream.cancel(reason)` also cancels; the reason remains local as the error cause.

Stream buffers have item and byte limits. Slow consumers can receive `LimitError`; the client does not grow an unbounded result queue or request replay after reconnection.

For cancellation of a normal call while connected:

```javascript
const controller = new AbortController();
const pending = client.invoke(
    "publish",
    [{ author: "Ada", text: "A cancellable operation" }],
    { signal: controller.signal, timeoutMs: 10000 },
);
controller.abort("The user cancelled");

try {
    await pending;
} catch (error) {
    console.log(error.code); // aborted.
}
```

Cancellation requests cannot undo side effects already completed on the server.

## Handling a call from the server

The [ConfirmationHub example](https://docs.orionis-framework.com/en/realtime/protocol-and-streaming) calls `confirmAction` on one client. Register the handler before connecting:

```javascript
const removeConfirmation = client.handle(
    "confirmAction",
    ([payload], { signal, connectionId }) => {
        console.log("Server call on:", connectionId);
        if (signal.aborted) return false;
        return window.confirm(String(payload.question));
    },
);
```

Handlers receive an argument array plus `{ signal, connectionId }`. They can return a JSON value or a promise; `undefined` becomes `null`. Async handlers should honor `signal` when they perform cancellable work. Replace the simple confirmation dialog with your application's UI as needed. Unregister with `removeConfirmation()`.

Event handlers registered with `on` return no result. Event and server-call handlers share the global `maxConcurrentHandlers` budget across connections and are bounded by `callbackTimeoutMs`. A deadline aborts the callback and, for server calls, sends an error response promptly. An application promise that ignores cancellation retains its admission slot until it actually settles. JavaScript cannot forcibly terminate that work.

At most 1024 event, method, state, and error subscriptions can be registered together. Unsubscribe when removing a component to release its registration budget.

Promise-returning `onState` and `onError` observers admit one in-flight call per registered function and at most 1024 unfinished observer promises per client. Notifications arriving while the function is pending or that global budget is full are skipped, rather than queued. Unsubscribing does not release an unfinished promise's slot before it settles. Keep observers brief; error-observer failures are contained.

## Connection options and defaults

All timing options below are milliseconds:

| Option | Default |
| --- | --- |
| `protocols` | `[]`; WebSocket subprotocols, not a codec selector. |
| `webSocketFactory` | Native `WebSocket`; synchronous `(url, protocols) => socket`. |
| `connectTimeoutMs` | `10000` |
| `closeTimeoutMs` | `1000` |
| `invocationTimeoutMs` | `30000`; `0` disables the local deadline. |
| `callbackTimeoutMs` | `30000` |
| `heartbeatIntervalMs` | `30000`; `0` disables heartbeat. |
| `heartbeatTimeoutMs` | `10000` |
| `sendTimeoutMs` | `10000` |
| `maxPendingInvocations` | `128` |
| `maxConcurrentHandlers` | `32` |
| `maxMessageBytes` | `1048576` |
| `maxQueuedBytes` | `2097152` |
| `maxBufferedAmount` | `262144` |
| `maxStreamBufferItems` | `64` |
| `maxStreamBufferBytes` | `1048576` |
| `maxJsonDepth` | `64` |
| `maxJsonNodes` | `100000` |
| `reconnect.enabled` | `true` |
| `reconnect.maxAttempts` | `10` |
| `reconnect.initialDelayMs` | `500` |
| `reconnect.maxDelayMs` | `30000` |
| `reconnect.factor` | `2` |
| `reconnect.jitter` | `0.2` |

A URL factory receives `{ attempt, signal }`; initial connection is attempt `0`, first reconnect is `1`. It may return a URL asynchronously so your application can resolve fresh connection details. `webSocketFactory` is separate and synchronous, for compatible transports in tests or other environments.

URL factories also have a fixed budget of 32 actually unfinished callbacks per client, across connection generations. A factory that ignores cancellation retains its slot until settlement. At capacity, another attempt fails with `connect_failed` and a `LimitError` cause without calling the factory; configured retry limits still apply.

Numeric budgets and millisecond deadlines are safe positive integers, except `invocationTimeoutMs`, `heartbeatIntervalMs`, and `maxBufferedAmount`, which allow zero. Timer delays cannot exceed `2147483647` milliseconds. Reconnect attempts and initial delay allow zero, `factor` is at least `1`, and `jitter` ranges from `0` to `1`; maximum delay must be at least the initial delay. Subprotocols are limited to 64 unique valid WebSocket tokens. Invocations accept at most 64 positional and 64 named arguments. Unknown option names are rejected.

Reconnection restores the **transport only**. Pending calls/streams fail on disconnection. There is no offline invocation queue, automatic resend, room-membership replay, or event history. The new server connection runs the Hub's connection lifecycle again; restore application state explicitly when needed.

Automatic reconnection is limited to close codes `1001`, `1006`, `1011`, `1012`, `1013`, and `1014`, within the configured attempt budget. Normal closure `1000`, protocol/authentication rejection, private `4xxx` codes, and other close codes leave the client idle. A malformed protocol message closes with `4002` and does not retry. `stop()` and `dispose()` also prevent reconnection. A transport `error` waits for its close code; if none arrives within `closeTimeoutMs`, it is treated as a connection failure eligible for retry. Initial handshake/URL/connection failures can retry too; the original `connect()` promise remains pending until readiness or exhaustion of those attempts.

Close details also depend on the server transport. Orionis's RSGI adapter does not expose custom outgoing WebSocket close codes or reasons, so a server-initiated closure may appear as normal code `1000`. The client cannot infer a restart or policy reason from that code and does not retry it automatically. ASGI supports explicit close details. After an idle nonrecoverable close, an application can choose a later explicit `connect()` using its own state and access policy.

Browser WebSocket cannot add arbitrary HTTP authentication headers. Subprotocols are not an authorization mechanism. Configure access in your application's server handshake and Hub operations.

## Errors and JSON boundaries

Named exports: `RealtimeError`, `ConnectionError`, `ProtocolError`, `RemoteError`, `TimeoutError`, `AbortError`, and `LimitError`.

- `RemoteError` preserves the remote `code` and `message`.
- Local operation timeout, abort, protocol, and resource-limit codes are `timeout`, `aborted`, `protocol_error`, and `limit_exceeded`. A send queue deadline uses `send_timeout`; concurrent stream reads use `concurrent_next`.
- Connection errors include `connection_lost`, `not_connected`, `connect_failed`, `connect_timeout`, `heartbeat_timeout`, `send_failed`, `stopped`, and `disposed`.
- Ordinary application-handler errors normalize to `callback_error`; existing `RealtimeError` codes are preserved. Error-observer failures are isolated.

Only plain JSON data is supported. Cycles, getters, nonfinite numbers, `undefined`, `bigint`, `Date`, and other nonplain objects are rejected rather than silently coerced. Special JSON keys such as `__proto__` and `constructor` remain inert data. Incoming messages are bounded and validated; malformed protocol frames end the connection rather than being treated as successful results.

JSON numbers use JavaScript `Number`. Integer identifiers outside the safe range `±Number.MAX_SAFE_INTEGER` can lose precision during parsing; represent such identifiers as strings in your application protocol. `BigInt` is not serialized automatically.

Type declarations describe JSON results; they do not replace server-side validation or prove that an arbitrary remote result has your application's shape.

## Development

The root `orionis-realtime.js` is both the source and the standalone distribution; public types are in `index.d.ts`. Tests, shared fixtures, and TypeScript consumers live in `test/`. The only maintenance script, `scripts/test-package.mjs`, validates the published archive.

```bash
npm ci
npm run check
```

The checks cover JavaScript syntax, client behavior, native WebSocket interoperability, and positive and negative TypeScript consumers. They also create a real tarball, check its exact contents and npm SHA-512 integrity, install it offline in an isolated consumer, and verify imports and declarations. No build or custom integrity manifest is needed. Tests and development dependencies are excluded from the package.

Use `npm run test:coverage` for runtime coverage. CI runs the validation through `npm pack --dry-run` on Node.js 22 and 24, on Windows and Linux. Keep both README translations and `test/types.ts` in agreement with public API changes.

## Publishing

Use an npm account authorized to publish `@orionis-framework/realtime-client` in the `orionis-framework` organization. Direct publishing requires two-factor authentication or an npm-supported publishing credential; never commit credentials to this project.

```bash
npm login --registry=https://registry.npmjs.org/
npm whoami --registry=https://registry.npmjs.org/
npm publish --dry-run
npm publish --access public
```

The `prepack` hook runs all checks before packing or publishing. `--dry-run` does not publish. The current stable release is `1.0.0`; each later release needs a new, unused version. Name availability and account permissions are enforced by npm at publication time.

## License

MIT. The original copyright and complete notice are preserved in [LICENSE](./LICENSE) and the standalone module.
