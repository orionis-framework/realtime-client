import {
    AbortError, ConnectionError, LimitError, ProtocolError, RealtimeClient,
    RealtimeError, RemoteError, TimeoutError,
} from "@orionis-framework/realtime-client";
import type {
    InvocationOptions, JsonObject, JsonValue, RealtimeClientOptions,
    RealtimeErrorOptions, RealtimeEventHandler, RealtimeHandlerContext,
    RealtimeMethodHandler, RealtimeSocketEvent, RealtimeState, RealtimeStateChange,
    RealtimeStream, RealtimeUrl, RealtimeUrlContext, RealtimeUrlFactory,
    ReconnectOptions, Unsubscribe, WebSocketFactory, WebSocketLike,
} from "@orionis-framework/realtime-client";

export const nativeTransport: WebSocketFactory = (url, protocols) => new WebSocket(url, protocols);
export function acceptTransport(socket: WebSocketLike, event: RealtimeSocketEvent): void {
    socket.send(JSON.stringify({ type: event.type }));
}

const reconnect: ReconnectOptions = { enabled: true, maxAttempts: 2 };
const options: RealtimeClientOptions = { webSocketFactory: nativeTransport, reconnect };
const factory: RealtimeUrlFactory = ({ attempt, signal }: RealtimeUrlContext) => {
    if (signal.aborted) throw new AbortError();
    return Promise.resolve(new URL(`wss://example.com/realtime?attempt=${attempt}`));
};
const url: RealtimeUrl = factory;
const client = new RealtimeClient(url, options);
export const state: RealtimeState = client.state;
export const connectionId: string | null = client.connectionId;

const kwargs: JsonObject = { room: "lobby", nested: { enabled: true }, values: [1, null] };
const invocation: InvocationOptions = {
    kwargs, timeoutMs: 1000, serverTimeout: 1, signal: new AbortController().signal,
};
const errorOptions: RealtimeErrorOptions = { code: "example", cause: new Error("cause") };
export const errors: RealtimeError[] = [
    new RealtimeError("failure", errorOptions), new ConnectionError(), new ProtocolError(),
    new RemoteError("remote", "failure"), new TimeoutError(), new AbortError(), new LimitError(),
];

const eventHandler: RealtimeEventHandler = args => {
    JSON.stringify(args);
    return Promise.resolve();
};
const methodHandler: RealtimeMethodHandler = (args, context: RealtimeHandlerContext) => {
    if (context.signal.aborted) return;
    return { value: args[0] ?? null, connectionId: context.connectionId };
};
export const subscriptions: Unsubscribe[] = [
    client.on("message", eventHandler),
    client.handle("confirm", methodHandler),
    client.onState((change: RealtimeStateChange) => { JSON.stringify(change); }),
    client.onError((error: RealtimeError) => { JSON.stringify(error.code); }),
    client.onState(async change => { await Promise.resolve(change.state); }),
    client.onError(async error => { await Promise.resolve(error.code); }),
];

export async function exerciseConsumer(): Promise<JsonValue> {
    await client.connect();
    try {
        const result: JsonValue = await client.invoke("publish", [kwargs], invocation);
        const stream: RealtimeStream = client.stream("updates", [], invocation);
        for await (const item of stream) {
            const value: JsonValue = item;
            JSON.stringify(value);
            if (value === null) break;
        }
        stream.cancel("done");
        await stream.return();
        await client.stop();
        return result;
    } finally {
        await client.dispose();
    }
}

export function exerciseIterator(stream: RealtimeStream): Promise<IteratorResult<JsonValue>>[] {
    const iterable: AsyncIterableIterator<JsonValue> = stream[Symbol.asyncIterator]();
    const next: Promise<IteratorResult<JsonValue>> = iterable.next();
    const returned: Promise<IteratorResult<JsonValue>> = stream.return("finished");
    const thrown: Promise<IteratorResult<JsonValue>> = stream.throw(new Error("consumer failed"));
    return [next, returned, thrown];
}

// These functions are compiled, never executed. Each expected error protects a
// public boundary from accidentally widening its declarations.
export function rejectInvalidJson(): void {
    // @ts-expect-error Undefined is not a JSON value.
    const missing: JsonValue = undefined;
    // @ts-expect-error BigInt is not serialized by the client.
    const bigint: JsonValue = 1n;
    // @ts-expect-error Date objects are not plain JSON objects.
    const date: JsonValue = new Date();
    // @ts-expect-error Functions are not JSON values.
    const callback: JsonValue = () => null;
    void [missing, bigint, date, callback];
}

export function rejectInvalidCalls(): void {
    // @ts-expect-error Positional arguments must be an array.
    void client.invoke("publish", kwargs);
    // @ts-expect-error Keyword arguments must be a JSON object.
    void client.invoke("publish", [], { kwargs: "lobby" });
    // @ts-expect-error Invocation options do not accept arbitrary fields.
    void client.invoke("publish", [], { unknown: true });
    // @ts-expect-error Deadlines are numbers, not strings.
    void client.invoke("publish", [], { timeoutMs: "1000" });
    // @ts-expect-error Cancellation requires an AbortSignal.
    void client.invoke("publish", [], { signal: new AbortController() });
    // @ts-expect-error RPC handlers must return JSON data or no result.
    client.handle("invalid", () => new Date());
    // @ts-expect-error Event handlers receive an argument array, not a string.
    client.on("message", (_args: string) => {});
    // @ts-expect-error Client options do not accept arbitrary fields.
    new RealtimeClient(url, { unknown: true });
    // @ts-expect-error A transport factory is synchronous and returns a socket.
    new RealtimeClient(url, { webSocketFactory: async () => new WebSocket("wss://example.com") });
    // @ts-expect-error URL factories must resolve to a string or URL.
    new RealtimeClient(async () => 42);
}

export function rejectMutableState(): void {
    // @ts-expect-error Connection state is a read-only observation.
    client.state = "connected";
    // @ts-expect-error Connection IDs are controlled by the server handshake.
    client.connectionId = "client-selected";
    // @ts-expect-error States use the documented finite set.
    const invalidState: RealtimeState = "open";
    void invalidState;
}
