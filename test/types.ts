import {
    AbortError, ConnectionError, LimitError, ProtocolError, RealtimeClient,
    RealtimeError, RemoteError, TimeoutError,
} from "orionis-realtime-client";
import type {
    InvocationOptions, JsonObject, JsonValue, RealtimeClientOptions,
    RealtimeErrorOptions, RealtimeEventHandler, RealtimeHandlerContext,
    RealtimeMethodHandler, RealtimeSocketEvent, RealtimeState, RealtimeStateChange,
    RealtimeStream, RealtimeUrl, RealtimeUrlContext, RealtimeUrlFactory,
    ReconnectOptions, Unsubscribe, WebSocketFactory, WebSocketLike,
} from "orionis-realtime-client";

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
