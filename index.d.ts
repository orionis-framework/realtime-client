/**
 * Orionis Realtime v1 JSON client.
 * Copyright (c) 2023 Raul Mauricio Uñate Castro.
 * MIT license; see the accompanying LICENSE file.
 */

/** JSON values only: no undefined, bigint, Date, getters, cycles, or nonfinite numbers. */
export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;
export interface JsonObject {
  [key: string]: JsonValue;
}

export type RealtimeState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'stopping'
  | 'disposed';

export type Unsubscribe = () => void;

export interface RealtimeUrlContext {
  /** 0 for the initial connection, 1 for the first reconnect attempt. */
  readonly attempt: number;
  readonly signal: AbortSignal;
}

/** At most 32 unfinished URL-factory callbacks are retained per client. */
export type RealtimeUrlFactory = (
  context: RealtimeUrlContext,
) => string | URL | Promise<string | URL>;

export type RealtimeUrl = string | URL | RealtimeUrlFactory;

/** Structural transport surface; the native browser WebSocket satisfies it. */
export interface WebSocketLike {
  readonly readyState: number;
  readonly bufferedAmount: number;
  addEventListener(type: string, listener: (event: RealtimeSocketEvent) => void): void;
  removeEventListener(type: string, listener: (event: RealtimeSocketEvent) => void): void;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

export interface RealtimeSocketEvent {
  readonly type: string;
  readonly data?: unknown;
  readonly code?: number;
  readonly reason?: string;
  readonly wasClean?: boolean;
}

export type WebSocketFactory = (url: string, protocols: string[]) => WebSocketLike;

export interface ReconnectOptions {
  /** Default true. Reconnects transport only; never replays invocations or streams. */
  enabled?: boolean;
  /** Default 10. */
  maxAttempts?: number;
  /** Milliseconds; default 500. */
  initialDelayMs?: number;
  /** Milliseconds; default 30000. */
  maxDelayMs?: number;
  /** Default 2. */
  factor?: number;
  /** Default 0.2. */
  jitter?: number;
}

export interface RealtimeClientOptions {
  /** WebSocket subprotocols, not a Realtime codec selector. Default []. */
  protocols?: string[];
  /** Synchronous factory. Defaults to the native WebSocket constructor. */
  webSocketFactory?: WebSocketFactory;
  /** Default 10000 milliseconds; includes the Realtime ready handshake. */
  connectTimeoutMs?: number;
  /** Default 1000 milliseconds. */
  closeTimeoutMs?: number;
  /** Default 30000 milliseconds; 0 disables the local invocation deadline. */
  invocationTimeoutMs?: number;
  /** Default 30000 milliseconds; applies to event handlers and server callbacks. */
  callbackTimeoutMs?: number;
  /** Default 30000 milliseconds; 0 disables heartbeat. */
  heartbeatIntervalMs?: number;
  /** Default 10000 milliseconds. */
  heartbeatTimeoutMs?: number;
  /** Default 10000 milliseconds. */
  sendTimeoutMs?: number;
  /** Default 128. */
  maxPendingInvocations?: number;
  /**
   * Global budget for event handlers and server RPC handlers. Default 32.
   * Timed-out handlers retain their slot until their actual promise settles.
   */
  maxConcurrentHandlers?: number;
  /** Default 1048576 bytes. */
  maxMessageBytes?: number;
  /** Default 2097152 bytes. */
  maxQueuedBytes?: number;
  /** Default 262144 bytes. */
  maxBufferedAmount?: number;
  /** Default 64 items per stream. */
  maxStreamBufferItems?: number;
  /** Default 1048576 bytes per stream. */
  maxStreamBufferBytes?: number;
  /** Default 64. */
  maxJsonDepth?: number;
  /** Default 100000. */
  maxJsonNodes?: number;
  reconnect?: ReconnectOptions;
}

export interface InvocationOptions {
  kwargs?: JsonObject;
  /** Total local deadline in milliseconds; 0 disables it. */
  timeoutMs?: number;
  /** Optional positive server deadline in seconds, sent as wire field timeout. */
  serverTimeout?: number;
  signal?: AbortSignal;
}

export interface RealtimeHandlerContext {
  readonly signal: AbortSignal;
  readonly connectionId: string;
}

export type RealtimeEventHandler = (args: JsonValue[]) => void | Promise<void>;
export type RealtimeMethodHandler = (
  args: JsonValue[],
  context: RealtimeHandlerContext,
) => JsonValue | void | Promise<JsonValue | void>;

export interface RealtimeStateChange {
  readonly state: RealtimeState;
  readonly previous: RealtimeState;
  readonly attempt: number;
}

/** Single-consumer iterator; only one pending next() is supported. */
export interface RealtimeStream extends AsyncIterableIterator<JsonValue> {
  next(): Promise<IteratorResult<JsonValue>>;
  /** Stops consumption and requests cancellation of the remote invocation. */
  return(value?: unknown): Promise<IteratorResult<JsonValue>>;
  throw(error?: unknown): Promise<IteratorResult<JsonValue>>;
  /** The reason is retained locally as AbortError.cause, not sent to the server. */
  cancel(reason?: unknown): void;
}

export interface RealtimeErrorOptions {
  code?: string;
  cause?: unknown;
}

export class RealtimeError extends Error {
  readonly code: string;
  constructor(message?: string, options?: RealtimeErrorOptions);
}

export class ConnectionError extends RealtimeError {
  constructor(message?: string, options?: RealtimeErrorOptions);
}

export class ProtocolError extends RealtimeError {
  constructor(message?: string, options?: RealtimeErrorOptions);
}

export class RemoteError extends RealtimeError {
  constructor(code: string, message: string);
}

export class TimeoutError extends RealtimeError {
  constructor(message?: string, options?: RealtimeErrorOptions);
}

export class AbortError extends RealtimeError {
  constructor(message?: string, options?: RealtimeErrorOptions);
}

export class LimitError extends RealtimeError {
  constructor(message?: string, options?: RealtimeErrorOptions);
}

export class RealtimeClient {
  constructor(url: RealtimeUrl, options?: RealtimeClientOptions);
  readonly state: RealtimeState;
  /** null unless state is connected. */
  readonly connectionId: string | null;
  /** Idempotent; resolves only after the v1 ready message. */
  connect(): Promise<void>;
  /** Restartable shutdown. Call connect() again to start a new connection. */
  stop(): Promise<void>;
  /** Permanent shutdown; this client cannot reconnect afterward. */
  dispose(): Promise<void>;
  /** Requires connected state; never queues or replays disconnected calls. */
  invoke(target: string, args?: JsonValue[], options?: InvocationOptions): Promise<JsonValue>;
  /** Eager: starts the invocation immediately; consume or cancel it. */
  stream(target: string, args?: JsonValue[], options?: InvocationOptions): RealtimeStream;
  on(target: string, handler: RealtimeEventHandler): Unsubscribe;
  /** Only one registered method handler per target is allowed. */
  handle(target: string, handler: RealtimeMethodHandler): Unsubscribe;
  /** Async observers coalesce: notifications are skipped while this handler is pending. */
  onState(handler: (change: RealtimeStateChange) => void): Unsubscribe;
  /** Async observers coalesce; at most 1024 unfinished observer callbacks are retained. */
  onError(handler: (error: RealtimeError) => void): Unsubscribe;
}
