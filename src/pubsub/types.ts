/**
 * PubSub Types and Interfaces for RESP Key-Value Store
 * Zero-dependency TypeScript implementation
 */

/**
 * PubSub subscriber interface representing any recipient that can receive RESP push messages.
 * Compatible with Node.js `net.Socket` and custom event sinks/mocks.
 */
export interface Subscriber {
  id?: string;
  write?(data: Buffer | Uint8Array | string): boolean | void;
  send?(data: Buffer): void;
  [key: string]: any;
}

/**
 * Tracks active subscription metrics and state for a subscriber.
 */
export interface SubscriberState {
  channels: Set<string>;
  patterns: Set<string>;
  readonly channelCount: number;
  readonly patternCount: number;
  readonly totalSubscriptions: number;
  readonly isPubSubMode: boolean;
}

/**
 * Subscription acknowledgment message action types.
 */
export type SubscriptionAckType = 'subscribe' | 'unsubscribe' | 'psubscribe' | 'punsubscribe';

/**
 * Structured representation of a subscription acknowledgment frame.
 */
export interface SubscriptionAckMessage {
  type: SubscriptionAckType;
  channelOrPattern: string | null;
  activeCount: number;
}

/**
 * PubSub push notification types.
 */
export type PubSubPushType = 'message' | 'pmessage';

/**
 * Direct channel push message payload.
 */
export interface ChannelPushMessage {
  type: 'message';
  channel: string;
  message: Buffer | string;
}

/**
 * Pattern-matched push message payload.
 */
export interface PatternPushMessage {
  type: 'pmessage';
  pattern: string;
  channel: string;
  message: Buffer | string;
}

/**
 * Union of all pubsub push message types.
 */
export type PubSubPushMessage = ChannelPushMessage | PatternPushMessage;

/**
 * Internal entry tracking compiled regex pattern and active subscribers.
 */
export interface PatternEntry {
  regex: RegExp;
  subscribers: Set<Subscriber>;
}
