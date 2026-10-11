/**
 * Zero-dependency Pub/Sub Messaging Engine for RESP Key-Value Store
 */

import { RESPSerializer } from '../resp/serializer.ts';
import type { Subscriber, SubscriberState, PatternEntry } from './types.ts';

/**
 * Converts a Redis glob-style pattern to a standard JavaScript RegExp.
 * Handles wildcards (*, ?), character classes ([abc], [a-z], [^a]), and escapes.
 */
export function globToRegex(pattern: string): RegExp {
  let regexStr = '^';
  let i = 0;
  while (i < pattern.length) {
    const c = pattern[i];
    if (c === '*') {
      regexStr += '.*';
      i++;
    } else if (c === '?') {
      regexStr += '.';
      i++;
    } else if (c === '[') {
      let j = i + 1;
      if (j < pattern.length && (pattern[j] === '^' || pattern[j] === '!')) {
        j++;
      }
      if (j < pattern.length && pattern[j] === ']') {
        j++;
      }
      while (j < pattern.length && pattern[j] !== ']') {
        if (pattern[j] === '\\' && j + 1 < pattern.length) {
          j += 2;
        } else {
          j++;
        }
      }
      if (j < pattern.length && pattern[j] === ']') {
        let inside = pattern.slice(i + 1, j);
        if (inside.startsWith('!')) {
          inside = '^' + inside.slice(1);
        }
        regexStr += '[' + inside + ']';
        i = j + 1;
      } else {
        regexStr += '\\[';
        i++;
      }
    } else if (c === '\\') {
      if (i + 1 < pattern.length) {
        regexStr += '\\' + pattern[i + 1];
        i += 2;
      } else {
        regexStr += '\\\\';
        i++;
      }
    } else if ('()+{}^$|.\\'.includes(c)) {
      regexStr += '\\' + c;
      i++;
    } else {
      regexStr += c;
      i++;
    }
  }
  regexStr += '$';
  return new RegExp(regexStr);
}

/**
 * PubSubHub coordinates channel and pattern-based message dissemination,
 * tracking subscriber states and encoding RESP notification frames.
 */
export class PubSubHub {
  /**
   * Direct channel subscriptions: Map<channel, Set<Subscriber>>
   */
  public readonly channels: Map<string, Set<Subscriber>> = new Map();

  /**
   * Pattern subscriptions: Map<pattern, { regex: RegExp; subscribers: Set<Subscriber> }>
   */
  public readonly patterns: Map<string, PatternEntry> = new Map();

  /**
   * Client state tracking: Map<Subscriber, { channels: Set<string>; patterns: Set<string> }>
   */
  private readonly subscriberStates: Map<Subscriber, { channels: Set<string>; patterns: Set<string> }> = new Map();

  /**
   * Delivers a RESP frame to a subscriber safely without throwing.
   */
  private deliver(subscriber: Subscriber, frame: Buffer): void {
    try {
      if (typeof subscriber.write === 'function') {
        subscriber.write(frame);
      } else if (typeof subscriber.send === 'function') {
        subscriber.send(frame);
      }
    } catch (err) {
      // Suppress delivery errors to prevent one bad socket from breaking broadcasts
    }
  }

  /**
   * Retrieves or initializes internal subscription state for a subscriber.
   */
  private getOrCreateState(subscriber: Subscriber): { channels: Set<string>; patterns: Set<string> } {
    let state = this.subscriberStates.get(subscriber);
    if (!state) {
      state = { channels: new Set<string>(), patterns: new Set<string>() };
      this.subscriberStates.set(subscriber, state);
    }
    return state;
  }

  /**
   * Returns a snapshot of a subscriber's subscription state.
   */
  public getSubscriberState(subscriber: Subscriber): SubscriberState {
    const state = this.subscriberStates.get(subscriber);
    const channelCount = state ? state.channels.size : 0;
    const patternCount = state ? state.patterns.size : 0;
    const totalSubscriptions = channelCount + patternCount;
    return {
      channels: new Set(state ? state.channels : []),
      patterns: new Set(state ? state.patterns : []),
      channelCount,
      patternCount,
      totalSubscriptions,
      isPubSubMode: totalSubscriptions > 0,
    };
  }

  /**
   * Checks if a subscriber has at least one active channel or pattern subscription.
   */
  public isPubSubMode(subscriber: Subscriber): boolean {
    const state = this.subscriberStates.get(subscriber);
    if (!state) return false;
    return state.channels.size + state.patterns.size > 0;
  }

  /**
   * Returns total active subscription count (channels + patterns) for a subscriber.
   */
  public getActiveSubscriptionCount(subscriber: Subscriber): number {
    const state = this.subscriberStates.get(subscriber);
    if (!state) return 0;
    return state.channels.size + state.patterns.size;
  }

  /**
   * Returns active channel count for a subscriber.
   */
  public getChannelCount(subscriber: Subscriber): number {
    const state = this.subscriberStates.get(subscriber);
    return state ? state.channels.size : 0;
  }

  /**
   * Returns active pattern count for a subscriber.
   */
  public getPatternCount(subscriber: Subscriber): number {
    const state = this.subscriberStates.get(subscriber);
    return state ? state.patterns.size : 0;
  }

  /**
   * Returns all channel names subscribed by a subscriber.
   */
  public getSubscriberChannels(subscriber: Subscriber): string[] {
    const state = this.subscriberStates.get(subscriber);
    return state ? Array.from(state.channels) : [];
  }

  /**
   * Returns all pattern strings subscribed by a subscriber.
   */
  public getSubscriberPatterns(subscriber: Subscriber): string[] {
    const state = this.subscriberStates.get(subscriber);
    return state ? Array.from(state.patterns) : [];
  }

  /**
   * Registers subscriber to one or more channels.
   * Returns RESP subscription notifications:
   * `*3\r\n$9\r\nsubscribe\r\n$<len>\r\n<channel>\r\n:<active_subs_count>\r\n`
   */
  public subscribe(subscriber: Subscriber, ...channels: string[]): Buffer[] {
    const notifications: Buffer[] = [];
    const state = this.getOrCreateState(subscriber);

    for (const channel of channels) {
      state.channels.add(channel);

      let subs = this.channels.get(channel);
      if (!subs) {
        subs = new Set();
        this.channels.set(channel, subs);
      }
      subs.add(subscriber);

      const activeCount = state.channels.size + state.patterns.size;
      const notification = RESPSerializer.encodeArray(['subscribe', channel, activeCount]);
      notifications.push(notification);
    }

    return notifications;
  }

  /**
   * Unregisters subscriber from specified channels, or from all if none provided.
   * Returns RESP unsubscribe notifications:
   * `*3\r\n$11\r\nunsubscribe\r\n$<len>\r\n<channel>\r\n:<active_subs_count>\r\n`
   */
  public unsubscribe(subscriber: Subscriber, ...channels: string[]): Buffer[] {
    const notifications: Buffer[] = [];
    const state = this.subscriberStates.get(subscriber);

    if (channels.length === 0) {
      // Unsubscribe from all active channels
      if (!state || state.channels.size === 0) {
        const activeCount = state ? state.patterns.size : 0;
        notifications.push(RESPSerializer.encodeArray(['unsubscribe', null, activeCount]));
      } else {
        const currentChannels = Array.from(state.channels);
        for (const channel of currentChannels) {
          state.channels.delete(channel);
          const subs = this.channels.get(channel);
          if (subs) {
            subs.delete(subscriber);
            if (subs.size === 0) {
              this.channels.delete(channel);
            }
          }
          const activeCount = state.channels.size + state.patterns.size;
          notifications.push(RESPSerializer.encodeArray(['unsubscribe', channel, activeCount]));
        }
      }
    } else {
      for (const channel of channels) {
        if (state && state.channels.has(channel)) {
          state.channels.delete(channel);
          const subs = this.channels.get(channel);
          if (subs) {
            subs.delete(subscriber);
            if (subs.size === 0) {
              this.channels.delete(channel);
            }
          }
        }
        const activeCount = state ? state.channels.size + state.patterns.size : 0;
        notifications.push(RESPSerializer.encodeArray(['unsubscribe', channel, activeCount]));
      }
    }

    // Clean up subscriber tracking if all subscriptions are cleared
    if (state && state.channels.size === 0 && state.patterns.size === 0) {
      this.subscriberStates.delete(subscriber);
    }

    return notifications;
  }

  /**
   * Registers glob pattern subscriptions.
   * Returns RESP psubscribe notifications:
   * `*3\r\n$10\r\npsubscribe\r\n$<len>\r\n<pattern>\r\n:<active_subs_count>\r\n`
   */
  public psubscribe(subscriber: Subscriber, ...patterns: string[]): Buffer[] {
    const notifications: Buffer[] = [];
    const state = this.getOrCreateState(subscriber);

    for (const pattern of patterns) {
      state.patterns.add(pattern);

      let entry = this.patterns.get(pattern);
      if (!entry) {
        entry = {
          regex: globToRegex(pattern),
          subscribers: new Set(),
        };
        this.patterns.set(pattern, entry);
      }
      entry.subscribers.add(subscriber);

      const activeCount = state.channels.size + state.patterns.size;
      const notification = RESPSerializer.encodeArray(['psubscribe', pattern, activeCount]);
      notifications.push(notification);
    }

    return notifications;
  }

  /**
   * Unregisters pattern subscriptions. If no patterns passed, unregisters from all patterns.
   * Returns RESP punsubscribe notifications:
   * `*3\r\n$12\r\npunsubscribe\r\n$<len>\r\n<pattern>\r\n:<active_subs_count>\r\n`
   */
  public punsubscribe(subscriber: Subscriber, ...patterns: string[]): Buffer[] {
    const notifications: Buffer[] = [];
    const state = this.subscriberStates.get(subscriber);

    if (patterns.length === 0) {
      // Unsubscribe from all patterns
      if (!state || state.patterns.size === 0) {
        const activeCount = state ? state.channels.size : 0;
        notifications.push(RESPSerializer.encodeArray(['punsubscribe', null, activeCount]));
      } else {
        const currentPatterns = Array.from(state.patterns);
        for (const pattern of currentPatterns) {
          state.patterns.delete(pattern);
          const entry = this.patterns.get(pattern);
          if (entry) {
            entry.subscribers.delete(subscriber);
            if (entry.subscribers.size === 0) {
              this.patterns.delete(pattern);
            }
          }
          const activeCount = state.channels.size + state.patterns.size;
          notifications.push(RESPSerializer.encodeArray(['punsubscribe', pattern, activeCount]));
        }
      }
    } else {
      for (const pattern of patterns) {
        if (state && state.patterns.has(pattern)) {
          state.patterns.delete(pattern);
          const entry = this.patterns.get(pattern);
          if (entry) {
            entry.subscribers.delete(subscriber);
            if (entry.subscribers.size === 0) {
              this.patterns.delete(pattern);
            }
          }
        }
        const activeCount = state ? state.channels.size + state.patterns.size : 0;
        notifications.push(RESPSerializer.encodeArray(['punsubscribe', pattern, activeCount]));
      }
    }

    // Clean up subscriber tracking if all subscriptions are cleared
    if (state && state.channels.size === 0 && state.patterns.size === 0) {
      this.subscriberStates.delete(subscriber);
    }

    return notifications;
  }

  /**
   * Delivers message to all direct channel subscribers AND all pattern subscribers whose pattern matches channel.
   * Message frame format: `*3\r\n$7\r\nmessage\r\n$<chanLen>\r\n<channel>\r\n$<msgLen>\r\n<message>\r\n`
   * Pmessage frame format: `*4\r\n$8\r\npmessage\r\n$<patLen>\r\n<pattern>\r\n$<chanLen>\r\n<channel>\r\n$<msgLen>\r\n<message>\r\n`
   * Returns the total count of receivers that received the message.
   */
  public publish(channel: string, message: Buffer | string): number {
    let receivers = 0;
    const msgFrame = RESPSerializer.encodeArray(['message', channel, message]);

    // 1. Deliver to direct channel subscribers
    const directSubs = this.channels.get(channel);
    if (directSubs && directSubs.size > 0) {
      for (const sub of directSubs) {
        this.deliver(sub, msgFrame);
        receivers++;
      }
    }

    // 2. Deliver to matching pattern subscribers
    for (const [pattern, entry] of this.patterns) {
      if (entry.regex.test(channel) && entry.subscribers.size > 0) {
        const pmsgFrame = RESPSerializer.encodeArray(['pmessage', pattern, channel, message]);
        for (const sub of entry.subscribers) {
          this.deliver(sub, pmsgFrame);
          receivers++;
        }
      }
    }

    return receivers;
  }

  /**
   * Cleans up all channel and pattern subscriptions when a socket disconnects.
   */
  public removeSubscriber(subscriber: Subscriber): void {
    const state = this.subscriberStates.get(subscriber);
    if (!state) return;

    for (const channel of state.channels) {
      const subs = this.channels.get(channel);
      if (subs) {
        subs.delete(subscriber);
        if (subs.size === 0) {
          this.channels.delete(channel);
        }
      }
    }

    for (const pattern of state.patterns) {
      const entry = this.patterns.get(pattern);
      if (entry) {
        entry.subscribers.delete(subscriber);
        if (entry.subscribers.size === 0) {
          this.patterns.delete(pattern);
        }
      }
    }

    this.subscriberStates.delete(subscriber);
  }

  /**
   * Returns subscriber counts per channel.
   */
  public numsub(...channels: string[]): Map<string, number> {
    const result = new Map<string, number>();
    for (const channel of channels) {
      const subs = this.channels.get(channel);
      result.set(channel, subs ? subs.size : 0);
    }
    return result;
  }

  /**
   * Returns total pattern subscriptions across all clients.
   */
  public numpat(): number {
    let total = 0;
    for (const entry of this.patterns.values()) {
      total += entry.subscribers.size;
    }
    return total;
  }

  /**
   * Lists all active channels with at least one subscriber, optionally matching a glob pattern.
   */
  public getChannels(pattern?: string): string[] {
    const channelNames = Array.from(this.channels.keys());
    if (!pattern) return channelNames;
    const regex = globToRegex(pattern);
    return channelNames.filter((name) => regex.test(name));
  }
}
