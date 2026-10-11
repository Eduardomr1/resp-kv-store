import test from 'node:test';
import assert from 'node:assert/strict';
import { PubSubHub, globToRegex } from '../src/pubsub/hub.ts';
import type { Subscriber } from '../src/pubsub/types.ts';
import { RESPParser } from '../src/resp/parser.ts';

class MockSubscriber implements Subscriber {
  public id: string;
  public messages: Buffer[] = [];
  public failOnWrite: boolean = false;

  constructor(id: string) {
    this.id = id;
  }

  public write(data: Buffer): boolean {
    if (this.failOnWrite) {
      throw new Error(`Write failed for ${this.id}`);
    }
    this.messages.push(data);
    return true;
  }

  public clear(): void {
    this.messages = [];
  }
}

test('PubSubHub: globToRegex translation', () => {
  const r1 = globToRegex('news.*');
  assert.equal(r1.test('news.world'), true);
  assert.equal(r1.test('news.tech'), true);
  assert.equal(r1.test('news.'), true);
  assert.equal(r1.test('news'), false);
  assert.equal(r1.test('other.news.world'), false);

  const r2 = globToRegex('chat.?');
  assert.equal(r2.test('chat.1'), true);
  assert.equal(r2.test('chat.x'), true);
  assert.equal(r2.test('chat.12'), false);
  assert.equal(r2.test('chat.'), false);

  const r3 = globToRegex('h[ae]llo');
  assert.equal(r3.test('hello'), true);
  assert.equal(r3.test('hallo'), true);
  assert.equal(r3.test('hxllo'), false);

  const r4 = globToRegex('sensor:[0-9]');
  assert.equal(r4.test('sensor:1'), true);
  assert.equal(r4.test('sensor:9'), true);
  assert.equal(r4.test('sensor:a'), false);

  const r5 = globToRegex('*');
  assert.equal(r5.test('anything'), true);
  assert.equal(r5.test(''), true);
});

test('PubSubHub: Exact channel subscription, publishing, and frame delivery', () => {
  const hub = new PubSubHub();
  const sub = new MockSubscriber('sub-1');

  // 1. Subscribe to 'news'
  const acks = hub.subscribe(sub, 'news');
  assert.equal(acks.length, 1);
  const expectedAck = Buffer.from('*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n', 'ascii');
  assert.deepEqual(acks[0], expectedAck);

  // Verify client state
  assert.equal(hub.isPubSubMode(sub), true);
  assert.equal(hub.getActiveSubscriptionCount(sub), 1);
  assert.equal(hub.getChannelCount(sub), 1);
  assert.equal(hub.getPatternCount(sub), 0);

  // 2. Publish string message to 'news'
  const count = hub.publish('news', 'breaking news');
  assert.equal(count, 1);
  assert.equal(sub.messages.length, 1);

  const expectedMsgFrame = Buffer.from('*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$13\r\nbreaking news\r\n', 'utf8');
  assert.deepEqual(sub.messages[0], expectedMsgFrame);

  // 3. Verify frame with RESP Parser
  const parser = new RESPParser();
  parser.feed(sub.messages[0]);
  const parsed = parser.parse();
  assert.equal(parsed.length, 1);
  assert.equal(parsed[0].type, 'array');
  const arr = (parsed[0] as any).value;
  assert.equal(arr[0].value.toString(), 'message');
  assert.equal(arr[1].value.toString(), 'news');
  assert.equal(arr[2].value.toString(), 'breaking news');

  // 4. Publish binary Buffer payload
  sub.clear();
  const binaryPayload = Buffer.from([0x00, 0xff, 0xaa, 0x55]);
  const countBin = hub.publish('news', binaryPayload);
  assert.equal(countBin, 1);
  assert.equal(sub.messages.length, 1);

  const parserBin = new RESPParser();
  parserBin.feed(sub.messages[0]);
  const parsedBin = parserBin.parse();
  const binArr = (parsedBin[0] as any).value;
  assert.deepEqual(binArr[2].value, binaryPayload);

  // 5. Publish to channel with no subscribers
  const emptyCount = hub.publish('empty-channel', 'no one hears');
  assert.equal(emptyCount, 0);

  // 6. Subscribing to multiple channels at once
  const multiAcks = hub.subscribe(sub, 'tech', 'sports');
  assert.equal(multiAcks.length, 2);
  assert.equal(multiAcks[0].toString(), '*3\r\n$9\r\nsubscribe\r\n$4\r\ntech\r\n:2\r\n');
  assert.equal(multiAcks[1].toString(), '*3\r\n$9\r\nsubscribe\r\n$6\r\nsports\r\n:3\r\n');
  assert.equal(hub.getActiveSubscriptionCount(sub), 3);
});

test('PubSubHub: Pattern subscription with glob wildcards and pmessage delivery', () => {
  const hub = new PubSubHub();
  const sub = new MockSubscriber('sub-pattern');

  // 1. Psubscribe to 'news.*' and 'chat.?'
  const acks = hub.psubscribe(sub, 'news.*', 'chat.?');
  assert.equal(acks.length, 2);
  assert.equal(acks[0].toString(), '*3\r\n$10\r\npsubscribe\r\n$6\r\nnews.*\r\n:1\r\n');
  assert.equal(acks[1].toString(), '*3\r\n$10\r\npsubscribe\r\n$6\r\nchat.?\r\n:2\r\n');

  assert.equal(hub.isPubSubMode(sub), true);
  assert.equal(hub.getPatternCount(sub), 2);
  assert.equal(hub.getChannelCount(sub), 0);

  // 2. Publish to 'news.tech' -> should match 'news.*'
  sub.clear();
  const receivers1 = hub.publish('news.tech', 'hello world');
  assert.equal(receivers1, 1);
  assert.equal(sub.messages.length, 1);

  const expectedPmsg = Buffer.from('*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$9\r\nnews.tech\r\n$11\r\nhello world\r\n', 'utf8');
  assert.deepEqual(sub.messages[0], expectedPmsg);

  // Parse pmessage with RESPParser
  const parser = new RESPParser();
  parser.feed(sub.messages[0]);
  const parsed = parser.parse();
  const arr = (parsed[0] as any).value;
  assert.equal(arr[0].value.toString(), 'pmessage');
  assert.equal(arr[1].value.toString(), 'news.*');
  assert.equal(arr[2].value.toString(), 'news.tech');
  assert.equal(arr[3].value.toString(), 'hello world');

  // 3. Publish to 'chat.1' -> matches 'chat.?'
  sub.clear();
  const receivers2 = hub.publish('chat.1', 'room 1 msg');
  assert.equal(receivers2, 1);
  assert.equal(sub.messages.length, 1);
  const p2 = new RESPParser();
  p2.feed(sub.messages[0]);
  const arr2 = (p2.parse()[0] as any).value;
  assert.equal(arr2[1].value.toString(), 'chat.?');
  assert.equal(arr2[2].value.toString(), 'chat.1');

  // 4. Publish to 'chat.12' -> should NOT match 'chat.?'
  sub.clear();
  const receivers3 = hub.publish('chat.12', 'room 12 msg');
  assert.equal(receivers3, 0);
  assert.equal(sub.messages.length, 0);

  // 5. Dual receipt: direct channel subscription + matching pattern
  const directSub = new MockSubscriber('sub-direct');
  hub.subscribe(directSub, 'news.tech');

  sub.clear();
  directSub.clear();
  const dualReceivers = hub.publish('news.tech', 'dual alert');
  assert.equal(dualReceivers, 2);

  // directSub got message frame
  assert.equal(directSub.messages.length, 1);
  assert.equal(directSub.messages[0].toString(), '*3\r\n$7\r\nmessage\r\n$9\r\nnews.tech\r\n$10\r\ndual alert\r\n');

  // sub got pmessage frame
  assert.equal(sub.messages.length, 1);
  assert.equal(sub.messages[0].toString(), '*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$9\r\nnews.tech\r\n$10\r\ndual alert\r\n');
});

test('PubSubHub: Multiple subscribers receiving the same broadcast', () => {
  const hub = new PubSubHub();
  const sub1 = new MockSubscriber('sub-1');
  const sub2 = new MockSubscriber('sub-2');
  const sub3 = new MockSubscriber('sub-3');

  hub.subscribe(sub1, 'alerts');
  hub.subscribe(sub2, 'alerts');
  hub.subscribe(sub3, 'alerts');

  assert.equal(hub.channels.get('alerts')?.size, 3);

  const receiverCount = hub.publish('alerts', 'critical system warning');
  assert.equal(receiverCount, 3);

  const expectedFrame = Buffer.from('*3\r\n$7\r\nmessage\r\n$6\r\nalerts\r\n$23\r\ncritical system warning\r\n', 'utf8');
  assert.deepEqual(sub1.messages[0], expectedFrame);
  assert.deepEqual(sub2.messages[0], expectedFrame);
  assert.deepEqual(sub3.messages[0], expectedFrame);

  // Mixed direct and pattern broadcast
  const patSub1 = new MockSubscriber('pat-1');
  const patSub2 = new MockSubscriber('pat-2');
  hub.psubscribe(patSub1, 'al*');
  hub.psubscribe(patSub2, '*erts');

  sub1.clear();
  sub2.clear();
  sub3.clear();
  patSub1.clear();
  patSub2.clear();

  const totalBroadcast = hub.publish('alerts', 'broadcast to all');
  assert.equal(totalBroadcast, 5); // 3 direct + 2 pattern
  assert.equal(sub1.messages.length, 1);
  assert.equal(sub2.messages.length, 1);
  assert.equal(sub3.messages.length, 1);
  assert.equal(patSub1.messages.length, 1);
  assert.equal(patSub2.messages.length, 1);

  // Failing subscriber does not prevent delivery to other subscribers
  const failingSub = new MockSubscriber('failing');
  failingSub.failOnWrite = true;
  hub.subscribe(failingSub, 'alerts');

  sub1.clear();
  const resilientReceivers = hub.publish('alerts', 'resilient');
  assert.equal(resilientReceivers, 6);
  assert.equal(sub1.messages.length, 1);
});

test('PubSubHub: Unsubscribe and punsubscribe behavior', () => {
  const hub = new PubSubHub();
  const sub = new MockSubscriber('sub-unsub');

  // Subscribe to three channels
  hub.subscribe(sub, 'c1', 'c2', 'c3');
  assert.equal(hub.getActiveSubscriptionCount(sub), 3);

  // 1. Unsubscribe from a single channel 'c2'
  const unsubAcks = hub.unsubscribe(sub, 'c2');
  assert.equal(unsubAcks.length, 1);
  assert.equal(unsubAcks[0].toString(), '*3\r\n$11\r\nunsubscribe\r\n$2\r\nc2\r\n:2\r\n');
  assert.equal(hub.getActiveSubscriptionCount(sub), 2);
  assert.deepEqual(hub.getSubscriberChannels(sub), ['c1', 'c3']);

  // Publish to 'c2' -> 0 receivers
  const countC2 = hub.publish('c2', 'no one');
  assert.equal(countC2, 0);

  // Publish to 'c1' -> delivered
  sub.clear();
  const countC1 = hub.publish('c1', 'hello c1');
  assert.equal(countC1, 1);
  assert.equal(sub.messages.length, 1);

  // 2. Unsubscribe without arguments -> unregisters all remaining channels
  const unsubAllAcks = hub.unsubscribe(sub);
  assert.equal(unsubAllAcks.length, 2);
  assert.equal(unsubAllAcks[0].toString(), '*3\r\n$11\r\nunsubscribe\r\n$2\r\nc1\r\n:1\r\n');
  assert.equal(unsubAllAcks[1].toString(), '*3\r\n$11\r\nunsubscribe\r\n$2\r\nc3\r\n:0\r\n');

  assert.equal(hub.isPubSubMode(sub), false);
  assert.equal(hub.getActiveSubscriptionCount(sub), 0);

  // 3. Unsubscribing when client has 0 subscriptions returns null bulk string $-1\r\n
  const emptyUnsubAcks = hub.unsubscribe(sub);
  assert.equal(emptyUnsubAcks.length, 1);
  assert.equal(emptyUnsubAcks[0].toString(), '*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n');

  // 4. Pattern unsubscriptions
  hub.psubscribe(sub, 'pat1.*', 'pat2.*');
  assert.equal(hub.getPatternCount(sub), 2);

  const punsubSingle = hub.punsubscribe(sub, 'pat1.*');
  assert.equal(punsubSingle.length, 1);
  assert.equal(punsubSingle[0].toString(), '*3\r\n$12\r\npunsubscribe\r\n$6\r\npat1.*\r\n:1\r\n');
  assert.equal(hub.getPatternCount(sub), 1);

  // Punsubscribe without arguments
  const punsubAll = hub.punsubscribe(sub);
  assert.equal(punsubAll.length, 1);
  assert.equal(punsubAll[0].toString(), '*3\r\n$12\r\npunsubscribe\r\n$6\r\npat2.*\r\n:0\r\n');
  assert.equal(hub.isPubSubMode(sub), false);

  // Punsubscribe when 0 pattern subscriptions returns null bulk string $-1\r\n
  const emptyPunsub = hub.punsubscribe(sub);
  assert.equal(emptyPunsub.length, 1);
  assert.equal(emptyPunsub[0].toString(), '*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n');
});

test('PubSubHub: Socket disconnection cleanup (removeSubscriber)', () => {
  const hub = new PubSubHub();
  const sub1 = new MockSubscriber('socket-1');
  const sub2 = new MockSubscriber('socket-2');

  hub.subscribe(sub1, 'chanA', 'chanB');
  hub.psubscribe(sub1, 'patA.*', 'patB.*');

  hub.subscribe(sub2, 'chanA');
  hub.psubscribe(sub2, 'patA.*');

  // Before disconnection
  const countsBefore = hub.numsub('chanA', 'chanB', 'chanNone');
  assert.equal(countsBefore.get('chanA'), 2);
  assert.equal(countsBefore.get('chanB'), 1);
  assert.equal(countsBefore.get('chanNone'), 0);
  assert.equal(hub.numpat(), 3); // sub1 has 2, sub2 has 1

  // Socket 1 disconnects
  hub.removeSubscriber(sub1);

  // After disconnection
  const countsAfter = hub.numsub('chanA', 'chanB');
  assert.equal(countsAfter.get('chanA'), 1);
  assert.equal(countsAfter.get('chanB'), 0);
  assert.equal(hub.numpat(), 1); // only sub2's pattern remains

  // chanB and patB.* entries should be removed from maps
  assert.equal(hub.channels.has('chanB'), false);
  assert.equal(hub.patterns.has('patB.*'), false);

  assert.equal(hub.isPubSubMode(sub1), false);
  assert.equal(hub.getActiveSubscriptionCount(sub1), 0);

  // Messages published to chanB return 0
  const count = hub.publish('chanB', 'message for closed socket');
  assert.equal(count, 0);

  // Socket 2 disconnects
  hub.removeSubscriber(sub2);
  assert.equal(hub.channels.has('chanA'), false);
  assert.equal(hub.patterns.has('patA.*'), false);
  assert.equal(hub.numpat(), 0);
});

test('PubSubHub: getChannels and client state helper introspection', () => {
  const hub = new PubSubHub();
  const sub = new MockSubscriber('sub-state');

  hub.subscribe(sub, 'user:100:feed', 'user:200:feed', 'system:logs');

  // List all channels
  const allChannels = hub.getChannels();
  assert.equal(allChannels.length, 3);
  assert.ok(allChannels.includes('user:100:feed'));
  assert.ok(allChannels.includes('user:200:feed'));
  assert.ok(allChannels.includes('system:logs'));

  // Filter channels with glob pattern
  const userFeeds = hub.getChannels('user:*:feed');
  assert.equal(userFeeds.length, 2);
  assert.ok(userFeeds.includes('user:100:feed'));
  assert.ok(userFeeds.includes('user:200:feed'));

  // Subscriber state snapshot
  const state = hub.getSubscriberState(sub);
  assert.equal(state.channelCount, 3);
  assert.equal(state.patternCount, 0);
  assert.equal(state.totalSubscriptions, 3);
  assert.equal(state.isPubSubMode, true);
  assert.equal(state.channels.has('system:logs'), true);
});
