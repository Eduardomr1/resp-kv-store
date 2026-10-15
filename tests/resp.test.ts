import test from 'node:test';
import assert from 'node:assert/strict';
import { RESPParser, ProtocolError } from '../src/resp/parser.ts';
import { RESPSerializer, RESP_CONSTANTS } from '../src/resp/serializer.ts';
import type { RESPValue } from '../src/resp/types.ts';

test('RESP Parser: Simple Strings, Errors, Integers', () => {
  const parser = new RESPParser();

  parser.feed(Buffer.from('+OK\r\n-ERR unknown command\r\n:1042\r\n:-99\r\n'));
  const values = parser.parse();

  assert.equal(values.length, 4);
  assert.deepEqual(values[0], { type: 'simple_string', value: 'OK' });
  assert.deepEqual(values[1], { type: 'error', value: 'ERR unknown command' });
  assert.deepEqual(values[2], { type: 'integer', value: 1042 });
  assert.deepEqual(values[3], { type: 'integer', value: -99 });
});

test('RESP Parser: Bulk Strings with Binary Data & Nulls', () => {
  const parser = new RESPParser();

  // Test empty, normal, null, and binary with embedded CRLF & \0
  const binaryPayload = Buffer.from('hello\r\n\0world', 'binary');
  const buffer = Buffer.concat([
    Buffer.from('$0\r\n\r\n'),
    Buffer.from('$5\r\nworld\r\n'),
    Buffer.from('$-1\r\n'),
    Buffer.from(`$${binaryPayload.length}\r\n`),
    binaryPayload,
    Buffer.from('\r\n'),
  ]);

  parser.feed(buffer);
  const values = parser.parse();

  assert.equal(values.length, 4);
  assert.equal(values[0].type, 'bulk_string');
  assert.equal((values[0] as any).value.length, 0);

  assert.equal(values[1].type, 'bulk_string');
  assert.equal((values[1] as any).value.toString(), 'world');

  assert.equal(values[2].type, 'null');

  assert.equal(values[3].type, 'bulk_string');
  assert.deepEqual((values[3] as any).value, binaryPayload);
});

test('RESP Parser: Arrays and Nested Arrays', () => {
  const parser = new RESPParser();

  const raw = '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*0\r\n*-1\r\n';
  parser.feed(Buffer.from(raw));
  const values = parser.parse();

  assert.equal(values.length, 3);
  assert.equal(values[0].type, 'array');
  const arr = (values[0] as any).value;
  assert.equal(arr.length, 3);
  assert.equal(arr[0].value.toString(), 'SET');
  assert.equal(arr[1].value.toString(), 'foo');
  assert.equal(arr[2].value.toString(), 'bar');

  assert.deepEqual(values[1], { type: 'array', value: [] });
  assert.deepEqual(values[2], { type: 'null' });
});

test('RESP Parser: TCP Chunk Fragmentation (Streaming edge-case)', () => {
  const parser = new RESPParser();

  const command = '*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n';
  const fullBuffer = Buffer.from(command);

  // Feed byte by byte
  const parsedAtEnd: RESPValue[] = [];
  for (let i = 0; i < fullBuffer.length; i++) {
    parser.feed(fullBuffer.subarray(i, i + 1));
    const res = parser.parse();
    parsedAtEnd.push(...res);
  }

  assert.equal(parsedAtEnd.length, 1);
  const cmd = parsedAtEnd[0] as any;
  assert.equal(cmd.type, 'array');
  assert.equal(cmd.value.length, 2);
  assert.equal(cmd.value[0].value.toString(), 'ECHO');
  assert.equal(cmd.value[1].value.toString(), 'hello world');
});

test('RESP Parser: Pipelined Commands in Single Packet', () => {
  const parser = new RESPParser();

  const pipelined =
    '*1\r\n$4\r\nPING\r\n' +
    '*1\r\n$4\r\nPING\r\n' +
    '*2\r\n$4\r\nECHO\r\n$2\r\nHI\r\n';

  parser.feed(Buffer.from(pipelined));
  const values = parser.parse();

  assert.equal(values.length, 3);
  assert.equal((values[0] as any).value[0].value.toString(), 'PING');
  assert.equal((values[1] as any).value[0].value.toString(), 'PING');
  assert.equal((values[2] as any).value[0].value.toString(), 'ECHO');
});

test('RESP Parser: Inline Telnet Commands', () => {
  const parser = new RESPParser();

  parser.feed(Buffer.from('PING\r\nSET user "John Doe"\r\n'));
  const values = parser.parse();

  assert.equal(values.length, 2);

  // PING
  assert.equal(values[0].type, 'array');
  assert.equal((values[0] as any).value[0].value.toString(), 'PING');

  // SET user "John Doe"
  assert.equal(values[1].type, 'array');
  const setCmd = (values[1] as any).value;
  assert.equal(setCmd.length, 3);
  assert.equal(setCmd[0].value.toString(), 'SET');
  assert.equal(setCmd[1].value.toString(), 'user');
  assert.equal(setCmd[2].value.toString(), 'John Doe');
});

test('RESP Serializer: Standard Encoding', () => {
  assert.deepEqual(RESPSerializer.encodeSimpleString('OK'), RESP_CONSTANTS.OK);
  assert.equal(RESPSerializer.encodeSimpleString('PONG').toString(), '+PONG\r\n');
  assert.equal(RESPSerializer.encodeError('WRONGTYPE invalid').toString(), '-WRONGTYPE invalid\r\n');
  assert.equal(RESPSerializer.encodeError('unknown command').toString(), '-ERR unknown command\r\n');
  assert.equal(RESPSerializer.encodeInteger(42).toString(), ':42\r\n');
  assert.equal(RESPSerializer.encodeBulkString(null).toString(), '$-1\r\n');
  assert.equal(RESPSerializer.encodeBulkString('test').toString(), '$4\r\ntest\r\n');
  assert.equal(
    RESPSerializer.encodeArray(['foo', 123, null]).toString(),
    '*3\r\n$3\r\nfoo\r\n:123\r\n$-1\r\n'
  );
});

test('RESP Parser: Strict Length & Integer Validation rejects malformed headers', () => {
  const parser = new RESPParser();

  assert.throws(() => {
    parser.feed(Buffer.from('$5abc\r\nhello\r\n'));
    parser.parse();
  }, ProtocolError);

  assert.throws(() => {
    parser.feed(Buffer.from('*2xyz\r\n$1\r\na\r\n$1\r\nb\r\n'));
    parser.parse();
  }, ProtocolError);

  assert.throws(() => {
    parser.feed(Buffer.from(':\r\n'));
    parser.parse();
  }, ProtocolError);

  assert.throws(() => {
    parser.feed(Buffer.from(':12.5\r\n'));
    parser.parse();
  }, ProtocolError);
});

test('RESP Parser: Enforces maxBulkLength, maxArrayLength, maxDepth, and maxInlineLength', () => {
  // 1. maxBulkLength rejects oversized header before buffering full payload
  const bulkParser = new RESPParser({ maxBulkLength: 32 });
  assert.throws(() => {
    bulkParser.feed(Buffer.from('$1024\r\n'));
    bulkParser.parse();
  }, /exceeds maximum allowed 32/);

  // 2. maxArrayLength rejects excessive element count
  const arrParser = new RESPParser({ maxArrayLength: 8 });
  assert.throws(() => {
    arrParser.feed(Buffer.from('*100\r\n'));
    arrParser.parse();
  }, /exceeds maximum allowed 8/);

  // 3. maxDepth rejects deeply nested arrays
  const depthParser = new RESPParser({ maxDepth: 3 });
  assert.throws(() => {
    depthParser.feed(Buffer.from('*1\r\n*1\r\n*1\r\n*1\r\n+OK\r\n'));
    depthParser.parse();
  }, /nesting depth exceeds maximum allowed 3/);

  // 4. maxInlineLength rejects unbounded unterminated lines
  const lineParser = new RESPParser({ maxInlineLength: 16 });
  assert.throws(() => {
    lineParser.feed(Buffer.from('PING_WITHOUT_CRLF_EXCEEDING_LIMIT'));
    lineParser.parse();
  }, /exceeds maximum allowed 16/);
});

test('RESP Parser: Inline Command Escapes, Single Quotes, and Unbalanced Quote Rejection', () => {
  const parser = new RESPParser();
  parser.feed(Buffer.from('SET msg "hello \\"world\\"\\nnext" \'literal quote\'\r\n'));
  const frames = parser.parse();
  assert.equal(frames.length, 1);
  const arr = (frames[0] as any).value;
  assert.equal(arr.length, 4);
  assert.equal(arr[0].value.toString(), 'SET');
  assert.equal(arr[1].value.toString(), 'msg');
  assert.equal(arr[2].value.toString(), 'hello "world"\nnext');
  assert.equal(arr[3].value.toString(), 'literal quote');

  assert.throws(() => {
    parser.feed(Buffer.from('SET key "unclosed quote\r\n'));
    parser.parse();
  }, /Unbalanced quotes/);
});
