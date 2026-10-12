/**
 * Zero-dependency Benchmark Harness for RESP Key-Value Engine
 * Measures operations/sec and latency for core primitives over native TCP sockets.
 */

import net from 'node:net';
import { performance } from 'node:perf_hooks';
import { TCPServer } from '../src/server.ts';
import { RESPSerializer } from '../src/resp/serializer.ts';

const PORT = 16390;
const HOST = '127.0.0.1';

async function runBenchmark() {
  console.log('='.repeat(70));
  console.log('  RESP IN-MEMORY STORAGE ENGINE - LOW-LATENCY BENCHMARK');
  console.log('  Platform: Node.js', process.version, '| OS:', process.platform, process.arch);
  console.log('='.repeat(70));

  const server = new TCPServer({ port: PORT, host: HOST });
  await server.start();

  const client = net.createConnection({ port: PORT, host: HOST });
  client.setNoDelay(true);
  await new Promise((resolve) => client.once('connect', resolve));

  async function executeRoundTrip(cmdBuffer: Buffer): Promise<Buffer> {
    return new Promise((resolve) => {
      client.once('data', resolve);
      client.write(cmdBuffer);
    });
  }

  // 1. PING Benchmark
  const pingCmd = Buffer.from('*1\r\n$4\r\nPING\r\n');
  const PING_COUNT = 10000;
  let start = performance.now();
  for (let i = 0; i < PING_COUNT; i++) {
    await executeRoundTrip(pingCmd);
  }
  let durationMs = performance.now() - start;
  let opsSec = Math.round((PING_COUNT / durationMs) * 1000);
  console.log(`[PING]        ${PING_COUNT.toLocaleString()} ops in ${durationMs.toFixed(1)}ms | ${opsSec.toLocaleString()} ops/sec | avg ${(durationMs / PING_COUNT * 1000).toFixed(2)} µs/op`);

  // 2. SET Benchmark (Sequential)
  const SET_COUNT = 10000;
  start = performance.now();
  for (let i = 0; i < SET_COUNT; i++) {
    const key = `key:${i}`;
    const val = `value_${i}_payload`;
    const frame = RESPSerializer.encodeArray([Buffer.from('SET'), Buffer.from(key), Buffer.from(val)]);
    await executeRoundTrip(frame);
  }
  durationMs = performance.now() - start;
  opsSec = Math.round((SET_COUNT / durationMs) * 1000);
  console.log(`[SET]         ${SET_COUNT.toLocaleString()} ops in ${durationMs.toFixed(1)}ms | ${opsSec.toLocaleString()} ops/sec | avg ${(durationMs / SET_COUNT * 1000).toFixed(2)} µs/op`);

  // 3. GET Benchmark (Sequential)
  const GET_COUNT = 10000;
  start = performance.now();
  for (let i = 0; i < GET_COUNT; i++) {
    const key = `key:${i}`;
    const frame = RESPSerializer.encodeArray([Buffer.from('GET'), Buffer.from(key)]);
    await executeRoundTrip(frame);
  }
  durationMs = performance.now() - start;
  opsSec = Math.round((GET_COUNT / durationMs) * 1000);
  console.log(`[GET]         ${GET_COUNT.toLocaleString()} ops in ${durationMs.toFixed(1)}ms | ${opsSec.toLocaleString()} ops/sec | avg ${(durationMs / GET_COUNT * 1000).toFixed(2)} µs/op`);

  // 4. HSET & HGET Benchmark
  const HASH_COUNT = 10000;
  start = performance.now();
  for (let i = 0; i < HASH_COUNT; i++) {
    const frame = RESPSerializer.encodeArray([Buffer.from('HSET'), Buffer.from('bench:hash'), Buffer.from(`field:${i}`), Buffer.from(`data_${i}`)]);
    await executeRoundTrip(frame);
  }
  durationMs = performance.now() - start;
  opsSec = Math.round((HASH_COUNT / durationMs) * 1000);
  console.log(`[HSET]        ${HASH_COUNT.toLocaleString()} ops in ${durationMs.toFixed(1)}ms | ${opsSec.toLocaleString()} ops/sec | avg ${(durationMs / HASH_COUNT * 1000).toFixed(2)} µs/op`);

  // 5. Pipelining Benchmark (Batches of 100 commands)
  const PIPELINE_TOTAL = 50000;
  const BATCH_SIZE = 100;
  start = performance.now();
  for (let b = 0; b < PIPELINE_TOTAL / BATCH_SIZE; b++) {
    const batchFrames: Buffer[] = [];
    for (let i = 0; i < BATCH_SIZE; i++) {
      const idx = b * BATCH_SIZE + i;
      batchFrames.push(RESPSerializer.encodeArray([Buffer.from('SET'), Buffer.from(`pipe:${idx}`), Buffer.from(`data_${idx}`)]));
    }
    const combined = Buffer.concat(batchFrames);

    // Write full pipeline batch and wait for all responses
    let receivedBytes = 0;
    const expectedBytes = BATCH_SIZE * 5; // '+OK\r\n' is 5 bytes
    await new Promise<void>((resolve) => {
      const onData = (chunk: Buffer) => {
        receivedBytes += chunk.length;
        if (receivedBytes >= expectedBytes) {
          client.off('data', onData);
          resolve();
        }
      };
      client.on('data', onData);
      client.write(combined);
    });
  }
  durationMs = performance.now() - start;
  opsSec = Math.round((PIPELINE_TOTAL / durationMs) * 1000);
  console.log(`[PIPELINE]    ${PIPELINE_TOTAL.toLocaleString()} ops in ${durationMs.toFixed(1)}ms | ${opsSec.toLocaleString()} ops/sec | avg ${(durationMs / PIPELINE_TOTAL * 1000).toFixed(2)} µs/op`);

  console.log('='.repeat(70));
  console.log('  BENCHMARK COMPLETED SUCCESSFULLY');
  console.log('='.repeat(70));

  client.destroy();
  await server.stop();
  process.exit(0);
}

runBenchmark().catch((err) => {
  console.error('[Benchmark Error]', err);
  process.exit(1);
});
