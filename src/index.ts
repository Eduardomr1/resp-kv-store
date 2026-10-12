import { TCPServer } from './server.ts';

const port = process.env.PORT ? parseInt(process.env.PORT, 10) : 6379;
const host = process.env.HOST ?? '127.0.0.1';

const server = new TCPServer({ port, host });

async function main() {
  await server.start();
  console.log(`[RESP KV-Store] Server ready. Test using:`);
  console.log(`  redis-cli -p ${port} PING`);
  console.log(`  or via telnet/netcat: telnet ${host} ${port}`);

  const shutdown = async (signal: string) => {
    console.log(`\nReceived ${signal}. Shutting down gracefully...`);
    await server.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('Fatal server startup failure:', err);
  process.exit(1);
});
