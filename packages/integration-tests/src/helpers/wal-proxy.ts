import net from 'node:net';

/**
 * Test-only loopback TCP proxy in front of Postgres, scoped to one runtime's walsender
 * connection. Classifies the active WAL connection by sniffing the StartupMessage for the
 * `replication` parameter (present only on the walsender, not the admin pool) so
 * injectCopyDone()/dropClient() always target the right socket even across reconnects.
 */
export function startWalProxy(targetHost: string, targetPort: number) {
  let activeClient: net.Socket | null = null;
  const sockets = new Set<net.Socket>();

  const server = net.createServer((client) => {
    const upstream = net.connect(targetPort, targetHost);
    sockets.add(client);
    sockets.add(upstream);
    client.on('error', () => {});
    upstream.on('error', () => {});

    // Classify only from the connection's first chunk (the StartupMessage) — later chunks on
    // an admin-pool connection can coincidentally contain the byte sequence "replication" (e.g.
    // a query against pg_replication_slots) and must not be allowed to steal activeClient.
    let sawFirstChunk = false;
    client.on('data', (chunk) => {
      if (!sawFirstChunk) {
        sawFirstChunk = true;
        if (chunk.includes('replication')) activeClient = client;
      }
      upstream.write(chunk);
    });
    upstream.pipe(client);

    const cleanup = (): void => {
      sockets.delete(client);
      sockets.delete(upstream);
      if (activeClient === client) activeClient = null;
    };
    client.on('close', () => {
      upstream.destroy();
      cleanup();
    });
    upstream.on('close', () => {
      client.destroy();
      cleanup();
    });
  });

  return {
    listen: (): Promise<number> =>
      new Promise((resolve) => {
        server.listen(0, '127.0.0.1', () => {
          resolve((server.address() as net.AddressInfo).port);
        });
      }),
    injectCopyDone: (): void => {
      activeClient?.write(Buffer.from([0x63, 0, 0, 0, 4]));
    },
    dropClient: (): void => {
      activeClient?.destroy();
    },
    close: (): Promise<void> =>
      new Promise((resolve) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => resolve());
      }),
  };
}

export function proxiedDatabaseUrl(baseUrl: string, port: number): string {
  const url = new URL(baseUrl);
  url.hostname = '127.0.0.1';
  url.port = String(port);
  return url.toString();
}
