import net from 'node:net';

interface AdminForwardState {
  client: net.Socket;
  paused: boolean;
  buffer: Buffer[];
}

/**
 * Test-only loopback TCP proxy in front of Postgres, scoped to one runtime's walsender
 * connection. Classifies the active WAL connection by sniffing the StartupMessage for the
 * `replication` parameter (present only on the walsender, not the admin pool) so
 * injectCopyDone()/dropClient() always target the right socket even across reconnects.
 */
export function startWalProxy(targetHost: string, targetPort: number) {
  let activeClient: net.Socket | null = null;
  const sockets = new Set<net.Socket>();
  const adminStates = new Set<AdminForwardState>();

  let stallArmed = false;
  let armedStallMs = 0;
  let stallTimer: ReturnType<typeof setTimeout> | null = null;

  // Flushes every currently-buffered admin connection FIFO and unpauses it — invoked either by
  // the timer naturally elapsing or by close() forcing an early release so no stall outlives the
  // proxy itself.
  const endStall = (): void => {
    if (stallTimer) {
      clearTimeout(stallTimer);
      stallTimer = null;
    }
    for (const state of adminStates) {
      state.paused = false;
      const buffered = state.buffer.splice(0);
      for (const chunk of buffered) {
        if (!state.client.destroyed) state.client.write(chunk);
      }
    }
  };

  const beginStall = (ms: number): void => {
    for (const state of adminStates) {
      state.paused = true;
    }
    stallTimer = setTimeout(endStall, ms);
  };

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
    let isReplication = false;
    const adminState: AdminForwardState = { client, paused: false, buffer: [] };
    adminStates.add(adminState);

    client.on('data', (chunk) => {
      if (!sawFirstChunk) {
        sawFirstChunk = true;
        if (chunk.includes('replication')) {
          activeClient = client;
          isReplication = true;
          adminStates.delete(adminState); // the replication connection is never stalled
        }
      }
      if (isReplication && stallArmed && chunk.includes('START_REPLICATION')) {
        stallArmed = false;
        beginStall(armedStallMs);
      }
      upstream.write(chunk);
    });

    // Manual upstream->client forwarding (mirrors the classification path above) so admin
    // connections can be paused mid-stream without dropping or reordering bytes.
    upstream.on('data', (chunk) => {
      if (adminState.paused) {
        adminState.buffer.push(chunk);
      } else {
        client.write(chunk);
      }
    });

    const cleanup = (): void => {
      sockets.delete(client);
      sockets.delete(upstream);
      adminStates.delete(adminState);
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
    // Arms a one-shot stall: the NEXT START_REPLICATION sent on the replication connection holds
    // every non-replication (admin) connection's upstream->client bytes for `ms`, buffered and
    // flushed FIFO when the stall ends.
    stallAdminOnStartReplication: (ms: number): void => {
      armedStallMs = ms;
      stallArmed = true;
    },
    close: (): Promise<void> =>
      new Promise((resolve) => {
        endStall();
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
