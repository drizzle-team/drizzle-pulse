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
 * dropClient() always targets the right socket even across reconnects.
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

    // Classify only once the whole StartupMessage is in hand — a bare first chunk can split the
    // message mid-parameter (or land only the leading length prefix), and "replication" straddling
    // a TCP segment boundary must not silently fall through to admin classification. Later chunks
    // on an admin-pool connection can coincidentally contain the byte sequence "replication" (e.g.
    // a query against pg_replication_slots) and must not be allowed to steal activeClient, so
    // classification is one-shot once the full startup message is assembled.
    let classified = false;
    let isReplication = false;
    let startupChunks: Buffer[] = [];
    let startupBytesSeen = 0;
    let startupLength: number | null = null;
    const adminState: AdminForwardState = { client, paused: false, buffer: [] };
    adminStates.add(adminState);

    client.on('data', (chunk) => {
      if (!classified) {
        startupChunks.push(chunk);
        startupBytesSeen += chunk.length;
        const startup = Buffer.concat(startupChunks);
        if (startupLength === null && startupBytesSeen >= 4) {
          startupLength = startup.readInt32BE(0);
        }
        // Bail out past any sane StartupMessage size so a malformed/non-startup lead-in can't
        // grow this buffer unboundedly.
        if (
          (startupLength !== null && startupBytesSeen >= startupLength) ||
          startupBytesSeen > 65536
        ) {
          classified = true;
          if (startup.includes('replication')) {
            activeClient = client;
            isReplication = true;
            adminStates.delete(adminState); // the replication connection is never stalled
          }
          startupChunks = [];
        }
      }
      // Arm on rep.start()'s FIRST walsender command, not literally START_REPLICATION. On PG14+
      // minipg precedes START_REPLICATION with a binary-negotiation catalog probe (a `Q` query
      // against pg_publication_tables); waiting for START_REPLICATION would arm one round-trip too
      // late — past the moment the snapshot session's admin-pool SELECT already flew — so the stall
      // would miss the read it must hold. The probe query is the same choreography point
      // START_REPLICATION used to be (rep.start()'s opening frame); the earlier CREATE_REPLICATION_SLOT
      // that recoverSlot sends before seeding contains neither marker, so seeding is never stalled.
      if (
        isReplication &&
        stallArmed &&
        (chunk.includes('pg_publication_tables') || chunk.includes('START_REPLICATION'))
      ) {
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
    dropClient: (): void => {
      activeClient?.destroy();
    },
    // Arms a one-shot stall: the NEXT rep.start() on the replication connection (recognized by its
    // opening frame — the binary-negotiation probe on PG14+, else START_REPLICATION) holds every
    // non-replication (admin) connection's upstream->client bytes for `ms`, buffered and flushed
    // FIFO when the stall ends.
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
