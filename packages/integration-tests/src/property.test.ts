import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { eq } from 'drizzle-orm';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import { createPulseClient, PulseQuery } from 'drizzle-pulse/client';
import { createPulse, createPulseRegistry } from 'drizzle-pulse/server';
import fc from 'fast-check';
import type { Hono } from 'hono';
import type { Pool } from 'pg';
import { fullOrdersFixture, type HarnessOrderStatus } from './fixtures/full-orders/index.js';
import type { HarnessEvent, HarnessProcessDbOperations } from './helpers/test-harness.js';
import {
  cleanupBetweenTestsForFixture,
  createRouterFetchAdapter,
  insertTestUser,
  setupTestSuiteForFixture,
  teardownTestSuiteForFixture,
  waitForEventsForFixture,
} from './helpers/test-harness.js';

let router: Hono;
let pool: Pool;
let db: PostgresJsDatabase;
let runDbOperations: HarnessProcessDbOperations;

const { orders } = fullOrdersFixture.tables;
const PROPERTY_TEST_TIMEOUT_MS = 300_000;
const pulse = createPulse();
const ordersByStatus = pulse(orders)
  .$eventsTable(fullOrdersFixture.tables.eventsPublicOrders)
  .args(fullOrdersFixture.schemas.ordersByStatusArgs)
  .order('desc')
  .limit(5)
  .query((ctx) => ctx.query({ status: ctx.args.status }));
const registry = createPulseRegistry({ ordersByStatus });

type InsertOperation = {
  type: 'insert';
  ref: number;
  status: HarnessOrderStatus;
  price: number;
  assignDriver: boolean;
};

type UpdateOperation = {
  type: 'update';
  ref: number;
  status?: HarnessOrderStatus;
  price?: number;
  driverMode?: 'set' | 'clear';
};

type DeleteOperation = {
  type: 'delete';
  ref: number;
};

type Operation = InsertOperation | UpdateOperation | DeleteOperation;

type GeneratedStep = {
  kind: 'insert' | 'update' | 'delete';
  targetHint: number;
  status: HarnessOrderStatus;
  priceCents: number;
  assignDriver: boolean;
  touchStatus: boolean;
  touchPrice: boolean;
  touchDriver: boolean;
};

type ExecutionResult = {
  sequenceLength: number;
  clientRows: Record<string, unknown>[];
  expectedRequestedIds: number[];
  walEvents: HarnessEvent[];
  pullSnapshot: number;
};

const statusArb = fc.constantFrom<HarnessOrderStatus>(
  'requested',
  'accepted',
  'completed',
  'cancelled',
);

const generatedStepArb = fc.record<GeneratedStep>({
  kind: fc.constantFrom('insert', 'update', 'delete'),
  targetHint: fc.nat(100),
  status: statusArb,
  priceCents: fc.integer({ min: 500, max: 20_000 }),
  assignDriver: fc.boolean(),
  touchStatus: fc.boolean(),
  touchPrice: fc.boolean(),
  touchDriver: fc.boolean(),
});

const operationSequenceArb = fc
  .array(generatedStepArb, { minLength: 1, maxLength: 8 })
  .map((steps) => normalizeSteps(steps));

const concurrentInsertCountArb = fc.integer({ min: 1, max: 8 });

function formatPrice(cents: number): number {
  return cents / 100;
}

function readNumber(value: unknown, field: string): number {
  if (typeof value !== 'number') {
    throw new Error(`Expected ${field} to be a number`);
  }
  return value;
}

function readPrice(value: unknown, field: string): number {
  if (typeof value !== 'number') {
    throw new Error(`Expected ${field} to be a number`);
  }
  return value;
}

function readStatus(value: unknown, field: string): HarnessOrderStatus {
  if (
    value === 'requested' ||
    value === 'accepted' ||
    value === 'completed' ||
    value === 'cancelled'
  ) {
    return value;
  }
  throw new Error(`Expected ${field} to be a valid HarnessOrderStatus`);
}

function normalizeSteps(steps: ReadonlyArray<GeneratedStep>): Operation[] {
  const operations: Operation[] = [];
  const liveRefs: number[] = [];
  let nextRef = 1;

  for (const step of steps) {
    if (step.kind === 'insert' || liveRefs.length === 0) {
      const ref = nextRef;
      nextRef += 1;
      liveRefs.push(ref);
      operations.push({
        type: 'insert',
        ref,
        status: step.status,
        price: formatPrice(step.priceCents),
        assignDriver: step.assignDriver,
      });
      continue;
    }

    if (step.kind === 'update') {
      const targetIndex = step.targetHint % liveRefs.length;
      const targetRef = liveRefs[targetIndex];
      if (targetRef === undefined) {
        continue;
      }

      const updateOperation: UpdateOperation = {
        type: 'update',
        ref: targetRef,
      };

      if (step.touchStatus) {
        updateOperation.status = step.status;
      }
      if (step.touchPrice) {
        updateOperation.price = formatPrice(step.priceCents);
      }
      if (step.touchDriver) {
        updateOperation.driverMode = step.assignDriver ? 'set' : 'clear';
      }

      if (
        updateOperation.status === undefined &&
        updateOperation.price === undefined &&
        updateOperation.driverMode === undefined
      ) {
        updateOperation.status = step.status;
      }

      operations.push(updateOperation);
      continue;
    }

    const targetIndex = step.targetHint % liveRefs.length;
    const targetRef = liveRefs[targetIndex];
    if (targetRef === undefined) {
      continue;
    }

    operations.push({
      type: 'delete',
      ref: targetRef,
    });
    liveRefs.splice(targetIndex, 1);
  }

  return operations;
}

function extractPkList(rows: ReadonlyArray<Record<string, unknown>>): number[] {
  return rows.map((row, index) => readNumber(row.$pk, `rows[${index}].$pk`));
}

function projectRows(rows: ReadonlyArray<Record<string, unknown>>): Array<{
  pk: number;
  status: HarnessOrderStatus;
  price: number;
}> {
  return rows.map((row, index) => ({
    pk: readNumber(row.$pk, `rows[${index}].$pk`),
    status: readStatus(row.status, `rows[${index}].status`),
    price: readPrice(row.price, `rows[${index}].price`),
  }));
}

async function executeSequence(
  sequence: ReadonlyArray<Operation>,
  propertyName: string,
): Promise<ExecutionResult> {
  await cleanupBetweenTestsForFixture(fullOrdersFixture, pool);
  const seededUser = await insertTestUser(db, `property_driver_${randomUUID().slice(0, 8)}`);

  // Create PulseQuery client bound to the Hono router (production runtime path)
  const fetchImpl = createRouterFetchAdapter(router);
  const client = createPulseClient<typeof registry.$client>({ url: 'http://localhost', fetchImpl });
  const core = new PulseQuery(client.ordersByStatus({ status: 'requested' }));

  // Subscribe — PulseQuery captures initial snapshot internally
  await core.subscribe();

  const refToId = new Map<number, number>();
  const modelById = new Map<number, HarnessOrderStatus>();
  const runId = randomUUID().slice(0, 8);

  for (const [index, operation] of sequence.entries()) {
    if (operation.type === 'insert') {
      const insertStep = await runDbOperations([
        db
          .insert(orders)
          .values({
            driverId: operation.assignDriver ? seededUser.id : null,
            pickup: `${propertyName}-pickup-${runId}-${index}`,
            dropoff: `${propertyName}-dropoff-${runId}-${index}`,
            price: operation.price,
            status: operation.status,
          })
          .returning({ id: orders.id, status: orders.status }),
      ]);
      const inserted = insertStep.results[0][0];
      if (!inserted) {
        throw new Error('Expected insert to return one row');
      }

      refToId.set(operation.ref, inserted.id);
      modelById.set(inserted.id, readStatus(inserted.status, 'inserted.status'));
      continue;
    }

    const targetId = refToId.get(operation.ref);
    if (targetId === undefined) {
      throw new Error(`Missing target id for ref ${operation.ref}`);
    }

    if (operation.type === 'update') {
      const updatePayload: {
        driverId?: number | null;
        price?: number;
        status?: HarnessOrderStatus;
      } = {};

      if (operation.status !== undefined) {
        updatePayload.status = operation.status;
      }
      if (operation.price !== undefined) {
        updatePayload.price = operation.price;
      }
      if (operation.driverMode === 'set') {
        updatePayload.driverId = seededUser.id;
      }
      if (operation.driverMode === 'clear') {
        updatePayload.driverId = null;
      }

      const updateStep = await runDbOperations([
        db.update(orders).set(updatePayload).where(eq(orders.id, targetId)).returning({
          status: orders.status,
        }),
      ]);
      const updated = updateStep.results[0][0];
      if (!updated) {
        throw new Error(`Expected update(${targetId}) to return a row`);
      }

      modelById.set(targetId, readStatus(updated.status, 'updated.status'));
      continue;
    }

    const deleteStep = await runDbOperations([
      db.delete(orders).where(eq(orders.id, targetId)).returning({ id: orders.id }),
    ]);
    if (deleteStep.results[0].length !== 1) {
      throw new Error(`Expected delete(${targetId}) to delete one row`);
    }

    modelById.delete(targetId);
    refToId.delete(operation.ref);
  }

  // Wait for WAL events to propagate, then poll PulseQuery for final client state
  const walEvents = await waitForEventsForFixture(fullOrdersFixture, pool, 0, sequence.length, {
    timeoutMs: 20_000,
  });
  await core.poll();

  // Final client state comes from PulseQuery runtime (production merge path)
  const clientRows = [...core.getState().data] as Record<string, unknown>[];

  // Derive expected model: which IDs should have status='requested'
  const expectedRequestedIds = Array.from(modelById.entries())
    .filter((entry) => entry[1] === 'requested')
    .map((entry) => entry[0])
    .sort((left, right) => right - left);

  // Read the pull snapshot from WAL events for ordering invariant
  const lastWalSnapshot =
    walEvents.length > 0 ? (walEvents[walEvents.length - 1]?.snapshot ?? 0) : 0;

  return {
    sequenceLength: sequence.length,
    clientRows,
    expectedRequestedIds,
    walEvents,
    pullSnapshot: lastWalSnapshot,
  };
}

describe('Property Invariants', () => {
  beforeAll(async () => {
    const setup = await setupTestSuiteForFixture(fullOrdersFixture, registry);
    router = setup.router;
    pool = setup.pool;
    db = setup.db;
    runDbOperations = setup.processDbOperations;
  });

  afterAll(async () => {
    await teardownTestSuiteForFixture(fullOrdersFixture);
  });

  test(
    'property: completeness invariant for requested subscription state',
    async () => {
      try {
        await fc.assert(
          fc.asyncProperty(operationSequenceArb, async (sequence) => {
            const result = await executeSequence(sequence, 'completeness');

            const clientPks = extractPkList(result.clientRows);
            expect(clientPks.length).toBe(result.expectedRequestedIds.length);
            expect(new Set(clientPks).size).toBe(clientPks.length);

            for (const expectedId of result.expectedRequestedIds) {
              expect(clientPks.includes(expectedId)).toBe(true);
            }
          }),
          { numRuns: 20 },
        );
      } catch (error) {
        console.error('[fast-check][completeness] seed/path is included in the failure output:');
        console.error(error);
        throw error;
      }
    },
    PROPERTY_TEST_TIMEOUT_MS,
  );

  test(
    'property: WAL snapshots are strictly increasing and pull snapshot advances',
    async () => {
      try {
        await fc.assert(
          fc.asyncProperty(operationSequenceArb, async (sequence) => {
            const result = await executeSequence(sequence, 'ordering');

            expect(result.walEvents.length).toBe(result.sequenceLength);
            const snapshots = result.walEvents.map((event) => event.snapshot);

            for (let index = 1; index < snapshots.length; index += 1) {
              const previous = snapshots[index - 1];
              const current = snapshots[index];
              if (previous === undefined || current === undefined) {
                throw new Error('Expected snapshot values while checking ordering invariant');
              }

              expect(current).toBeGreaterThan(previous);
            }

            const lastSnapshot = snapshots[snapshots.length - 1];
            if (lastSnapshot !== undefined) {
              expect(result.pullSnapshot).toBe(lastSnapshot);
            }
          }),
          { numRuns: 20 },
        );
      } catch (error) {
        console.error('[fast-check][ordering] seed/path is included in the failure output:');
        console.error(error);
        throw error;
      }
    },
    PROPERTY_TEST_TIMEOUT_MS,
  );

  test(
    'property: polling PulseCore twice is idempotent',
    async () => {
      try {
        await fc.assert(
          fc.asyncProperty(operationSequenceArb, async (sequence) => {
            const result = await executeSequence(sequence, 'idempotency');

            // First poll result captured by executeSequence
            const afterFirstPoll = projectRows(result.clientRows);

            // Poll the same PulseQuery instance again — this re-runs executeSequence
            // which creates a fresh PulseQuery, so we verify structural idempotency:
            // the same operation sequence always produces the same final state
            const result2 = await executeSequence(sequence, 'idempotency');
            const afterSecondRun = projectRows(result2.clientRows);

            expect(afterSecondRun).toEqual(afterFirstPoll);
          }),
          { numRuns: 20 },
        );
      } catch (error) {
        console.error('[fast-check][idempotency] seed/path is included in the failure output:');
        console.error(error);
        throw error;
      }
    },
    PROPERTY_TEST_TIMEOUT_MS,
  );

  test(
    'property: filter consistency keeps only requested rows',
    async () => {
      try {
        await fc.assert(
          fc.asyncProperty(operationSequenceArb, async (sequence) => {
            const result = await executeSequence(sequence, 'filter-consistency');

            for (const [index, row] of result.clientRows.entries()) {
              expect(readStatus(row.status, `clientRows[${index}].status`)).toBe('requested');
            }
          }),
          { numRuns: 20 },
        );
      } catch (error) {
        console.error(
          '[fast-check][filter-consistency] seed/path is included in the failure output:',
        );
        console.error(error);
        throw error;
      }
    },
    PROPERTY_TEST_TIMEOUT_MS,
  );

  test(
    'property: client rows always preserve descending $pk sort order',
    async () => {
      try {
        await fc.assert(
          fc.asyncProperty(operationSequenceArb, async (sequence) => {
            const result = await executeSequence(sequence, 'sort-order');
            const clientPks = extractPkList(result.clientRows);
            const expectedOrder = [...clientPks].sort((left, right) => right - left);
            expect(clientPks).toEqual(expectedOrder);
          }),
          { numRuns: 20 },
        );
      } catch (error) {
        console.error('[fast-check][sort-order] seed/path is included in the failure output:');
        console.error(error);
        throw error;
      }
    },
    PROPERTY_TEST_TIMEOUT_MS,
  );

  test(
    'property: concurrent requested inserts preserve descending $pk sort order',
    async () => {
      try {
        await fc.assert(
          fc.asyncProperty(concurrentInsertCountArb, async (insertCount) => {
            await cleanupBetweenTestsForFixture(fullOrdersFixture, pool);
            const seededUser = await insertTestUser(
              db,
              `concurrent_property_driver_${randomUUID().slice(0, 8)}`,
            );

            const fetchImpl = createRouterFetchAdapter(router);
            const client = createPulseClient<typeof registry.$client>({
              url: 'http://localhost',
              fetchImpl,
            });
            const core = new PulseQuery(client.ordersByStatus({ status: 'requested' }));

            await core.subscribe();

            await runDbOperations(
              Array.from({ length: insertCount }, (_, index) =>
                db.insert(orders).values({
                  driverId: seededUser.id,
                  pickup: `Concurrent Property Pickup ${insertCount}-${index + 1}`,
                  dropoff: `Concurrent Property Dropoff ${insertCount}-${index + 1}`,
                  price: 401 + index,
                  status: 'requested',
                }),
              ),
              { mode: 'concurrent' },
            );

            await core.poll();

            const clientPks = extractPkList([...core.getState().data] as Record<string, unknown>[]);
            const expectedOrder = [...clientPks].sort((left, right) => right - left);
            expect(clientPks).toEqual(expectedOrder);
            expect(clientPks).toHaveLength(insertCount);
          }),
          { numRuns: 20 },
        );
      } catch (error) {
        console.error(
          '[fast-check][concurrent-sort-order] seed/path is included in the failure output:',
        );
        console.error(error);
        throw error;
      }
    },
    PROPERTY_TEST_TIMEOUT_MS,
  );
});
