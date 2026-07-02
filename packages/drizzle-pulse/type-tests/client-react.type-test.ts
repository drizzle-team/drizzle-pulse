import { expectTypeOf } from 'bun:test';
import type { UsePulseQueryResult, usePulseQuery } from '../src/index.js';
import type { QueryDescriptor } from '../src/react/index.js';
import { createPulseClient } from '../src/react/index.js';
import type { usePulseQuery as usePulseQueryFromReact } from '../src/react/use-pulse-query.js';
import { createPulse } from '../src/server/pulse.js';
import { createPulseRegistry } from '../src/server/pulse-registry.js';
import type { WithPk } from '../src/server/pulse-types.js';
import { orders, statusSchema } from './fixtures.js';

const pulse = createPulse();
const registry = createPulseRegistry({
  ordersByStatus: pulse(orders)
    .columns({ id: true, status: true })
    .args(statusSchema)
    .query((ctx) => ctx.query({ status: ctx.args.status })),
  allOrders: pulse(orders),
});

type Client = typeof registry.$client;
const pulseClient = createPulseClient<Client>({ url: '/api/realtime' });

const statusDescriptor = pulseClient.ordersByStatus({ status: 'requested' });
const allOrdersDescriptor = pulseClient.allOrders();
// @ts-expect-error no-args pulse query must not accept an empty object
pulseClient.allOrders({});

expectTypeOf(statusDescriptor).toEqualTypeOf<
  QueryDescriptor<{
    id: number;
    status: 'requested' | 'accepted' | 'completed' | 'cancelled';
    $pk: unknown;
  }>
>();

expectTypeOf(allOrdersDescriptor).toEqualTypeOf<
  QueryDescriptor<{
    id: number;
    driverId: number | null;
    pickup: string;
    dropoff: string;
    price: number;
    status: 'requested' | 'accepted' | 'completed' | 'cancelled';
    acceptedAt: Date | null;
    createdAt: Date;
    updatedAt: Date;
    $pk: unknown;
  }>
>();

expectTypeOf<typeof usePulseQuery>().toEqualTypeOf<
  <TResult extends WithPk<Record<string, unknown>>>(
    descriptor: QueryDescriptor<TResult>,
  ) => UsePulseQueryResult<TResult>
>();
expectTypeOf<typeof usePulseQueryFromReact>().toEqualTypeOf<
  <TResult extends WithPk<Record<string, unknown>>>(
    descriptor: QueryDescriptor<TResult>,
  ) => UsePulseQueryResult<TResult>
>();

expectTypeOf<ReturnType<typeof usePulseQuery<{ id: number; $pk: unknown }>>>().toEqualTypeOf<
  UsePulseQueryResult<{ id: number; $pk: unknown }>
>();
