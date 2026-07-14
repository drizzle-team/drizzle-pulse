export type {
  ColumnOperators,
  QueryDescriptor,
  ResolvedPulseQuery,
  WhereClause,
  WhereCondition,
} from '../types.js';

// ---------------------------------------------------------------------------
// Pulse API
// ---------------------------------------------------------------------------

export type {
  LoadMoreRequest,
  PullRequest,
  SubscribeRequest,
  SubscribeResponse,
} from '../shared/protocol-types.js';
export type { PulseAuthContext } from '../types.js';
// Events-table convention
export {
  buildEventsTable,
  DEFAULT_EVENTS_SCHEMA,
  getEventsTableName,
} from './events-table-resolver.js';
export {
  type ExposeConfig,
  expose,
  LogLevel,
  PulseRuntime,
} from './expose.js';
// Builder
export { PulseBuilder } from './pulse-builder.js';
export type { AnyPulseBuilder, AnyPulseBuilders as AnyQueries } from './pulse-registry.js';
// Registry
export { createPulseRegistry, PulseRegistry } from './pulse-registry.js';
export { buildSelectQuery } from './pulse-sql.js';
export { PulseStore } from './pulse-store.js';
// Pulse types (public API surface)
export type {
  ApplyColumns,
  ColumnsSelection,
  InferColumnSelection,
  PulseClientContract,
  PulseQueryConfig,
  PulseQueryContext,
  PulseTransformFn,
  WithPk,
} from './pulse-types.js';
export { applyColumnFilter } from './pulse-types.js';
export { serializeResponse } from './superjson-utils.js';
