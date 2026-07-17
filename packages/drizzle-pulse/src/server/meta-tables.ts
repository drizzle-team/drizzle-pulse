import { pgSchema, text, uuid } from 'drizzle-orm/pg-core';

// Drizzle objects for the two bookkeeping tables reconcile() creates in the events schema, so
// their DML runs through the query builder. The raw CREATE TABLE IF NOT EXISTS statements in
// reconcile() must keep producing the identical column definitions declared here.
export function buildMetaTables(eventsSchema: string) {
  const schema = pgSchema(eventsSchema);
  return {
    // One row per events table: the DDL hash reconcile() compares against, plus the epoch that
    // rotates on every recreate.
    pulseMeta: schema.table('pulse_meta', {
      tableName: text('table_name').primaryKey(),
      ddlHash: text('ddl_hash').notNull(),
      epoch: uuid('epoch').notNull(),
    }),
    // Durable commit-LSN dedupe watermark, keyed by slot.
    pulseStream: schema.table('pulse_stream', {
      slotName: text('slot_name').primaryKey(),
      lastLsn: text('last_lsn').notNull(),
    }),
  };
}
