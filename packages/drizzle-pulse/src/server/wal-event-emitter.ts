export type WalTapPayload = {
  operation: 'insert' | 'update' | 'delete';
  rowData: Record<string, unknown>;
  oldRowData: Record<string, unknown> | null;
  /** Commit LSN of the transaction this event belongs to; shared by every event in that transaction. */
  lsn: string;
  /**
   * True when `oldRowData` is a genuinely complete old tuple (REPLICA IDENTITY FULL, or
   * pull:true which always forces FULL) and can be evaluated against a query's WHERE. False
   * when it's null or a pk-only degradation under a non-full identity — evaluating WHERE
   * against a partial tuple would misclassify columns pgoutput never sent as non-matching.
   */
  oldRowComplete: boolean;
};

export type WalTapListener = (payload: WalTapPayload) => void;

export class WalEventEmitter {
  private readonly listeners = new Map<string, Set<WalTapListener>>();

  subscribe(tableQualifiedName: string, listener: WalTapListener): () => void {
    let set = this.listeners.get(tableQualifiedName);
    if (!set) {
      set = new Set();
      this.listeners.set(tableQualifiedName, set);
    }
    const listeners = set;
    listeners.add(listener);
    return () => listeners.delete(listener);
  }

  emit(
    tableQualifiedName: string,
    operation: WalTapPayload['operation'],
    rowData: Record<string, unknown>,
    oldRowData: Record<string, unknown> | null,
    lsn: string,
    oldRowComplete = false,
  ): void {
    const set = this.listeners.get(tableQualifiedName);
    if (!set) return;
    const payload: WalTapPayload = { operation, rowData, oldRowData, lsn, oldRowComplete };
    for (const listener of set) {
      try {
        listener(payload);
      } catch (err) {
        // Log but continue — a listener error must not prevent remaining listeners
        // from firing or block the WAL acknowledge path upstream.
        console.error('[WalEventEmitter] listener error:', err);
      }
    }
  }
}
