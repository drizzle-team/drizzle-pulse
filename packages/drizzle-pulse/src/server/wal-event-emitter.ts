export type WalTapPayload = {
  operation: 'insert' | 'update' | 'delete';
  rowData: Record<string, unknown>;
  oldRowData: Record<string, unknown> | null;
  $snapshot: number;
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
    $snapshot: number,
  ): void {
    const set = this.listeners.get(tableQualifiedName);
    if (!set) return;
    const payload: WalTapPayload = { operation, rowData, oldRowData, $snapshot };
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
