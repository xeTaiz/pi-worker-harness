// Simple event emitter for internal extension communication.
// Avoids needing SharedEventEmitter type which isn't exported from pi-coding-agent.

type Listener<T> = (data: T) => void;

export class EventEmitter {
  private listeners = new Map<string, Set<Listener<unknown>>>();

  on<T>(event: string, listener: Listener<T>): () => void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this.listeners.get(event)!.add(listener as Listener<any>);
    return () => {
      this.listeners.get(event)?.delete(listener as Listener<any>);
    };
  }

  emit<T>(event: string, data: T): void {
    this.listeners.get(event)?.forEach(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (l) => (l as Listener<T>)(data)
    );
  }
}

// Singleton instance shared across the extension
export const events = new EventEmitter();
