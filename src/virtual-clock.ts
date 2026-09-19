import type { Clock, Sleeper } from './types.js';

interface SleepEntry {
  id: number;
  due: number;
  resolve: () => void;
  reject: (err: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

const toAbortError = (reason: unknown): unknown =>
  reason ?? new DOMException('This operation was aborted', 'AbortError');

/** Yield to the microtask queue several times so promise chains can settle. */
const flushMicrotasks = async (rounds = 100): Promise<void> => {
  for (let i = 0; i < rounds; i++) {
    await Promise.resolve();
  }
};

/**
 * Deterministic virtual time source for tests (and simulations). Implements
 * both {@link Clock} and {@link Sleeper}; pass the same instance as both
 * `clock` and `sleeper` so the coordinator never touches real time.
 *
 * Timers fire in (dueTime, insertionOrder) order, so timers scheduled for
 * the same instant fire FIFO. Between firings the microtask queue is
 * flushed, which lets promise-based attempt functions make progress.
 */
export class VirtualClock implements Clock, Sleeper {
  private current: number;
  private nextId = 0;
  private queue: SleepEntry[] = [];

  constructor(startMs = 0) {
    this.current = startMs;
  }

  now(): number {
    return this.current;
  }

  /** Number of pending (not yet fired, not yet cancelled) sleeps. */
  get pendingCount(): number {
    return this.queue.length;
  }

  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (signal?.aborted) {
        reject(toAbortError(signal.reason));
        return;
      }
      const entry: SleepEntry = {
        id: ++this.nextId,
        due: this.current + Math.max(0, ms),
        resolve,
        reject,
      };
      if (signal) {
        entry.signal = signal;
        entry.onAbort = () => {
          this.remove(entry);
          reject(toAbortError(signal.reason));
        };
        signal.addEventListener('abort', entry.onAbort, { once: true });
      }
      this.insert(entry);
    });
  }

  /** Advances time by `ms`, firing every timer that comes due. */
  async advanceBy(ms: number): Promise<void> {
    if (ms < 0) {
      throw new RangeError('cannot advance by a negative amount');
    }
    // Let already-settled promises (e.g. instantly-failing attempts) run
    // their continuations before any timer fires.
    await flushMicrotasks();
    const target = this.current + ms;
    while (this.queue.length > 0) {
      const next = this.queue[0]!;
      if (next.due > target) {
        break;
      }
      this.fire(next);
      await flushMicrotasks();
    }
    this.current = Math.max(this.current, target);
    await flushMicrotasks();
  }

  /** Advances time to the absolute instant `ms`. */
  async advanceTo(ms: number): Promise<void> {
    await this.advanceBy(ms - this.current);
  }

  /**
   * Fires timers until none remain. Throws if timers keep rescheduling
   * themselves past `maxIterations` firings (a likely test bug).
   */
  async runAll(maxIterations = 10_000): Promise<void> {
    await flushMicrotasks();
    let fired = 0;
    while (this.queue.length > 0) {
      if (++fired > maxIterations) {
        throw new Error('VirtualClock.runAll: too many iterations (timers keep rescheduling?)');
      }
      this.fire(this.queue[0]!);
      await flushMicrotasks();
    }
  }

  private insert(entry: SleepEntry): void {
    // Keep the queue sorted by (due, id) so equal-time timers fire FIFO.
    let lo = 0;
    let hi = this.queue.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      const e = this.queue[mid]!;
      if (e.due < entry.due || (e.due === entry.due && e.id < entry.id)) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    this.queue.splice(lo, 0, entry);
  }

  private remove(entry: SleepEntry): void {
    const i = this.queue.indexOf(entry);
    if (i >= 0) {
      this.queue.splice(i, 1);
    }
  }

  private fire(entry: SleepEntry): void {
    this.remove(entry);
    // The sleep resolved naturally; detach so it can no longer react to aborts.
    if (entry.signal && entry.onAbort) {
      entry.signal.removeEventListener('abort', entry.onAbort);
    }
    this.current = Math.max(this.current, entry.due);
    entry.resolve();
  }
}
