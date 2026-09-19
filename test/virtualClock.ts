import type { Clock, Rng, Sleep } from '../src/index.js';

interface Entry {
  /** Absolute virtual due time. */
  at: number;
  /** Insertion order keeps same-time timers in FIFO order. */
  seq: number;
  callback: () => void;
}

/**
 * Deterministic clock for tests. Timers live in a binary min-heap keyed by
 * (at, seq): same-time callbacks fire in scheduling order, and a timer
 * scheduled while another callback runs at the same time is inserted AFTER
 * all entries already due at that time.
 *
 * `advance` is async: after each due callback it flushes microtasks before
 * popping the next entry, which reproduces how promise reactions of timers
 * fired within one macrotask settle before the next macrotask.
 */
export class VirtualClock implements Clock {
  private nowMs: number;
  private heap: Entry[] = [];
  private nextSeq = 0;

  constructor(start = 0) {
    this.nowMs = start;
  }

  now(): number {
    return this.nowMs;
  }

  pending(): number {
    return this.heap.length;
  }

  setTimeout(callback: () => void, delayMs: number): number {
    const entry: Entry = {
      at: this.nowMs + Math.max(0, delayMs),
      seq: this.nextSeq++,
      callback,
    };
    this.heap.push(entry);
    this.bubbleUp(this.heap.length - 1);
    return entry.seq;
  }

  clearTimeout(handle: unknown): void {
    const seq = handle as number;
    const index = this.heap.findIndex((entry) => entry.seq === seq);
    if (index === -1) return;
    const last = this.heap.pop() as Entry;
    if (index < this.heap.length) {
      this.heap[index] = last;
      this.bubbleUp(index);
      this.sinkDown(index);
    }
  }

  /**
   * Advance virtual time up to `ms`, firing every timer due by then
   * (including timers registered during the run at times within the run).
   * Time only jumps to actual due instants: if nothing more is scheduled
   * within the horizon, the clock stops at the last fired timestamp rather
   * than jumping to the horizon, so timers armed after an advance are not
   * silently shortened (e.g. a backoff armed at t=10 due at t=60 keeps its
   * due time when the test later advances to t=40).
   */
  async advance(ms: number): Promise<void> {
    const horizon = this.nowMs + ms;
    while (this.heap.length > 0 && this.peek().at <= horizon) {
      const entry = this.pop();
      this.nowMs = entry.at;
      entry.callback();
      // Flush reactions (promise thens scheduled by the callback, sleeps
      // settling, AbortSignal listeners' promise chains) before the next
      // timer of the same instant gets to run.
      await Promise.resolve();
      await Promise.resolve();
    }
  }

  /** Advance exactly to an absolute timestamp. */
  async advanceTo(at: number): Promise<void> {
    if (at < this.nowMs) throw new RangeError('cannot rewind virtual clock');
    await this.advance(at - this.nowMs);
  }

  private peek(): Entry {
    return this.heap[0] as Entry;
  }

  private pop(): Entry {
    const top = this.heap[0] as Entry;
    const last = this.heap.pop() as Entry;
    if (this.heap.length > 0) {
      this.heap[0] = last;
      this.sinkDown(0);
    }
    return top;
  }

  private static less(a: Entry, b: Entry): boolean {
    return a.at < b.at || (a.at === b.at && a.seq < b.seq);
  }

  private bubbleUp(index: number): void {
    const heap = this.heap;
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (VirtualClock.less(heap[index] as Entry, heap[parent] as Entry)) {
        this.swap(index, parent);
        index = parent;
      } else {
        break;
      }
    }
  }

  private sinkDown(index: number): void {
    const heap = this.heap;
    const size = heap.length;
    for (;;) {
      const left = index * 2 + 1;
      const right = left + 1;
      let smallest = index;
      if (left < size && VirtualClock.less(heap[left] as Entry, heap[smallest] as Entry)) {
        smallest = left;
      }
      if (right < size && VirtualClock.less(heap[right] as Entry, heap[smallest] as Entry)) {
        smallest = right;
      }
      if (smallest === index) break;
      this.swap(index, smallest);
      index = smallest;
    }
  }

  private swap(a: number, b: number): void {
    const heap = this.heap;
    const tmp = heap[a] as Entry;
    heap[a] = heap[b] as Entry;
    heap[b] = tmp;
  }
}

/**
 * Sleep bound to a VirtualClock. On abort it rejects with an AbortError,
 * removes its timer (so pending() stays at zero) and detaches its listener.
 */
export function virtualSleep(clock: VirtualClock): Sleep {
  return function sleep(delayMs: number, signal: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const fail = (): void => reject(makeAbort(signal));
      if (signal.aborted) {
        fail();
        return;
      }
      const onAbort = (): void => {
        signal.removeEventListener('abort', onAbort);
        clock.clearTimeout(handle);
        fail();
      };
      const handle = clock.setTimeout(() => {
        signal.removeEventListener('abort', onAbort);
        resolve();
      }, delayMs);
      signal.addEventListener('abort', onAbort, { once: true });
    });
  };
}

function makeAbort(signal: AbortSignal): Error {
  const reason = (signal as AbortSignal & { reason?: unknown }).reason;
  if (reason instanceof Error) return reason;
  const error = new Error('The operation was aborted');
  error.name = 'AbortError';
  return error;
}

/** Scripted RNG returning queued values in order (cycling). */
export interface ScriptedRng extends Rng {
  readonly calls: number;
}

export function scriptedRng(values: number[] = [0]): ScriptedRng {
  if (values.length === 0) throw new RangeError('scriptedRng needs at least one value');
  let calls = 0;
  const fn = ((): number => {
    const value = values[calls % values.length] as number;
    calls += 1;
    return value;
  }) as ScriptedRng;
  Object.defineProperty(fn, 'calls', {
    get(): number {
      return calls;
    },
  });
  return fn;
}
