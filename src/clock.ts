import type { Clock, Rng, Sleep } from './types.js';

/** Real-time clock backed by Date.now / global timers. */
export const systemClock: Clock = {
  now(): number {
    return Date.now();
  },
  setTimeout(callback: () => void, delayMs: number): unknown {
    return setTimeout(callback, delayMs);
  },
  clearTimeout(handle: unknown): void {
    clearTimeout(handle as ReturnType<typeof setTimeout>);
  },
};

/** Default jitter source. */
export const systemRng: Rng = () => Math.random();

/**
 * Default sleep, driven by the given clock (normally {@link systemClock}).
 * Rejects with an AbortError (wrapping the signal reason) when aborted,
 * and removes both the timer and the abort listener in every path.
 */
export function createSystemSleep(clock: Clock = systemClock): Sleep {
  return function sleep(delayMs: number, signal: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (signal.aborted) {
        reject(abortError(signal));
        return;
      }
      const onAbort = (): void => {
        clock.clearTimeout(timer);
        signal.removeEventListener('abort', onAbort);
        reject(abortError(signal));
      };
      const timer = clock.setTimeout(() => {
        signal.removeEventListener('abort', onAbort);
        resolve();
      }, delayMs);
      signal.addEventListener('abort', onAbort, { once: true });
    });
  };
}

function abortError(signal: AbortSignal): Error {
  const reason = (signal as AbortSignal & { reason?: unknown }).reason;
  if (reason instanceof Error) return reason;
  const err = new Error('The operation was aborted');
  err.name = 'AbortError';
  return err;
}
