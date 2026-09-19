import type { Clock, Sleeper } from './types.js';

/** Real wall clock. Pair with {@link systemSleeper}. */
export const systemClock: Clock = {
  now: () => Date.now(),
};

const defaultAbortError = (): Error =>
  new DOMException('This operation was aborted', 'AbortError');

/** Real cancellable sleep built on setTimeout. Pair with {@link systemClock}. */
export const systemSleeper: Sleeper = {
  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (signal?.aborted) {
        reject(signal.reason ?? defaultAbortError());
        return;
      }
      const onAbort = (): void => {
        clearTimeout(timer);
        reject(signal?.reason ?? defaultAbortError());
      };
      const timer = setTimeout(() => {
        signal?.removeEventListener('abort', onAbort);
        resolve();
      }, Math.max(0, ms));
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  },
};
