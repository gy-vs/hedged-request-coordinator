/**
 * Error markers used for control flow. Attempt functions can reject with
 * {@link PermanentError} (or use {@link toPermanent}) to short-circuit
 * retries without configuring a custom `isRetryable` predicate.
 */

export class PermanentError extends Error {
  override readonly cause: unknown;

  constructor(cause: unknown, message?: string) {
    super(message ?? (cause instanceof Error ? cause.message : 'permanent error'));
    this.name = 'PermanentError';
    this.cause = cause;
  }
}

/** Wrap any error so the retry policy treats it as permanent. */
export function toPermanent(cause: unknown, message?: string): PermanentError {
  return cause instanceof PermanentError ? cause : new PermanentError(cause, message);
}

export class RetryableError extends Error {
  override readonly cause: unknown;

  constructor(cause: unknown, message?: string) {
    super(message ?? (cause instanceof Error ? cause.message : 'retryable error'));
    this.name = 'RetryableError';
    this.cause = cause;
  }
}

/** Wrap any error so the default retry policy considers it retryable. */
export function toRetryable(cause: unknown, message?: string): RetryableError {
  return cause instanceof RetryableError ? cause : new RetryableError(cause, message);
}

/**
 * True for errors raised by an aborted AbortSignal (name `AbortError` or
 * DOMException code `ABORT_ERR`). Handy inside a custom `isRetryable`
 * policy — abort errors are never retried by the coordinator regardless.
 */
export function isAbortLike(error: unknown): boolean {
  if (error instanceof Error && error.name === 'AbortError') return true;
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { code?: unknown }).code === 'ABORT_ERR'
  );
}
