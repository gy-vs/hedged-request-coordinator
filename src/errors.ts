import type { AttemptSnapshot } from './types.js';

/**
 * Base class for every error the coordinator itself raises. Always carries
 * the per-attempt snapshots collected up to the moment of failure.
 */
export class CoordinationError extends Error {
  readonly attempts: readonly AttemptSnapshot[];

  constructor(message: string, attempts: readonly AttemptSnapshot[], options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
    this.attempts = attempts;
  }
}

/** The absolute deadline was reached before any attempt succeeded. */
export class DeadlineExceededError extends CoordinationError {
  readonly deadlineMs: number;

  constructor(deadlineMs: number, attempts: readonly AttemptSnapshot[]) {
    super(`execution exceeded its deadline (${deadlineMs}ms)`, attempts);
    this.deadlineMs = deadlineMs;
  }
}

/** The caller's AbortSignal fired before the execution settled. */
export class ExecutionAbortedError extends CoordinationError {
  constructor(cause: unknown, attempts: readonly AttemptSnapshot[]) {
    super('execution aborted by the caller', attempts, { cause });
  }
}

/** Why an execution ran out of attempts. */
export type FailureReason =
  /** A permanent error ended the execution. */
  | 'permanent-error'
  /** The error was retryable but the request is not idempotent (no key). */
  | 'not-idempotent'
  /** The retry budget (maxRetries) was used up. */
  | 'retries-exhausted'
  /** The shared token budget could not pay for the next retry. */
  | 'budget-exhausted'
  /** The injected sleeper misbehaved. */
  | 'internal-error';

/** The execution failed; `cause` holds the underlying attempt error. */
export class ExecutionFailedError extends CoordinationError {
  readonly reason: FailureReason;

  constructor(reason: FailureReason, cause: unknown, attempts: readonly AttemptSnapshot[]) {
    super(`execution failed: ${reason}`, attempts, { cause });
    this.reason = reason;
  }
}

/**
 * Throw (or reject with) this from an attempt function to mark the failure
 * as retryable when using the default error classifier.
 */
export class RetryableError extends Error {
  constructor(message?: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'RetryableError';
  }
}

/**
 * Throw (or reject with) this from an attempt function to mark the failure
 * as permanent when using the default error classifier.
 */
export class PermanentError extends Error {
  constructor(message?: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'PermanentError';
  }
}
