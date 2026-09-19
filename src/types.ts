/**
 * Public type surface for the hedged request coordinator.
 *
 * The coordinator never touches HTTP. Callers supply an {@link AttemptFn}
 * that performs the actual operation.
 */

/** Why a single attempt stopped running. */
export type AttemptEndReason =
  /** The attempt fulfilled and its value won the execution. */
  | 'success'
  /** The attempt fulfilled after the execution had already settled. */
  | 'superseded'
  /** The attempt rejected with an error the retry policy considers permanent. */
  | 'permanent-error'
  /** The attempt rejected with an error eligible for backoff/retry. */
  | 'retryable-error'
  /** The attempt was cancelled because the deadline elapsed. */
  | 'deadline-aborted'
  /** The attempt was cancelled because the caller's AbortSignal fired. */
  | 'external-aborted'
  /** The attempt was cancelled after a sibling attempt won. */
  | 'cancelled';

/** Identifies which role an attempt played in an execution. */
export type AttemptKind = 'original' | 'retry' | 'hedge';

/** Context handed to every attempt invocation. */
export interface AttemptContext {
  /** 1-based position of this attempt within the execution. */
  readonly attemptNumber: number;
  /** Whether the attempt is the original, a retry or a hedge. */
  readonly kind: AttemptKind;
  /** Absolute deadline timestamp (same time basis as the injected clock). */
  readonly deadline: number;
  /** Idempotency key, present exactly when the call is idempotent. */
  readonly idempotencyKey?: string;
  /** Aborts on deadline, external cancellation or sibling success. */
  readonly signal: AbortSignal;
}

/**
 * Caller-provided operation. The implementation SHOULD respect
 * `context.signal`; attempts that ignore it simply cannot be stopped
 * (the coordinator still settles without waiting for them).
 */
export type AttemptFn<T> = (context: AttemptContext) => PromiseLike<T> | T;

/** Immutable snapshot of one attempt lifecycle. */
export interface AttemptSnapshot {
  readonly attemptNumber: number;
  readonly kind: AttemptKind;
  /** Clock timestamp when the attempt was started. */
  readonly startedAt: number;
  /** Clock timestamp when the attempt ended (or was forced closed). */
  readonly endedAt: number;
  readonly endReason: AttemptEndReason;
  /** Rejection value for the `*-error`, `*-aborted` and `cancelled` reasons. */
  readonly error?: unknown;
}

/** Failure codes reported on an unsuccessful execution. */
export type FailureCode =
  /** Absolute deadline elapsed before a successful attempt completed. */
  | 'deadline-exceeded'
  /** The caller's AbortSignal fired. */
  | 'aborted'
  /** The last attempt failed with a permanent (non-retryable) error. */
  | 'permanent-error'
  /** The retry policy exhausted `maxAttempts`. */
  | 'attempts-exhausted'
  /** The shared token budget had no token left for a retry/hedge. */
  | 'budget-exhausted'
  /** A retryable error occurred on a call not enabled for retries. */
  | 'retries-disabled';

/** Successful execution outcome. */
export interface ExecuteSuccess<T> {
  readonly ok: true;
  readonly value: T;
  readonly idempotencyKey?: string;
  readonly startedAt: number;
  readonly endedAt: number;
  /** Number of attempts actually started (including the winner). */
  readonly attemptsStarted: number;
  readonly attempts: readonly AttemptSnapshot[];
}

/** Failed execution outcome. */
export interface ExecuteFailure {
  readonly ok: false;
  readonly code: FailureCode;
  readonly error?: unknown;
  readonly idempotencyKey?: string;
  readonly startedAt: number;
  readonly endedAt: number;
  readonly attemptsStarted: number;
  readonly attempts: readonly AttemptSnapshot[];
}

export type ExecuteOutcome<T> = ExecuteSuccess<T> | ExecuteFailure;

/** Exponential backoff policy with full jitter. */
export interface RetryPolicy {
  /** First backoff delay, in clock milliseconds. Default 100. */
  readonly baseDelayMs: number;
  /** Backoff cap, in clock milliseconds. Default 5_000. */
  readonly maxDelayMs: number;
  /** Growth factor applied between consecutive retries. Default 2. */
  readonly factor: number;
  /**
   * Decides whether an error is retryable. Defaults to rejecting nothing:
   * every non-abort error is retryable. Errors marked with
   * {@link PermanentError} are never retried regardless of this hook.
   */
  readonly isRetryable?: (error: unknown, context: AttemptContext) => boolean;
}

/** Injectable clock. Timestamps are opaque numbers on the caller's time basis. */
export interface Clock {
  now(): number;
  setTimeout(
    callback: () => void,
    delayMs: number,
  ): unknown;
  clearTimeout(handle: unknown): void;
}

/** Deterministic random source for jitter; must return [0, 1). */
export type Rng = () => number;

/**
 * Cancellable sleep. Implementations MUST reject with the given cause when
 * `signal` aborts and MUST clear their timer on abort.
 */
export type Sleep = (delayMs: number, signal: AbortSignal) => Promise<void>;

/**
 * Shared token budget. Reservations are synchronous so concurrent callers
 * cannot both observe the same token (no check-then-deduct race).
 *
 * Only completion of an `original` attempt ever returns a token; retries
 * and hedges never refill.
 */
export interface TokenBudget {
  /** Atomically reserve one token. Returns false when empty. */
  reserve(): boolean;
  /** Refill one token (called once per completed original attempt). */
  refund(): void;
  /** Tokens currently available (for metrics/tests). */
  available(): number;
}

export interface CoordinatorDeps {
  /** Shared budget across every execution on this coordinator. Required. */
  budget: TokenBudget;
  /** Defaults to a real-time clock. */
  clock?: Clock;
  /** Defaults to Math.random. */
  rng?: Rng;
  /**
   * Defaults to an AbortSignal-aware sleep driven by the injected clock.
   * Tests inject a virtual sleep so no real delay is ever awaited.
   */
  sleep?: Sleep;
  /** Coordinator-level default retry policy. */
  retry?: RetryPolicy;
}

export interface ExecuteOptions<T> {
  /** The operation to attempt. */
  attempt: AttemptFn<T>;
  /**
   * Absolute deadline on the clock's time basis. Exclusive: an attempt
   * completing exactly AT the deadline still wins (success breaks ties).
   * Either this or `timeoutMs` must be supplied.
   */
  deadline?: number;
  /**
   * Convenience for `clock.now() + timeoutMs`; ignored when `deadline` is
   * also provided.
   */
  timeoutMs?: number;
  /**
   * Idempotency key. When absent the call is treated as non-idempotent:
   * hedging and automatic retries are disabled.
   */
  idempotencyKey?: string;
  /** External cancellation. */
  signal?: AbortSignal;
  /** Delay after which a hedge may be scheduled. Defaults to no hedging. */
  hedgeDelayMs?: number;
  /**
   * Maximum attempts ever started by this execution (original + retries +
   * hedges counted together). Default 3.
   */
  maxAttempts?: number;
  /**
   * Maximum attempts running at once. Defaults to 2 when hedging is
   * enabled for an idempotent call, otherwise 1.
   */
  maxParallelAttempts?: number;
  /** Per-call retry policy; `false` disables automatic retries. */
  retry?: RetryPolicy | false;
  /** Per-call budget override; otherwise the coordinator budget is used. */
  budget?: TokenBudget;
}
