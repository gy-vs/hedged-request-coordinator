/**
 * Shared type definitions for the request coordinator.
 *
 * The library is transport-agnostic: the caller provides an `attempt` function
 * and the coordinator decides when attempts start, retry, hedge, and stop.
 */

/** Wall-clock source. Must agree with the {@link Sleeper} used alongside it. */
export interface Clock {
  /** Current time in milliseconds (any epoch, as long as it is consistent). */
  now(): number;
}

/**
 * Cancellable delay primitive. The promise resolves after `ms` milliseconds
 * and rejects (ideally with an AbortError) when `signal` aborts first.
 */
export interface Sleeper {
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
}

/** Source of randomness in [0, 1). Injectable for deterministic tests. */
export type RandomSource = () => number;

/** What a running attempt represents. */
export type AttemptKind = 'original' | 'retry' | 'hedge';

/** Context handed to the caller's attempt function on every attempt. */
export interface AttemptContext {
  /** 0-based index of this attempt within the execution. */
  readonly attemptIndex: number;
  /** Whether this attempt is the original request, a retry, or a hedge. */
  readonly kind: AttemptKind;
  /**
   * Aborted when the execution settles for any reason (success elsewhere,
   * permanent error, deadline, external cancellation). Attempt functions
   * should stop their work when this fires.
   */
  readonly signal: AbortSignal;
  /** Absolute deadline of the whole execution, in the clock's time domain. */
  readonly deadlineMs: number;
  /** The caller-provided idempotency key, if any. */
  readonly idempotencyKey?: string;
}

/** The unit of work the caller wants coordinated. */
export type AttemptFn<T> = (ctx: AttemptContext) => Promise<T> | T;

export type ErrorClassification = 'retryable' | 'permanent';

/** Caller-provided error classifier. */
export type ClassifyError = (error: unknown) => ErrorClassification;

/** How an attempt ended, from the coordinator's point of view. */
export type AttemptEndReason =
  | 'success'
  | 'retryable-error'
  | 'permanent-error'
  | 'cancelled';

/** Immutable snapshot of one attempt's lifecycle. */
export interface AttemptSnapshot {
  readonly index: number;
  readonly kind: AttemptKind;
  readonly startedAt: number;
  readonly endedAt: number;
  readonly endReason: AttemptEndReason;
  /** The error the attempt failed with, when it failed. */
  readonly error?: unknown;
}

export interface ExecuteOptions<T> {
  /** The work to perform. Called once per attempt. */
  readonly attempt: AttemptFn<T>;
  /** Absolute deadline for the whole execution, in the clock's time domain. */
  readonly deadlineMs: number;
  /** External cancellation signal. */
  readonly signal?: AbortSignal;
  /**
   * Marks the operation as safe to run more than once. Retries and hedging
   * are only enabled when this is true OR an idempotency key is provided.
   */
  readonly idempotent?: boolean;
  /**
   * Explicit idempotency key. Providing one opts the execution into retries
   * and hedging even when `idempotent` is not set, and is forwarded to every
   * attempt via the context.
   */
  readonly idempotencyKey?: string;
  /** Per-execution override of the error classifier. */
  readonly classifyError?: ClassifyError;
  /** Per-execution override of the maximum number of retries. */
  readonly maxRetries?: number;
}

export interface ExecuteResult<T> {
  /** The value produced by the winning attempt. */
  readonly value: T;
  /** Index (into `attempts`) of the attempt that produced the value. */
  readonly winnerIndex: number;
  /** Snapshot of every attempt, in start order. */
  readonly attempts: readonly AttemptSnapshot[];
}
