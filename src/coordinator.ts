import { TokenBudget } from './budget.js';
import {
  DeadlineExceededError,
  ExecutionAbortedError,
  ExecutionFailedError,
  PermanentError,
  RetryableError,
  type FailureReason,
} from './errors.js';
import { systemClock, systemSleeper } from './time.js';
import type {
  AttemptContext,
  AttemptEndReason,
  AttemptKind,
  AttemptSnapshot,
  ClassifyError,
  Clock,
  ExecuteOptions,
  ExecuteResult,
  RandomSource,
  Sleeper,
} from './types.js';

export interface CoordinatorOptions {
  /** Time source. Must agree with `sleeper`. Defaults to the wall clock. */
  readonly clock?: Clock;
  /** Cancellable delay primitive. Defaults to a setTimeout-based sleeper. */
  readonly sleeper?: Sleeper;
  /** Randomness for jitter, in [0, 1). Defaults to Math.random. */
  readonly random?: RandomSource;
  /** Hard cap on simultaneously in-flight attempts per execution. Default 2. */
  readonly maxParallelAttempts?: number;
  /**
   * If the original attempt is still running after this many milliseconds,
   * one hedge attempt may be launched. Default: Infinity (hedging disabled).
   */
  readonly hedgeDelayMs?: number;
  /** Base delay for the first retry, doubled per retry. Default 100. */
  readonly baseBackoffMs?: number;
  /** Upper bound for a single backoff delay. Default 5000. */
  readonly maxBackoffMs?: number;
  /** Symmetric jitter factor in [0, 1]. Default 0.2. */
  readonly jitter?: number;
  /** Maximum number of retries (beyond the original attempt). Default 3. */
  readonly maxRetries?: number;
  /**
   * Capacity of the token budget shared by all executions of this
   * coordinator. Retries and hedges spend tokens; completed original
   * attempts return them. Default: Infinity.
   */
  readonly budgetTokens?: number;
}

interface AttemptRecord {
  index: number;
  kind: AttemptKind;
  startedAt: number;
  endedAt?: number;
  endReason?: AttemptEndReason;
  error?: unknown;
  recorded: boolean;
}

type SettleOutcome<T> =
  | { readonly type: 'success'; readonly value: T; readonly winnerIndex: number }
  | { readonly type: 'failure'; readonly makeError: (attempts: readonly AttemptSnapshot[]) => Error };

const defaultClassify: ClassifyError = (error) => {
  if (error instanceof PermanentError) {
    return 'permanent';
  }
  if (error instanceof RetryableError) {
    return 'retryable';
  }
  // Unknown errors are treated as permanent: retrying an unclassified
  // failure is not safe by default.
  return 'permanent';
};

/**
 * Coordinates retries and hedged (parallel backup) attempts for
 * caller-provided work, under an absolute deadline, an optional external
 * AbortSignal, a parallelism cap, and a shared token budget.
 *
 * The coordinator itself is transport-agnostic: it never performs I/O, it
 * only decides *when* attempts run and *which* outcome wins.
 */
export class RequestCoordinator {
  private readonly clock: Clock;
  private readonly sleeper: Sleeper;
  private readonly random: RandomSource;
  private readonly maxParallelAttempts: number;
  private readonly hedgeDelayMs: number;
  private readonly baseBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly jitter: number;
  private readonly maxRetries: number;
  private readonly budget: TokenBudget;

  constructor(options: CoordinatorOptions = {}) {
    this.clock = options.clock ?? systemClock;
    this.sleeper = options.sleeper ?? systemSleeper;
    this.random = options.random ?? Math.random;
    this.maxParallelAttempts = options.maxParallelAttempts ?? 2;
    this.hedgeDelayMs = options.hedgeDelayMs ?? Number.POSITIVE_INFINITY;
    this.baseBackoffMs = options.baseBackoffMs ?? 100;
    this.maxBackoffMs = options.maxBackoffMs ?? 5_000;
    this.jitter = options.jitter ?? 0.2;
    this.maxRetries = options.maxRetries ?? 3;
    this.budget = new TokenBudget(options.budgetTokens ?? Number.POSITIVE_INFINITY);

    if (!Number.isInteger(this.maxParallelAttempts) || this.maxParallelAttempts < 1) {
      throw new RangeError('maxParallelAttempts must be an integer >= 1');
    }
    if (this.hedgeDelayMs < 0) {
      throw new RangeError('hedgeDelayMs must be >= 0');
    }
    if (!(this.baseBackoffMs >= 0) || !(this.maxBackoffMs >= 0)) {
      throw new RangeError('backoff delays must be >= 0');
    }
    if (this.jitter < 0 || this.jitter > 1) {
      throw new RangeError('jitter must be within [0, 1]');
    }
    if (!Number.isInteger(this.maxRetries) || this.maxRetries < 0) {
      throw new RangeError('maxRetries must be an integer >= 0');
    }
  }

  /** Tokens currently left in the shared budget. */
  get budgetAvailable(): number {
    return this.budget.available;
  }

  /**
   * Runs `options.attempt` under coordination. The returned promise settles
   * exactly once: with the first successful attempt's value, or with a
   * {@link CoordinationError} subclass carrying every attempt snapshot.
   */
  execute<T>(options: ExecuteOptions<T>): Promise<ExecuteResult<T>> {
    if (!options || typeof options.attempt !== 'function') {
      throw new TypeError('ExecuteOptions.attempt must be a function');
    }
    if (typeof options.deadlineMs !== 'number' || Number.isNaN(options.deadlineMs)) {
      throw new TypeError('ExecuteOptions.deadlineMs must be a number');
    }

    const clock = this.clock;
    const deadline = options.deadlineMs;
    const externalSignal = options.signal;
    const classify: ClassifyError = options.classifyError ?? defaultClassify;
    const maxRetries = options.maxRetries ?? this.maxRetries;
    // Retrying and hedging duplicate the request: only allowed when the
    // caller marks the operation idempotent or supplies an idempotency key.
    const mayDuplicate = options.idempotent === true || options.idempotencyKey !== undefined;

    return new Promise<ExecuteResult<T>>((resolve, reject) => {
      // ---- per-execution state -------------------------------------------
      const records: AttemptRecord[] = [];
      const inFlight = new Map<number, AttemptRecord>();
      const disposers: Array<() => void> = [];
      const attemptController = new AbortController();
      let settled = false;
      let retryCount = 0;
      let lastError: unknown;
      let hedgeLaunched = false;
      let cancelHedgeTimer: (() => void) | null = null;
      let onExternalAbort: (() => void) | null = null;

      const snapshot = (rec: AttemptRecord): AttemptSnapshot => {
        const base = {
          index: rec.index,
          kind: rec.kind,
          startedAt: rec.startedAt,
          endedAt: rec.endedAt ?? clock.now(),
          endReason: rec.endReason ?? 'cancelled',
        };
        return rec.error === undefined ? base : { ...base, error: rec.error };
      };

      const settle = (outcome: SettleOutcome<T>): void => {
        if (settled) {
          return; // The result may only be decided once.
        }
        settled = true;

        // 1. Stop every pending timer (deadline, hedge, backoff).
        for (const dispose of disposers.splice(0)) {
          try {
            dispose();
          } catch {
            // Disposers are best-effort; never let cleanup throw.
          }
        }
        // 2. Detach from the caller's signal.
        if (externalSignal && onExternalAbort) {
          externalSignal.removeEventListener('abort', onExternalAbort);
          onExternalAbort = null;
        }
        // 3. Tell in-flight attempts to stop, then record them as cancelled.
        if (!attemptController.signal.aborted) {
          attemptController.abort();
        }
        for (const rec of [...inFlight.values()]) {
          recordEnd(rec, 'cancelled');
        }

        const attempts = records.map(snapshot);
        if (outcome.type === 'success') {
          resolve({ value: outcome.value, winnerIndex: outcome.winnerIndex, attempts });
        } else {
          reject(outcome.makeError(attempts));
        }
      };

      const failWith = (reason: FailureReason, cause: unknown): void => {
        settle({
          type: 'failure',
          makeError: (attempts) => new ExecutionFailedError(reason, cause, attempts),
        });
      };

      const settleDeadline = (): void => {
        settle({
          type: 'failure',
          makeError: (attempts) => new DeadlineExceededError(deadline, attempts),
        });
      };

      const settleAborted = (): void => {
        settle({
          type: 'failure',
          makeError: (attempts) => new ExecutionAbortedError(externalSignal?.reason, attempts),
        });
      };

      /**
       * Arms a cancellable timer on the injected sleeper. The sleep call is
       * made synchronously so that, with a VirtualClock, timer ordering
       * matches arming order. Returns a cancel function.
       */
      const armTimer = (ms: number, cb: () => void): (() => void) => {
        const ac = new AbortController();
        const cancel = (): void => ac.abort();
        disposers.push(cancel);
        let pending: Promise<void>;
        try {
          pending = this.sleeper.sleep(Math.max(0, ms), ac.signal);
        } catch (err) {
          failWith('internal-error', err);
          return cancel;
        }
        Promise.resolve(pending).then(
          () => {
            if (!ac.signal.aborted) {
              cb();
            }
          },
          (err: unknown) => {
            // Rejection because we cancelled is normal; anything else means
            // the sleeper itself is broken and we cannot keep time.
            if (!ac.signal.aborted) {
              failWith('internal-error', err);
            }
          },
        );
        return cancel;
      };

      const recordEnd = (rec: AttemptRecord, reason: AttemptEndReason, error?: unknown): void => {
        if (rec.recorded) {
          return;
        }
        rec.recorded = true;
        rec.endedAt = clock.now();
        rec.endReason = reason;
        rec.error = error;
        inFlight.delete(rec.index);
        if (rec.kind === 'original') {
          // Only original completions replenish the shared budget.
          this.budget.release();
          // The first request is over: hedging is no longer meaningful.
          cancelHedgeTimer?.();
        }
      };

      const startAttempt = (kind: AttemptKind): boolean => {
        if (settled) {
          return false;
        }
        // Parallelism cap is checked before spending budget so a refused
        // attempt never consumes a token.
        if (inFlight.size >= this.maxParallelAttempts) {
          return false;
        }
        // Atomic check-and-decrement: no await between check and spend.
        if (kind !== 'original' && !this.budget.tryAcquire()) {
          return false;
        }

        const rec: AttemptRecord = {
          index: records.length,
          kind,
          startedAt: clock.now(),
          recorded: false,
        };
        records.push(rec);
        inFlight.set(rec.index, rec);

        const ctx: AttemptContext = {
          attemptIndex: rec.index,
          kind,
          signal: attemptController.signal,
          deadlineMs: deadline,
          idempotencyKey: options.idempotencyKey,
        };

        let attemptPromise: Promise<T>;
        try {
          attemptPromise = Promise.resolve(options.attempt(ctx));
        } catch (err) {
          attemptPromise = Promise.reject(err);
        }
        attemptPromise.then(
          (value) => onAttemptSuccess(rec, value),
          (err) => onAttemptError(rec, err),
        );
        return true;
      };

      const onAttemptSuccess = (rec: AttemptRecord, value: T): void => {
        if (rec.recorded) {
          return; // Already cancelled when the execution settled.
        }
        if (settled) {
          recordEnd(rec, 'cancelled');
          return;
        }
        recordEnd(rec, 'success');
        settle({ type: 'success', value, winnerIndex: rec.index });
      };

      const safeClassify = (err: unknown): 'retryable' | 'permanent' => {
        try {
          return classify(err);
        } catch {
          return 'permanent';
        }
      };

      const onAttemptError = (rec: AttemptRecord, err: unknown): void => {
        if (rec.recorded) {
          return;
        }
        if (settled || attemptController.signal.aborted) {
          recordEnd(rec, 'cancelled');
          return;
        }
        const classification = safeClassify(err);
        recordEnd(rec, classification === 'retryable' ? 'retryable-error' : 'permanent-error', err);

        if (classification === 'permanent') {
          failWith('permanent-error', err);
          return;
        }
        if (!mayDuplicate) {
          failWith('not-idempotent', err);
          return;
        }
        lastError = err;
        maybeScheduleRetry();
      };

      const computeBackoff = (retryNumber: number): number => {
        const exponential = Math.min(this.maxBackoffMs, this.baseBackoffMs * 2 ** retryNumber);
        // Symmetric jitter: random() === 0.5 yields exactly the exponential delay.
        const factor = 1 + this.jitter * (2 * this.random() - 1);
        return Math.max(0, exponential * factor);
      };

      const maybeScheduleRetry = (): void => {
        if (settled) {
          return;
        }
        if (inFlight.size > 0) {
          // A hedge is still running; the retry decision is revisited when
          // it ends.
          return;
        }
        if (retryCount >= maxRetries) {
          failWith('retries-exhausted', lastError);
          return;
        }
        const delay = computeBackoff(retryCount);
        retryCount += 1;
        armTimer(delay, () => {
          if (settled) {
            return;
          }
          if (clock.now() >= deadline) {
            settleDeadline();
            return;
          }
          if (inFlight.size > 0) {
            return; // Defensive: another attempt is running; its end re-drives this.
          }
          if (!startAttempt('retry')) {
            failWith('budget-exhausted', lastError);
          }
        });
      };

      const onHedgeTimer = (): void => {
        cancelHedgeTimer = null;
        if (settled || hedgeLaunched || !mayDuplicate) {
          return;
        }
        const original = records[0];
        if (!original || original.recorded) {
          return; // The first request already finished; no hedge needed.
        }
        if (inFlight.size >= this.maxParallelAttempts) {
          return; // No parallel slot: hedging is best-effort, attempted once.
        }
        hedgeLaunched = true; // At most one hedge per execution.
        startAttempt('hedge'); // May still fail on an empty budget.
      };

      // ---- startup --------------------------------------------------------
      if (externalSignal?.aborted) {
        settleAborted();
        return;
      }
      if (deadline <= clock.now()) {
        settleDeadline();
        return;
      }

      if (externalSignal) {
        onExternalAbort = () => settleAborted();
        externalSignal.addEventListener('abort', onExternalAbort, { once: true });
      }

      // The deadline timer is armed before anything else so that, with a
      // FIFO clock, a tie between an attempt completing and the deadline is
      // resolved in favour of the deadline.
      armTimer(deadline - clock.now(), () => settleDeadline());

      if (mayDuplicate && Number.isFinite(this.hedgeDelayMs)) {
        const cancel = armTimer(this.hedgeDelayMs, onHedgeTimer);
        cancelHedgeTimer = () => {
          cancelHedgeTimer = null;
          cancel();
        };
      }

      startAttempt('original');
    });
  }
}
