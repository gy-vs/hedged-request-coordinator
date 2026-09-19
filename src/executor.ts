import { createSystemSleep, systemClock, systemRng } from './clock.js';
import { PermanentError } from './errors.js';
import type {
  AttemptContext,
  AttemptEndReason,
  AttemptFn,
  AttemptKind,
  AttemptSnapshot,
  Clock,
  CoordinatorDeps,
  ExecuteFailure,
  ExecuteOptions,
  ExecuteOutcome,
  FailureCode,
  RetryPolicy,
  Rng,
  Sleep,
  TokenBudget,
} from './types.js';

export interface Coordinator {
  /**
   * Execute `options.attempt` with retries and at most one hedge, bounded
   * by an absolute deadline and the shared token budget. Never throws for
   * attempt-level failures: they are reported in the returned outcome.
   * Configuration errors reject the returned promise.
   */
  execute<T>(options: ExecuteOptions<T>): Promise<ExecuteOutcome<T>>;
  readonly budget: TokenBudget;
}

interface AttemptRecord {
  readonly attemptNumber: number;
  readonly kind: AttemptKind;
  readonly context: AttemptContext;
  readonly startedAt: number;
  endedAt: number | undefined;
  reason: AttemptEndReason | undefined;
  error: unknown;
}

interface TerminalLatch {
  code: FailureCode;
  error: unknown;
}

const DEFAULT_POLICY: Required<Pick<RetryPolicy, 'baseDelayMs' | 'maxDelayMs' | 'factor'>> = {
  baseDelayMs: 100,
  maxDelayMs: 5_000,
  factor: 2,
};

/** Build a request coordinator with the given injectable dependencies. */
export function createCoordinator(deps: CoordinatorDeps): Coordinator {
  if (!deps || typeof deps !== 'object' || !deps.budget) {
    throw new TypeError('createCoordinator: deps.budget is required');
  }
  const clock: Clock = deps.clock ?? systemClock;
  const rng: Rng = deps.rng ?? systemRng;
  const sleep: Sleep = deps.sleep ?? createSystemSleep(clock);

  function execute<T>(options: ExecuteOptions<T>): Promise<ExecuteOutcome<T>> {
    return new Promise<ExecuteOutcome<T>>((resolve) => {
      run<T>(options, resolve);
    });
  }

  function run<T>(
    options: ExecuteOptions<T>,
    resolve: (outcome: ExecuteOutcome<T>) => void,
  ): void {
    validateOptions(options);

    const attemptFn: AttemptFn<T> = options.attempt;
    const budget: TokenBudget = options.budget ?? deps.budget;
    const key = options.idempotencyKey;
    const idempotent = key !== undefined;
    const startedAt = clock.now();
    const deadlineRaw =
      options.deadline ??
      (options.timeoutMs !== undefined ? startedAt + options.timeoutMs : undefined);

    if (deadlineRaw === undefined || !Number.isFinite(deadlineRaw)) {
      throw new TypeError('execute: either deadline or timeoutMs is required');
    }
    const deadline: number = deadlineRaw;

    // The deadline is exclusive and success breaks ties: a call whose
    // deadline has already arrived never starts an attempt.
    if (startedAt >= deadline) {
      resolve(
        freezeFailure({
          ok: false,
          code: 'deadline-exceeded',
          idempotencyKey: key,
          startedAt,
          endedAt: startedAt,
          attemptsStarted: 0,
          attempts: [],
        }),
      );
      return;
    }

    const maxAttempts = options.maxAttempts ?? 3;
    const hedgeEnabled =
      idempotent &&
      options.hedgeDelayMs !== undefined &&
      Number.isFinite(options.hedgeDelayMs) &&
      options.hedgeDelayMs >= 0;
    const maxParallel = options.maxParallelAttempts ?? (hedgeEnabled ? 2 : 1);
    // Non-idempotent calls never hedge and never auto-retry, even when the
    // caller configures delays/policies. An explicit idempotency key is the
    // only thing that unlocks them.
    const canRetry = idempotent && options.retry !== false;
    const policy = mergePolicy(deps.retry, options.retry === false ? undefined : options.retry);
    validatePolicy(policy);
    const externalSignal = options.signal;

    // ---- mutable execution state ------------------------------------
    const internal = new AbortController();
    const internalAbortError = makeAbortError('attempt aborted by coordinator');
    const deadlineError = makeAbortError('deadline exceeded');
    const attempts: AttemptRecord[] = [];
    const inflight = new Set<AttemptRecord>();
    let settled = false;
    let retryableFailures = 0;
    let terminalLatch: TerminalLatch | undefined;
    let deadlineTimer: unknown;
    let deadlineDrainTimer: unknown;
    let hedgeTimer: unknown;
    let deadlineDrainScheduled = false;

    function clearDeadlineDrain(): void {
      if (deadlineDrainTimer !== undefined) {
        clock.clearTimeout(deadlineDrainTimer);
        deadlineDrainTimer = undefined;
      }
      deadlineDrainScheduled = false;
    }

    const toSnapshot = (a: AttemptRecord): AttemptSnapshot =>
      Object.freeze({
        attemptNumber: a.attemptNumber,
        kind: a.kind,
        startedAt: a.startedAt,
        endedAt: a.endedAt as number,
        endReason: a.reason as AttemptEndReason,
        ...(a.error !== undefined ? { error: a.error } : {}),
      });

    function buildOutcome(
      ok: boolean,
      fields: { value?: T; code?: FailureCode; error?: unknown; endedAt: number },
    ): ExecuteOutcome<T> {
      const base = {
        idempotencyKey: key,
        startedAt,
        endedAt: fields.endedAt,
        attemptsStarted: attempts.length,
        attempts: Object.freeze(attempts.map(toSnapshot)),
      };
      if (ok) {
        return Object.freeze({ ok: true as const, value: fields.value as T, ...base });
      }
      const failure: ExecuteFailure = Object.freeze({
        ok: false as const,
        code: fields.code as FailureCode,
        ...(fields.error !== undefined ? { error: fields.error } : {}),
        ...base,
      });
      return failure;
    }

    // Close an attempt that is still in flight, removing all of its
    // bookkeeping. Refunding the original's token happens exactly here:
    // only a completed (or force-closed) original ever returns a token.
    function close(
      record: AttemptRecord,
      now: number,
      reason: AttemptEndReason,
      error: unknown,
    ): void {
      if (record.endedAt !== undefined) return;
      record.endedAt = now;
      record.reason = reason;
      record.error = error;
      inflight.delete(record);
      if (record.kind === 'original') {
        clock.clearTimeout(hedgeTimer);
        budget.refund();
      }
    }

    function forceCloseInflight(
      now: number,
      reason: AttemptEndReason,
      error: unknown,
    ): void {
      // Snapshot first: close() deletes from the same Set, and iterating
      // while mutating would otherwise call close() twice on a record
      // (the second call was a no-op for timestamps but must never reach
      // the original-attempt refund path a second time).
      const records = [...inflight];
      inflight.clear();
      for (const record of records) {
        close(record, now, reason, error);
      }
    }

    function finish(
      code: FailureCode,
      error: unknown,
      abortReason: AttemptEndReason,
    ): void {
      if (settled) return;
      settled = true;
      const now = clock.now();
      clock.clearTimeout(deadlineTimer);
      clearDeadlineDrain();
      clock.clearTimeout(hedgeTimer);
      if (externalSignal) {
        externalSignal.removeEventListener('abort', onExternalAbort);
      }
      forceCloseInflight(now, abortReason, error);
      // Abort AFTER snapshots are finalized, so reactions to the abort see
      // a settled execution and every attempt with an end timestamp.
      internal.abort(error);
      resolve(buildOutcome(false, { code, error, endedAt: now }));
    }

    function settleSuccess(record: AttemptRecord, value: T): void {
      const now = clock.now();
      close(record, now, 'success', undefined);
      settled = true;
      clock.clearTimeout(deadlineTimer);
      clearDeadlineDrain();
      clock.clearTimeout(hedgeTimer);
      if (externalSignal) {
        externalSignal.removeEventListener('abort', onExternalAbort);
      }
      forceCloseInflight(now, 'cancelled', internalAbortError);
      internal.abort(internalAbortError);
      resolve(buildOutcome(true, { value, endedAt: now }));
    }

    function onExternalAbort(): void {
      if (settled) return;
      const reason = abortReasonOf(externalSignal);
      finish('aborted', reason, 'external-aborted');
    }

    // Deadline handling deliberately neither aborts nor settles inline:
    // it arms a fresh 0-delay drain. The drain is inserted AFTER every
    // timer already due at the deadline instant, so callbacks of attempts
    // that complete exactly AT the deadline (same virtual tick) — and the
    // promise reactions they schedule — run first and win the tie. The
    // internal signal is only aborted inside the drain, meaning abort
    // listeners cannot queue rejection reactions ahead of an exact-tick
    // fulfillment. Only if nobody won does the drain abort and settle as
    // deadline-exceeded.
    function onDeadline(): void {
      if (settled || deadlineDrainScheduled) return;
      deadlineDrainScheduled = true;
      deadlineDrainTimer = clock.setTimeout(() => {
        deadlineDrainTimer = undefined;
        if (settled) return;
        internal.abort(deadlineError);
        finish('deadline-exceeded', deadlineError, 'deadline-aborted');
      }, 0);
    }

    function armHedge(): void {
      if (!hedgeEnabled) return;
      const delay = options.hedgeDelayMs as number;
      hedgeTimer = clock.setTimeout(() => {
        if (settled) return;
        // Synchronous gate: parallel cap, total cap and budget are all
        // evaluated and deducted without an intervening await.
        if (inflight.size >= maxParallel) return;
        if (attempts.length >= maxAttempts) return;
        if (!budget.reserve()) return;
        startAttempt('hedge');
      }, delay);
    }

    function startAttempt(kind: AttemptKind): void {
      if (settled) return;
      const attemptNumber = attempts.length + 1;
      if (attemptNumber > maxAttempts || inflight.size >= maxParallel) return;
      const context: AttemptContext = {
        attemptNumber,
        kind,
        deadline,
        ...(key !== undefined ? { idempotencyKey: key } : {}),
        signal: internal.signal,
      };
      const record: AttemptRecord = {
        attemptNumber,
        kind,
        context,
        startedAt: clock.now(),
        endedAt: undefined,
        reason: undefined,
        error: undefined,
      };
      attempts.push(record);
      inflight.add(record);

      let result: PromiseLike<T> | T;
      try {
        result = attemptFn(context);
      } catch (error) {
        queueMicrotask(() => {
          onRejected(record, error);
        });
        if (kind === 'original') armHedge();
        return;
      }
      Promise.resolve(result).then(
        (value) => {
          onFulfilled(record, value);
        },
        (error: unknown) => {
          onRejected(record, error);
        },
      );
      if (kind === 'original') armHedge();
    }

    function onFulfilled(record: AttemptRecord, value: T): void {
      if (record.endedAt !== undefined) {
        // Force-closed earlier because the execution settled; a late
        // fulfillment changes the recorded reason but not timestamps.
        if (settled) record.reason = 'superseded';
        return;
      }
      if (settled) return;
      // Tie rule: a value available at exactly the deadline instant wins.
      if (clock.now() > deadline) {
        close(record, clock.now(), 'deadline-aborted', deadlineError);
        onDeadline();
        return;
      }
      settleSuccess(record, value);
    }

    function onRejected(record: AttemptRecord, error: unknown): void {
      if (record.endedAt !== undefined || settled) return;
      const now = clock.now();

      if (externalSignal !== undefined && externalSignal.aborted) {
        close(record, now, 'external-aborted', error);
        finish('aborted', abortReasonOf(externalSignal), 'external-aborted');
        return;
      }
      if (internal.signal.aborted) {
        // Internal abort before settlement only happens via the deadline
        // path (success aborts after setting `settled`).
        close(record, now, 'deadline-aborted', error);
        onDeadline();
        return;
      }

      const permanentMarked = error instanceof PermanentError;
      const retryable =
        !permanentMarked &&
        canRetry &&
        evaluateRetryable(error, record.context, policy);

      if (permanentMarked || !retryable) {
        const reason: AttemptEndReason = permanentMarked
          ? 'permanent-error'
          : canRetry
            ? 'permanent-error'
            : 'retryable-error';
        close(record, now, reason, error);
        terminalLatch ??= {
          code: permanentMarked
            ? 'permanent-error'
            : canRetry
              ? 'permanent-error'
              : 'retries-disabled',
          error,
        };
        afterTerminalCompletion(record);
        return;
      }

      close(record, now, 'retryable-error', error);
      retryableFailures += 1;
      afterTerminalCompletion(record);
    }

    // Called with `record` already closed. Retries launch only once the
    // whole in-flight batch drained, so a failing sibling never spawns a
    // retry while another attempt is still running.
    function afterTerminalCompletion(record: AttemptRecord): void {
      if (settled || inflight.size > 0) return;
      if (terminalLatch) {
        finish(terminalLatch.code, terminalLatch.error, 'cancelled');
        return;
      }
      if (record.reason === 'permanent-error') {
        finish('permanent-error', record.error, 'cancelled');
        return;
      }
      if (!canRetry) {
        finish('retries-disabled', record.error, 'cancelled');
        return;
      }
      if (attempts.length >= maxAttempts) {
        finish('attempts-exhausted', record.error, 'cancelled');
        return;
      }
      // Atomic deduction for the retry: reserve() is one synchronous
      // read-and-decrement with no await around it.
      if (!budget.reserve()) {
        finish('budget-exhausted', record.error, 'cancelled');
        return;
      }

      const backoff =
        rng() *
        Math.min(
          policy.maxDelayMs,
          policy.baseDelayMs * Math.pow(policy.factor, retryableFailures - 1),
        );
      void runBackoff(Math.max(0, backoff));
    }

    async function runBackoff(delayMs: number): Promise<void> {
      try {
        await sleep(delayMs, internal.signal);
      } catch (error) {
        if (settled) return;
        if (externalSignal !== undefined && externalSignal.aborted) {
          finish('aborted', abortReasonOf(externalSignal), 'external-aborted');
        } else {
          finish('deadline-exceeded', deadlineError, 'deadline-aborted');
        }
        return;
      }
      if (settled || inflight.size > 0) return;
      // A new batch begins: permanence observed in the previous batch must
      // not leak into this one.
      terminalLatch = undefined;
      if (attempts.length >= maxAttempts) {
        const last = attempts[attempts.length - 1];
        finish('attempts-exhausted', last?.error, 'cancelled');
        return;
      }
      if (clock.now() >= deadline) {
        finish('deadline-exceeded', deadlineError, 'deadline-aborted');
        return;
      }
      startAttempt('retry');
    }

    // ---- wiring ------------------------------------------------------
    deadlineTimer = clock.setTimeout(onDeadline, Math.max(0, deadline - startedAt));
    if (externalSignal) {
      if (externalSignal.aborted) {
        onExternalAbort();
        return;
      }
      externalSignal.addEventListener('abort', onExternalAbort, { once: true });
    }
    startAttempt('original');

    function validateOptions(opts: ExecuteOptions<T>): void {
      if (!opts || typeof opts.attempt !== 'function') {
        throw new TypeError('execute: options.attempt must be a function');
      }
      if (
        opts.deadline === undefined &&
        (opts.timeoutMs === undefined || !Number.isFinite(opts.timeoutMs) || opts.timeoutMs < 0)
      ) {
        throw new TypeError('execute: a finite deadline or non-negative timeoutMs is required');
      }
      if (opts.idempotencyKey !== undefined && typeof opts.idempotencyKey !== 'string') {
        throw new TypeError('execute: idempotencyKey must be a string');
      }
      if (opts.maxAttempts !== undefined && (!Number.isInteger(opts.maxAttempts) || opts.maxAttempts < 1)) {
        throw new RangeError('execute: maxAttempts must be a positive integer');
      }
      if (
        opts.maxParallelAttempts !== undefined &&
        (!Number.isInteger(opts.maxParallelAttempts) || opts.maxParallelAttempts < 1)
      ) {
        throw new RangeError('execute: maxParallelAttempts must be a positive integer');
      }
      if (
        opts.hedgeDelayMs !== undefined &&
        (!Number.isFinite(opts.hedgeDelayMs) || opts.hedgeDelayMs < 0)
      ) {
        throw new RangeError('execute: hedgeDelayMs must be a non-negative finite number');
      }
    }
  }

  return {
    execute,
    get budget(): TokenBudget {
      return deps.budget;
    },
  };
}

function mergePolicy(base: RetryPolicy | undefined, override: RetryPolicy | undefined): RetryPolicy {
  return {
    baseDelayMs: override?.baseDelayMs ?? base?.baseDelayMs ?? DEFAULT_POLICY.baseDelayMs,
    maxDelayMs: override?.maxDelayMs ?? base?.maxDelayMs ?? DEFAULT_POLICY.maxDelayMs,
    factor: override?.factor ?? base?.factor ?? DEFAULT_POLICY.factor,
    isRetryable: override?.isRetryable ?? base?.isRetryable,
  };
}

function validatePolicy(policy: RetryPolicy): void {
  if (
    !Number.isFinite(policy.baseDelayMs) ||
    policy.baseDelayMs < 0 ||
    !Number.isFinite(policy.maxDelayMs) ||
    policy.maxDelayMs < 0 ||
    !Number.isFinite(policy.factor) ||
    policy.factor < 1
  ) {
    throw new RangeError(
      'execute: retry policy requires 0 <= baseDelayMs, 0 <= maxDelayMs and factor >= 1',
    );
  }
}

function evaluateRetryable(
  error: unknown,
  context: AttemptContext,
  policy: RetryPolicy,
): boolean {
  if (!policy.isRetryable) return true;
  try {
    return policy.isRetryable(error, context) === true;
  } catch {
    return false;
  }
}

function abortReasonOf(signal: AbortSignal | undefined): unknown {
  if (!signal) return undefined;
  return (signal as AbortSignal & { reason?: unknown }).reason;
}

function makeAbortError(message: string): Error {
  const error = new Error(message);
  error.name = 'AbortError';
  return error;
}

function freezeFailure(failure: ExecuteFailure): ExecuteFailure {
  return Object.freeze({
    ...failure,
    attempts: Object.freeze([...failure.attempts]),
  });
}
