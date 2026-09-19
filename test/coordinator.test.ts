import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  createCoordinator,
  createTokenBudget,
  PermanentError,
  type AttemptContext,
  type Coordinator,
  type Rng,
  type Sleep,
  type TokenBudget,
} from '../src/index.js';
import { scriptedRng, virtualSleep, VirtualClock } from './virtualClock.js';

interface Harness {
  clock: VirtualClock;
  coordinator: Coordinator;
}

// Deterministic by default: jitter multiplier 1 makes each backoff equal
// to its capped exponential delay exactly, independent of real randomness.
function makeHarness(budget: TokenBudget, rng?: Rng): Harness {
  const clock = new VirtualClock(0);
  const sleep: Sleep = virtualSleep(clock);
  return {
    clock,
    coordinator: createCoordinator({ budget, clock, sleep, rng: rng ?? scriptedRng([1]) }),
  };
}

interface Controlled<T> {
  attempts: ReadonlyArray<{
    context: AttemptContext;
    resolve: (value: T) => void;
    reject: (error: unknown) => void;
  }>;
}

/**
 * Build an attempt fn whose invocations can be settled externally.
 * `signalAware` attempts reject with an AbortError when their signal fires;
 * non-aware ones ignore the signal (used for deadline tie tests).
 */
function controlledAttempt<T>(
  sink: Controlled<T>,
  options: { signalAware?: boolean } = {},
): (context: AttemptContext) => Promise<T> {
  const signalAware = options.signalAware ?? true;
  return (context: AttemptContext) =>
    new Promise<T>((resolve, reject) => {
      const entry = { context, resolve, reject };
      (sink.attempts as typeof sink.attempts & typeof entry[]).push(entry);
      if (signalAware) {
        context.signal.addEventListener(
          'abort',
          () => {
            const error = new Error('aborted');
            error.name = 'AbortError';
            reject(error);
          },
          { once: true },
        );
      }
    });
}

const STANDARD_RETRY = { baseDelayMs: 50, maxDelayMs: 1000, factor: 2 } as const;

test('success resolving at exactly the deadline instant wins over timeout', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(0));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink, { signalAware: false }),
    deadline: 100,
    idempotencyKey: 'k-1',
  });

  // Original resolves at t=100 — the exact deadline tick. The deadline
  // timer (armed at t=0) fires in the same tick, but the attempt's promise
  // reaction runs before the deferred deadline drain.
  clock.setTimeout(() => sink.attempts[0]!.resolve('ok'), 100);

  await clock.advanceTo(100);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, true);
  if (outcome.ok) assert.equal(outcome.value, 'ok');
  assert.equal(outcome.attempts.length, 1);
  assert.equal(outcome.attempts[0]!.endReason, 'success');
  assert.equal(clock.pending(), 0);
});

test('success breaks the tie even though the deadline abort fired microtasks earlier', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(0));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    // Signal-aware: abort schedules a rejection reaction, but here the
    // external completion promise resolves in the same microtask batch.
    attempt: controlledAttempt(sink, { signalAware: true }),
    deadline: 100,
    idempotencyKey: 'k-tie',
  });

  // At t=100: deadline timer pops, aborts (queueing reject reactions),
  // then this completion timer pops and resolves. The fulfill reaction is
  // queued after the reject reaction of the FIRST attempt... but only the
  // promise resolution that reaches the coordinator decides; fulfillment
  // at exactly the deadline wins by rule regardless of abort ordering.
  clock.setTimeout(() => sink.attempts[0]!.resolve('exact'), 100);

  await clock.advanceTo(100);
  const outcome = await outcomePromise;
  assert.equal(outcome.ok, true);
  if (outcome.ok) assert.equal(outcome.value, 'exact');
  assert.equal(outcome.attempts[0]!.endReason, 'success');
  assert.equal(clock.pending(), 0);
});

test('budget with one token: concurrent retries cannot both reserve it', async () => {
  const rng = scriptedRng([1]);
  // Wrapper that swallows the refund of completed originals, so the only
  // token in the system is the initial one — the two failing originals do
  // not each "return" a token and mask contention.
  const shared = createTokenBudget(1);
  const noRefundBudget: TokenBudget = {
    reserve: () => shared.reserve(),
    refund: () => {},
    available: () => shared.available(),
  };
  const clock = new VirtualClock(0);
  const sleep: Sleep = virtualSleep(clock);
  const coordinator = createCoordinator({ budget: noRefundBudget, clock, sleep, rng });
  const sinkA: Controlled<string> = { attempts: [] };
  const sinkB: Controlled<string> = { attempts: [] };

  const outcomeA = coordinator.execute<string>({
    attempt: controlledAttempt(sinkA),
    deadline: 10_000,
    idempotencyKey: 'k-a',
    retry: STANDARD_RETRY,
  });
  const outcomeB = coordinator.execute<string>({
    attempt: controlledAttempt(sinkB),
    deadline: 10_000,
    idempotencyKey: 'k-b',
    retry: STANDARD_RETRY,
  });

  // Both originals fail retryably at t=10 and refund nothing. Backoff
  // sleeps are both due at t=60 (10 + baseDelay 50 under rng=1); both
  // drains call reserve() in the same instant and exactly one can win.
  clock.setTimeout(() => sinkA.attempts[0]!.reject(new Error('boom-a')), 10);
  clock.setTimeout(() => sinkB.attempts[0]!.reject(new Error('boom-b')), 10);
  await clock.advanceTo(60);

  assert.equal(sinkA.attempts.length + sinkB.attempts.length, 3);
  assert.equal(noRefundBudget.available(), 0); // never overdrawn

  // The retry that started succeeds; the other execution failed already.
  const retry = [...sinkA.attempts, ...sinkB.attempts].find(
    (a) => a.context.kind === 'retry',
  )!;
  retry.resolve('winner');
  await clock.advance(0);

  const [resultA, resultB] = await Promise.all([outcomeA, outcomeB]);
  const results = [resultA, resultB];
  assert.equal(results.filter((r) => r.ok).length, 1);
  assert.equal(results.filter((r) => !r.ok && r.code === 'budget-exhausted').length, 1);
  assert.equal(clock.pending(), 0);
});

test('hedge succeeds first: original is cancelled and its timer cleaned up', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(2));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 1_000,
    idempotencyKey: 'k-hedge',
    hedgeDelayMs: 30,
    retry: STANDARD_RETRY,
    maxAttempts: 3,
  });

  await clock.advanceTo(30);
  assert.equal(sink.attempts.length, 2);
  assert.equal(sink.attempts[1]!.context.kind, 'hedge');

  sink.attempts[1]!.resolve('hedge-wins');
  await clock.advance(0);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, true);
  if (outcome.ok) assert.equal(outcome.value, 'hedge-wins');
  assert.equal(outcome.attempts[0]!.endReason, 'cancelled');
  assert.equal(outcome.attempts[1]!.endReason, 'success');
  assert.equal(clock.pending(), 0);
  // Original completion refunds its token; hedge never does.
  assert.equal(coordinator.budget.available(), 2 - 1 + 1);
});

test('external cancellation during backoff rejects once and leaves no timers', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(2));
  const sink: Controlled<string> = { attempts: [] };
  const external = new AbortController();

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 10_000,
    idempotencyKey: 'k-cancel',
    retry: STANDARD_RETRY,
    signal: external.signal,
  });

  // Arm while virtual time is still 0: original fails at absolute t=10,
  // the caller aborts at absolute t=40 (during backoff), while the
  // backoff sleep (full jitter with rng=1) is due at t=60.
  clock.setTimeout(() => sink.attempts[0]!.reject(new Error('transient')), 10);
  clock.setTimeout(() => external.abort(new Error('caller gave up')), 40);
  await clock.advanceTo(10);
  assert.equal(sink.attempts.length, 1); // sleeping in backoff, no new attempt

  await clock.advanceTo(40);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, false);
  if (!outcome.ok) {
    assert.equal(outcome.code, 'aborted');
    assert.match(String((outcome.error as Error).message), /caller gave up/);
  }
  assert.equal(outcome.attempts.length, 1);
  assert.equal(outcome.attempts[0]!.endReason, 'retryable-error');
  assert.equal(clock.pending(), 0);
  // Ledger: the failed original refunds 1 (2 -> 3), the retry reservation
  // deducts 1 (3 -> 2), and cancellation never returns that reservation.
  assert.equal(coordinator.budget.available(), 2);
});

test('permanent error ends immediately without retry or hedge', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(2));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 1_000,
    idempotencyKey: 'k-perm',
    hedgeDelayMs: 5,
    retry: STANDARD_RETRY,
  });

  clock.setTimeout(
    () => sink.attempts[0]!.reject(new PermanentError(new Error('nope'))),
    3,
  );
  await clock.advanceTo(3);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, false);
  if (!outcome.ok) assert.equal(outcome.code, 'permanent-error');
  assert.equal(outcome.attempts.length, 1);
  assert.equal(clock.pending(), 0);
  // Hedge token was never deducted; the completed original refunds its
  // completion (2 -> 3).
  assert.equal(coordinator.budget.available(), 3);
});

test('non-idempotent calls never hedge and never retry even when configured', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(5));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 10_000,
    hedgeDelayMs: 5,
    retry: STANDARD_RETRY,
  });

  clock.setTimeout(() => sink.attempts[0]!.reject(new Error('transient')), 10);
  await clock.advanceTo(200);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, false);
  if (!outcome.ok) assert.equal(outcome.code, 'retries-disabled');
  assert.equal(outcome.attempts.length, 1);
  assert.equal(outcome.attempts[0]!.endReason, 'retryable-error');
  assert.equal(clock.pending(), 0);
  // No retry/hedge tokens deducted; the completed original refunds (5->6).
  assert.equal(coordinator.budget.available(), 6);
});

test('parallel cap is respected when hedge is due and a retry is in flight', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(10));
  const sink: Controlled<string> = { attempts: [] };

  // Two executions: a failing original spawns a retry; meanwhile a second
  // execution's original is still running when ITS hedge is due. With
  // maxParallel=1 the hedge must never start.
  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 10_000,
    idempotencyKey: 'k-cap',
    hedgeDelayMs: 20,
    retry: STANDARD_RETRY,
    maxParallelAttempts: 1,
  });

  await clock.advanceTo(20);
  assert.equal(sink.attempts.length, 1); // hedge suppressed by cap

  sink.attempts[0]!.reject(new Error('x')); // rejected at virtual t=20
  // Let the rejection reaction run so the coordinator arms its backoff
  // sleep before virtual time advances again.
  await Promise.resolve();
  await clock.advanceTo(80); // backoff (20 + 50) has elapsed
  assert.equal(sink.attempts.length, 2);
  assert.equal(sink.attempts[1]!.context.kind, 'retry');

  sink.attempts[1]!.resolve('done');
  await clock.advance(0);
  const outcome = await outcomePromise;
  assert.equal(outcome.ok, true);
});

test('budget exhausted reports failure with snapshots for the in-flight batch', async () => {
  // Zero tokens and refunds swallowed: after both originals fail there is
  // nothing to reserve, so both executions settle budget-exhausted instead
  // of hanging on retries that could never be paid for.
  const shared = createTokenBudget(0);
  const budget: TokenBudget = {
    reserve: () => shared.reserve(),
    refund: () => {},
    available: () => shared.available(),
  };
  const { clock, coordinator } = makeHarness(budget);
  const sinkA: Controlled<string> = { attempts: [] };
  const sinkB: Controlled<string> = { attempts: [] };

  const a = coordinator.execute<string>({
    attempt: controlledAttempt(sinkA),
    deadline: 10_000,
    idempotencyKey: 'k-ea',
    retry: STANDARD_RETRY,
  });
  const b = coordinator.execute<string>({
    attempt: controlledAttempt(sinkB),
    deadline: 10_000,
    idempotencyKey: 'k-eb',
    retry: STANDARD_RETRY,
  });

  clock.setTimeout(() => sinkA.attempts[0]!.reject(new Error('a')), 10);
  clock.setTimeout(() => sinkB.attempts[0]!.reject(new Error('b')), 10);
  await clock.advanceTo(100);

  const [ra, rb] = await Promise.all([a, b]);
  assert.equal(ra.ok, false);
  assert.equal(rb.ok, false);
  assert.equal(ra.code, 'budget-exhausted');
  assert.equal(rb.code, 'budget-exhausted');
  assert.equal(clock.pending(), 0);
});

test('already-elapsed deadline settles immediately without starting an attempt', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(0));
  let called = 0;
  const outcome = await coordinator.execute<string>({
    attempt: () => {
      called += 1;
      return 'never';
    },
    deadline: clock.now(), // exactly now: start >= deadline
    idempotencyKey: 'k-past',
  });
  assert.equal(outcome.ok, false);
  if (!outcome.ok) assert.equal(outcome.code, 'deadline-exceeded');
  assert.equal(called, 0);
  assert.equal(outcome.attemptsStarted, 0);
});

test('exponential backoff uses full jitter from the injected rng', async () => {
  const rng = scriptedRng([0.5]);
  const { clock, coordinator } = makeHarness(createTokenBudget(10), rng);
  const sink: Controlled<string> = { attempts: [] };

  const startedAt: number[] = [];
  const outcomePromise = coordinator.execute<string>({
    attempt: (ctx) => {
      startedAt.push(clock.now());
      return controlledAttempt(sink)(ctx);
    },
    deadline: 10_000,
    idempotencyKey: 'k-jitter',
    maxAttempts: 3,
    retry: { baseDelayMs: 100, maxDelayMs: 10_000, factor: 2 },
  });

  // Absolute timeline (rng=0.5, base=100, factor=2):
  //   t=10   original rejects         -> backoff 0.5*100*2^0 = 50
  //   t=60   retry 1 starts
  //   t=80   retry 1 rejects          -> backoff 0.5*100*2^1 = 100
  //   t=180  retry 2 starts
  clock.setTimeout(() => sink.attempts[0]!.reject(new Error('1')), 10);
  clock.setTimeout(() => sink.attempts[1]!.reject(new Error('2')), 80);
  await clock.advanceTo(180);
  assert.deepEqual(startedAt, [0, 60, 180]);

  sink.attempts[2]!.resolve('ok');
  await clock.advance(0);
  const outcome = await outcomePromise;
  assert.equal(outcome.ok, true);
});

test('deadline during backoff fails as deadline-exceeded', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(10));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 40,
    idempotencyKey: 'k-db',
    retry: STANDARD_RETRY, // backoff 50 > remaining time
  });

  clock.setTimeout(() => sink.attempts[0]!.reject(new Error('x')), 10);
  await clock.advanceTo(60);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, false);
  if (!outcome.ok) assert.equal(outcome.code, 'deadline-exceeded');
  assert.equal(outcome.attempts.length, 1);
  assert.equal(clock.pending(), 0);
});

test('attempts-exhausted after maxAttempts retryable failures', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(10));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 10_000,
    idempotencyKey: 'k-max',
    maxAttempts: 2,
    retry: { ...STANDARD_RETRY, baseDelayMs: 10 },
  });

  clock.setTimeout(() => sink.attempts[0]!.reject(new Error('1')), 1);
  await clock.advanceTo(20);
  assert.equal(sink.attempts.length, 2);
  clock.setTimeout(() => sink.attempts[1]!.reject(new Error('2')), 21);
  await clock.advanceTo(40);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, false);
  if (!outcome.ok) assert.equal(outcome.code, 'attempts-exhausted');
  assert.equal(outcome.attempts.length, 2);
  assert.equal(clock.pending(), 0);
});

test('snapshots carry timestamps, kinds and end reasons for every attempt', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(10));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 1_000,
    idempotencyKey: 'k-snap',
    hedgeDelayMs: 30,
    maxAttempts: 3,
    retry: STANDARD_RETRY,
  });

  await clock.advanceTo(30); // hedge starts at 30
  sink.attempts[0]!.reject(new Error('original-fails'));
  await clock.advance(0); // hedge still running: no retry yet
  sink.attempts[1]!.resolve('hedge-value');
  await clock.advance(0);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, true);
  assert.equal(outcome.attemptsStarted, 2);
  const [a0, a1] = outcome.attempts;
  assert.equal(a0!.kind, 'original');
  assert.equal(a0!.startedAt, 0);
  assert.equal(typeof a0!.endedAt, 'number');
  assert.equal(a0!.endReason, 'retryable-error');
  assert.match(String((a0!.error as Error).message), /original-fails/);
  assert.equal(a1!.kind, 'hedge');
  assert.equal(a1!.startedAt, 30);
  assert.equal(a1!.endReason, 'success');
});

test('original success with a key refunds its completion token', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(2));
  const sink: Controlled<string> = { attempts: [] };

  const outcomePromise = coordinator.execute<string>({
    attempt: controlledAttempt(sink),
    deadline: 1_000,
    idempotencyKey: 'k-ok',
    hedgeDelayMs: 30,
  });
  sink.attempts[0]!.resolve('value');
  await clock.advance(0);
  const outcome = await outcomePromise;

  assert.equal(outcome.ok, true);
  if (outcome.ok) assert.equal(outcome.value, 'value');
  assert.equal(outcome.attempts[0]!.endReason, 'success');
  assert.equal(clock.pending(), 0);
  assert.equal(coordinator.budget.available(), 3); // 2 + completion refund
});

test('non-idempotent immediate success never touches the token budget', async () => {
  const { clock, coordinator } = makeHarness(createTokenBudget(1));
  const outcome = await coordinator.execute<string>({
    attempt: () => 'plain',
    deadline: clock.now() + 1000,
  });
  assert.equal(outcome.ok, true);
  if (outcome.ok) assert.equal(outcome.value, 'plain');
  assert.equal(outcome.idempotencyKey, undefined);
  assert.equal(coordinator.budget.available(), 2); // original completion refund
  assert.equal(clock.pending(), 0);
});

test('real clock smoke test (short real delay) for default injectables', async () => {
  const coordinator = createCoordinator({ budget: createTokenBudget(0) });
  const outcome = await coordinator.execute<string>({
    attempt: () => 'immediate',
    timeoutMs: 1000,
    idempotencyKey: 'k-real',
  });
  assert.equal(outcome.ok, true);
  if (outcome.ok) assert.equal(outcome.value, 'immediate');
});
