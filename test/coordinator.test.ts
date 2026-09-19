import { test } from 'node:test';
import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import {
  DeadlineExceededError,
  ExecutionAbortedError,
  ExecutionFailedError,
  PermanentError,
  RequestCoordinator,
  RetryableError,
  VirtualClock,
  type AttemptContext,
  type CoordinatorOptions,
} from '../src/index.js';

/**
 * All tests run on a VirtualClock: no real time passes, and timer ordering
 * is deterministic (equal-time timers fire FIFO).
 */
function setup(overrides: CoordinatorOptions = {}) {
  const clock = new VirtualClock(0);
  const coordinator = new RequestCoordinator({
    clock,
    sleeper: clock,
    random: () => 0.5, // symmetric jitter collapses to the exact backoff
    maxParallelAttempts: 2,
    hedgeDelayMs: 100,
    baseBackoffMs: 100,
    maxBackoffMs: 1_000,
    jitter: 0.2,
    maxRetries: 3,
    budgetTokens: 10,
    ...overrides,
  });
  return { clock, coordinator };
}

test('success resolves with the value and a complete attempt snapshot', async () => {
  const { clock, coordinator } = setup();
  const controller = new AbortController();

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      await clock.sleep(50, ctx.signal);
      return 'ok';
    },
    deadlineMs: 1_000,
    idempotent: true,
    signal: controller.signal,
  });
  await clock.runAll();
  const res = await p;

  assert.equal(res.value, 'ok');
  assert.equal(res.winnerIndex, 0);
  assert.equal(res.attempts.length, 1);
  const attempt = res.attempts[0]!;
  assert.equal(attempt.kind, 'original');
  assert.equal(attempt.startedAt, 0);
  assert.equal(attempt.endedAt, 50);
  assert.equal(attempt.endReason, 'success');

  // Everything cleaned up: no timers left, external listener detached.
  assert.equal(clock.pendingCount, 0);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
});

test('retryable errors retry with exponential backoff', async () => {
  const { clock, coordinator } = setup();

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      if (ctx.attemptIndex < 2) {
        throw new RetryableError('flaky');
      }
      return 'ok';
    },
    deadlineMs: 10_000,
    idempotent: true,
  });
  await clock.runAll();
  const res = await p;

  assert.equal(res.value, 'ok');
  assert.deepEqual(res.attempts.map((a) => a.kind), ['original', 'retry', 'retry']);
  // backoffs: 100 (100 * 2^0), then 200 (100 * 2^1)
  assert.deepEqual(res.attempts.map((a) => a.startedAt), [0, 100, 300]);
  assert.deepEqual(
    res.attempts.map((a) => a.endReason),
    ['retryable-error', 'retryable-error', 'success'],
  );
});

test('permanent errors end the execution immediately', async () => {
  const { clock, coordinator } = setup();

  const p = coordinator.execute({
    attempt: async () => {
      throw new PermanentError('nope');
    },
    deadlineMs: 10_000,
    idempotent: true,
  });
  const assertion = assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof ExecutionFailedError);
    assert.equal(err.reason, 'permanent-error');
    assert.ok(err.cause instanceof PermanentError);
    assert.equal(err.attempts.length, 1);
    assert.equal(err.attempts[0]!.endReason, 'permanent-error');
    return true;
  });
  await clock.runAll();
  await assertion;
  assert.equal(clock.pendingCount, 0);
});

test('non-idempotent executions are not retried', async () => {
  const { clock, coordinator } = setup();
  let calls = 0;

  const p = coordinator.execute({
    attempt: async () => {
      calls++;
      throw new RetryableError('flaky');
    },
    deadlineMs: 10_000,
  });
  const assertion = assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof ExecutionFailedError);
    assert.equal(err.reason, 'not-idempotent');
    assert.equal(err.attempts.length, 1);
    return true;
  });
  await clock.runAll();
  await assertion;
  assert.equal(calls, 1);
});

test('non-idempotent executions are not hedged', async () => {
  const { clock, coordinator } = setup();
  let calls = 0;

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      calls++;
      await clock.sleep(500, ctx.signal);
      return 'ok';
    },
    deadlineMs: 10_000,
  });
  await clock.runAll();
  const res = await p;

  assert.equal(res.value, 'ok');
  assert.equal(calls, 1); // hedgeDelay of 100 elapsed, but no hedge was launched
  assert.equal(res.attempts.length, 1);
});

test('an explicit idempotency key opts a non-idempotent request into hedging', async () => {
  const { clock, coordinator } = setup();
  const seenKeys: Array<string | undefined> = [];

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      seenKeys.push(ctx.idempotencyKey);
      if (ctx.kind === 'hedge') {
        await clock.sleep(50, ctx.signal);
        return 'hedge-ok';
      }
      await clock.sleep(500, ctx.signal);
      return 'original-ok';
    },
    deadlineMs: 10_000,
    idempotencyKey: 'key-123',
  });
  await clock.runAll();
  const res = await p;

  assert.equal(res.value, 'hedge-ok');
  assert.deepEqual(seenKeys, ['key-123', 'key-123']);
  assert.deepEqual(res.attempts.map((a) => a.kind), ['original', 'hedge']);
});

test('the deadline cancels in-flight attempts', async () => {
  const { clock, coordinator } = setup({ hedgeDelayMs: 5_000 });

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      await clock.sleep(5_000, ctx.signal);
      return 'late';
    },
    deadlineMs: 1_000,
    idempotent: true,
  });
  const assertion = assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof DeadlineExceededError);
    assert.equal(err.attempts.length, 1);
    assert.equal(err.attempts[0]!.endReason, 'cancelled');
    assert.equal(err.attempts[0]!.endedAt, 1_000);
    return true;
  });
  await clock.runAll();
  await assertion;
  assert.equal(clock.pendingCount, 0);
});

test('success and deadline at the same instant settle exactly once (deadline wins the tie)', async () => {
  const { clock, coordinator } = setup({ hedgeDelayMs: 5_000 });

  // The attempt completes at exactly t=1000, the deadline is also t=1000.
  // The deadline timer is armed before any attempt starts, so with FIFO
  // ordering the deadline wins. The important guarantees: one settlement,
  // a cancelled-attempt snapshot, and full cleanup.
  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      await clock.sleep(1_000, ctx.signal);
      return 'photo-finish';
    },
    deadlineMs: 1_000,
    idempotent: true,
  });
  const assertion = assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof DeadlineExceededError);
    assert.equal(err.attempts.length, 1);
    assert.equal(err.attempts[0]!.endReason, 'cancelled');
    assert.equal(err.attempts[0]!.startedAt, 0);
    assert.equal(err.attempts[0]!.endedAt, 1_000);
    return true;
  });
  await clock.runAll();
  await assertion;

  assert.equal(clock.pendingCount, 0);
  // The original's token was returned exactly once (no double settle).
  assert.equal(coordinator.budgetAvailable, 10);
});

test('success just before the deadline wins', async () => {
  const { clock, coordinator } = setup({ hedgeDelayMs: 5_000 });

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      await clock.sleep(999, ctx.signal);
      return 'ok';
    },
    deadlineMs: 1_000,
    idempotent: true,
  });
  await clock.runAll();
  assert.equal((await p).value, 'ok');
});

test('a single remaining budget token pays for exactly one of two concurrent retries', async () => {
  const { clock, coordinator } = setup({
    budgetTokens: 1,
    maxRetries: 1,
    hedgeDelayMs: 5_000,
  });

  let e1Calls = 0;
  const p1 = coordinator.execute({
    attempt: async () => {
      e1Calls++;
      if (e1Calls === 1) {
        throw new RetryableError('flaky');
      }
      return 'e1-ok';
    },
    deadlineMs: 10_000,
    idempotent: true,
  });
  const p2 = coordinator.execute({
    attempt: async () => {
      throw new RetryableError('flaky');
    },
    deadlineMs: 10_000,
    idempotent: true,
  });
  const settled = Promise.allSettled([p1, p2]);
  await clock.runAll();
  const [r1, r2] = await settled;

  // Both originals failed and returned their token (capped at capacity 1),
  // so exactly one token existed when both retries became due. The first
  // retry scheduled wins it; the second sees an empty budget.
  assert.equal(r1.status, 'fulfilled');
  assert.equal(r1.status === 'fulfilled' && r1.value.value, 'e1-ok');
  assert.equal(r2.status, 'rejected');
  if (r2.status === 'rejected') {
    assert.ok(r2.reason instanceof ExecutionFailedError);
    assert.equal((r2.reason as ExecutionFailedError).reason, 'budget-exhausted');
    assert.ok((r2.reason as ExecutionFailedError).cause instanceof RetryableError);
    assert.equal((r2.reason as ExecutionFailedError).attempts.length, 1);
  }
  assert.equal(coordinator.budgetAvailable, 0);
  assert.equal(clock.pendingCount, 0);
});

test('completed original attempts replenish the shared budget', async () => {
  const { clock, coordinator } = setup({
    budgetTokens: 1,
    maxRetries: 1,
    hedgeDelayMs: 5_000,
  });

  const makeAttempt = () => {
    let calls = 0;
    return async () => {
      calls++;
      if (calls === 1) {
        throw new RetryableError('flaky');
      }
      return 'ok';
    };
  };

  const p1 = coordinator.execute({ attempt: makeAttempt(), deadlineMs: 10_000, idempotent: true });
  await clock.runAll();
  assert.equal((await p1).value, 'ok');
  assert.equal(coordinator.budgetAvailable, 0); // the retry spent the only token

  // A second execution can still retry: its own original completion
  // returned a token to the budget before the retry needed it.
  const p2 = coordinator.execute({ attempt: makeAttempt(), deadlineMs: 10_000, idempotent: true });
  await clock.runAll();
  assert.equal((await p2).value, 'ok');
});

test('a faster hedge wins, cancels the original, and balances the budget', async () => {
  const { clock, coordinator } = setup({ budgetTokens: 5 });
  let originalSawAbort = false;

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      if (ctx.kind === 'hedge') {
        await clock.sleep(200, ctx.signal);
        return 'hedge-ok';
      }
      try {
        await clock.sleep(500, ctx.signal);
      } catch (err) {
        originalSawAbort = ctx.signal.aborted;
        throw err;
      }
      return 'original-ok';
    },
    deadlineMs: 5_000,
    idempotent: true,
  });
  await clock.runAll();
  const res = await p;

  assert.equal(res.value, 'hedge-ok');
  assert.equal(res.winnerIndex, 1);
  assert.equal(res.attempts.length, 2);

  const [original, hedge] = res.attempts as [any, any];
  assert.deepEqual(
    [original.kind, original.startedAt, original.endedAt, original.endReason],
    ['original', 0, 300, 'cancelled'],
  );
  assert.deepEqual(
    [hedge.kind, hedge.startedAt, hedge.endedAt, hedge.endReason],
    ['hedge', 100, 300, 'success'],
  );

  // The loser was signalled to stop.
  assert.ok(originalSawAbort);
  // Hedge spent 1 token; the original's completion returned 1.
  assert.equal(coordinator.budgetAvailable, 5);
  assert.equal(clock.pendingCount, 0);
});

test('original success cancels an in-flight hedge', async () => {
  const { clock, coordinator } = setup({ budgetTokens: 5 });

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      if (ctx.kind === 'hedge') {
        await clock.sleep(400, ctx.signal);
        return 'hedge-ok';
      }
      await clock.sleep(300, ctx.signal);
      return 'original-ok';
    },
    deadlineMs: 5_000,
    idempotent: true,
  });
  await clock.runAll();
  const res = await p;

  assert.equal(res.value, 'original-ok');
  assert.equal(res.winnerIndex, 0);
  assert.deepEqual(
    res.attempts.map((a) => a.endReason),
    ['success', 'cancelled'],
  );
  assert.equal(res.attempts[1]!.endedAt, 300);
});

test('external abort during the retry backoff settles once and cleans up', async () => {
  const { clock, coordinator } = setup({ baseBackoffMs: 1_000, hedgeDelayMs: 5_000 });
  const controller = new AbortController();
  let calls = 0;

  const p = coordinator.execute({
    attempt: async () => {
      calls++;
      throw new RetryableError('flaky');
    },
    deadlineMs: 60_000,
    idempotent: true,
    signal: controller.signal,
  });
  const assertion = assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof ExecutionAbortedError);
    assert.equal(err.cause, controller.signal.reason);
    assert.equal(err.attempts.length, 1);
    assert.equal(err.attempts[0]!.endReason, 'retryable-error');
    return true;
  });

  // The original fails at t=0; the retry is scheduled for t=1000.
  await clock.advanceBy(500);
  assert.equal(calls, 1);
  assert.equal(clock.pendingCount, 2); // backoff timer + deadline timer

  // Cancel in the middle of the backoff wait.
  controller.abort();
  await assertion;

  // Backoff and deadline timers cancelled, listener detached, no retry ran.
  assert.equal(clock.pendingCount, 0);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  await clock.runAll();
  assert.equal(calls, 1);
});

test('the deadline pre-empts a pending retry backoff', async () => {
  const { clock, coordinator } = setup({ baseBackoffMs: 1_000, hedgeDelayMs: 5_000 });

  const p = coordinator.execute({
    attempt: async () => {
      throw new RetryableError('flaky');
    },
    deadlineMs: 500,
    idempotent: true,
  });
  const assertion = assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof DeadlineExceededError);
    assert.equal(err.attempts.length, 1);
    return true;
  });
  await clock.runAll();
  await assertion;
  assert.equal(clock.pendingCount, 0);
});

test('maxParallelAttempts of 1 disables hedging', async () => {
  const { clock, coordinator } = setup({ maxParallelAttempts: 1 });
  let calls = 0;

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      calls++;
      await clock.sleep(300, ctx.signal);
      return 'ok';
    },
    deadlineMs: 5_000,
    idempotent: true,
  });
  await clock.runAll();

  assert.equal((await p).value, 'ok');
  assert.equal(calls, 1);
});

test('a hedge is skipped when the budget is empty', async () => {
  const { clock, coordinator } = setup({ budgetTokens: 0 });
  let calls = 0;

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      calls++;
      await clock.sleep(300, ctx.signal);
      return 'ok';
    },
    deadlineMs: 5_000,
    idempotent: true,
  });
  await clock.runAll();

  assert.equal((await p).value, 'ok');
  assert.equal(calls, 1);
});

test('retries are not hedged (hedging applies to the first request only)', async () => {
  const { clock, coordinator } = setup({ baseBackoffMs: 100 });
  const kinds: string[] = [];

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      kinds.push(ctx.kind);
      if (ctx.kind === 'original') {
        throw new RetryableError('flaky'); // fails at t=0, before hedgeDelay
      }
      await clock.sleep(500, ctx.signal); // retry runs long past hedgeDelay
      return 'ok';
    },
    deadlineMs: 10_000,
    idempotent: true,
  });
  await clock.runAll();

  assert.equal((await p).value, 'ok');
  assert.deepEqual(kinds, ['original', 'retry']);
});

test('a retry is deferred while a hedge is still in flight', async () => {
  const { clock, coordinator } = setup({ baseBackoffMs: 100 });

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      if (ctx.kind === 'original') {
        await clock.sleep(150, ctx.signal); // fails at t=150, after the hedge launched
        throw new RetryableError('original-flaky');
      }
      if (ctx.kind === 'hedge') {
        await clock.sleep(100, ctx.signal); // fails at t=200
        throw new RetryableError('hedge-flaky');
      }
      await clock.sleep(10, ctx.signal);
      return 'retry-ok';
    },
    deadlineMs: 10_000,
    idempotent: true,
  });
  await clock.runAll();
  const res = await p;

  assert.equal(res.value, 'retry-ok');
  assert.deepEqual(
    res.attempts.map((a) => a.kind),
    ['original', 'hedge', 'retry'],
  );
  // The retry was scheduled only once the hedge ended: t=200 + backoff 100.
  assert.equal(res.attempts[2]!.startedAt, 300);
});

test('a permanent error from a hedge ends the execution immediately', async () => {
  const { clock, coordinator } = setup();

  const p = coordinator.execute({
    attempt: async (ctx: AttemptContext) => {
      if (ctx.kind === 'hedge') {
        await clock.sleep(50, ctx.signal);
        throw new PermanentError('hedge says no');
      }
      await clock.sleep(500, ctx.signal);
      return 'original-ok';
    },
    deadlineMs: 10_000,
    idempotent: true,
  });
  const assertion = assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof ExecutionFailedError);
    assert.equal(err.reason, 'permanent-error');
    assert.equal(err.attempts.length, 2);
    assert.equal(err.attempts[0]!.endReason, 'cancelled');
    assert.equal(err.attempts[1]!.endReason, 'permanent-error');
    return true;
  });
  await clock.runAll();
  await assertion;
});

test('gives up after maxRetries and reports the last error', async () => {
  const { clock, coordinator } = setup({ maxRetries: 2, hedgeDelayMs: 5_000 });

  const p = coordinator.execute({
    attempt: async () => {
      throw new RetryableError('always');
    },
    deadlineMs: 60_000,
    idempotent: true,
  });
  const assertion = assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof ExecutionFailedError);
    assert.equal(err.reason, 'retries-exhausted');
    assert.ok(err.cause instanceof RetryableError);
    assert.equal(err.attempts.length, 3); // original + 2 retries
    return true;
  });
  await clock.runAll();
  await assertion;
});

test('rejects immediately when the signal is already aborted', async () => {
  const { clock, coordinator } = setup();
  const controller = new AbortController();
  controller.abort();

  const p = coordinator.execute({
    attempt: async () => 'never',
    deadlineMs: 1_000,
    idempotent: true,
    signal: controller.signal,
  });
  await assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof ExecutionAbortedError);
    assert.equal(err.attempts.length, 0);
    return true;
  });
  assert.equal(clock.pendingCount, 0);
});

test('rejects immediately when the deadline has already passed', async () => {
  const { clock, coordinator } = setup();
  await clock.advanceTo(500);

  const p = coordinator.execute({
    attempt: async () => 'never',
    deadlineMs: 100,
    idempotent: true,
  });
  await assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof DeadlineExceededError);
    assert.equal(err.attempts.length, 0);
    return true;
  });
  assert.equal(clock.pendingCount, 0);
});
