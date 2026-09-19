# hedged-request-coordinator

Async request coordination for Node.js 20+: **hedging**, **retries** with
exponential backoff and jitter, **absolute deadlines**, and a **shared token
budget** — around a caller-provided `attempt` function. It is not tied to any
HTTP client, performs no I/O itself, and has no runtime dependencies.

## Install / build / test

```sh
npm install      # dev dependencies only (typescript, @types/node)
npm run build    # compile to dist/
npm test         # build + run the node:test suite (virtual clock, no real delays)
```

## Quick start

```ts
import { RequestCoordinator, RetryableError } from 'hedged-request-coordinator';

const coordinator = new RequestCoordinator({
  maxParallelAttempts: 2,
  hedgeDelayMs: 250,
  baseBackoffMs: 100,
  maxBackoffMs: 5_000,
  jitter: 0.2,
  maxRetries: 3,
  budgetTokens: 100, // shared by ALL executions of this coordinator
});

const result = await coordinator.execute({
  deadlineMs: Date.now() + 2_000,   // absolute, in the clock's domain
  idempotent: true,                 // or pass idempotencyKey instead
  signal: abortController.signal,   // optional external cancellation
  attempt: async (ctx) => {
    const res = await fetch(url, { signal: ctx.signal });
    if (res.status >= 500) throw new RetryableError('server error');
    if (!res.ok) throw new Error('permanent failure'); // unknown => permanent
    return res.json();
  },
});

result.value;        // the winning attempt's value
result.attempts;     // snapshot of every attempt (start/end/endReason)
```

## Semantics

### Attempts

Every `execute` call runs one **original** attempt. Depending on
configuration and outcomes it may add:

- **retries** — after a *retryable* error, delayed by exponential backoff
  (`baseBackoffMs * 2^n`, capped at `maxBackoffMs`) with symmetric jitter
  `1 + jitter * (2 * random() - 1)`;
- **one hedge** — if the original is still running when `hedgeDelayMs`
  elapses. Hedging applies to the first request only, never to retries.

At most `maxParallelAttempts` attempts are in flight at any moment.

### Errors

The default classifier treats `RetryableError` as retryable and everything
else (including `PermanentError` and unknown errors) as permanent. Override
per coordinator or per execution with `classifyError`.

- **retryable** → backoff, then retry (budget and retries permitting);
- **permanent** → the execution ends immediately;
- **non-idempotent** executions (no `idempotent: true` and no
  `idempotencyKey`) are never retried or hedged.

### Token budget

One budget per coordinator, shared by all of its executions:

- retries and hedges each **spend one token, permanently**;
- a completed **original** attempt (any outcome, including cancellation)
  **returns one token**, capped at the capacity;
- originals are never gated on the budget.

`tryAcquire` is a synchronous check-and-decrement, so concurrent executions
cannot race each other into spending the same last token. A retry that
cannot be paid for fails the execution with
`ExecutionFailedError` (`reason: 'budget-exhausted'`); a hedge that cannot
be paid for is simply skipped.

### Settling and cleanup

The returned promise settles **exactly once**. Success, permanent error,
retries/budget exhaustion, deadline, and external abort may coincide — the
first processed outcome wins and the rest are discarded. On settlement the
coordinator cancels every pending timer, detaches the external signal
listener, aborts every in-flight attempt's `ctx.signal`, and records each
unfinished attempt as `cancelled`.

Tie-breaking note: the deadline timer is armed before any attempt starts,
so an attempt completing at exactly the deadline instant loses to the
deadline (with the FIFO `VirtualClock`, and effectively also with real
timers).

### Failure surface

All failures are `CoordinationError` subclasses carrying
`attempts: AttemptSnapshot[]`:

| Error                    | When                                        |
| ------------------------ | ------------------------------------------- |
| `DeadlineExceededError`  | absolute deadline reached                   |
| `ExecutionAbortedError`  | the caller's `signal` aborted (`cause` = signal reason) |
| `ExecutionFailedError`   | attempt failure ended the run; see `reason` (`permanent-error`, `not-idempotent`, `retries-exhausted`, `budget-exhausted`, `internal-error`) and `cause` |

## Injectable time and randomness

```ts
new RequestCoordinator({
  clock,    // { now(): number }                — default: Date.now
  sleeper,  // { sleep(ms, signal): Promise }  — default: setTimeout-based
  random,   // () => number in [0, 1)          — default: Math.random
});
```

`clock` and `sleeper` must agree with each other. For deterministic tests,
use the exported `VirtualClock` as both:

```ts
import { VirtualClock } from 'hedged-request-coordinator';

const clock = new VirtualClock();
const coordinator = new RequestCoordinator({ clock, sleeper: clock, random: () => 0.5 });

const p = coordinator.execute({ /* ... */ });
await clock.advanceBy(1_000); // or: await clock.runAll();
await p;
```

`VirtualClock` fires timers in `(dueTime, insertionOrder)` order and flushes
the microtask queue between firings, so promise-based attempt functions make
progress without any real delay. `clock.pendingCount` exposes how many
sleeps are still pending — the test suite uses it to assert cleanup.

## Attempt context

```ts
interface AttemptContext {
  attemptIndex: number;          // 0-based, per execution
  kind: 'original' | 'retry' | 'hedge';
  signal: AbortSignal;           // aborted when the execution settles
  deadlineMs: number;            // absolute deadline
  idempotencyKey?: string;       // forwarded from execute options
}
```

Attempt functions should abort their work when `ctx.signal` fires; results
from attempts that ignore it are discarded once the execution has settled.
