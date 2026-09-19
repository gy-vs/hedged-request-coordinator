# hedged-request-coordinator

一个与传输层无关的异步请求协调库：调用者提供自己的 `attempt` 函数，库负责
**重试（retry）**、**对冲（hedge）**、绝对截止时间、幂等门控和共享令牌预算。
不绑定任何 HTTP 客户端，不包含命令行，零运行时依赖，也不使用任何第三方重试 /
限流 / 调度库。

- TypeScript + Node.js 20（仅使用语言内置能力：`AbortController`、`setTimeout`）
- 时钟、随机数、sleep 全部可注入；测试使用虚拟时钟，**不依赖任何真实延时**
- 结果只会落定一次；成功立即返回并取消其它尝试，所有计时器与监听器都被清理
- 每次尝试的开始时间、结束时间、结束原因都以不可变快照形式返回

## 安装与测试

```bash
npm install
npm test        # tsc 编译 + node --test，全部基于虚拟时钟
```

## 快速开始

```ts
import { createCoordinator, createTokenBudget } from 'hedged-request-coordinator';

// 所有执行共享一个令牌预算：初始 32 个补充令牌。
const coordinator = createCoordinator({
  budget: createTokenBudget(32),
});

const outcome = await coordinator.execute<string>({
  // 调用者自己的操作；应响应 context.signal（不响应也不会破坏正确性，
  // 只是无法被真正停止）。
  attempt: async ({ signal, idempotencyKey }) => {
    const res = await fetch(`/jobs/${idempotencyKey}`, { signal });
    if (res.status >= 500) throw new Error('transient'); // 默认视为可重试
    if (res.status === 404) throw new PermanentError(res.statusText);
    return res.text();
  },
  deadline: Date.now() + 2_000, // 绝对截止时间（也可以用 timeoutMs）
  idempotencyKey: 'job-123',    // 提供幂等键才允许对冲与自动重试
  hedgeDelayMs: 120,            // 原始请求 120ms 未完成则发起一个对冲
  retry: { baseDelayMs: 50, maxDelayMs: 2_000, factor: 2 },
});

if (outcome.ok) {
  console.log(outcome.value, outcome.attempts);
} else {
  // 'deadline-exceeded' | 'aborted' | 'permanent-error'
  // | 'attempts-exhausted' | 'budget-exhausted' | 'retries-disabled'
  console.error(outcome.code, outcome.error, outcome.attempts);
}
```

非幂等调用（不传 `idempotencyKey`）**默认禁止对冲和自动重试**；可重试错误会直接
以 `retries-disabled` 结束。传 `retry: false` 可对幂等调用显式关闭重试。

## 语义约定

### 对冲与并行上限

- 原始请求运行超过 `hedgeDelayMs` 仍未完成时，可发起**至多一个**对冲。
- 任意时刻在飞尝试数不超过 `maxParallelAttempts`（启用对冲时默认 2，否则 1），
  尝试总数不超过 `maxAttempts`（默认 3，原始 + 重试 + 对冲合并计数）。
- 只有上一批尝试全部结束后才会发起新的重试，避免失败的兄弟尝试在另一个尝试仍在
  运行时额外放大并行数。

### 退避与抖动

指数退避 + 全抖动（AWS 风格）：

```
delay = rng() * min(maxDelayMs, baseDelayMs * factor ** (连续可重试失败数 - 1))
```

`rng` 可注入（默认 `Math.random`）；虚拟时钟测试用脚本化 RNG 得到确定性结果。

### 令牌预算

- 预算是协调器级别的，所有执行共享。
- 重试与对冲在启动前通过 `budget.reserve()` **同步原子扣减**（一次读-改-写，中间
  没有 `await`），并发执行不可能同时观察到最后一个令牌、也不会透支。
- **只有原始请求完成（成功或失败，包括被强制关闭）会 `refund()`**；重试与对冲
  永不补充，退避期间被取消时已预留的重试令牌也不退还。
- 令牌不足时执行以 `budget-exhausted` 结束。

### 截止时间、取消与同刻决胜

- 截止时间是**绝对时间戳**且为排他边界：在**恰好等于**截止时间的时刻可用的成功
  结果仍然获胜（成功与超时同刻时，成功优先）。实现上，截止时刻只安排一个排在所有
  同刻定时器之后的 0 延时 drain，并且内部信号在 drain 中才 abort，因此 abort
  监听器无法抢先把精确同刻的完成变成拒绝。
- 外部 `AbortSignal` 触发时立即以 `aborted` 落定，并把信号原因作为 `error`。
- 成功一落定：取消所有其它在飞尝试、清除截止/对冲/退避计时器、摘除外部 abort
  监听器，然后再 abort 内部信号，保证快照先定稿、后触发取消反应。
- 永久错误（`PermanentError` 或自定义 `isRetryable` 返回 false）立即结束；一批中
  若同时出现永久错误与可重试错误，永久错误优先作为最终失败码。

### 尝试快照

`outcome.attempts` 是冻结数组，每个元素包含：

| 字段 | 含义 |
| --- | --- |
| `attemptNumber` | 从 1 开始的序号 |
| `kind` | `original` / `retry` / `hedge` |
| `startedAt` / `endedAt` | 注入时钟上的时间戳 |
| `endReason` | 见下表 |
| `error` | 错误类原因对应的原始拒绝值 |

`endReason`：

- `success` — 获胜的成功
- `superseded` — 执行已定后才完成的迟到成功
- `permanent-error` / `retryable-error` — 按策略分类的失败
- `deadline-aborted` — 截止时间导致的取消
- `external-aborted` — 调用者信号导致的取消
- `cancelled` — 兄弟尝试获胜后被取消

## 注入测试依赖

```ts
import { createCoordinator, createTokenBudget } from '../src/index.js';
import { VirtualClock, virtualSleep, scriptedRng } from './virtualClock.js';

const clock = new VirtualClock(0);
const coordinator = createCoordinator({
  budget: createTokenBudget(4),
  clock,                          // 虚拟时间 + 二叉堆定时器
  sleep: virtualSleep(clock),     // 跟随虚拟时间、abort 时清理计时器
  rng: scriptedRng([0.5]),        // 确定性抖动
});

await clock.advanceTo(120);       // 推进虚拟时间，无真实等待
```

`VirtualClock` 的同刻定时器严格按注册顺序（`(at, seq)` 最小堆）触发，每次回调后
排空微任务，因此"成功与超时同刻""预算只剩一个令牌时的并发争夺"等竞态都能确定性
复现。生产环境默认使用 `Date.now()`、全局计时器和 AbortSignal 感知的真实 sleep。

## 目录

```
src/
  types.ts      公共类型：选项、结果、快照、时钟/随机/sleep/预算接口
  errors.ts     PermanentError / RetryableError / isAbortLike
  budget.ts     同步原子的令牌预算
  clock.ts      系统时钟、默认 RNG、可取消 sleep
  executor.ts   协调器核心（状态机、对冲、退避、同刻决胜、清理）
  index.ts
test/
  virtualClock.ts        VirtualClock / virtualSleep / scriptedRng
  coordinator.test.ts    node:test 用例（含四个指定竞态场景）
```
