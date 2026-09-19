import type { TokenBudget } from './types.js';

/**
 * Create the shared token budget.
 *
 * `reserve` performs a single synchronous read-and-decrement. JavaScript
 * is single-threaded and no `await` occurs between observing and deducting
 * the balance, so two concurrent executions racing for the last token can
 * never both win.
 */
export function createTokenBudget(initialTokens: number): TokenBudget {
  if (!Number.isInteger(initialTokens) || initialTokens < 0) {
    throw new RangeError('initialTokens must be a non-negative integer');
  }
  let tokens = initialTokens;
  return {
    reserve(): boolean {
      if (tokens <= 0) return false;
      tokens -= 1;
      return true;
    },
    refund(): void {
      tokens += 1;
    },
    available(): number {
      return tokens;
    },
  };
}
