/**
 * A token budget shared by all executions of one coordinator.
 *
 * Semantics:
 *  - Retries and hedges each cost one token, spent permanently.
 *  - Completing an *original* attempt (for any reason, including
 *    cancellation) returns one token, capped at the capacity.
 *  - Original attempts themselves are never gated on the budget.
 *
 * `tryAcquire` is deliberately synchronous: the check and the decrement
 * happen in the same tick of the event loop, so two concurrent executions
 * can never both observe and spend the last token (no check-then-act race).
 */
export class TokenBudget {
  private tokens: number;

  constructor(private readonly capacity: number) {
    if (!(capacity >= 0)) {
      throw new RangeError('budget capacity must be >= 0');
    }
    this.tokens = capacity;
  }

  get available(): number {
    return this.tokens;
  }

  /** Atomically takes one token, or returns false when the budget is empty. */
  tryAcquire(): boolean {
    if (this.tokens <= 0) {
      return false;
    }
    this.tokens -= 1;
    return true;
  }

  /** Returns one token, never exceeding the configured capacity. */
  release(): void {
    this.tokens = Math.min(this.capacity, this.tokens + 1);
  }
}
