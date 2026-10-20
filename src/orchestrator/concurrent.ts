import { logger } from "../utils/logger"
import { shouldStop } from "../server/runState"

const MAX_RATE_LIMIT_RETRIES = 6
const RETRY_BASE_DELAY_MS = 1000
const RETRY_MAX_DELAY_MS = 30_000
const RATE_LIMIT_MESSAGE = /rate.?limit|too many requests|throttl/i

export interface ConcurrentTaskContext<T> {
  item: T
  index: number
  total: number
}

export interface ConcurrentExecutionOptions<T, R> {
  items: T[]
  concurrency: number
  /** At most `concurrency` tasks start per `rateLimitMs` */
  rateLimitMs: number
  runId: string
  phaseName: string
  executeTask: (context: ConcurrentTaskContext<T>) => Promise<R>
  onTaskComplete?: (context: ConcurrentTaskContext<T>, result: R) => void
  onError?: (context: ConcurrentTaskContext<T>, error: Error) => void
}

export class ConcurrentExecutor {
  /**
   * Execute tasks with a worker pool that keeps up to `concurrency` tasks in flight
   * Rate-limited tasks are retried as the pool shrinks; other errors fail fast once in-flight tasks finish
   */
  static async executePooled<T, R>(options: ConcurrentExecutionOptions<T, R>): Promise<R[]> {
    const {
      items,
      concurrency,
      rateLimitMs,
      runId,
      phaseName,
      executeTask,
      onTaskComplete,
      onError,
    } = options

    if (items.length === 0) return []
    if (concurrency <= 0) throw new Error("Concurrency must be positive")

    logger.info(`[${phaseName}] Processing ${items.length} items with concurrency ${concurrency}`)

    const limit = new AdaptiveLimit(concurrency)
    const pace = createPacer(rateLimitMs / concurrency)
    const results = new Array<R>(items.length)
    let nextIndex = 0
    let failure: Error | undefined

    const attempt = async (context: ConcurrentTaskContext<T>): Promise<R> => {
      for (let retry = 0; ; retry++) {
        await pace()
        const startedAt = Date.now()
        try {
          const result = await executeTask(context)
          limit.succeeded()
          return result
        } catch (error) {
          if (retry >= MAX_RATE_LIMIT_RETRIES || !isRateLimitError(error)) throw error
          if (limit.rateLimited(startedAt)) {
            logger.warn(`[${phaseName}] Rate limited, lowering concurrency to ${limit.value}`)
          }
          limit.release()
          await sleep(
            Math.random() * Math.min(RETRY_MAX_DELAY_MS, RETRY_BASE_DELAY_MS * 2 ** retry)
          )
          await limit.acquire()
        }
      }
    }

    const worker = async () => {
      while (true) {
        await limit.acquire()
        try {
          if (failure || nextIndex >= items.length) return
          if (shouldStop(runId)) {
            logger.info(`[${phaseName}] Run ${runId} stopped by user`)
            failure = new Error("Run stopped by user. Resume with the same run ID.")
            return
          }

          const index = nextIndex++
          const context = { item: items[index], index, total: items.length }
          try {
            results[index] = await attempt(context)
            onTaskComplete?.(context, results[index])
          } catch (e) {
            const error = e instanceof Error ? e : new Error(String(e))
            onError?.(context, error)
            failure ??= error
          }
        } finally {
          limit.release()
        }
      }
    }

    await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker))

    if (failure) throw failure
    return results
  }

  /**
   * Simple concurrent execution without batching (for phases without rate limits)
   */
  static async execute<T, R>(
    items: T[],
    concurrency: number,
    runId: string,
    phaseName: string,
    executeTask: (context: ConcurrentTaskContext<T>) => Promise<R>
  ): Promise<R[]> {
    return this.executePooled({
      items,
      concurrency,
      rateLimitMs: 0,
      runId,
      phaseName,
      executeTask,
    })
  }
}

/** AIMD limit: halves when rate limited, grows by one per `value` successes, capped at `max` */
class AdaptiveLimit {
  private current: number
  private active = 0
  private lastDecreaseAt = 0
  private waiters: Array<() => void> = []

  constructor(private readonly max: number) {
    this.current = max
  }

  get value(): number {
    return Math.floor(this.current)
  }

  async acquire(): Promise<void> {
    while (this.active >= this.value) {
      await new Promise<void>((resolve) => this.waiters.push(resolve))
    }
    this.active++
  }

  release(): void {
    this.active--
    this.wakeAll()
  }

  succeeded(): void {
    const before = this.value
    this.current = Math.min(this.max, this.current + 1 / this.current)
    if (this.value > before) this.wakeAll()
  }

  /** Requests sent before the last decrease are ignored, so one burst of 429s halves once */
  rateLimited(startedAt: number): boolean {
    if (startedAt <= this.lastDecreaseAt) return false
    this.current = Math.max(1, this.current / 2)
    this.lastDecreaseAt = Date.now()
    return true
  }

  private wakeAll(): void {
    const waiters = this.waiters
    this.waiters = []
    for (const wake of waiters) wake()
  }
}

type ErrorLike = {
  status?: unknown
  statusCode?: unknown
  message?: unknown
  cause?: unknown
  lastError?: unknown
}

/** Checks status codes down the error chain, falling back to the message for SDKs that drop them */
function isRateLimitError(error: unknown): boolean {
  let current = error as ErrorLike | undefined
  for (let depth = 0; current && depth < 5; depth++) {
    if (current.status === 429 || current.statusCode === 429) return true
    if (RATE_LIMIT_MESSAGE.test(String(current.message ?? ""))) return true
    current = (current.cause ?? current.lastError) as ErrorLike | undefined
  }
  return false
}

function createPacer(intervalMs: number): () => Promise<void> {
  let nextStartAt = 0
  return async () => {
    if (intervalMs <= 0) return
    const now = Date.now()
    const startAt = Math.max(now, nextStartAt)
    nextStartAt = startAt + intervalMs
    if (startAt > now) await sleep(startAt - now)
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
