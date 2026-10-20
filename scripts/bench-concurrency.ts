import { ConcurrentExecutor } from "../src/orchestrator/concurrent"
import { logger } from "../src/utils/logger"

const ITEMS = 300
const MEDIAN_LATENCY_MS = 50
const PROVIDER_RATE_PER_SEC = 200
const PROVIDER_BURST = 20
const SDK_RETRIES = 2

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const latencies = Array.from({ length: ITEMS }, () => {
  const z = Math.sqrt(-2 * Math.log(1 - Math.random())) * Math.cos(2 * Math.PI * Math.random())
  return MEDIAN_LATENCY_MS * Math.exp(z)
})

/** The scheduler this branch replaces: every batch waits for its slowest task */
async function lockstep(concurrency: number, task: (ms: number) => Promise<unknown>) {
  for (let i = 0; i < ITEMS; i += concurrency) {
    const settled = await Promise.allSettled(latencies.slice(i, i + concurrency).map(task))
    const failed = settled.find((result) => result.status === "rejected")
    if (failed) throw (failed as PromiseRejectedResult).reason
  }
}

function pooled(concurrency: number, task: (ms: number) => Promise<unknown>) {
  return ConcurrentExecutor.execute(latencies, concurrency, "bench", "bench", ({ item }) =>
    task(item)
  )
}

async function timed(run: () => Promise<unknown>) {
  const start = performance.now()
  try {
    await run()
    return `${Math.round(performance.now() - start)}ms`
  } catch {
    return "failed"
  }
}

function rateLimitedProvider() {
  let tokens = PROVIDER_BURST
  let refilledAt = performance.now()
  const stats = { completed: 0, rateLimited: 0 }

  const send = async (ms: number) => {
    const now = performance.now()
    tokens = Math.min(PROVIDER_BURST, tokens + ((now - refilledAt) / 1000) * PROVIDER_RATE_PER_SEC)
    refilledAt = now
    if (tokens < 1) {
      stats.rateLimited++
      throw Object.assign(new Error("Too Many Requests"), { status: 429 })
    }
    tokens--
    await sleep(ms)
    stats.completed++
  }

  const request = async (ms: number) => {
    for (let retry = 0; ; retry++) {
      try {
        return await send(ms)
      } catch (error) {
        if (retry >= SDK_RETRIES) throw error
        await sleep(50 * 2 ** retry)
      }
    }
  }

  return { stats, request }
}

logger.setLevel("error")

console.log(`${ITEMS} tasks, lognormal latency (median ${MEDIAN_LATENCY_MS}ms)`)
const lowerBound = (concurrency: number) =>
  Math.round(Math.max(latencies.reduce((a, b) => a + b) / concurrency, ...latencies))
const scaling = []
for (const concurrency of [5, 10, 25, 50, 100]) {
  scaling.push({
    concurrency,
    lockstep: await timed(() => lockstep(concurrency, sleep)),
    pool: await timed(() => pooled(concurrency, sleep)),
    lowerBound: `${lowerBound(concurrency)}ms`,
  })
}
console.table(scaling)

console.log(`Concurrency 100 against a provider allowing ${PROVIDER_RATE_PER_SEC} req/s`)
const rateLimited = []
for (const [scheduler, run] of [
  ["lockstep", lockstep],
  ["pool", pooled],
] as const) {
  const provider = rateLimitedProvider()
  const time = await timed(() => run(100, provider.request))
  rateLimited.push({ scheduler, time, ...provider.stats })
}
console.table(rateLimited)
