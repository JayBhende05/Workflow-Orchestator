import type { QueuedStep } from "../types/workflow"
import { redis } from "./redis-client"
import { log } from "../utils/logger"

const QUEUE_NAMESPACE = process.env.REDIS_QUEUE_PREFIX || `workflow:${process.pid}`
const STEP_QUEUE_KEY = `${QUEUE_NAMESPACE}:step-queue`

console.log(`📚 Step queue key: ${STEP_QUEUE_KEY}`)

function parseQueuedStep(value: string): QueuedStep {
  return JSON.parse(value) as QueuedStep
}

/**
 * A fully implemented FIFO queue backed by Redis.
 *
 * Students should use this queue from the orchestrator instead of
 * reimplementing Redis list operations.
 *
 * Use LPUSH to enqueue and RPOP to dequeue.
 * Items are serialized as JSON strings in Redis.
 */
export class StepQueue {
  async enqueue(step: QueuedStep): Promise<void> {
    const depth = await redis.lpush(STEP_QUEUE_KEY, JSON.stringify(step))
    log.info("QUEUE", `enqueued ${step.workflowId}/${step.stepId} (queue depth: ${depth})`)
  }

  async dequeue(): Promise<QueuedStep | null> {
    const value = await redis.rpop(STEP_QUEUE_KEY)

    if (!value) return null

    const step = parseQueuedStep(value)
    log.info("QUEUE", `dequeued ${step.workflowId}/${step.stepId}`)
    return step
  }

  async peek(): Promise<QueuedStep | null> {
    const values = await redis.lrange(STEP_QUEUE_KEY, -1, -1)
    const value = values[0]
    if (!value) return null

    return parseQueuedStep(value)
  }

  async size(): Promise<number> {
    return redis.llen(STEP_QUEUE_KEY)
  }
}

export const stepQueue = new StepQueue()
