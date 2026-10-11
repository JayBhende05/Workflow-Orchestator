import type { QueuedStep } from "../types/workflow"
import { podPool } from "../k8s/pod-pool"
import { resultQueue } from "../queue/result-queue"
import { stepQueue } from "../queue/step-queue"

/**
 * TODO: Implement this class.
 *
 * The pod manager receives a step, runs it inside a leased pod,
 * and publishes status events to the result queue.
 *
 * dispatch(step) must:
 *   1. Call podPool.acquirePod() to lease a free pod
 *   2. Push { status: "RUNNING", podId, stepId, workflowId } to resultQueue
 *   3. Call podPool.execInPod(podId, step.command)
 *   4. Push { status: "COMPLETED", stdout, exitCode: 0 } to resultQueue
 *   5. Call podPool.releasePod(podId)
 *
 *   On any error:
 *   4. Push { status: "FAILED", error: err.message } to resultQueue
 *   5. Call podPool.releasePod(podId)  ← always release in finally block
 *
 * IMPORTANT: Pod manager never reads or writes workflow state.
 * It only pushes to the result queue.
 */
export class PodManager {
  async dispatch(step: QueuedStep): Promise<void> {
    console.log("***********INSIDE DISPATCH***********************")
    const pod = await podPool.acquirePod()
    const podId = pod.podId

    console.log("Pods accquired are ", podId)

    try {
      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        podId,
        status: "RUNNING",
      })

      let stdout = await podPool.execInPod(podId, step.command)

      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        podId,
        status: "COMPLETED",
        stdout,
        exitCode: 0
      })


    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        podId,
        status: "FAILED",
        error: message
      })
    }finally{
      await podPool.releasePod(podId)
    }
    
  }
}

export const podManager = new PodManager()
