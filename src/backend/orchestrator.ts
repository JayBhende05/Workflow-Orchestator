import type { QueuedStep, StepResult, StepStatus, Workflow, WorkflowState } from "../types/workflow"
import { getWorkflow, setWorkflow } from "./workflow-store"
import { getDownstreamSteps, getReadySteps } from "./dag"
import { resultQueue } from "../queue/result-queue"
import { podManager } from "../pod-manager/pod-manager"
import { stepQueue } from "../queue/step-queue"
import { podPool } from "../k8s/pod-pool"
import { log } from "../utils/logger"


function getStatusMap(data: WorkflowState): Record<string, StepStatus> {
  let status: Record<string, StepStatus> = {}
  for (const step of Object.values(data.stepState)) {

    status[step.stepId] = step.status

  }
  return status
}

function formatSeconds(ms: number): string {
  return `${(ms / 1000).toFixed(1)}s`
}

// One-line summary of a command's output so it fits in a log line
function shortOutput(stdout: string | undefined): string {
  const text = (stdout ?? "").trim().replace(/\s+/g, " ")
  return text.length > 60 ? `${text.slice(0, 57)}...` : text
}

export class Orchestrator {
  private running = 0;
  private readonly workflowStartedAt = new Map<string, number>()
  private readonly stepStartedAt = new Map<string, number>()

  async submitWorkflow(workflow: Workflow): Promise<void> {
    const state: WorkflowState = {
      workflowId: workflow.workflowId,
      status: "pending",
      steps: workflow.steps,
      stepState: {},
    }

    for (const step of workflow.steps) {
      state.stepState[step.id] = {
        stepId: step.id,
        status: "PENDING",
        podId: null,
        retriesLeft: step.retries ?? 0,
      }
    }

    log.banner(`WORKFLOW ${workflow.workflowId}`)
    log.info("WORKFLOW", `${workflow.workflowId} submitted with ${workflow.steps.length} steps`)
    for (const step of workflow.steps) {
      const deps = step.dependsOn?.length ? `depends on [${step.dependsOn.join(", ")}]` : "no dependencies"
      log.info("WORKFLOW", `  ${step.id}: "${step.command}" (${deps})`)
    }

    this.workflowStartedAt.set(workflow.workflowId, Date.now())
    let completedSteps: Record<string, StepStatus> = getStatusMap(state)
    setWorkflow(workflow.workflowId, state)

    let readyState = getReadySteps(workflow.steps, completedSteps)

    for (let step of readyState) {
      const newReadyStep: QueuedStep = {
        "stepId": step.id,
        workflowId: workflow.workflowId,
        command: step.command,
        enqueuedAt: Date.now(),
      }

      state.stepState[step.id] = {
        ...state.stepState[step.id],
        status: "QUEUED",
      }
      log.info("STEP", `${workflow.workflowId}/${step.id}  PENDING → QUEUED (no unmet dependencies)`)

      await stepQueue.enqueue(newReadyStep);
    }
  }

  async handleResult(event: StepResult): Promise<void> {
    const workflow = getWorkflow(event.workflowId)
    if (!workflow) {
      log.warn("STEP", `Ignoring ${event.status} for unknown workflow "${event.workflowId}"`)
      return
    }

    const label = `${event.workflowId}/${event.stepId}`
    const stepKey = label

    switch (event.status) {
      case "RUNNING":
        workflow.stepState[event.stepId] = {
          ...workflow.stepState[event.stepId],
          status: "RUNNING",
          podId: event.podId,
        }
        this.stepStartedAt.set(stepKey, Date.now())
        log.info("STEP", `${label}  QUEUED → RUNNING on pod ${event.podId}`)
        setWorkflow(workflow.workflowId, workflow)
        break

      case "COMPLETED": {
        workflow.stepState[event.stepId] = {
          ...workflow.stepState[event.stepId],
          status: "COMPLETED",
          podId: event.podId,
          stdout: event.stdout,
          exitCode: event.exitCode,
        }

        const startedAt = this.stepStartedAt.get(stepKey)
        const took = startedAt ? ` in ${formatSeconds(Date.now() - startedAt)}` : ""
        this.stepStartedAt.delete(stepKey)
        log.success(
          "STEP",
          `${label}  RUNNING → COMPLETED${took} (exit ${event.exitCode ?? 0}) output: "${shortOutput(event.stdout)}"`
        )

        const readySteps = getReadySteps(workflow.steps, getStatusMap(workflow))
        for (const step of readySteps) {
          workflow.stepState[step.id] = {
            ...workflow.stepState[step.id],
            status: "QUEUED",
          }
          log.info(
            "STEP",
            `${event.workflowId}/${step.id}  PENDING → QUEUED (dependencies done: ${(step.dependsOn ?? []).join(", ")})`
          )
          void stepQueue.enqueue({
            stepId: step.id,
            workflowId: workflow.workflowId,
            command: step.command,
            enqueuedAt: Date.now(),
          })
        }
        setWorkflow(workflow.workflowId, workflow)
        break
      }

      case "FAILED": {
        workflow.stepState[event.stepId] = {
          ...workflow.stepState[event.stepId],
          status: "FAILED",
          podId: event.podId,
          error: event.error,
        }
        this.stepStartedAt.delete(stepKey)
        log.error("STEP", `${label}  RUNNING → FAILED: ${event.error ?? "unknown error"}`)

        const downstreamSteps = getDownstreamSteps(workflow.steps, event.stepId)
        for (const step of downstreamSteps) {
          workflow.stepState[step.id] = {
            ...workflow.stepState[step.id],
            status: "SKIPPED",
          }
          log.warn("STEP", `${event.workflowId}/${step.id}  → SKIPPED (upstream step ${event.stepId} failed)`)
        }

        setWorkflow(workflow.workflowId, workflow)
        break
      }

      default:
        break
    }

    this.logIfWorkflowFinished(workflow)
  }

  // Logs a closing summary once every step is COMPLETED, FAILED or SKIPPED
  private logIfWorkflowFinished(workflow: WorkflowState): void {
    const steps = Object.values(workflow.stepState)
    const finished = steps.every((s) => s.status === "COMPLETED" || s.status === "FAILED" || s.status === "SKIPPED")
    if (!finished) return

    const count = (status: StepStatus) => steps.filter((s) => s.status === status).length
    const startedAt = this.workflowStartedAt.get(workflow.workflowId)
    const took = startedAt ? ` in ${formatSeconds(Date.now() - startedAt)}` : ""
    this.workflowStartedAt.delete(workflow.workflowId)

    const summary = `${count("COMPLETED")} completed, ${count("FAILED")} failed, ${count("SKIPPED")} skipped`
    if (count("FAILED") > 0) {
      log.error("WORKFLOW", `${workflow.workflowId} FAILED${took} (${summary})`)
    } else {
      log.success("WORKFLOW", `${workflow.workflowId} COMPLETED${took} (${summary})`)
    }
  }

  async startStepDispatcher(): Promise<void> {
    const inUsePods = podPool.getPoolStatus().total
    let loggedFull = false

    log.info("DISPATCH", `Dispatcher started (capacity: ${inUsePods} pods)`)

    while (true) {

      if (inUsePods === this.running) {
        if (!loggedFull) {
          const waiting = await stepQueue.size()
          log.warn("DISPATCH", `All ${inUsePods} pods busy; ${waiting} step(s) waiting in the queue`)
          loggedFull = true
        }
        await Bun.sleep(300)
        continue
      }
      loggedFull = false

      const step = await stepQueue.dequeue()

      if (!step) {
        await Bun.sleep(200)
        continue
      }

      this.running++
      log.info("DISPATCH", `${step.workflowId}/${step.stepId} dispatched (${this.running}/${inUsePods} pods in use)`)
      podManager
        .dispatch(step)
        .catch((err) => log.error("DISPATCH", `${step.workflowId}/${step.stepId} dispatch failed: ${err}`))
        .finally(() => { this.running-- })
    }
  }


  async start(): Promise<void> {

    resultQueue.consume((event) => this.handleResult(event))
  }
}

export const orchestrator = new Orchestrator()
