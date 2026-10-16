import type { QueuedStep, StepStatus, Workflow, WorkflowState } from "../types/workflow"
import { getWorkflow, setWorkflow } from "./workflow-store"
import { getReadySteps } from "./dag"
// import { StepQueue, stepQueue } from "../queue/step-queue"
import { resultQueue } from "../queue/result-queue"
import { podManager } from "../pod-manager/pod-manager"
import { read } from "node:fs"
import { stepQueue, StepQueue } from "../queue/step-queue"
import { podPool } from "../k8s/pod-pool"


function getStatusMap(data: WorkflowState): Record<string, StepStatus> {
  let status: Record<string, StepStatus> = {}
  for (const step of Object.values(data.stepState)) {

    status[step.stepId] = step.status

  }
  return status
}

export class Orchestrator {
  private running = 0;

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


    console.log("STATE IS ", state)
    let completedSteps: Record<string, StepStatus> = getStatusMap(state)
    setWorkflow(workflow.workflowId, state)

    let readyState = getReadySteps(workflow.steps, completedSteps)

    // console.log("ReadySteps = ", readyState)

    for (let step of readyState) {
      const newReadyStep: QueuedStep = {
        "stepId": step.id,
        workflowId: workflow.workflowId,
        command: step.command,
        enqueuedAt: Date.now(),
      }
      console.log("STEP Queue", newReadyStep);

      state.stepState[step.id] = {
        ...state.stepState[step.id],
        status: "QUEUED",
      }

      await stepQueue.enqueue(newReadyStep);



      console.log("Final STEPS are", state.stepState)

    }

    resultQueue.consume((event) => {
      console.log("Received result:", event)
      const worflow = getWorkflow(event.workflowId)
      if (!worflow) {
        return
      }

      switch (event.status) {
        case "RUNNING":
          state.stepState[event.stepId] = {
            ...state.stepState[event.stepId],
            status: "RUNNING",
            podId: event.podId
          }

          break;

        case "COMPLETED":
          const currentWorkflow = getWorkflow(event.workflowId)
          if (!currentWorkflow) return

          currentWorkflow.stepState[event.stepId] = {
            ...currentWorkflow.stepState[event.stepId],
            status: "COMPLETED",
            podId: event.podId,
            stdout: event.stdout,
            exitCode: event.exitCode,
          }

          const completedSteps = getStatusMap(currentWorkflow)
          const readySteps = getReadySteps(
            currentWorkflow.steps,
            completedSteps,
          )

          for (const step of readySteps) {
            currentWorkflow.stepState[step.id] = {
              ...currentWorkflow.stepState[step.id],
              status: "QUEUED",
            }

            stepQueue.enqueue({
              stepId: step.id,
              workflowId: currentWorkflow.workflowId,
              command: step.command,
              enqueuedAt: Date.now(),
            })
          }

          setWorkflow(currentWorkflow.workflowId, currentWorkflow)
          break



        case "FAILED":
          state.stepState[event.stepId] = {
            ...state.stepState[event.stepId],
            status: "FAILED",
            podId: event.podId,
            error: event.error
          }

          break;





        default:
          break;
      }




    })






  }


  async startStepDispatcher(): Promise<void> {
    const inUsePods = podPool.getPoolStatus().total

    while (true) {

      if (inUsePods === this.running) {
        await Bun.sleep(300)
        continue
      }

      const step = await stepQueue.dequeue()

      if (!step) {
        await Bun.sleep(200)
        continue
      }

      this.running++
      podManager
        .dispatch(step)
        .catch((err) => console.error("dispatch failed", err))
        .finally(() => { this.running-- })
    }
  }


  async start(): Promise<void> {


  }
}

export const orchestrator = new Orchestrator()
