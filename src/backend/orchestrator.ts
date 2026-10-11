import type { QueuedStep, Workflow, WorkflowState } from "../types/workflow"
import { getWorkflow, setWorkflow } from "./workflow-store"
import { getReadySteps } from "./dag"
// import { StepQueue, stepQueue } from "../queue/step-queue"
import { resultQueue } from "../queue/result-queue"
import { podManager } from "../pod-manager/pod-manager"
import { read } from "node:fs"
import { stepQueue, StepQueue } from "../queue/step-queue"

/**
 * TODO: Implement this class.
 *
 * The orchestrator is the brain of the system. It ties together
 * the DAG resolver, step queue, result queue, and pod manager.
 *
 * submitWorkflow(workflow):
 *   1. Store workflow in workflow-store with all steps as PENDING D
 *   2. Run getReadySteps() to find immediately runnable steps
 *   3. Push each ready step into the step queue
 *   4. Mark those steps as QUEUED in workflow-store
 *
 * IMPORTANT: Store the workflow state before enqueueing the first ready step.
 * Result events can come back quickly, and the result handler must be able
 * to find the workflow in workflow-store.
 *
 * start():
 *   - Begin consuming from the result queue
 *   - Begin draining the step queue (send steps to pod manager)
 *   - For Section 1: sequential is fine
 *   - For Section 2: run parallel dispatch
 *
 * On StepResult received (from result queue consumer):
 *   1. Update the step's status in workflow-store
 *   2. If COMPLETED: run getReadySteps() again, enqueue newly unblocked steps
 *   3. Check if all steps done → update workflow status to "completed" or "failed"
 *
 * INVARIANT: Backend is the ONLY place that writes stepStatus.
 * Pod manager only pushes events. Orchestrator reads events and updates state.
 */
export class Orchestrator {
  async submitWorkflow(workflow: Workflow): Promise<void> {
    void workflow
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

    setWorkflow(workflow.workflowId , state)
    let readyState = getReadySteps(workflow.steps, {})

    console.log("ReadySteps = " , readyState)

   for(let step of readyState){
    const newReadyStep : QueuedStep = {
        "stepId": step.id,
    workflowId: workflow.workflowId,
    command: step.command,
    enqueuedAt: Date.now(),
    }
    console.log("STEP Queue" , newReadyStep);
    await stepQueue.enqueue(newReadyStep);
    
      state.stepState[step.id] = {
        ...state.stepState[step.id] ,
        status: "QUEUED",
      }
    
    console.log("Final STEPS are" , state)
    
   } 



    

  }

    
// void setWorkflow
// void getReadySteps
// void stepQueue
// throw new Error("TODO: implement submitWorkflow")
  

  async start(): Promise < void> {
  // void resultQueue
    // void podManager
    throw new Error("TODO: implement start")
}
}

export const orchestrator = new Orchestrator()
