import express from "express"
import { podPool } from "../k8s/pod-pool"
import { StepState, WorkflowState, WorkflowStatus } from "../types/workflow"
import { getWorkflow, setWorkflow } from "./workflow-store"
import { getReadySteps } from "./dag"
import { orchestrator } from "./orchestrator"
import { stdout } from "process"

const app = express()
app.use(express.json())




// POST /workflow
// Students implement: store workflow, find ready steps, enqueue them
app.post("/workflow", async (req, res) => {


    const workflow = req.body;
    if(!workflow.workflowId){
      return  res.status(400).json({ error: "Worflow Missing" })
    }

     await orchestrator.submitWorkflow(workflow)

  
  res.status(202).json({ workflowId: workflow.workflowId , status: "accepted" })
})

// GET /workflow/:id
// Students implement: read from workflow-store and return current state
function getWorkflowStatus(
  stepState: Record<string, StepState>
): WorkflowStatus {
  const states = Object.values(stepState);

  if (states.some((step) => step.status === "FAILED")) {
    return "failed";
  }

  if (states.every((step) => step.status === "PENDING")) {
    return "pending";
  }

  if (states.every((step) => step.status === "COMPLETED")) {
    return "completed";
  }

  return "running";
}

app.get("/workflow/:id", (req, res) => {
  const workflowId = req.params.id
  console.log("Workflow id is " , workflowId)
  
  const workflow =  getWorkflow(workflowId);

  if (!workflow) {
    return res.status(404).json({ error: "Workflow not found" });
  }

  const steps = workflow.steps.map((step) => ({
    id: step.id,
    status: workflow.stepState[step.id].status,
    podId: workflow.stepState[step.id].podId,
    stdout : workflow.stepState[step.id].stdout,
    exitCode : workflow.stepState[step.id].exitCode
  }));

  const status = getWorkflowStatus(workflow.stepState);

  return res.json({
    workflowId: workflow.workflowId,
    status,
    steps,
    
  });
});




// GET /pods  — already implemented, useful for debugging
app.get("/pods", async (_req, res) => {
  const status = podPool.getPoolStatus()
  res.json(status)
})

export { app }
