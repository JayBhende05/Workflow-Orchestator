import express from "express"
import { podPool } from "../k8s/pod-pool"
import { StepState, WorkflowState, WorkflowStatus } from "../types/workflow"
import { getWorkflow, setWorkflow } from "./workflow-store"
import { getReadySteps } from "./dag"
import { orchestrator } from "./orchestrator"
import { checkGraph, workflowSchema } from "./validation"
import { requireApiKey } from "./auth"
import { log } from "../utils/logger"

const app = express()
// Logs one line per request with its final status code and duration
app.use((req, res, next) => {
  const startedAt = Date.now()
  res.on("finish", () => {
    const line = `${req.method} ${req.originalUrl} → ${res.statusCode} (${Date.now() - startedAt}ms)`
    if (res.statusCode >= 400) log.warn("HTTP", line)
    else log.info("HTTP", line)
  })
  next()
})

app.use(requireApiKey)
app.use(express.json())




// POST /workflow
// Students implement: store workflow, find ready steps, enqueue them
app.post("/workflow", async (req, res) => {


    const parsed = workflowSchema.safeParse(req.body)
    if (!parsed.success) {
      const errors = parsed.error.issues.map(
        (issue) => `${issue.path.join(".") || "body"}: ${issue.message}`
      )
      return res.status(400).json({ errors })
    }

    const workflow = parsed.data

    const graphErrors = checkGraph(workflow.steps)
    if (graphErrors.length > 0) {
      return res.status(400).json({ errors: graphErrors })
    }

    if (getWorkflow(workflow.workflowId)) {
      return res.status(409).json({ error: `Workflow "${workflow.workflowId}" already exists` })
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

  // A failure only ends the workflow once no other branch is still working
  const hasFailed = states.some((step) => step.status === "FAILED")
  const hasActive = states.some(
    (step) => step.status === "QUEUED" || step.status === "RUNNING"
  )

  if (hasFailed && !hasActive) {
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
