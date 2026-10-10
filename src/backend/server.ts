import express from "express"
import { podPool } from "../k8s/pod-pool"

const app = express()
app.use(express.json())

// POST /workflow
// Students implement: store workflow, find ready steps, enqueue them
app.post("/workflow", async (req, res) => {
  const workflow = req.body

  // TODO: students implement this in orchestrator.ts
  // Call orchestrator.submitWorkflow(workflow)
  void workflow
  res.status(501).json({ error: "Not implemented" })
})

// GET /workflow/:id
// Students implement: read from workflow-store and return current state
app.get("/workflow/:id", (req, res) => {
  const workflowId = req.params.id

  // TODO: students implement this
  void workflowId
  res.status(501).json({ error: "Not implemented" })
})

// GET /pods  — already implemented, useful for debugging
app.get("/pods", async (_req, res) => {
  const status = podPool.getPoolStatus()
  res.json(status)
})

export { app }
