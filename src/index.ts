import { log } from "./utils/logger"
import { orchestrator } from "./backend/orchestrator"
import { assertApiKeyConfigured } from "./backend/auth"
import { app } from "./backend/server"
import { setWorkflow } from "./backend/workflow-store"
import { podPool } from "./k8s/pod-pool"
import { redis } from "./queue/redis-client"
import { Workflow, WorkflowState } from "./types/workflow"

async function main(): Promise<void> {
  assertApiKeyConfigured()

  await redis.ping()
  log.success("SERVER", "Redis connected")

  await podPool.ready
  const status = podPool.getPoolStatus()
  log.success("SERVER", `Pod pool ready: ${status.total} pods`)

  const port = process.env.PORT || 3000
  const redisHost = process.env.REDIS_HOST || "localhost"

  await orchestrator.start()
  void orchestrator.startStepDispatcher().catch((err) => {
    log.error("DISPATCH", `Step dispatcher failed: ${err}`)
  })

  app.listen(port, () => {
    log.success("SERVER", `Server running on port ${port}`)
    log.info("SERVER", `Redis host: ${redisHost}`)
    log.info("SERVER", `Pod count: ${status.total}`)
  })
}

main().catch((err: unknown) => {
  console.error("Startup failed:", err)
  process.exit(1)
})
