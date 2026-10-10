import { app } from "./backend/server"
import { podPool } from "./k8s/pod-pool"
import { redis } from "./queue/redis-client"

async function main(): Promise<void> {
  await redis.ping()
  console.log("✅ Redis connected")

  await podPool.ready
  const status = podPool.getPoolStatus()
  console.log(`✅ Pod pool ready: ${status.total} pods`)

  const port = process.env.PORT || 3000
  const redisHost = process.env.REDIS_HOST || "localhost"

  app.listen(port, () => {
    console.log(`✅ Server running on port ${port}`)
    console.log(`✅ Redis host: ${redisHost}`)
    console.log(`✅ Pod count: ${status.total}`)
  })
}

main().catch((err: unknown) => {
  console.error("Startup failed:", err)
  process.exit(1)
})
