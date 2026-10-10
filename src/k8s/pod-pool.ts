import { Writable } from "node:stream"
import { CoreV1Api, Exec, KubeConfig, type V1Status } from "@kubernetes/client-node"
import type { Pod, PoolStatus } from "../types/workflow"

const ACQUIRING_LEASE = "__acquiring__"
const DEFAULT_NAMESPACE = "workflow-runner"
const DEFAULT_LABEL_SELECTOR = "app=runner"
const RUNNER_CONTAINER_NAME = "runner"
const EXEC_API_TIMEOUT_MS = 1000

interface MutableCluster {
  skipTLSVerify: boolean
}

interface MutableUser {
  token?: string
}

function createCaptureStream(chunks: string[]): Writable {
  return new Writable({
    write(chunk: unknown, _encoding, callback) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk))
      callback()
    },
  })
}

function getPodId(podName: string): string {
  return podName.startsWith("runner-") ? podName.slice("runner-".length) : podName
}

function getStatusMessage(status: V1Status): string | undefined {
  const causeMessages = status.details?.causes
    ?.map((cause) => cause.message)
    .filter((message): message is string => Boolean(message))

  if (causeMessages && causeMessages.length > 0) return causeMessages.join(", ")
  return status.message
}

function getServiceAccountToken(namespace: string): string | undefined {
  const tokenProcess = Bun.spawnSync([
    "kubectl",
    "create",
    "token",
    "default",
    "-n",
    namespace,
  ])

  if (tokenProcess.exitCode !== 0) return undefined

  const token = tokenProcess.stdout.toString().trim()
  return token.length > 0 ? token : undefined
}

function runKubectlExec(namespace: string, podName: string, command: string): string {
  const execProcess = Bun.spawnSync([
    "kubectl",
    "exec",
    "-n",
    namespace,
    podName,
    "-c",
    RUNNER_CONTAINER_NAME,
    "--",
    "sh",
    "-c",
    command,
  ])

  const stdout = execProcess.stdout.toString()
  const stderr = execProcess.stderr.toString()

  if (execProcess.exitCode !== 0) throw new Error(stderr || `Command exited with ${execProcess.exitCode}`)

  return stdout
}

function timeoutAfter(ms: number): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error("KUBERNETES_EXEC_TIMEOUT")), ms)
  })
}

export class PodPool {
  private readonly namespace = process.env.KUBE_NAMESPACE || DEFAULT_NAMESPACE
  private readonly labelSelector = process.env.KUBE_POD_LABEL || DEFAULT_LABEL_SELECTOR
  private readonly leaseMap = new Map<string, string | null>()
  private readonly pods = new Map<string, Pod>()
  private readonly coreV1Api: CoreV1Api
  private readonly execClient: Exec

  readonly ready: Promise<void>

  constructor() {
    const kubeConfig = new KubeConfig()
    kubeConfig.loadFromDefault()

    const currentCluster = kubeConfig.getCurrentCluster()
    if (currentCluster) {
      const mutableCluster = currentCluster as unknown as MutableCluster
      mutableCluster.skipTLSVerify = true
      console.log("🔐 Kubernetes TLS verification disabled for local kind cluster")
    }

    const currentUser = kubeConfig.getCurrentUser()
    const serviceAccountToken = getServiceAccountToken(this.namespace)
    if (currentUser && serviceAccountToken) {
      const mutableUser = currentUser as unknown as MutableUser
      mutableUser.token = serviceAccountToken
      console.log("🔑 Kubernetes service account token loaded")
    }

    this.coreV1Api = kubeConfig.makeApiClient(CoreV1Api)
    this.execClient = new Exec(kubeConfig)
    this.ready = this.initialize()
  }

  async acquirePod(): Promise<Pod> {
    await this.ready

    for (const [podId, stepId] of this.leaseMap.entries()) {
      if (stepId !== null) continue

      this.leaseMap.set(podId, ACQUIRING_LEASE)

      const pod = this.pods.get(podId)
      if (!pod) throw new Error(`Pod ${podId} was not found in the pool`)

      console.log(`🔒 Acquired pod ${pod.podName}`)
      return pod
    }

    throw new Error("NO_POD_AVAILABLE")
  }

  async releasePod(podId: string): Promise<void> {
    await this.ready

    if (!this.leaseMap.has(podId)) throw new Error(`Unknown pod id: ${podId}`)

    this.leaseMap.set(podId, null)

    const pod = this.pods.get(podId)
    console.log(`✅ Released pod ${pod?.podName || `runner-${podId}`}`)
  }

  async execInPod(podId: string, command: string): Promise<string> {
    await this.ready

    const pod = this.pods.get(podId)
    const podName = pod?.podName || `runner-${podId}`
    const stdoutChunks: string[] = []
    const stderrChunks: string[] = []
    let statusError: string | undefined

    console.log(`📦 execInPod ${podName}: ${command}`)

    try {
      await Promise.race(
        [
          this.execClient.exec(
            this.namespace,
            podName,
            RUNNER_CONTAINER_NAME,
            ["sh", "-c", command],
            createCaptureStream(stdoutChunks),
            createCaptureStream(stderrChunks),
            null,
            false,
            (status) => {
              if (status.status !== "Success") statusError = getStatusMessage(status) || "Command failed"
            },
          ),
          timeoutAfter(EXEC_API_TIMEOUT_MS),
        ]
      )
    } catch (error) {
      console.log(`⚠️ Kubernetes Exec API failed for ${podName}; falling back to kubectl exec`)
      return runKubectlExec(this.namespace, podName, command)
    }

    const stdout = stdoutChunks.join("")
    const stderr = stderrChunks.join("")

    if (statusError) throw new Error(stderr || statusError)

    return stdout
  }

  async getPods(): Promise<Pod[]> {
    const podList = await this.coreV1Api.listNamespacedPod({
      namespace: this.namespace,
      labelSelector: this.labelSelector,
    })

    return podList.items
      .map((pod) => pod.metadata?.name)
      .filter((podName): podName is string => Boolean(podName))
      .sort()
      .map((podName) => ({
        podId: getPodId(podName),
        podName,
        namespace: this.namespace,
      }))
  }

  getPoolStatus(): PoolStatus {
    const leased = Array.from(this.leaseMap.entries())
      .filter(([, stepId]) => stepId !== null)
      .map(([podId]) => podId)

    return {
      total: this.leaseMap.size,
      available: this.leaseMap.size - leased.length,
      leased,
    }
  }

  private async initialize(): Promise<void> {
    console.log("🔎 Discovering runner pods...")

    const pods = await this.getPods()

    this.pods.clear()
    this.leaseMap.clear()

    for (const pod of pods) {
      this.pods.set(pod.podId, pod)
      this.leaseMap.set(pod.podId, null)
      console.log(`✅ Discovered pod ${pod.podName}`)
    }
  }
}

export const podPool = new PodPool()
