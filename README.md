# Mini Workflow Orchestrator

A lightweight workflow orchestration engine built with **Bun**, **Express**, **Redis**, and **Kubernetes**. It accepts workflows containing shell commands and dependencies, schedules ready steps, executes commands inside Kubernetes runner pods, and tracks execution status.

## Progress

- [x] Section 1: Core orchestration
- [x] Section 2: Parallel execution
- [ ] Section 3: Failure handling, retries, and timeouts

## Architecture

```mermaid
flowchart LR
    Client[Client] --> API[Express API]
    API --> Store[Workflow Store]
    API --> Orchestrator[Orchestrator]
    Orchestrator --> DAG[DAG Resolver]
    Orchestrator --> StepQueue[Redis Step Queue]
    StepQueue --> PodManager[Pod Manager]
    PodManager --> PodPool[Kubernetes Pod Pool]
    PodPool --> Runners[7 Runner Pods]
    PodManager --> ResultQueue[Redis Result Queue]
    ResultQueue --> Orchestrator
```

## How It Works

1. A client submits a workflow through `POST /workflow`.
2. The orchestrator initializes and stores the workflow, then identifies ready steps using the DAG resolver.
3. Ready steps are marked `QUEUED` and added to the Redis step queue.
4. The dispatcher hands queued steps to the pod manager, up to one per runner pod, without waiting for earlier steps to finish. The pod manager leases a runner pod and executes each command.
5. Execution events (`RUNNING`, `COMPLETED`, or `FAILED`) are published to the Redis result queue.
6. The orchestrator consumes these events, updates workflow state, and schedules newly unblocked steps.

> **Design principle:** The orchestrator owns workflow state. The pod manager executes commands and reports results without modifying workflow state directly.

## Section 1 Implementation

| File | Implementation |
| --- | --- |
| `src/backend/dag.ts` | Resolves steps that are ready to execute |
| `src/backend/orchestrator.ts` | Workflow submission, background processing, result handling, and step dispatching |
| `src/pod-manager/pod-manager.ts` | Pod leasing, command execution, result reporting, and pod release |
| `src/backend/server.ts` | Workflow submission and status endpoints |
| `src/index.ts` | Starts the orchestrator on application startup |

The Redis client, queue infrastructure, pod pool, workflow store, and shared types are provided by the starter boilerplate.

### DAG Resolution

A step is ready when:

- Its status is `PENDING`.
- All dependencies have status `COMPLETED`.

Steps without dependencies are immediately eligible. Steps that are already `QUEUED`, `RUNNING`, or `COMPLETED` are not scheduled again.

The resolver uses the current statuses from the workflow store to avoid duplicate scheduling.

### Workflow Submission

`submitWorkflow()`:

1. Initializes every step as `PENDING` with `podId: null`.
2. Saves the workflow before queuing any steps.
3. Resolves the initially ready steps.
4. Marks ready steps as `QUEUED` and pushes them into the Redis step queue.

Saving the workflow first ensures that the result handler can find its state when execution events arrive.

### Execution and Result Handling

The pod manager:

1. Acquires a runner pod.
2. Reports the `RUNNING` event.
3. Executes the shell command.
4. Reports `COMPLETED` with output and exit code, or `FAILED` with an error.
5. Releases the pod in a `finally` block.

The orchestrator runs two background loops: a **result reader** and a **step dispatcher**. On successful completion, the result reader updates the workflow, resolves the DAG again, and queues newly eligible steps.

### Step Lifecycle

```text
PENDING → QUEUED → RUNNING → COMPLETED
                            ↘ FAILED
```

Workflow status is derived from the current step statuses:

| Workflow status | Condition |
| --- | --- |
| `pending` | All steps are `PENDING` |
| `running` | At least one step is `QUEUED` or `RUNNING` |
| `completed` | All steps are `COMPLETED` |
| `failed` | At least one step is `FAILED` |

## Section 2 Implementation

Section 2 runs independent steps at the same time. In the diamond workflow below, B and C both depend only on A, so once A completes they run in parallel on different pods.

```text
   A
 /   \
B     C
 \   /
   D
```

The change is in the dispatcher loop in `src/backend/orchestrator.ts`. The pod manager, result handler, and API are unchanged.

### Parallel Dispatcher

In Section 1 the dispatcher waited for each step to finish before taking the next one. It now starts a step and immediately goes back for the next, until every runner pod is busy.

```text
loop forever:
  if running steps >= total pods:  wait briefly, try again   (step stays in Redis)
  take the next step from the step queue
  if the queue is empty:           wait briefly, try again
  running += 1
  start the step on the pod manager without awaiting it
    on error:  log it
    finally:   running -= 1
```

- **Capacity is checked before dequeuing.** When all pods are busy, waiting steps stay in the Redis step queue, in order, until a pod is released.
- **Dispatch is not awaited.** Each step runs in the background. A `.catch()` handles its errors and a `.finally()` frees its slot, whether it succeeded or failed.

### Concurrency Counter

The orchestrator keeps its own count of running steps instead of relying on the pod pool's `available` value.

`acquirePod()` contains an `await` before it marks a pod as leased. If the dispatcher read `available` right after starting a step, that step's pod would not be counted yet. With one pod left, the dispatcher could take two steps off the queue, and the second would fail with `NO_POD_AVAILABLE`.

The counter is increased on the same line that starts the step, with no `await` in between, so it is never out of date. This is how the "lease map must be checked atomically" rule is met.

### Queued Before Enqueue

A step is marked `QUEUED` before it is pushed onto the step queue. Steps now start almost instantly, so if the order were reversed, the step's `RUNNING` event could arrive while the push was still being awaited, and would then be overwritten back to `QUEUED`.

### Concurrency Rules

| Rule | How it is met |
| --- | --- |
| Run ready steps simultaneously | The dispatcher does not await a step before starting the next |
| Never assign the same pod to two steps | `acquirePod()` leases each pod to one caller, and the dispatcher never starts more steps than there are pods |
| Check the lease map atomically | The running-step counter changes on the same line that starts a step |
| Keep steps queued when no pods are free | Capacity is checked before a step is taken off the queue |

### Execution Timeline

For the Section 2 test workflow (A: `sleep 2`, B and C: `sleep 5`, D: `echo`):

```text
t=0s   A starts on pod 0
t=2s   A completes -> B and C are queued together
t=2s   B starts on pod 0, C starts on pod 1 (parallel)
t=7s   B and C complete -> D is queued
t=7s   D starts on pod 0 and completes
```

Run sequentially this would take about 12 seconds; in parallel it takes about 7.

## Prerequisites

- Docker
- kind
- kubectl
- Bun

Redis runs locally in Docker.

## Setup

### 1. Install dependencies

```bash
bun install
```

### 2. Start Redis

```bash
docker run -d --name workflow-redis -p 6379:6379 redis
```

If the container already exists:

```bash
docker start workflow-redis
```

### 3. Create the Kubernetes cluster and runner pods

```bash
chmod +x k8s/setup.sh
./k8s/setup.sh
```

### 4. Verify runner pods

```bash
kubectl get pods -n workflow-runner
```

Expected: `runner-0` through `runner-6` are all `Running`.

### 5. Start the server

```bash
bun run dev
```

## API Endpoints

### `POST /workflow`

Submits a workflow for background execution.

Example request:

```bash
curl -X POST http://localhost:3000/workflow -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "workflowId": "wf-section-1",
    "steps": [
      { "id": "A", "command": "echo A" },
      { "id": "B", "command": "echo B", "dependsOn": ["A"] },
      { "id": "C", "command": "echo C", "dependsOn": ["B"] }
    ]
  }'
```

Expected response:

```json
{
  "workflowId": "wf-section-1",
  "status": "accepted"
}
```

### `GET /workflow/:id`

Returns the current workflow and step statuses.

```bash
curl -H "x-api-key: $API_KEY" http://localhost:3000/workflow/wf-section-1
```

Returns `404` if the workflow ID is unknown.

### `GET /pods`

Shows runner pod availability and active leases.

```bash
curl -H "x-api-key: $API_KEY" http://localhost:3000/pods
```

Example response when all seven pods are available:

```json
{
  "total": 7,
  "available": 7,
  "leased": []
}
```

## Testing

### Run tests

```bash
bun test
```

### Verify Section 1 end to end

Submit the three-step chain shown above, then poll:

```bash
curl -H "x-api-key: $API_KEY" http://localhost:3000/workflow/wf-section-1
```

Expected final result:

- All three steps reach `COMPLETED`.
- Each command executes in dependency order: `A → B → C`.
- Successful commands report exit code `0`.
- Workflow status becomes `completed`.

### Verify Section 2 parallel execution

Submit the diamond workflow:

```bash
curl -X POST http://localhost:3000/workflow -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "workflowId": "wf-section-2",
    "steps": [
      { "id": "A", "command": "sleep 2 && echo A" },
      { "id": "B", "command": "sleep 5 && echo B", "dependsOn": ["A"] },
      { "id": "C", "command": "sleep 5 && echo C", "dependsOn": ["A"] },
      { "id": "D", "command": "echo D", "dependsOn": ["B", "C"] }
    ]
  }'
```

Poll while it runs:

```bash
curl -H "x-api-key: $API_KEY" http://localhost:3000/workflow/wf-section-2
```

Expected state after A completes. B and C are both `RUNNING`, on different pods:

```json
{
  "workflowId": "wf-section-2",
  "status": "running",
  "steps": [
    { "id": "A", "status": "COMPLETED", "podId": "0", "stdout": "A\n", "exitCode": 0 },
    { "id": "B", "status": "RUNNING", "podId": "0" },
    { "id": "C", "status": "RUNNING", "podId": "1" },
    { "id": "D", "status": "PENDING", "podId": null }
  ]
}
```

Expected final state:

```json
{
  "workflowId": "wf-section-2",
  "status": "completed",
  "steps": [
    { "id": "A", "status": "COMPLETED", "podId": "0", "stdout": "A\n", "exitCode": 0 },
    { "id": "B", "status": "COMPLETED", "podId": "0", "stdout": "B\n", "exitCode": 0 },
    { "id": "C", "status": "COMPLETED", "podId": "1", "stdout": "C\n", "exitCode": 0 },
    { "id": "D", "status": "COMPLETED", "podId": "0", "stdout": "D\n", "exitCode": 0 }
  ]
}
```

### Verify queuing when all pods are busy

Submit ten independent steps, three more than there are pods:

```bash
curl -X POST http://localhost:3000/workflow -H "x-api-key: $API_KEY" -H "Content-Type: application/json" -d '{
  "workflowId": "wf-full",
  "steps": [
    {"id":"S0","command":"sleep 3 && echo 0"}, {"id":"S1","command":"sleep 3 && echo 1"},
    {"id":"S2","command":"sleep 3 && echo 2"}, {"id":"S3","command":"sleep 3 && echo 3"},
    {"id":"S4","command":"sleep 3 && echo 4"}, {"id":"S5","command":"sleep 3 && echo 5"},
    {"id":"S6","command":"sleep 3 && echo 6"}, {"id":"S7","command":"sleep 3 && echo 7"},
    {"id":"S8","command":"sleep 3 && echo 8"}, {"id":"S9","command":"sleep 3 && echo 9"}
  ]
}'
```

While it runs, poll `GET /workflow/wf-full` and `GET /pods`. Expected:

- At most 7 steps are `RUNNING` at any moment, each on a different pod.
- The remaining steps show `QUEUED` with `podId: null` until a pod is released.
- All 10 steps end `COMPLETED`.
- `GET /pods` shows `"available": 7` at the end.
- The server log shows no `NO_POD_AVAILABLE` errors.

### Inspect Redis queues

Use the queue keys printed when the server starts:

```bash
docker exec workflow-redis redis-cli LRANGE "workflow:12345:step-queue" 0 -1
docker exec workflow-redis redis-cli LRANGE "workflow:12345:result-queue" 0 -1
```

Replace the example keys with the actual keys from your server logs.

## Design Decisions

- **Single source of truth:** The orchestrator is responsible for workflow state updates.
- **Save before enqueue:** Prevents execution results from arriving before the workflow is stored.
- **State-driven scheduling:** The DAG resolver uses actual step statuses to avoid duplicate scheduling.
- **Reliable pod cleanup:** Acquired pods are released in a `finally` block.
- **Derived workflow status:** Workflow status is calculated from step statuses rather than stored separately.
- **Asynchronous workflow submission:** The API acknowledges a workflow while execution continues in the background.
- **Capacity before dequeue:** A step is only taken off the queue when a pod is free to run it, so waiting steps keep their place in Redis.
- **Own concurrency counter:** The dispatcher counts its running steps itself, because the pod pool's `available` value updates a moment late.
- **Queued before enqueue:** A step is marked `QUEUED` before it is pushed, so a fast `RUNNING` event can never be overwritten.

## Current Limitations

- **Failed dependencies:** Dependent steps can remain `PENDING` after an upstream step fails.
- **No retries or timeout handling:** Failed or stuck executions are not automatically recovered.
- **A stuck step holds its slot:** A command that never finishes keeps its pod and its place in the running-step count. Section 3 adds lease timeouts to recover it.
- **In-memory workflow storage:** Workflow state is lost when the server restarts.
- **Potential blocking execution:** The provided pod pool can fall back to a blocking `kubectl exec`. While a command runs this way, other HTTP requests wait, and parallel steps finish one after another even though both show `RUNNING`.

## Next Steps

- **Section 3:** Add failure propagation, skipped steps, retry policies, timeouts, and stuck-execution recovery.