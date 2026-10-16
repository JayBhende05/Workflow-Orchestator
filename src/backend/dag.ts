import type { WorkflowStep, StepStatus } from "../types/workflow"

// Every step that directly or transitively depends on failedId (breadth-first search)
export function getDownstreamSteps(
  steps: WorkflowStep[],
  failedId: string
): WorkflowStep[] {
  const downstream: WorkflowStep[] = []
  const seen = new Set<string>([failedId])
  const queue: string[] = [failedId]

  while (queue.length > 0) {
    const currentId = queue.shift()!

    for (const step of steps) {
      if (seen.has(step.id)) continue
      if (!(step.dependsOn ?? []).includes(currentId)) continue

      seen.add(step.id)
      downstream.push(step)
      queue.push(step.id)
    }
  }

  return downstream
}

export function getReadySteps(
  steps: WorkflowStep[],
  stepStatus: Record<string, StepStatus>
): WorkflowStep[] {

  // console.log("Steps in ReadySteps" , steps);
  // console.log("StepStatus in readyState" , stepStatus);
  return steps.filter((step) => {
    // Skip steps that have already started or finished
    if (stepStatus[step.id] !== "PENDING") return false

    // A step is ready if all dependencies are completed
    return (step.dependsOn ?? []).every(
      (dependencyId) => stepStatus[dependencyId] === "COMPLETED"
    );
  });
}






