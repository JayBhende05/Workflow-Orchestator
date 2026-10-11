import type { WorkflowStep, StepStatus } from "../types/workflow"

const steps = {
  "workflowId": "wf-123",
  "steps": [
    { "id": "A", "command": "sleep 2" },
    { "id": "B", "command": "ls /",      "dependsOn": ["A"] },
    { "id": "C", "command": "pwd",          "dependsOn": ["A"] },
    { "id": "D", "command": "date","dependsOn": ["B", "C"] }
  ]
}

export function getReadySteps(
  steps: WorkflowStep[],
  stepStatus: Record<string, StepStatus>
): WorkflowStep[] {
  
  console.log("Steps in ReadySteps" , steps);
  console.log("StepStatus in readyState" , stepStatus);
   return steps.filter((step) => {
    // Skip steps that have already started or finished
    if (stepStatus[step.id]) {
      return false;
    }

    // A step is ready if all dependencies are completed
    return (step.dependsOn ?? []).every(
      (dependencyId) => stepStatus[dependencyId] === "COMPLETED"
    );
  });
}






