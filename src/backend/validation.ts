import { z } from "zod"
import type { WorkflowStep } from "../types/workflow"

const stepSchema = z.object({
  id: z.string().min(1),
  command: z.string().min(1),
  dependsOn: z.array(z.string()).optional(),
  retries: z.number().int().min(0).max(5).optional(),
  timeoutMs: z.number().int().positive().optional(),
})

export const workflowSchema = z.object({
  workflowId: z.string().min(1),
  steps: z.array(stepSchema).min(1),
})

// Returns a list of human-readable errors; empty means the graph is valid
export function checkGraph(steps: WorkflowStep[]): string[] {
  const errors: string[] = []

  const ids = new Set<string>()
  for (const step of steps) {
    if (ids.has(step.id)) errors.push(`Duplicate step id: "${step.id}"`)
    ids.add(step.id)
  }

  for (const step of steps) {
    for (const dep of step.dependsOn ?? []) {
      if (dep === step.id) {
        errors.push(`Step "${step.id}" depends on itself`)
      } else if (!ids.has(dep)) {
        errors.push(`Step "${step.id}" depends on unknown step "${dep}"`)
      }
    }
  }

  // Kahn's algorithm: steps left over after removing zero-indegree steps are in a cycle.
  // Self-dependencies and unknown ids are already reported above, so skip them here.
  const indegree = new Map<string, number>()
  const dependents = new Map<string, string[]>()
  for (const id of ids) {
    indegree.set(id, 0)
    dependents.set(id, [])
  }

  for (const step of steps) {
    for (const dep of new Set(step.dependsOn ?? [])) {
      if (dep === step.id || !ids.has(dep)) continue
      indegree.set(step.id, (indegree.get(step.id) ?? 0) + 1)
      dependents.get(dep)!.push(step.id)
    }
  }

  const queue = [...ids].filter((id) => indegree.get(id) === 0)
  let processed = 0

  while (queue.length > 0) {
    const id = queue.shift()!
    processed++

    for (const next of dependents.get(id)!) {
      const remaining = indegree.get(next)! - 1
      indegree.set(next, remaining)
      if (remaining === 0) queue.push(next)
    }
  }

  if (processed < ids.size) {
    const stuck = [...ids].filter((id) => indegree.get(id)! > 0)
    errors.push(`Dependency cycle detected involving: ${stuck.join(", ")}`)
  }

  return errors
}
