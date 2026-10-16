import { describe, expect, test } from "bun:test"
import { checkGraph } from "../backend/validation"

describe("checkGraph", () => {
  test("accepts a valid diamond", () => {
    const steps = [
      { id: "A", command: "echo a" },
      { id: "B", command: "echo b", dependsOn: ["A"] },
      { id: "C", command: "echo c", dependsOn: ["A"] },
      { id: "D", command: "echo d", dependsOn: ["B", "C"] },
    ]

    expect(checkGraph(steps)).toEqual([])
  })

  test("detects a 2-step cycle", () => {
    const steps = [
      { id: "A", command: "echo a", dependsOn: ["B"] },
      { id: "B", command: "echo b", dependsOn: ["A"] },
    ]

    const errors = checkGraph(steps)
    expect(errors).toHaveLength(1)
    expect(errors[0]).toContain("cycle")
  })

  test("detects a missing dependency", () => {
    const steps = [{ id: "A", command: "echo a", dependsOn: ["Z"] }]

    expect(checkGraph(steps)).toEqual(['Step "A" depends on unknown step "Z"'])
  })

  test("detects duplicate ids", () => {
    const steps = [
      { id: "A", command: "echo a" },
      { id: "A", command: "echo again" },
    ]

    expect(checkGraph(steps)).toEqual(['Duplicate step id: "A"'])
  })

  test("detects a step depending on itself", () => {
    const steps = [{ id: "A", command: "echo a", dependsOn: ["A"] }]

    expect(checkGraph(steps)).toEqual(['Step "A" depends on itself'])
  })
})
