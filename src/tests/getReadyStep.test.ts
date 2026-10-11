import {describe, expect, test} from "bun:test"
import { getReadySteps } from "../dag"


describe('getReadySteps', ()=>{
  const steps = [ 
    { "id": "A", "command": "sleep 2" },
    { "id": "B", "command": "ls /",      "dependsOn": ["A"] },
    { "id": "C", "command": "pwd",          "dependsOn": ["A"] },
    { "id": "D", "command": "date","dependsOn": ["B", "C"] }
  ]


  test("returns whose dependencies are COMPLETED" , () =>{

    const readyStep = getReadySteps(steps,{"A" : "COMPLETED"})

    expect(readyStep.map((step) => step.id)).toEqual(["B","C"])


  })


  test("return a step with no dependencies", () =>{
    const readyStep = getReadySteps(steps , {});

    expect(readyStep.map((step) => step.id)).toEqual(["A"])  })
})