import { describe, expect, test } from "bun:test"

import { parseWorkflowXml, WorkflowMilestoneID, WorkflowParseError } from "@/workflow"

describe("parseWorkflowXml", () => {
  test("parses milestones with metadata and prompt text", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <milestone id="plan" title="Plan" agent="planner">Write a plan</milestone>
      </workflow>
    `)

    expect(workflow.milestones).toEqual([
      {
        type: "milestone",
        id: WorkflowMilestoneID.make("plan"),
        title: "Plan",
        department: "planner",
        prompt: "Write a plan",
        dependsOn: [],
      },
    ])
  })

  test("turns ordered groups into dependency edges", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <ordered>
          <milestone id="plan">Plan</milestone>
          <parallel>
            <milestone id="api">API</milestone>
            <milestone id="tests">Tests</milestone>
          </parallel>
          <milestone id="finish">Finish</milestone>
        </ordered>
      </workflow>
    `)

    expect(workflow.milestones.map((milestone) => [String(milestone.id), milestone.dependsOn.map(String)])).toEqual([
      ["plan", []],
      ["api", ["plan"]],
      ["tests", ["plan"]],
      ["finish", ["api", "tests"]],
    ])
  })

  test("preserves explicit dependencies and removes duplicates", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <ordered>
          <milestone id="a">A</milestone>
          <milestone id="b" depends="a">B</milestone>
          <milestone id="c" depends="a, b">C</milestone>
        </ordered>
      </workflow>
    `)

    expect(workflow.milestones.map((milestone) => [String(milestone.id), milestone.dependsOn.map(String)])).toEqual([
      ["a", []],
      ["b", ["a"]],
      ["c", ["b", "a"]],
    ])
  })

  test("rejects unknown elements and missing milestone ids", () => {
    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <task id="a">A</task>
        </workflow>
      `),
    ).toThrow(WorkflowParseError)

    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <milestone>Missing id</milestone>
        </workflow>
      `),
    ).toThrow(WorkflowParseError)
  })

  test("rejects empty groups with no milestones", () => {
    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <ordered></ordered>
        </workflow>
      `),
    ).toThrow("<ordered> must contain at least one step")
  })

  test("rejects duplicate ids, unknown dependencies, and cycles", () => {
    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <parallel>
            <milestone id="a">A</milestone>
            <milestone id="a">Duplicate</milestone>
          </parallel>
        </workflow>
      `),
    ).toThrow("workflow milestone ids must be unique")

    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <milestone id="a" depends="missing">A</milestone>
        </workflow>
      `),
    ).toThrow('workflow milestone depends on unknown id "missing"')

    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <parallel>
            <milestone id="a" depends="b">A</milestone>
            <milestone id="b" depends="a">B</milestone>
          </parallel>
        </workflow>
      `),
    ).toThrow("workflow milestone dependencies cannot contain a cycle")
  })
})
