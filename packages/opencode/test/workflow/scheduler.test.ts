import { describe, expect, test } from "bun:test"

import {
  nextWorkflowStatus,
  parseWorkflowXml,
  readyMilestones,
  WorkflowMilestoneID,
  type WorkflowDefinition,
  type WorkflowMilestoneState,
} from "@/workflow"

const id = WorkflowMilestoneID.make

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

const blockedWorkflow: WorkflowDefinition = {
  steps: {
    type: "milestone",
    id: id("blocked"),
    prompt: "Blocked",
    dependsOn: [id("external")],
  },
  milestones: [
    {
      type: "milestone",
      id: id("blocked"),
      prompt: "Blocked",
      dependsOn: [id("external")],
    },
  ],
}

describe("readyMilestones", () => {
  test("returns pending milestones whose dependencies are complete", () => {
    expect(readyMilestones(workflow, []).map((milestone) => String(milestone.id))).toEqual(["plan"])

    expect(
      readyMilestones(workflow, [
        { id: id("plan"), status: "completed" },
        { id: id("api"), status: "running" },
      ]).map((milestone) => String(milestone.id)),
    ).toEqual(["tests"])

    expect(
      readyMilestones(workflow, [
        { id: id("plan"), status: "completed" },
        { id: id("api"), status: "completed" },
        { id: id("tests"), status: "completed" },
      ]).map((milestone) => String(milestone.id)),
    ).toEqual(["finish"])
  })

  test("treats skipped dependencies as satisfied", () => {
    expect(
      readyMilestones(workflow, [
        { id: id("plan"), status: "completed" },
        { id: id("api"), status: "skipped" },
        { id: id("tests"), status: "completed" },
      ]).map((milestone) => String(milestone.id)),
    ).toEqual(["finish"])
  })
})

describe("nextWorkflowStatus", () => {
  test("reports pending before work starts", () => {
    expect(nextWorkflowStatus(workflow, [])).toBe("pending")
  })

  test("reports running when work is active or newly unblocked", () => {
    expect(nextWorkflowStatus(workflow, [{ id: id("plan"), status: "running" }])).toBe("running")
    expect(nextWorkflowStatus(workflow, [{ id: id("plan"), status: "completed" }])).toBe("running")
  })

  test("reports terminal and blocked states", () => {
    expect(
      nextWorkflowStatus(workflow, [
        { id: id("plan"), status: "completed" },
        { id: id("api"), status: "completed" },
        { id: id("tests"), status: "completed" },
        { id: id("finish"), status: "completed" },
      ]),
    ).toBe("completed")

    expect(nextWorkflowStatus(workflow, [{ id: id("plan"), status: "failed" }])).toBe("failed")
    expect(nextWorkflowStatus(workflow, [{ id: id("plan"), status: "cancelled" }])).toBe("cancelled")
    expect(nextWorkflowStatus(workflow, [{ id: id("plan"), status: "blocked" }])).toBe("blocked")

    const blocked: WorkflowMilestoneState[] = [
      { id: id("plan"), status: "skipped" },
      { id: id("api"), status: "failed" },
      { id: id("tests"), status: "pending" },
    ]
    expect(nextWorkflowStatus(workflow, blocked)).toBe("failed")
  })

  test("reports blocked when pending work cannot run", () => {
    expect(
      nextWorkflowStatus(workflow, [
        { id: id("plan"), status: "running" },
        { id: id("api"), status: "pending" },
        { id: id("tests"), status: "pending" },
        { id: id("finish"), status: "pending" },
      ]),
    ).toBe("running")

    expect(nextWorkflowStatus(blockedWorkflow, [])).toBe("blocked")
  })
})
