import { describe, expect, test } from "bun:test"

import {
  dependencyBlockedMilestones,
  dependencyUnblockedMilestones,
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

  test("allows pipeline item review before other item audits complete", () => {
    const pipeline = parseWorkflowXml(
      `
        <workflow>
          <ordered>
            <milestone id="prepare">Prepare shared inputs</milestone>
            <pipeline items="work/shared/audit-tracks.json">
              <milestone id="audit">Audit {item.id}</milestone>
              <milestone id="review" depends="audit">Review {item.id}</milestone>
            </pipeline>
            <milestone id="finish">Finish synthesis</milestone>
          </ordered>
        </workflow>
      `,
      {
        readPipelineItems: () =>
          Array.from({ length: 13 }, (_, index) => ({
            id: `track-${String(index + 1).padStart(2, "0")}`,
          })),
      },
    )

    expect(pipeline.milestones.filter((milestone) => String(milestone.id).startsWith("audit@"))).toHaveLength(13)
    expect(pipeline.milestones.filter((milestone) => String(milestone.id).startsWith("review@"))).toHaveLength(13)
    expect(
      readyMilestones(pipeline, [
        { id: id("prepare"), status: "done" },
        { id: id("audit@track-01"), status: "done" },
      ]).map((milestone) => String(milestone.id)),
    ).toEqual([
      "review@track-01",
      "audit@track-02",
      "audit@track-03",
      "audit@track-04",
      "audit@track-05",
      "audit@track-06",
      "audit@track-07",
      "audit@track-08",
      "audit@track-09",
      "audit@track-10",
      "audit@track-11",
      "audit@track-12",
      "audit@track-13",
    ])
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

describe("dependencyBlockedMilestones", () => {
  test("returns pending transitive dependents of failed dependencies", () => {
    expect(
      dependencyBlockedMilestones(workflow, [
        { id: id("plan"), status: "done" },
        { id: id("api"), status: "failed" },
        { id: id("tests"), status: "pending" },
        { id: id("finish"), status: "pending" },
      ]).map((milestone) => String(milestone.id)),
    ).toEqual(["finish"])
  })

  test("returns dependency-blocked milestones that can run after a force skip", () => {
    expect(
      dependencyUnblockedMilestones(workflow, [
        { id: id("plan"), status: "done" },
        { id: id("api"), status: "skipped" },
        { id: id("tests"), status: "done" },
        { id: id("finish"), status: "blocked" },
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
