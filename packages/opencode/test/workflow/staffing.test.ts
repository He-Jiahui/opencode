import { describe, expect, test } from "bun:test"

import { SessionID } from "@/session/schema"
import { WorkflowMilestoneID, type WorkflowMemberInfo, type WorkflowMilestoneInfo } from "@/workflow"
import { selectWorkflowMember, workflowSessionAssignment } from "@/workflow/workflow"

const sessionID = SessionID.make
const milestoneID = WorkflowMilestoneID.make

function member(input: {
  role: WorkflowMemberInfo["role"]
  specialty: string
  sessionID: string
  created?: number
  status?: WorkflowMemberInfo["status"]
}): WorkflowMemberInfo {
  return {
    id: `${input.role}-${input.specialty}`,
    workflowID: "wfl_test" as WorkflowMemberInfo["workflowID"],
    role: input.role,
    specialty: input.specialty,
    title: `${input.role} ${input.specialty}`,
    sessionID: sessionID(input.sessionID),
    capacity: 1,
    status: input.status ?? "active",
    time: {
      created: input.created ?? 1,
      updated: 1,
    },
  }
}

function milestone(input: {
  id: string
  status: WorkflowMilestoneInfo["status"]
  role: WorkflowMemberInfo["role"]
  sessionID: string
}): WorkflowMilestoneInfo {
  return {
    id: milestoneID(input.id),
    prompt: input.id,
    dependsOn: [],
    status: input.status,
    attempt: 1,
    session: [
      {
        role: input.role,
        sessionID: sessionID(input.sessionID),
        milestoneID: milestoneID(input.id),
        attempt: 1,
      },
    ],
  }
}

describe("selectWorkflowMember", () => {
  test("prefers an idle member with the requested specialty", () => {
    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [
          member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1" }),
          member({ role: "executor", specialty: "engineering-2", sessionID: "ses_exec_2" }),
        ],
        milestones: [],
      })?.sessionID,
    ).toBe(sessionID("ses_exec_1"))
  })

  test("does not assign the same busy executor to a parallel executing milestone", () => {
    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [
          member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1" }),
          member({ role: "executor", specialty: "engineering-2", sessionID: "ses_exec_2" }),
        ],
        milestones: [
          milestone({
            id: "current",
            status: "executing",
            role: "executor",
            sessionID: "ses_exec_1",
          }),
        ],
      })?.sessionID,
    ).toBe(sessionID("ses_exec_2"))
  })

  test("returns undefined when all role members are busy", () => {
    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [
          member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1" }),
          member({ role: "executor", specialty: "engineering-2", sessionID: "ses_exec_2" }),
        ],
        milestones: [
          milestone({ id: "one", status: "executing", role: "executor", sessionID: "ses_exec_1" }),
          milestone({ id: "two", status: "executing", role: "executor", sessionID: "ses_exec_2" }),
        ],
      }),
    ).toBeUndefined()
  })

  test("does not treat completed assignments as busy", () => {
    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1" })],
        milestones: [milestone({ id: "done", status: "approved", role: "executor", sessionID: "ses_exec_1" })],
      })?.sessionID,
    ).toBe(sessionID("ses_exec_1"))
  })

  test("balances sequential assignments across same-role staff", () => {
    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [
          member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1", created: 1 }),
          member({ role: "executor", specialty: "engineering-2", sessionID: "ses_exec_2", created: 2 }),
          member({ role: "executor", specialty: "engineering-3", sessionID: "ses_exec_3", created: 3 }),
        ],
        milestones: [
          milestone({ id: "m01", status: "approved", role: "executor", sessionID: "ses_exec_1" }),
          milestone({ id: "m02", status: "approved", role: "executor", sessionID: "ses_exec_1" }),
        ],
      })?.sessionID,
    ).toBe(sessionID("ses_exec_2"))
  })

  test("keeps a reopened milestone with its existing owner before balancing", () => {
    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [
          member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1", created: 1 }),
          member({ role: "executor", specialty: "engineering-2", sessionID: "ses_exec_2", created: 2 }),
        ],
        milestones: [
          milestone({ id: "m01", status: "pending", role: "executor", sessionID: "ses_exec_1" }),
          milestone({ id: "m02", status: "approved", role: "executor", sessionID: "ses_exec_1" }),
        ],
        excludeMilestoneID: milestoneID("m01"),
      })?.sessionID,
    ).toBe(sessionID("ses_exec_1"))
  })

  test("treats department PM review work as busy", () => {
    expect(
      selectWorkflowMember({
        role: "department_pm",
        specialty: "product",
        members: [
          member({ role: "department_pm", specialty: "product", sessionID: "ses_pm_1" }),
          member({ role: "department_pm", specialty: "product-2", sessionID: "ses_pm_2" }),
        ],
        milestones: [milestone({ id: "review", status: "reviewing", role: "department_pm", sessionID: "ses_pm_1" })],
      })?.sessionID,
    ).toBe(sessionID("ses_pm_2"))
  })

  test("allows the same employee to continue a reopened milestone they already own", () => {
    expect(
      selectWorkflowMember({
        role: "department_pm",
        specialty: "product",
        members: [member({ role: "department_pm", specialty: "product", sessionID: "ses_pm_1" })],
        milestones: [milestone({ id: "implementation", status: "planning", role: "department_pm", sessionID: "ses_pm_1" })],
        excludeMilestoneID: milestoneID("implementation"),
      })?.sessionID,
    ).toBe(sessionID("ses_pm_1"))
  })

  test("does not use members beyond the current staffing limit", () => {
    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [
          member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1", created: 1 }),
          member({ role: "executor", specialty: "engineering-2", sessionID: "ses_exec_2", created: 2 }),
        ],
        milestones: [milestone({ id: "current", status: "executing", role: "executor", sessionID: "ses_exec_1" })],
        limit: 1,
      }),
    ).toBeUndefined()

    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [
          member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1", created: 1 }),
          member({ role: "executor", specialty: "engineering-2", sessionID: "ses_exec_2", created: 2 }),
        ],
        milestones: [milestone({ id: "current", status: "executing", role: "executor", sessionID: "ses_exec_1" })],
        limit: 2,
      })?.sessionID,
    ).toBe(sessionID("ses_exec_2"))
  })

  test("does not assign paused employees even when they are inside the staffing limit", () => {
    expect(
      selectWorkflowMember({
        role: "executor",
        specialty: "engineering",
        members: [
          member({ role: "executor", specialty: "engineering", sessionID: "ses_exec_1", created: 1, status: "paused" }),
          member({ role: "executor", specialty: "engineering-2", sessionID: "ses_exec_2", created: 2 }),
        ],
        milestones: [],
        limit: 2,
      })?.sessionID,
    ).toBe(sessionID("ses_exec_2"))
  })

  test("uses the current active assignment for a reused long-lived employee session", () => {
    expect(
      workflowSessionAssignment(
        [
          milestone({ id: "previous", status: "approved", role: "executor", sessionID: "ses_exec_1" }),
          milestone({ id: "current", status: "executing", role: "executor", sessionID: "ses_exec_1" }),
        ],
        sessionID("ses_exec_1"),
      )?.milestone.id,
    ).toBe(milestoneID("current"))
  })

  test("falls back to the latest assignment when a reused employee has no active work", () => {
    expect(
      workflowSessionAssignment(
        [
          milestone({ id: "first", status: "approved", role: "executor", sessionID: "ses_exec_1" }),
          milestone({ id: "second", status: "done", role: "executor", sessionID: "ses_exec_1" }),
        ],
        sessionID("ses_exec_1"),
      )?.milestone.id,
    ).toBe(milestoneID("second"))
  })
})
