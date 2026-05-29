import type { WorkflowDefinition, WorkflowMilestoneID, WorkflowMilestoneState, WorkflowStatus } from "./schema"

export function readyMilestones(workflow: WorkflowDefinition, states: WorkflowMilestoneState[]) {
  const stateByID = new Map(states.map((state) => [state.id, state.status]))
  return workflow.milestones.filter(
    (milestone) =>
      (stateByID.get(milestone.id) ?? "pending") === "pending" &&
      milestone.dependsOn.every((id) => {
        const status = stateByID.get(id)
        return status === "approved" || status === "done" || status === "completed" || status === "skipped"
      }),
  )
}

export function nextWorkflowStatus(workflow: WorkflowDefinition, states: WorkflowMilestoneState[]): WorkflowStatus {
  const stateByID = new Map(states.map((state) => [state.id, state.status]))
  const statuses = workflow.milestones.map((milestone) => stateByID.get(milestone.id) ?? "pending")

  if (statuses.some((status) => status === "cancelled")) return "cancelled"
  if (statuses.some((status) => status === "failed")) return "failed"
  if (statuses.some((status) => status === "blocked")) return "blocked"
  if (statuses.every((status) => status === "done" || status === "completed" || status === "skipped")) return "completed"
  if (statuses.some((status) => status === "testing")) return "testing"
  if (statuses.some((status) => status === "reviewing")) return "reviewing"
  if (statuses.some((status) => status === "planning")) return "dispatching"
  if (statuses.some((status) => status === "executing" || status === "running" || status === "rejected" || status === "approved"))
    return "running"
  if (readyMilestones(workflow, states).length > 0)
    return statuses.every((status) => status === "pending") ? "pending" : "running"
  return "blocked"
}

export function completeMilestone(
  workflow: WorkflowDefinition,
  states: WorkflowMilestoneState[],
  id: WorkflowMilestoneID,
) {
  const ids = new Set(workflow.milestones.map((milestone) => milestone.id))
  if (!ids.has(id)) return states
  const existing = new Set(states.map((state) => state.id))
  if (existing.has(id)) {
    return states.map((state) => (state.id === id ? { ...state, status: "approved" as const } : state))
  }
  return [...states, { id, status: "approved" as const }]
}
