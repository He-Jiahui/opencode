import { Schema } from "effect"

import { Identifier } from "@/id/id"
import { ModelID, ProviderID } from "@/provider/schema"
import { ProjectID } from "@/project/schema"
import { SessionID } from "@/session/schema"
import { optionalOmitUndefined, withStatics } from "@opencode-ai/core/schema"

const workflowIdSchema = Schema.String.check(Schema.isStartsWith("wfl")).pipe(Schema.brand("WorkflowID"))
export type WorkflowID = typeof workflowIdSchema.Type
export const WorkflowID = workflowIdSchema.pipe(
  withStatics((schema: typeof workflowIdSchema) => ({
    ascending: (id?: string) => schema.make(Identifier.ascending("workflow", id)),
    descending: (id?: string) => schema.make(Identifier.descending("workflow", id)),
  })),
)

export const WorkflowMilestoneID = Schema.NonEmptyString.pipe(Schema.brand("WorkflowMilestoneID"))
export type WorkflowMilestoneID = typeof WorkflowMilestoneID.Type

export const WorkflowGroupKind = Schema.Literals(["ordered", "parallel"])
export type WorkflowGroupKind = typeof WorkflowGroupKind.Type

export const WorkflowRole = Schema.Literals([
  "requester",
  "main_pm",
  "department_pm",
  "executor",
  "reviewer",
  "tester",
  "expert",
])
export type WorkflowRole = typeof WorkflowRole.Type

const WorkflowStaffLimit = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(12))

export const WorkflowStaffingConfig = Schema.Struct({
  mainPM: Schema.optional(WorkflowStaffLimit),
  departmentPM: Schema.optional(WorkflowStaffLimit),
  executor: Schema.optional(WorkflowStaffLimit),
  reviewer: Schema.optional(WorkflowStaffLimit),
  tester: Schema.optional(WorkflowStaffLimit),
  expert: Schema.optional(WorkflowStaffLimit),
}).annotate({ identifier: "WorkflowStaffingConfig" })
export type WorkflowStaffingConfig = typeof WorkflowStaffingConfig.Type

export const WorkflowMemberStatus = Schema.Literals(["active", "paused"])
export type WorkflowMemberStatus = typeof WorkflowMemberStatus.Type

export const WorkflowMember = Schema.Struct({
  id: Schema.String,
  workflowID: WorkflowID,
  role: WorkflowRole,
  specialty: Schema.String,
  title: Schema.String,
  sessionID: SessionID,
  capacity: Schema.Number,
  status: WorkflowMemberStatus,
  time: Schema.Struct({
    created: Schema.Number,
    updated: Schema.Number,
  }),
}).annotate({ identifier: "WorkflowMember" })
export type WorkflowMemberInfo = typeof WorkflowMember.Type

export const WorkflowMilestoneStatus = Schema.Literals([
  "pending",
  "planning",
  "executing",
  "reviewing",
  "rejected",
  "approved",
  "blocked",
  "testing",
  "done",
  "failed",
  "skipped",
  "running",
  "completed",
  "cancelled",
])
export type WorkflowMilestoneStatus = typeof WorkflowMilestoneStatus.Type

export const WorkflowStatus = Schema.Literals([
  "pending",
  "running",
  "planning",
  "dispatching",
  "executing",
  "reviewing",
  "testing",
  "accepting",
  "blocked",
  "completed",
  "failed",
  "cancelled",
])
export type WorkflowStatus = typeof WorkflowStatus.Type

export const WorkflowSessionRef = Schema.Struct({
  role: WorkflowRole,
  sessionID: SessionID,
  milestoneID: Schema.optional(WorkflowMilestoneID),
  attempt: Schema.optional(Schema.Number),
}).annotate({ identifier: "WorkflowSessionRef" })
export type WorkflowSessionRef = typeof WorkflowSessionRef.Type

export const WorkflowConsultationStatus = Schema.Literals(["pending", "answered", "failed"])
export type WorkflowConsultationStatus = typeof WorkflowConsultationStatus.Type

export const WorkflowCommunicationTiming = Schema.Literals(["after-task", "interrupt", "temporary-interrupt"])
export type WorkflowCommunicationTiming = typeof WorkflowCommunicationTiming.Type

export const WorkflowConsultation = Schema.Struct({
  id: Schema.String,
  workflowID: WorkflowID,
  fromSessionID: SessionID,
  toSessionID: SessionID,
  fromRole: WorkflowRole,
  toRole: WorkflowRole,
  milestoneID: Schema.optional(WorkflowMilestoneID),
  reason: Schema.optional(Schema.String),
  timing: Schema.optional(WorkflowCommunicationTiming),
  question: Schema.String,
  answer: Schema.String,
  status: WorkflowConsultationStatus,
  time: Schema.Struct({
    created: Schema.Number,
    updated: Schema.Number,
  }),
}).annotate({ identifier: "WorkflowConsultation" })
export type WorkflowConsultationInfo = typeof WorkflowConsultation.Type

export const WorkflowInterventionStatus = Schema.Literals(["queued", "delivered", "blocked", "failed"])
export type WorkflowInterventionStatus = typeof WorkflowInterventionStatus.Type

export const WorkflowIntervention = Schema.Struct({
  id: Schema.String,
  workflowID: WorkflowID,
  fromSessionID: Schema.optional(SessionID),
  targetSessionID: Schema.optional(SessionID),
  targetRole: WorkflowRole,
  timing: WorkflowCommunicationTiming,
  message: Schema.String,
  response: Schema.optional(Schema.String),
  path: Schema.String,
  status: WorkflowInterventionStatus,
  time: Schema.Struct({
    created: Schema.Number,
    updated: Schema.Number,
  }),
}).annotate({ identifier: "WorkflowIntervention" })
export type WorkflowInterventionInfo = typeof WorkflowIntervention.Type

export type WorkflowStep =
  | {
      type: "milestone"
      id: WorkflowMilestoneID
      title?: string
      department?: string
      prompt: string
      dependsOn: ReadonlyArray<WorkflowMilestoneID>
    }
  | {
      type: WorkflowGroupKind
      children: WorkflowStep[]
    }

export type WorkflowMilestone = Extract<WorkflowStep, { type: "milestone" }>

export type WorkflowDefinition = {
  steps: WorkflowStep
  milestones: WorkflowMilestone[]
}

export const WorkflowMilestone = Schema.Struct({
  id: WorkflowMilestoneID,
  title: Schema.optional(Schema.String),
  department: Schema.optional(Schema.String),
  prompt: Schema.String,
  dependsOn: Schema.Array(WorkflowMilestoneID),
  status: WorkflowMilestoneStatus,
  attempt: Schema.Number,
  planPath: Schema.optional(Schema.String),
  reviewPath: Schema.optional(Schema.String),
  session: Schema.Array(WorkflowSessionRef),
}).annotate({ identifier: "WorkflowMilestone" })
export type WorkflowMilestoneInfo = typeof WorkflowMilestone.Type

const WorkflowModel = Schema.Struct({
  providerID: ProviderID,
  modelID: ModelID,
  variant: Schema.optional(Schema.String),
})

export const WorkflowInfo = Schema.Struct({
  id: WorkflowID,
  projectID: ProjectID,
  rootSessionID: optionalOmitUndefined(SessionID),
  pmSessionID: Schema.optional(SessionID),
  testerSessionID: Schema.optional(SessionID),
  request: Schema.String,
  title: Schema.String,
  directory: Schema.String,
  path: Schema.String,
  xml: Schema.String,
  status: WorkflowStatus,
  staffing: Schema.optional(WorkflowStaffingConfig),
  model: Schema.optional(WorkflowModel),
  agent: Schema.optional(Schema.String),
  testPath: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
  time: Schema.Struct({
    created: Schema.Number,
    updated: Schema.Number,
    completed: Schema.optional(Schema.Number),
  }),
}).annotate({ identifier: "Workflow" })
export type WorkflowInfo = typeof WorkflowInfo.Type

export const WorkflowGraphNode = Schema.Struct({
  id: Schema.String,
  type: Schema.Literals(["workflow", "session", "milestone", "document"]),
  title: Schema.String,
  role: Schema.optional(WorkflowRole),
  status: Schema.optional(Schema.Union([WorkflowStatus, WorkflowMilestoneStatus])),
  sessionID: Schema.optional(SessionID),
  milestoneID: Schema.optional(WorkflowMilestoneID),
  path: Schema.optional(Schema.String),
  summary: Schema.optional(Schema.String),
}).annotate({ identifier: "WorkflowGraphNode" })
export type WorkflowGraphNode = typeof WorkflowGraphNode.Type

export const WorkflowGraphEdgeKind = Schema.Literals([
  "entry",
  "dependency",
  "session",
  "tester",
  "consultation",
  "document",
])
export type WorkflowGraphEdgeKind = typeof WorkflowGraphEdgeKind.Type

export const WorkflowGraphEdge = Schema.Struct({
  id: Schema.String,
  from: Schema.String,
  to: Schema.String,
  kind: Schema.optional(WorkflowGraphEdgeKind),
  label: Schema.optional(Schema.String),
  summary: Schema.optional(Schema.String),
  question: Schema.optional(Schema.String),
  answer: Schema.optional(Schema.String),
  path: Schema.optional(Schema.String),
}).annotate({ identifier: "WorkflowGraphEdge" })
export type WorkflowGraphEdge = typeof WorkflowGraphEdge.Type

export const WorkflowGraph = Schema.Struct({
  workflow: WorkflowInfo,
  milestones: Schema.Array(WorkflowMilestone),
  members: Schema.Array(WorkflowMember),
  consultations: Schema.Array(WorkflowConsultation),
  interventions: Schema.Array(WorkflowIntervention),
  nodes: Schema.Array(WorkflowGraphNode),
  edges: Schema.Array(WorkflowGraphEdge),
}).annotate({ identifier: "WorkflowGraph" })
export type WorkflowGraph = typeof WorkflowGraph.Type

export type WorkflowMilestoneState = {
  id: WorkflowMilestoneID
  status: WorkflowMilestoneStatus
}
