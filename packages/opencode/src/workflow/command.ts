import { Schema } from "effect"

import { BusEvent } from "@/bus/bus-event"
import { SessionID } from "@/session/schema"

import {
  WorkflowCommunicationTiming,
  WorkflowID,
  WorkflowMemberAvailability,
  WorkflowMilestoneID,
  WorkflowMilestoneStatus,
  WorkflowRole,
  WorkflowSchedulingMode,
  type WorkflowStatus,
} from "./schema"

export const WorkflowToolAction = Schema.Literals([
  "status",
  "resume",
  "block",
  "update_xml",
  "milestone_status",
  "plan_complete",
  "force_complete",
  "force_skip",
  "status_update",
  "scheduling",
  "workflow_status",
  "complete",
])
export type WorkflowToolAction = typeof WorkflowToolAction.Type

export const WorkflowToolCommand = Schema.Struct({
  id: Schema.optional(Schema.String),
  action: WorkflowToolAction,
  workflowID: Schema.optional(WorkflowID),
  sourceSessionID: SessionID,
  sourceAgent: Schema.optional(Schema.String),
  milestoneID: Schema.optional(WorkflowMilestoneID),
  milestoneStatus: Schema.optional(WorkflowMilestoneStatus),
  workflowStatus: Schema.optional(
    Schema.Literals([
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
    ]),
  ),
  targetRole: Schema.optional(WorkflowRole),
  availability: Schema.optional(WorkflowMemberAvailability),
  currentFocus: Schema.optional(Schema.String),
  blockers: Schema.optional(Schema.Array(Schema.String)),
  progressNote: Schema.optional(Schema.String),
  schedulingMode: Schema.optional(WorkflowSchedulingMode),
  schedulingMaxActive: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1), Schema.isLessThanOrEqualTo(64))),
  xml: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
}).annotate({ identifier: "WorkflowToolCommand" })
export type WorkflowToolCommand = Omit<typeof WorkflowToolCommand.Type, "sourceSessionID" | "workflowStatus"> & {
  sourceSessionID: typeof SessionID.Type
  workflowStatus?: WorkflowStatus
}

export const WorkflowToolCommandEvent = BusEvent.define("workflow.tool.command", WorkflowToolCommand)

export const WorkflowToolCommandRejection = Schema.Struct({
  code: Schema.Literals([
    "illegal_transition",
    "not_authorized",
    "invalid_xml",
    "unknown_milestone",
    "workflow_not_active",
    "precondition_failed",
  ]),
  reason: Schema.String,
  allowedTransitions: Schema.optional(Schema.Array(Schema.String)),
}).annotate({ identifier: "WorkflowToolCommandRejection" })
export type WorkflowToolCommandRejection = typeof WorkflowToolCommandRejection.Type

export const WorkflowToolCommandResult = Schema.Struct({
  id: Schema.String,
  action: WorkflowToolAction,
  applied: Schema.Boolean,
  workflowID: Schema.optional(WorkflowID),
  message: Schema.String,
  rejection: Schema.optional(WorkflowToolCommandRejection),
}).annotate({ identifier: "WorkflowToolCommandResult" })
export type WorkflowToolCommandResult = typeof WorkflowToolCommandResult.Type

export const WorkflowToolCommandResultEvent = BusEvent.define(
  "workflow.tool.command.result",
  WorkflowToolCommandResult,
)

const workflowToolCommandDispatchers = new Map<
  string,
  (input: WorkflowToolCommand) => Promise<WorkflowToolCommandResult>
>()

export function registerWorkflowToolCommandDispatcher(
  directory: string,
  dispatcher: (input: WorkflowToolCommand) => Promise<WorkflowToolCommandResult>,
) {
  workflowToolCommandDispatchers.set(directory, dispatcher)
  return () => {
    if (workflowToolCommandDispatchers.get(directory) === dispatcher) workflowToolCommandDispatchers.delete(directory)
  }
}

export function dispatchWorkflowToolCommand(directory: string, input: WorkflowToolCommand) {
  return workflowToolCommandDispatchers.get(directory)?.(input)
}

export const WorkflowMessageSendKind = Schema.Literals(["consultation", "intervention", "handoff"])
export type WorkflowMessageSendKind = typeof WorkflowMessageSendKind.Type

export const WorkflowMessageSendCommand = Schema.Struct({
  workflowID: WorkflowID,
  sourceSessionID: SessionID,
  kind: WorkflowMessageSendKind,
  targetSessionID: Schema.optional(SessionID),
  targetRole: Schema.optional(WorkflowRole),
  targetSpecialty: Schema.optional(Schema.String),
  timing: Schema.optional(WorkflowCommunicationTiming),
  milestoneID: Schema.optional(WorkflowMilestoneID),
  message: Schema.String,
  reason: Schema.optional(Schema.String),
  attachments: Schema.optional(Schema.Array(Schema.String)),
}).annotate({ identifier: "WorkflowMessageSendCommand" })
export type WorkflowMessageSendCommand = Omit<typeof WorkflowMessageSendCommand.Type, "sourceSessionID"> & {
  sourceSessionID: typeof SessionID.Type
}

export const WorkflowMessageSendResult = Schema.Struct({
  workflowID: WorkflowID,
  applied: Schema.Boolean,
  kind: WorkflowMessageSendKind,
  message: Schema.String,
  messageID: Schema.optional(Schema.String),
  status: Schema.optional(Schema.String),
  rejection: Schema.optional(WorkflowToolCommandRejection),
}).annotate({ identifier: "WorkflowMessageSendResult" })
export type WorkflowMessageSendResult = typeof WorkflowMessageSendResult.Type

const workflowMessageSendDispatchers = new Map<
  string,
  (input: WorkflowMessageSendCommand) => Promise<WorkflowMessageSendResult>
>()

export function registerWorkflowMessageSendDispatcher(
  directory: string,
  dispatcher: (input: WorkflowMessageSendCommand) => Promise<WorkflowMessageSendResult>,
) {
  workflowMessageSendDispatchers.set(directory, dispatcher)
  return () => {
    if (workflowMessageSendDispatchers.get(directory) === dispatcher) workflowMessageSendDispatchers.delete(directory)
  }
}

export function dispatchWorkflowMessageSend(directory: string, input: WorkflowMessageSendCommand) {
  return workflowMessageSendDispatchers.get(directory)?.(input)
}
