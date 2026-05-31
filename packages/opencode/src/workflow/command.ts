import { Schema } from "effect"

import { BusEvent } from "@/bus/bus-event"
import { SessionID } from "@/session/schema"

import {
  WorkflowID,
  WorkflowMilestoneID,
  WorkflowMilestoneStatus,
  WorkflowRole,
  type WorkflowStatus,
} from "./schema"

export const WorkflowToolAction = Schema.Literals([
  "status",
  "resume",
  "block",
  "update_xml",
  "milestone_status",
  "workflow_status",
  "complete",
])
export type WorkflowToolAction = typeof WorkflowToolAction.Type

export const WorkflowToolCommand = Schema.Struct({
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
  xml: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
}).annotate({ identifier: "WorkflowToolCommand" })
export type WorkflowToolCommand = Omit<typeof WorkflowToolCommand.Type, "sourceSessionID" | "workflowStatus"> & {
  sourceSessionID: typeof SessionID.Type
  workflowStatus?: WorkflowStatus
}

export const WorkflowToolCommandEvent = BusEvent.define("workflow.tool.command", WorkflowToolCommand)
