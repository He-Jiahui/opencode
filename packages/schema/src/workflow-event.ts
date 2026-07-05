export * as WorkflowEvent from "./workflow-event"

import { Schema } from "effect"
import { Event } from "./event"
import { optional } from "./schema"

const WorkflowRecord = Schema.Record(Schema.String, Schema.Unknown)

export const Created = Event.define({
  type: "workflow.created",
  schema: {
    info: WorkflowRecord,
  },
})

export const Updated = Event.define({
  type: "workflow.updated",
  schema: {
    info: WorkflowRecord,
  },
})

export const NodeUpdated = Event.define({
  type: "workflow.node.updated",
  schema: {
    workflowID: Schema.String,
    milestone: WorkflowRecord,
  },
})

export const GraphUpdated = Event.define({
  type: "workflow.graph.updated",
  schema: {
    workflowID: Schema.String,
    graph: optional(WorkflowRecord),
  },
})

export const Definitions = Event.inventory(Created, Updated, NodeUpdated, GraphUpdated)
