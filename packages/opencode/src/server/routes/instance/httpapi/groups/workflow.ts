import { Workflow } from "@/workflow/workflow"
import { WorkflowGraph, WorkflowID, WorkflowInfo } from "@/workflow/schema"
import { Schema, Struct } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import { ApiNotFoundError, InvalidRequestError } from "../errors"
import { Authorization } from "../middleware/authorization"
import { InstanceContextMiddleware } from "../middleware/instance-context"
import {
  WorkspaceRoutingMiddleware,
  WorkspaceRoutingQuery,
  WorkspaceRoutingQueryFields,
} from "../middleware/workspace-routing"
import { described } from "./metadata"

const root = "/workflow"

export const ListQuery = Schema.Struct({
  ...WorkspaceRoutingQueryFields,
  sessionID: Schema.optional(Workflow.ListInput.fields.sessionID),
})
export const StartPayload = Workflow.StartInput
export const UpdateXmlPayload = Schema.Struct(Struct.omit(Workflow.UpdateXmlInput.fields, ["workflowID"]))

export const WorkflowPaths = {
  list: root,
  start: root,
  get: `${root}/:workflowID`,
  graph: `${root}/:workflowID/graph`,
  updateXml: `${root}/:workflowID/xml`,
  resume: `${root}/:workflowID/resume`,
  cancel: `${root}/:workflowID/cancel`,
} as const

export const WorkflowApi = HttpApi.make("workflow")
  .add(
    HttpApiGroup.make("workflow")
      .add(
        HttpApiEndpoint.get("list", WorkflowPaths.list, {
          query: ListQuery,
          success: described(Schema.Array(WorkflowInfo), "List workflows"),
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "workflow.list",
            summary: "List workflows",
            description: "List automated agent workflows in the current project.",
          }),
        ),
        HttpApiEndpoint.post("start", WorkflowPaths.start, {
          query: WorkspaceRoutingQuery,
          payload: [HttpApiSchema.NoContent, StartPayload],
          success: described(WorkflowInfo, "Workflow started"),
          error: [HttpApiError.BadRequest, InvalidRequestError],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "workflow.start",
            summary: "Start workflow",
            description: "Start an automated product-manager, executor, reviewer, and tester workflow.",
          }),
        ),
        HttpApiEndpoint.get("get", WorkflowPaths.get, {
          params: { workflowID: WorkflowID },
          query: WorkspaceRoutingQuery,
          success: described(WorkflowInfo, "Get workflow"),
          error: [HttpApiError.BadRequest, InvalidRequestError, ApiNotFoundError],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "workflow.get",
            summary: "Get workflow",
            description: "Get workflow status and metadata.",
          }),
        ),
        HttpApiEndpoint.get("graph", WorkflowPaths.graph, {
          params: { workflowID: WorkflowID },
          query: WorkspaceRoutingQuery,
          success: described(WorkflowGraph, "Get workflow graph"),
          error: [HttpApiError.BadRequest, InvalidRequestError, ApiNotFoundError],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "workflow.graph",
            summary: "Get workflow graph",
            description: "Get graph nodes and edges for a workflow.",
          }),
        ),
        HttpApiEndpoint.patch("updateXml", WorkflowPaths.updateXml, {
          params: { workflowID: WorkflowID },
          query: WorkspaceRoutingQuery,
          payload: UpdateXmlPayload,
          success: described(WorkflowGraph, "Workflow graph updated"),
          error: [HttpApiError.BadRequest, InvalidRequestError, ApiNotFoundError],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "workflow.updateXml",
            summary: "Update workflow XML",
            description: "Replace the canonical workflow XML and rebuild the dependency graph.",
          }),
        ),
        HttpApiEndpoint.post("resume", WorkflowPaths.resume, {
          params: { workflowID: WorkflowID },
          query: WorkspaceRoutingQuery,
          success: described(WorkflowInfo, "Workflow resumed"),
          error: [HttpApiError.BadRequest, InvalidRequestError, ApiNotFoundError],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "workflow.resume",
            summary: "Resume workflow",
            description: "Resume a blocked or paused workflow.",
          }),
        ),
        HttpApiEndpoint.post("cancel", WorkflowPaths.cancel, {
          params: { workflowID: WorkflowID },
          query: WorkspaceRoutingQuery,
          success: described(WorkflowInfo, "Workflow cancelled"),
          error: [HttpApiError.BadRequest, InvalidRequestError, ApiNotFoundError],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "workflow.cancel",
            summary: "Cancel workflow",
            description: "Cancel an automated workflow.",
          }),
        ),
      )
      .annotateMerge(OpenApi.annotations({ title: "workflow", description: "Automated agent workflow routes." }))
      .middleware(InstanceContextMiddleware)
      .middleware(WorkspaceRoutingMiddleware)
      .middleware(Authorization),
  )
