import { Workflow } from "@/workflow/workflow"
import { WorkflowID } from "@/workflow/schema"
import { Effect } from "effect"
import { HttpApiBuilder, HttpApiError } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import { InvalidRequestError, notFound } from "../errors"
import { IntervenePayload, ListQuery, UpdateStaffingPayload, UpdateXmlPayload } from "../groups/workflow"

const mapWorkflowError = (error: Workflow.Error) =>
  error.message.toLowerCase().includes("not found")
    ? notFound(error.message)
    : new InvalidRequestError({ message: error.message })

const mapWorkflowStartError = (error: Workflow.Error) =>
  new InvalidRequestError({ message: error.message })

export const workflowHandlers = HttpApiBuilder.group(InstanceHttpApi, "workflow", (handlers) =>
  Effect.gen(function* () {
    const workflow = yield* Workflow.Service

    const list = Effect.fn("WorkflowHttpApi.list")(function* (ctx: { query: typeof ListQuery.Type }) {
      return yield* workflow.list(ctx.query)
    })

    const start = Effect.fn("WorkflowHttpApi.start")(function* (ctx: { payload: void | Workflow.StartInput }) {
      if (!ctx.payload) return yield* new HttpApiError.BadRequest({})
      return yield* workflow.start(ctx.payload).pipe(Effect.mapError(mapWorkflowStartError))
    })

    const get = Effect.fn("WorkflowHttpApi.get")(function* (ctx: { params: { workflowID: WorkflowID } }) {
      return yield* workflow.get(ctx.params.workflowID).pipe(Effect.mapError(mapWorkflowError))
    })

    const graph = Effect.fn("WorkflowHttpApi.graph")(function* (ctx: { params: { workflowID: WorkflowID } }) {
      return yield* workflow.graph(ctx.params.workflowID).pipe(Effect.mapError(mapWorkflowError))
    })

    const updateXml = Effect.fn("WorkflowHttpApi.updateXml")(function* (ctx: {
      params: { workflowID: WorkflowID }
      payload: typeof UpdateXmlPayload.Type
    }) {
      return yield* workflow
        .updateXml({ workflowID: ctx.params.workflowID, xml: ctx.payload.xml })
        .pipe(Effect.mapError(mapWorkflowError))
    })

    const updateStaffing = Effect.fn("WorkflowHttpApi.updateStaffing")(function* (ctx: {
      params: { workflowID: WorkflowID }
      payload: typeof UpdateStaffingPayload.Type
    }) {
      return yield* workflow
        .updateStaffing({
          workflowID: ctx.params.workflowID,
          staffing: ctx.payload.staffing,
          modelWhitelist: ctx.payload.modelWhitelist,
        })
        .pipe(Effect.mapError(mapWorkflowError))
    })

    const intervene = Effect.fn("WorkflowHttpApi.intervene")(function* (ctx: {
      params: { workflowID: WorkflowID }
      payload: typeof IntervenePayload.Type
    }) {
      return yield* workflow
        .intervene({
          workflowID: ctx.params.workflowID,
          message: ctx.payload.message,
          timing: ctx.payload.timing,
          targetRole: ctx.payload.targetRole,
          targetSessionID: ctx.payload.targetSessionID,
        })
        .pipe(Effect.mapError(mapWorkflowError))
    })

    const resume = Effect.fn("WorkflowHttpApi.resume")(function* (ctx: { params: { workflowID: WorkflowID } }) {
      return yield* workflow.resume(ctx.params.workflowID).pipe(Effect.mapError(mapWorkflowError))
    })

    const cancel = Effect.fn("WorkflowHttpApi.cancel")(function* (ctx: { params: { workflowID: WorkflowID } }) {
      return yield* workflow.cancel(ctx.params.workflowID).pipe(Effect.mapError(mapWorkflowError))
    })

    return handlers
      .handle("list", list)
      .handle("start", start)
      .handle("get", get)
      .handle("graph", graph)
      .handle("updateXml", updateXml)
      .handle("updateStaffing", updateStaffing)
      .handle("intervene", intervene)
      .handle("resume", resume)
      .handle("cancel", cancel)
  }),
)
