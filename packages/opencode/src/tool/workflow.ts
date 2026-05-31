import { Effect, Schema } from "effect"

import { Bus } from "@/bus"
import type { SessionID } from "@/session/schema"
import { Database, desc, eq, or } from "@/storage/db"
import { WorkflowToolAction, WorkflowToolCommandEvent } from "@/workflow/command"
import type { WorkflowToolAction as WorkflowToolActionType } from "@/workflow/command"
import { WorkflowMemberTable, WorkflowMilestoneTable, WorkflowTable } from "@/workflow/workflow.sql"
import {
  WorkflowID,
  WorkflowMilestoneID,
  WorkflowMilestoneStatus,
  type WorkflowMilestoneInfo,
} from "@/workflow/schema"

import * as Tool from "./tool"

export const Parameters = Schema.Struct({
  action: WorkflowToolAction.annotate({
    description:
      "Workflow command to run. Use status to inspect, resume to continue scheduling, update_xml after changing workflow XML, milestone_status to request a milestone state change, workflow_status for workflow-level recovery, block for real blockers, and complete only when all workflow work is finished.",
  }),
  workflowID: Schema.optional(
    WorkflowID.annotate({
      description: "Workflow id. Omit inside a workflow-owned session; the current workflow is inferred.",
    }),
  ),
  milestoneID: Schema.optional(WorkflowMilestoneID.annotate({ description: "Milestone id for milestone_status." })),
  milestoneStatus: Schema.optional(
    WorkflowMilestoneStatus.annotate({ description: "New milestone status for milestone_status." }),
  ),
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
    ]).annotate({ description: "New workflow status for workflow_status." }),
  ),
  xml: Schema.optional(Schema.String.annotate({ description: "Full workflow XML for update_xml." })),
  message: Schema.optional(
    Schema.String.annotate({ description: "Short reason, handoff note, blocker detail, or completion evidence." }),
  ),
})

type Metadata = {
  action: WorkflowToolActionType
  queued: boolean
  workflowID?: WorkflowID
}

export const WorkflowTool = Tool.define<typeof Parameters, Metadata, Bus.Service>(
  "workflow",
  Effect.gen(function* () {
    const bus = yield* Bus.Service

    return {
      description: [
        "Inspect and control the current opencode workflow.",
        "Use this instead of merely saying that dispatch, retry, resume, or status changes happened.",
        "Workflow-owned agents should call action=status at the start to learn their workflow, role, milestone, and nearby state.",
        "Use action=resume when planning or a blocked/stale workflow should continue scheduling.",
        "Use action=update_xml only with the full canonical workflow XML.",
        "Use action=milestone_status only when you intentionally need the workflow manager to reopen, block, approve, or retry a specific milestone.",
        "State-changing actions are queued to the workflow runtime, which then updates the graph and schedules follow-up sessions.",
      ].join("\n"),
      parameters: Parameters,
      execute: (params, ctx) =>
        Effect.gen(function* () {
          const workflowID = resolveWorkflowID(ctx.sessionID, params.workflowID)
          yield* ctx.ask({
            permission: "workflow",
            patterns: [workflowID ?? "*", params.action],
            always: ["*"],
            metadata: {
              action: params.action,
              ...(workflowID ? { workflowID } : {}),
            },
          })

          const invalid = validateCommand(params)
          if (invalid) {
            return {
              title: "Workflow command needs more input",
              output: [invalid, "", summarizeWorkflow(workflowID)].filter(Boolean).join("\n"),
              metadata: { action: params.action, queued: false, ...(workflowID ? { workflowID } : {}) },
            }
          }
          if (!workflowID) {
            return {
              title: "No workflow found",
              output: "No workflow is associated with this session. Pass workflowID explicitly or open a workflow-owned session.",
              metadata: { action: params.action, queued: false },
            }
          }
          if (params.action !== "status") {
            yield* bus.publish(WorkflowToolCommandEvent, {
              action: params.action,
              workflowID,
              sourceSessionID: ctx.sessionID,
              sourceAgent: ctx.agent,
              ...(params.milestoneID ? { milestoneID: params.milestoneID } : {}),
              ...(params.milestoneStatus ? { milestoneStatus: params.milestoneStatus } : {}),
              ...(params.workflowStatus ? { workflowStatus: params.workflowStatus } : {}),
              ...(params.xml ? { xml: params.xml } : {}),
              ...(params.message ? { message: params.message } : {}),
            })
          }

          return {
            title: params.action === "status" ? "Workflow status" : `Workflow ${params.action} queued`,
            output: [
              params.action === "status"
                ? "Current workflow state:"
                : `Queued workflow command: ${params.action}. The workflow runtime will apply it and schedule follow-up work.`,
              "",
              summarizeWorkflow(workflowID),
            ].join("\n"),
            metadata: { action: params.action, queued: params.action !== "status", workflowID },
          }
        }),
    } satisfies Tool.DefWithoutID<typeof Parameters, Metadata>
  }),
)

function validateCommand(params: Schema.Schema.Type<typeof Parameters>) {
  if (params.action === "update_xml" && !params.xml?.trim()) return "update_xml requires xml."
  if (params.action === "milestone_status" && !params.milestoneID) return "milestone_status requires milestoneID."
  if (params.action === "milestone_status" && !params.milestoneStatus) return "milestone_status requires milestoneStatus."
  if (params.action === "workflow_status" && !params.workflowStatus) return "workflow_status requires workflowStatus."
  if (params.action === "block" && !params.message?.trim()) return "block requires message."
}

function resolveWorkflowID(sessionID: SessionID, explicit?: WorkflowID) {
  if (explicit) return explicit
  if (!workflowTableReady("workflow")) return
  if (workflowTableReady("workflow_member")) {
    const member = Database.use((db) =>
      db
        .select()
        .from(WorkflowMemberTable)
        .where(eq(WorkflowMemberTable.session_id, sessionID))
        .orderBy(desc(WorkflowMemberTable.time_updated))
        .get(),
    )
    if (member) return member.workflow_id
  }
  return Database.use((db) =>
    db
      .select()
      .from(WorkflowTable)
      .where(
        or(
          eq(WorkflowTable.root_session_id, sessionID),
          eq(WorkflowTable.pm_session_id, sessionID),
          eq(WorkflowTable.tester_session_id, sessionID),
        ),
      )
      .orderBy(desc(WorkflowTable.time_updated))
      .get(),
  )?.id
}

function summarizeWorkflow(workflowID: WorkflowID | undefined) {
  if (!workflowID) return ""
  if (!workflowTableReady("workflow")) return "Workflow storage has not been initialized yet."
  const workflow = Database.use((db) => db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflowID)).get())
  if (!workflow) return `Workflow not found: ${workflowID}`
  const rows = workflowTableReady("workflow_milestone")
    ? Database.use((db) =>
        db
          .select()
          .from(WorkflowMilestoneTable)
          .where(eq(WorkflowMilestoneTable.workflow_id, workflowID))
          .orderBy(WorkflowMilestoneTable.time_created)
          .all(),
      )
    : []
  return [
    `workflow: ${workflow.id}`,
    `title: ${workflow.title}`,
    `status: ${workflow.status}${workflow.error ? ` (${workflow.error})` : ""}`,
    "milestones:",
    ...rows.map((row) => milestoneLine(row)),
  ].join("\n")
}

function workflowTableReady(table: string) {
  return !!Database.Client().$client.prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?").get(table)
}

function milestoneLine(row: typeof WorkflowMilestoneTable.$inferSelect) {
  return `- ${row.id}: ${row.status}${row.title ? ` - ${row.title}` : ""}${sessionSummary(row.session)}`
}

function sessionSummary(session: ReadonlyArray<WorkflowMilestoneInfo["session"][number]>) {
  if (session.length === 0) return ""
  const latest = session.toSorted((a, b) => (b.attempt ?? 0) - (a.attempt ?? 0))[0]
  if (!latest) return ""
  return ` (${latest.role} ${latest.sessionID})`
}
