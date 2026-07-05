import { Effect, Schema } from "effect"
import { closeSync, existsSync, openSync, readSync, statSync } from "node:fs"
import path from "node:path"

import type { SessionID } from "@/session/schema"
import { Database, desc, eq, or } from "@/storage/db"
import { Bus } from "@/bus"
import { dispatchWorkflowToolCommand, WorkflowToolAction } from "@/workflow/command"
import { WorkflowMemberTable, WorkflowMilestoneTable, WorkflowTable } from "@/workflow/workflow.sql"
import { readyMilestones } from "@/workflow/scheduler"
import {
  WorkflowID,
  WorkflowMemberAvailability,
  WorkflowMilestoneID,
  WorkflowMilestoneStatus,
  WorkflowSchedulingMode,
  type WorkflowDefinition,
  type WorkflowMilestoneInfo,
  type WorkflowSessionRef,
  type WorkflowStaffingConfig,
} from "@/workflow/schema"

import * as Tool from "./tool"

export const Parameters = Schema.Struct({
  action: WorkflowToolAction.annotate({
    description:
      "Workflow command to run. Use status to inspect, status_update to report this agent session's availability/focus/blockers, scheduling to change eager/staged/economical dispatch mode, resume to continue scheduling, update_xml after changing workflow XML, milestone_status for legal milestone transitions, plan_complete for planning handoff, force_complete/force_skip for requester or main PM override, workflow_status for workflow-level recovery, block for real blockers, and complete only when all workflow work is finished.",
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
  availability: Schema.optional(
    WorkflowMemberAvailability.annotate({
      description: "Current availability for status_update: idle, working, or blocked_waiting.",
    }),
  ),
  currentFocus: Schema.optional(
    Schema.String.annotate({ description: "Short current focus for status_update, shown in workflow organization state." }),
  ),
  blockers: Schema.optional(
    Schema.Array(Schema.String).annotate({
      description: "Concrete blockers for status_update. Use an empty list when there are none.",
    }),
  ),
  progressNote: Schema.optional(
    Schema.String.annotate({ description: "Short progress note for status_update." }),
  ),
  schedulingMode: Schema.optional(
    WorkflowSchedulingMode.annotate({
      description: "Scheduling mode for action=scheduling: eager, staged, or economical.",
    }),
  ),
  schedulingMaxActive: Schema.optional(
    Schema.Int.check(Schema.isGreaterThanOrEqualTo(1), Schema.isLessThanOrEqualTo(64)).annotate({
      description: "Maximum active milestones when schedulingMode=economical.",
    }),
  ),
  xml: Schema.optional(Schema.String.annotate({ description: "Full workflow XML for update_xml." })),
  message: Schema.optional(
    Schema.String.annotate({ description: "Short reason, handoff note, blocker detail, or completion evidence." }),
  ),
})

type Metadata = {
  action: typeof WorkflowToolAction.Type
  queued: boolean
  confirmed?: boolean
  applied?: boolean
  workflowID?: WorkflowID
}

type WorkflowCommandJournalEntry = {
  seq?: number
  id?: string
  action?: string
  milestoneID?: string
  outcome?: string
  message?: string
  rejection?: {
    code?: string
    reason?: string
    allowedTransitions?: ReadonlyArray<string>
  }
  from?: {
    workflowStatus?: string
    milestoneStatus?: string
  }
  to?: {
    workflowStatus?: string
    milestoneStatus?: string
  }
}

export const WorkflowTool = Tool.define<typeof Parameters, Metadata, never>(
  "workflow",
  Effect.gen(function* () {
    return {
      description: [
        "Inspect and control the current opencode workflow.",
        "Use this instead of merely saying that dispatch, retry, resume, or status changes happened.",
        "Workflow-owned agents should call action=status at the start to learn their workflow, role, milestone, and nearby state.",
        "Use action=resume when planning or a blocked/stale workflow should continue scheduling.",
        "Use action=scheduling when requester or main PM needs to switch workflow dispatch mode without editing XML.",
        "Use action=update_xml only with the full canonical workflow XML.",
        "Use action=status_update at the start and end of workflow-owned turns so the manager can see whether this employee is idle, working, or blocked_waiting.",
        "Use action=milestone_status only for legal milestone state transitions. If it fails with illegal_transition, follow the allowed transitions or use force_complete/force_skip only when you own that decision.",
        "Use action=plan_complete when a department PM has finished planning and wants the runtime to move the milestone to the next executable stage.",
        "Use action=force_complete only when requester/main PM intentionally closes a gate despite the current state; it is not a routine progress update.",
        "Commands call the workflow runtime directly and return applied or rejected with a concrete reason. Do not claim a transition happened unless this tool reports applied=true.",
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
          const row = Database.use((db) => db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflowID)).get())
          const result = row
            ? yield* Effect.promise(() =>
                Promise.resolve(
                  dispatchWorkflowToolCommand(row.directory, {
                    id: Bus.createID(),
                    action: params.action,
                    workflowID,
                    sourceSessionID: ctx.sessionID,
                    sourceAgent: ctx.agent,
                    ...(params.milestoneID ? { milestoneID: params.milestoneID } : {}),
                    ...(params.milestoneStatus ? { milestoneStatus: params.milestoneStatus } : {}),
                    ...(params.workflowStatus ? { workflowStatus: params.workflowStatus } : {}),
                    ...(params.availability ? { availability: params.availability } : {}),
                    ...(params.currentFocus ? { currentFocus: params.currentFocus } : {}),
                    ...(params.blockers ? { blockers: params.blockers } : {}),
                    ...(params.progressNote ? { progressNote: params.progressNote } : {}),
                    ...(params.schedulingMode ? { schedulingMode: params.schedulingMode } : {}),
                    ...(params.schedulingMaxActive ? { schedulingMaxActive: params.schedulingMaxActive } : {}),
                    ...(params.xml ? { xml: params.xml } : {}),
                    ...(params.message ? { message: params.message } : {}),
                  }),
                ),
              )
            : undefined
          if (!result) {
            return {
              title: `Workflow ${params.action} unavailable`,
              output: [
                `Workflow runtime is not available for ${workflowID}. The command was not applied.`,
                "Open the workflow in the active project or restart the opencode server, then run workflow status before retrying.",
                "",
                summarizeWorkflow(workflowID),
              ].join("\n"),
              metadata: { action: params.action, queued: false, confirmed: false, applied: false, workflowID },
            }
          }

          return {
            title:
              params.action === "status"
                ? "Workflow status"
                : result.applied
                  ? `Workflow ${params.action} applied`
                  : `Workflow ${params.action} failed`,
            output: [
              params.action === "status"
                ? `Workflow status refreshed. ${result.message}`
                : [
                    `${result.applied ? "Applied" : "Failed to apply"} workflow command: ${params.action}. ${result.message}`,
                    result.rejection
                      ? `Rejection code: ${result.rejection.code}\nReason: ${result.rejection.reason}${result.rejection.allowedTransitions?.length ? `\nAllowed transitions: ${result.rejection.allowedTransitions.join(", ")}` : ""}`
                      : "",
                  ]
                    .filter(Boolean)
                    .join("\n"),
              "",
              summarizeWorkflow(workflowID),
            ].join("\n"),
            metadata: {
              action: params.action,
              queued: false,
              confirmed: true,
              applied: result.applied === true,
              workflowID,
            },
          }
        }),
    } satisfies Tool.DefWithoutID<typeof Parameters, Metadata>
  }),
)

function validateCommand(params: Schema.Schema.Type<typeof Parameters>) {
  if (params.action === "update_xml" && !params.xml?.trim()) return "update_xml requires xml."
  if (params.action === "milestone_status" && !params.milestoneID) return "milestone_status requires milestoneID."
  if (params.action === "milestone_status" && !params.milestoneStatus) return "milestone_status requires milestoneStatus."
  if (params.action === "plan_complete" && !params.milestoneID) return "plan_complete requires milestoneID."
  if (params.action === "force_complete" && !params.milestoneID) return "force_complete requires milestoneID."
  if (params.action === "force_skip" && !params.milestoneID) return "force_skip requires milestoneID."
  if (params.action === "status_update" && !params.availability) return "status_update requires availability."
  if (params.action === "scheduling" && !params.schedulingMode) return "scheduling requires schedulingMode."
  if (params.action === "scheduling" && params.schedulingMode === "economical" && !params.schedulingMaxActive)
    return "scheduling with economical mode requires schedulingMaxActive."
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
    ...workflowDispatchSummary(workflowID),
    ...workflowCommandJournalSummary(workflowID),
  ].join("\n")
}

export function workflowDispatchSummary(workflowID: WorkflowID | undefined) {
  if (!workflowID) return []
  if (!workflowTableReady("workflow") || !workflowTableReady("workflow_milestone")) return []
  const workflow = Database.use((db) => db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflowID)).get())
  if (!workflow) return []
  const rows = Database.use((db) =>
    db
      .select()
      .from(WorkflowMilestoneTable)
      .where(eq(WorkflowMilestoneTable.workflow_id, workflowID))
      .orderBy(WorkflowMilestoneTable.time_created)
      .all(),
  )
  const definition: WorkflowDefinition = {
    steps: { type: "parallel", children: [] },
    milestones: rows.map((row) => ({
      type: "milestone" as const,
      id: row.id,
      ...(row.title ? { title: row.title } : {}),
      ...(row.department ? { department: row.department } : {}),
      prompt: row.prompt,
      dependsOn: row.depends_on,
    })),
  }
  const ready = readyMilestones(
    definition,
    rows.map((row) => ({ id: row.id, status: row.status })),
  )
  const active = rows.filter((row) => ["planning", "executing", "reviewing", "testing"].includes(row.status))
  const departmentPMCapacity = staffLimitForWorkflowRole(workflow.staffing, "department_pm") - activeRoleCount(rows, "department_pm")
  const activeLimit = workflow.scheduling?.mode === "economical" ? workflow.scheduling.maxActive ?? 2 : undefined
  const schedulingCapacity = activeLimit === undefined ? ready.length : Math.max(0, activeLimit - active.length)
  const availableReady = Math.max(0, Math.min(departmentPMCapacity, schedulingCapacity))
  const capacityWaiting = ready.slice(availableReady)
  const capacityWaitingReason = departmentPMCapacity <= schedulingCapacity ? "staffing: department_pm capacity" : "scheduling: active limit"
  const explicitWaiting = rows.filter((row) => row.status === "pending" && row.waiting_for)
  const waiting = [
    ...explicitWaiting.map((row) => `${row.id} (${row.waiting_for})`),
    ...capacityWaiting
      .filter((milestone) => !explicitWaiting.some((row) => row.id === milestone.id))
      .map((milestone) => `${milestone.id} (${capacityWaitingReason})`),
  ]
  return [
    "dispatch:",
    `- scheduling: ${workflow.scheduling?.mode ?? "eager"}${workflow.scheduling?.mode === "economical" ? ` maxActive=${workflow.scheduling.maxActive ?? 2}` : ""}`,
    `- ready: ${ready.length ? ready.map((milestone) => milestone.id).join(", ") : "none"}`,
    `- waiting: ${waiting.length ? waiting.join(", ") : "none"}`,
    `- active: ${active.length ? active.map((row) => `${row.id}:${row.status}`).join(", ") : "none"}`,
    `- staffing: ${workflowStaffingLines(rows, workflow.staffing).join("; ")}`,
  ]
}

function workflowStaffingLines(
  rows: (typeof WorkflowMilestoneTable.$inferSelect)[],
  staffing: WorkflowStaffingConfig | null | undefined,
) {
  return (["department_pm", "expert", "executor", "reviewer", "tester"] as const).map(
    (role) => `${role} ${activeRoleCount(rows, role)}/${staffLimitForWorkflowRole(staffing, role)}`,
  )
}

function activeRoleCount(rows: (typeof WorkflowMilestoneTable.$inferSelect)[], role: WorkflowSessionRef["role"]) {
  return new Set(
    rows
      .filter((row) => roleBusyForWorkflowStatus(role, row.status))
      .flatMap((row) => row.session.filter((ref) => ref.role === role).map((ref) => ref.sessionID)),
  ).size
}

function roleBusyForWorkflowStatus(role: WorkflowSessionRef["role"], status: WorkflowMilestoneInfo["status"]) {
  if (role === "department_pm") return status === "planning" || status === "reviewing"
  if (role === "expert") return status === "planning"
  if (role === "executor") return status === "executing" || status === "running"
  if (role === "reviewer") return status === "reviewing"
  if (role === "tester") return status === "testing"
  return false
}

function staffLimitForWorkflowRole(staffing: WorkflowStaffingConfig | null | undefined, role: WorkflowSessionRef["role"]) {
  const config = normalizeWorkflowStaffing(staffing)
  if (role === "main_pm") return config.mainPM
  if (role === "department_pm") return config.departmentPM
  if (role === "executor") return config.executor
  if (role === "reviewer") return config.reviewer
  if (role === "tester") return config.tester
  if (role === "expert") return config.expert
  return 1
}

function normalizeWorkflowStaffing(input: WorkflowStaffingConfig | null | undefined) {
  return {
    mainPM: workflowStaffLimit(input?.mainPM, 1, 1),
    departmentPM: workflowStaffLimit(input?.departmentPM, 2, 1),
    executor: workflowStaffLimit(input?.executor, 4, 1),
    reviewer: workflowStaffLimit(input?.reviewer, 2, 1),
    tester: workflowStaffLimit(input?.tester, 1, 1),
    expert: workflowStaffLimit(input?.expert, 1, 1),
  }
}

function workflowStaffLimit(value: number | undefined, fallback: number, minimum: number) {
  if (value === undefined || !Number.isFinite(value)) return fallback
  return Math.max(minimum, Math.min(64, Math.trunc(value)))
}

export function workflowCommandJournalSummary(workflowID: WorkflowID | undefined, limit = 5) {
  if (!workflowID) return []
  if (!workflowTableReady("workflow")) return []
  const workflow = Database.use((db) => db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflowID)).get())
  if (!workflow) return []
  const file = path.join(workflow.directory, workflow.path, "journal", "commands.jsonl")
  if (!existsSync(file)) return []
  const rows = readWorkflowCommandJournalTail(file)
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
    .slice(-limit)
    .map(parseWorkflowCommandJournalEntry)
    .filter((row): row is WorkflowCommandJournalEntry => !!row)
  if (rows.length === 0) return []
  return ["recent commands:", ...rows.map(commandJournalLine)]
}

function readWorkflowCommandJournalTail(file: string) {
  const size = statSync(file).size
  if (size === 0) return ""
  const length = Math.min(size, 64 * 1024)
  const buffer = Buffer.allocUnsafe(length)
  const fd = openSync(file, "r")
  try {
    readSync(fd, buffer, 0, length, size - length)
  } finally {
    closeSync(fd)
  }
  return buffer.toString("utf8")
}

function parseWorkflowCommandJournalEntry(line: string) {
  try {
    const value: unknown = JSON.parse(line)
    if (!value || typeof value !== "object") return
    return value as WorkflowCommandJournalEntry
  } catch {
    return
  }
}

function commandJournalLine(row: WorkflowCommandJournalEntry) {
  const name = [row.action ?? "unknown", row.milestoneID].filter(Boolean).join(" ")
  const state = [stateTransition("workflow", row.from?.workflowStatus, row.to?.workflowStatus), stateTransition("milestone", row.from?.milestoneStatus, row.to?.milestoneStatus)]
    .filter(Boolean)
    .join("; ")
  const rejection = row.rejection?.code ? ` ${row.rejection.code}` : ""
  const allowed = row.rejection?.allowedTransitions?.length
    ? ` allowed: ${row.rejection.allowedTransitions.join(", ")}`
    : ""
  const detail = truncateJournalMessage(row.rejection?.reason ?? row.message ?? allowed, 180)
  return `- ${row.seq ? `#${row.seq}` : row.id?.slice(0, 8) ?? "command"} ${name}: ${row.outcome ?? "unknown"}${rejection}${state ? ` (${state})` : ""}${detail ? ` - ${detail}` : ""}`
}

function stateTransition(label: string, from: string | undefined, to: string | undefined) {
  if (!from && !to) return
  if (from === to) return `${label} ${from}`
  return `${label} ${from ?? "?"}->${to ?? "?"}`
}

function truncateJournalMessage(text: string, max: number) {
  if (!text) return ""
  const normalized = text.replace(/\s+/g, " ").trim()
  if (normalized.length <= max) return normalized
  return `${normalized.slice(0, max - 1)}...`
}

function workflowTableReady(table: string) {
  return !!Database.Client().$client.prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?").get(table)
}

function milestoneLine(row: typeof WorkflowMilestoneTable.$inferSelect) {
  return `- ${row.id}: ${row.status}${row.waiting_for ? ` waitingFor=${row.waiting_for}` : ""}${row.title ? ` - ${row.title}` : ""}${sessionSummary(row.session)}`
}

function sessionSummary(session: ReadonlyArray<WorkflowMilestoneInfo["session"][number]>) {
  if (session.length === 0) return ""
  const latest = session.toSorted((a, b) => (b.attempt ?? 0) - (a.attempt ?? 0))[0]
  if (!latest) return ""
  return ` (${latest.role} ${latest.sessionID})`
}
