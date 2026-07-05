import { Effect, Schema } from "effect"
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "fs"
import path from "path"

import { Database, and, desc, eq, or, sql } from "@/storage/db"
import { SessionID } from "@/session/schema"
import { WorkflowCommunicationTiming, WorkflowID, WorkflowMilestoneID, WorkflowRole } from "@/workflow/schema"
import { dispatchWorkflowMessageSend } from "@/workflow/command"
import {
  WorkflowConsultationTable,
  WorkflowInterventionTable,
  WorkflowMessageTable,
  WorkflowMemberTable,
  WorkflowTable,
} from "@/workflow/workflow.sql"
import { Tool } from "./tool"

const WorkflowMessageAction = Schema.Literals(["inbox", "send", "ack", "answer"])
const WorkflowMessageSendKind = Schema.Literals(["consultation", "intervention", "handoff"])
const workflowMessageKinds = ["consultation", "intervention", "handoff", "standup", "report"] as const

type WorkflowMessageKind = (typeof workflowMessageKinds)[number]

const Parameters = Schema.Struct({
  action: WorkflowMessageAction,
  workflowID: Schema.optional(WorkflowID),
  kind: Schema.optional(WorkflowMessageSendKind),
  targetSessionID: Schema.optional(SessionID),
  targetRole: Schema.optional(WorkflowRole),
  targetSpecialty: Schema.optional(Schema.String),
  timing: Schema.optional(WorkflowCommunicationTiming),
  milestoneID: Schema.optional(WorkflowMilestoneID),
  messageID: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
  reason: Schema.optional(Schema.String),
  attachments: Schema.optional(Schema.Array(Schema.String)),
  answer: Schema.optional(Schema.String),
})

type Metadata = {
  action: typeof WorkflowMessageAction.Type
  workflowID?: WorkflowID
  messageID?: string
  kind?: WorkflowMessageKind
  status?: string
  updated?: boolean
}

export const WorkflowMessageTool = Tool.define<typeof Parameters, Metadata, never>(
  "workflow_message",
  Effect.succeed({
    description: [
      "Inspect and close workflow collaboration messages for the current workflow session.",
      "Use action=inbox to see pending consultations and interventions assigned to this session.",
      "Use action=send with kind=consultation, kind=intervention, or kind=handoff plus targetSessionID or targetRole to create a tracked workflow collaboration message.",
      "Use kind=handoff with non-empty attachments containing workflow-relative artifact paths when transferring completed context to another role.",
      "workflow_message is consultation/notification only; it cannot dispatch milestone work, attach sessions to milestones, or unblock ordered/parallel dependencies.",
      "Use action=answer with messageID and answer to answer a consultation.",
      "Use action=ack with messageID to acknowledge an intervention, or action=answer to acknowledge it with a response.",
    ].join(" "),
    parameters: Parameters,
    execute: (args: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context<Metadata>) =>
      Effect.gen(function* () {
        const workflow = resolveWorkflow(ctx.sessionID, args.workflowID)
        if (!workflow) {
          return {
            title: "No workflow message inbox",
            metadata: { action: args.action, updated: false },
            output:
              "No workflow could be resolved for this session. Pass workflowID explicitly or open a workflow-owned session.",
          }
        }
        if (args.action === "inbox") return yield* inbox(workflow, ctx.sessionID)
        if (args.action === "send") return yield* send(workflow, ctx.sessionID, args)
        if (!args.messageID?.trim()) {
          return {
            title: "Missing workflow message id",
            metadata: { action: args.action, workflowID: workflow.id, updated: false },
            output: "messageID is required for ack and answer.",
          }
        }
        if (args.action === "ack") return yield* ack(workflow, ctx.sessionID, args.messageID.trim())
        return yield* answer(workflow, ctx.sessionID, args.messageID.trim(), args.answer?.trim())
      }),
  }),
)

type WorkflowRow = typeof WorkflowTable.$inferSelect

function resolveWorkflow(sessionID: SessionID, workflowID?: WorkflowID) {
  return Database.use((db) => {
    if (workflowID) return db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflowID)).get()
    return (
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
        .get() ??
      db
        .select({ workflow: WorkflowTable })
        .from(WorkflowMemberTable)
        .innerJoin(WorkflowTable, eq(WorkflowTable.id, WorkflowMemberTable.workflow_id))
        .where(eq(WorkflowMemberTable.session_id, sessionID))
        .orderBy(desc(WorkflowTable.time_updated))
        .get()?.workflow ??
      db
        .select({ workflow: WorkflowTable })
        .from(WorkflowMessageTable)
        .innerJoin(WorkflowTable, eq(WorkflowTable.id, WorkflowMessageTable.workflow_id))
        .where(
          or(
            eq(WorkflowMessageTable.from_session_id, sessionID),
            eq(WorkflowMessageTable.to_session_id, sessionID),
          ),
        )
        .orderBy(desc(WorkflowMessageTable.time_updated))
        .get()?.workflow ??
      db
        .select({ workflow: WorkflowTable })
        .from(WorkflowConsultationTable)
        .innerJoin(WorkflowTable, eq(WorkflowTable.id, WorkflowConsultationTable.workflow_id))
        .where(
          or(
            eq(WorkflowConsultationTable.from_session_id, sessionID),
            eq(WorkflowConsultationTable.to_session_id, sessionID),
          ),
        )
        .orderBy(desc(WorkflowConsultationTable.time_updated))
        .get()?.workflow ??
      db
        .select({ workflow: WorkflowTable })
        .from(WorkflowInterventionTable)
        .innerJoin(WorkflowTable, eq(WorkflowTable.id, WorkflowInterventionTable.workflow_id))
        .where(
          or(
            eq(WorkflowInterventionTable.from_session_id, sessionID),
            eq(WorkflowInterventionTable.target_session_id, sessionID),
          ),
        )
        .orderBy(desc(WorkflowInterventionTable.time_updated))
        .get()?.workflow
    )
  })
}

function inbox(workflow: WorkflowRow, sessionID: SessionID): Effect.Effect<Tool.ExecuteResult<Metadata>> {
  return Effect.sync(() => {
    const items = workflowInbox(workflow.id, sessionID)
    return {
      title: items.length ? `${items.length} workflow message(s)` : "Workflow inbox empty",
      metadata: { action: "inbox", workflowID: workflow.id },
      output: [
        `Workflow: ${workflow.id}`,
        `Session: ${sessionID}`,
        "",
        ...(items.length
          ? items.flatMap((item) => [
              `- ${item.kind} ${item.id} [${item.status}]`,
              `  from: ${item.from}`,
              `  message: ${item.message}`,
            ])
          : ["No pending workflow collaboration messages are assigned to this session."]),
      ].join("\n"),
    }
  })
}

function workflowInbox(workflowID: WorkflowID, sessionID: SessionID) {
  return Database.use((db) => {
    const messages = db
      .select()
      .from(WorkflowMessageTable)
      .where(
        and(
          eq(WorkflowMessageTable.workflow_id, workflowID),
          eq(WorkflowMessageTable.to_session_id, sessionID),
          or(
            eq(WorkflowMessageTable.status, "pending"),
            eq(WorkflowMessageTable.status, "queued"),
            eq(WorkflowMessageTable.status, "delivered"),
            eq(WorkflowMessageTable.status, "blocked"),
          ),
        ),
      )
      .orderBy(desc(WorkflowMessageTable.time_updated))
      .all()
    const messageIDs = new Set(messages.map((item) => item.id))
    return [
      ...messages.flatMap((item) => {
        const kind = workflowMessageKind(item.kind)
        if (!kind) return []
        return [
          {
            kind,
            id: item.id,
            status: item.status,
            from: workflowMessageFrom(item),
            message: item.body,
          },
        ]
      }),
      ...db
        .select()
        .from(WorkflowConsultationTable)
        .where(
          and(
            eq(WorkflowConsultationTable.workflow_id, workflowID),
            eq(WorkflowConsultationTable.to_session_id, sessionID),
            eq(WorkflowConsultationTable.status, "pending"),
          ),
        )
        .all()
        .filter((item) => !messageIDs.has(item.id))
        .map((item) => ({
          kind: "consultation" as const,
          id: item.id,
          status: item.status,
          from: `${item.from_role} ${item.from_session_id}`,
          message: item.question,
        })),
      ...db
        .select()
        .from(WorkflowInterventionTable)
        .where(
          and(
            eq(WorkflowInterventionTable.workflow_id, workflowID),
            eq(WorkflowInterventionTable.target_session_id, sessionID),
            or(
              eq(WorkflowInterventionTable.status, "queued"),
              eq(WorkflowInterventionTable.status, "delivered"),
              eq(WorkflowInterventionTable.status, "blocked"),
            ),
          ),
        )
        .all()
        .filter((item) => !messageIDs.has(item.id))
        .map((item) => ({
          kind: "intervention" as const,
          id: item.id,
          status: item.status,
          from: item.from_session_id ? `requester ${item.from_session_id}` : "requester",
          message: item.message,
        })),
    ]
  })
}

function workflowMessageFrom(item: typeof WorkflowMessageTable.$inferSelect) {
  const role = workflowRole(item.from_role) ?? "workflow"
  return item.from_session_id ? `${role} ${item.from_session_id}` : role
}

function send(workflow: WorkflowRow, sessionID: SessionID, args: Schema.Schema.Type<typeof Parameters>) {
  return Effect.gen(function* () {
    if (!args.kind) {
      return {
        title: "Workflow message kind required",
        metadata: { action: "send" as const, workflowID: workflow.id, updated: false },
        output: "send requires kind=consultation, kind=intervention, or kind=handoff.",
      }
    }
    if (!args.message?.trim()) {
      return {
        title: "Workflow message body required",
        metadata: { action: "send" as const, workflowID: workflow.id, kind: args.kind, updated: false },
        output: "send requires message.",
      }
    }
    const sourceRole = workflowRoleForSession(workflow, sessionID)
    if (!sourceRole) {
      return {
        title: "Workflow sender is not registered",
        metadata: { action: "send" as const, workflowID: workflow.id, kind: args.kind, updated: false },
        output: `Session ${sessionID} is not a member of workflow ${workflow.id}. Open a workflow-owned session before sending workflow messages.`,
      }
    }
    const target = resolveWorkflowMessageTarget(workflow, args.targetSessionID, args.targetRole, args.targetSpecialty)
    if (!target && (args.kind === "consultation" || !args.targetRole)) {
      return {
        title: "Workflow message target unavailable",
        metadata: { action: "send" as const, workflowID: workflow.id, kind: args.kind, updated: false },
        output: "send requires a valid targetSessionID or targetRole with an available workflow session.",
      }
    }
    const targetRole = target?.role ?? args.targetRole
    if (targetRole && workflowMessageSendDispatchMisuse(sourceRole, targetRole, [args.reason, args.message].filter(Boolean).join("\n"))) {
      return {
        title: "Workflow message is not dispatch",
        metadata: { action: "send" as const, workflowID: workflow.id, kind: args.kind, updated: false },
        output: [
          "workflow_message send is only for consultation or notification.",
          "It cannot assign executor/reviewer/tester work, create milestone jobs, attach sessions to milestones, or close planning gates.",
          "Use the workflow tool with action=update_xml, resume, or plan_complete, then confirm the command result reports applied=true.",
        ].join("\n"),
      }
    }
    const attachments = workflowMessageAttachments(args.attachments)
    if (args.kind === "handoff") {
      const invalid = attachments.find(workflowMessageAttachmentInvalid)
      if (attachments.length === 0 || invalid) {
        return {
          title: attachments.length === 0 ? "Workflow handoff attachments required" : "Workflow handoff attachment invalid",
          metadata: {
            action: "send" as const,
            workflowID: workflow.id,
            kind: args.kind,
            status: "precondition_failed",
            updated: false,
          },
          output: attachments.length === 0
            ? "precondition_failed: handoff requires at least one workflow-relative artifact path in attachments."
            : `precondition_failed: Invalid handoff attachment '${invalid}'. Attachments must be relative workflow artifact paths and cannot use absolute paths or '..'.`,
        }
      }
    }
    if (args.kind === "consultation") return createConsultation(workflow, sessionID, sourceRole, target!, args)
    const message = workflowMessageWithAttachments(args.message!.trim(), attachments)
    const runtimeDispatch = dispatchWorkflowMessageSend(workflow.directory, {
      workflowID: workflow.id,
      sourceSessionID: sessionID,
      kind: args.kind,
      targetSessionID: args.targetSessionID,
      targetRole: args.targetRole,
      targetSpecialty: args.targetSpecialty,
      timing: args.timing,
      milestoneID: args.milestoneID,
      message,
      reason: args.reason,
      attachments: attachments.length ? attachments : undefined,
    })
    if (!runtimeDispatch) {
      return {
        title: "Workflow message runtime unavailable",
        metadata: { action: "send" as const, workflowID: workflow.id, kind: args.kind, updated: false },
        output:
          "The workflow runtime is not registered for this workspace, so intervention delivery was not queued. Open the workflow runtime and retry.",
      }
    }
    const result = yield* Effect.promise(() => runtimeDispatch)
    if (!result.applied || !result.messageID) {
      return {
        title: "Workflow intervention was not sent",
        metadata: {
          action: "send" as const,
          workflowID: workflow.id,
          kind: args.kind,
          updated: false,
          ...(result.status ? { status: result.status } : {}),
        },
        output: [result.message, result.rejection ? `Reason: ${result.rejection.reason}` : undefined]
          .filter(Boolean)
          .join("\n"),
      }
    }
    upsertWorkflowMessageRecord({
      workflowID: workflow.id,
      id: result.messageID,
      kind: args.kind,
      fromSessionID: sessionID,
      fromRole: sourceRole,
      toSessionID: target?.sessionID ?? args.targetSessionID,
      toRole: targetRole,
      milestoneID: args.milestoneID,
      timing: args.timing ?? "temporary-interrupt",
      body: message,
      response: null,
      attachments: attachments.length ? attachments : undefined,
      status: result.status ?? "queued",
      timeCreated: Date.now(),
      timeUpdated: Date.now(),
    })
    appendWorkflowMessageJournal(workflow, {
      action: "send",
      kind: args.kind,
      messageID: result.messageID,
      sessionID,
      targetSessionID: target?.sessionID ?? args.targetSessionID,
      status: result.status ?? "queued",
      response: message,
      attachments: attachments.length ? attachments : undefined,
    })
    return {
      title: args.kind === "handoff" ? "Workflow handoff sent" : "Workflow intervention sent",
      metadata: {
        action: "send" as const,
        workflowID: workflow.id,
        messageID: result.messageID,
        kind: args.kind,
        status: result.status ?? "queued",
        updated: true,
      },
      output: result.message,
    }
  })
}

function workflowMessageSendDispatchMisuse(
  sourceRole: typeof WorkflowRole.Type,
  targetRole: typeof WorkflowRole.Type,
  text: string,
) {
  if (!["requester", "main_pm", "department_pm"].includes(sourceRole)) return false
  if (!["department_pm", "executor", "reviewer", "tester"].includes(targetRole)) return false
  return workflowMessageSendClaimsDispatch(text)
}

function workflowMessageSendClaimsDispatch(text: string) {
  return /(\byour assignment\b|\bassignment\s*:|\bassign(?:ing|ed|ment)?\b[\s\S]{0,120}\b(executor|reviewer|tester|department[_\s-]*pm|sessions?)\b|\bdispatch(?:ed|ing)?\b[\s\S]{0,120}\b(executor|reviewer|tester|session|milestone|wave)\b|\broute\b[\s\S]{0,120}\bexecutor\b|\bprompt\b[\s\S]{0,120}\bexecutor sessions\b|\bwave\s*\d\b|第一波|第[一二三四五六七八九十]+波|直接继续到执行者|直接(?:派发|分配|启动)|开始派发|派发(?:执行者|会话|任务|第一波)|启动(?:执行者|后续流程)|路由到执行者|分配给执行者)/i.test(
    text,
  )
}

function workflowMessageAttachments(input?: readonly string[]) {
  return (input ?? []).map((item) => item.trim()).filter(Boolean)
}

function workflowMessageAttachmentInvalid(value: string) {
  return (
    path.isAbsolute(value) ||
    value.includes("\0") ||
    path.normalize(value) === "." ||
    value.split(/[\\/]+/).includes("..")
  )
}

function workflowMessageWithAttachments(message: string, attachments: readonly string[]) {
  if (attachments.length === 0) return message
  return [message, "", "Attachments:", ...attachments.map((item) => `- ${item}`)].join("\n")
}

function createConsultation(
  workflow: WorkflowRow,
  sessionID: SessionID,
  sourceRole: typeof WorkflowRole.Type,
  target: { sessionID: SessionID; role: typeof WorkflowRole.Type },
  args: Schema.Schema.Type<typeof Parameters>,
) {
  const id = `consult_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
  const now = Date.now()
  Database.use((db) =>
    db
      .insert(WorkflowConsultationTable)
      .values({
        workflow_id: workflow.id,
        id,
        from_session_id: sessionID,
        to_session_id: target.sessionID,
        from_role: sourceRole,
        to_role: target.role,
        ...(args.milestoneID ? { milestone_id: args.milestoneID } : {}),
        ...(args.reason ? { reason: args.reason } : {}),
        ...(args.timing ? { timing: args.timing } : {}),
        question: args.message!.trim(),
        answer: "_Pending answer._",
        status: "pending",
        time_created: now,
        time_updated: now,
      })
      .run(),
  )
  upsertWorkflowMessageRecord({
    workflowID: workflow.id,
    id,
    kind: "consultation",
    fromSessionID: sessionID,
    fromRole: sourceRole,
    toSessionID: target.sessionID,
    toRole: target.role,
    milestoneID: args.milestoneID,
    timing: args.timing,
    body: args.message!.trim(),
    response: "_Pending answer._",
    status: "pending",
    timeCreated: now,
    timeUpdated: now,
  })
  appendWorkflowMessageJournal(workflow, {
    action: "send",
    kind: "consultation",
    messageID: id,
    sessionID,
    targetSessionID: target.sessionID,
    status: "pending",
    response: args.message!.trim(),
  })
  return {
    title: "Workflow consultation sent",
    metadata: {
      action: "send" as const,
      workflowID: workflow.id,
      messageID: id,
      kind: "consultation" as const,
      status: "pending",
      updated: true,
    },
    output: `Consultation ${id} was sent to ${target.role} ${target.sessionID} and is visible in that session's workflow inbox.`,
  }
}

function createIntervention(
  workflow: WorkflowRow,
  sessionID: SessionID,
  target: { sessionID: SessionID; role: typeof WorkflowRole.Type },
  args: Schema.Schema.Type<typeof Parameters>,
) {
  const id = `intervention_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
  const now = Date.now()
  Database.use((db) =>
    db
      .insert(WorkflowInterventionTable)
      .values({
        workflow_id: workflow.id,
        id,
        from_session_id: sessionID,
        target_session_id: target.sessionID,
        target_role: target.role,
        timing: args.timing ?? "temporary-interrupt",
        message: args.message!.trim(),
        path: path.join(workflow.path, "interventions", `${id}.md`),
        status: "queued",
        time_created: now,
        time_updated: now,
      })
      .run(),
  )
  appendWorkflowMessageJournal(workflow, {
    action: "send",
    kind: "intervention",
    messageID: id,
    sessionID,
    targetSessionID: target.sessionID,
    status: "queued",
    response: args.message!.trim(),
  })
  return {
    title: "Workflow intervention sent",
    metadata: {
      action: "send" as const,
      workflowID: workflow.id,
      messageID: id,
      kind: "intervention" as const,
      status: "queued",
      updated: true,
    },
    output: `Intervention ${id} was sent to ${target.role} ${target.sessionID} and is visible in that session's workflow inbox.`,
  }
}

function workflowRoleForSession(workflow: WorkflowRow, sessionID: SessionID) {
  if (workflow.root_session_id === sessionID) return "requester"
  if (workflow.pm_session_id === sessionID) return "main_pm"
  if (workflow.tester_session_id === sessionID) return "tester"
  return Database.use((db) =>
    db
      .select()
      .from(WorkflowMemberTable)
      .where(and(eq(WorkflowMemberTable.workflow_id, workflow.id), eq(WorkflowMemberTable.session_id, sessionID)))
      .orderBy(desc(WorkflowMemberTable.time_updated))
      .get(),
  )?.role
}

function resolveWorkflowMessageTarget(
  workflow: WorkflowRow,
  targetSessionID: SessionID | undefined,
  targetRole: typeof WorkflowRole.Type | undefined,
  targetSpecialty: string | undefined,
) {
  if (targetSessionID) {
    const role = workflowRoleForSession(workflow, targetSessionID) ?? targetRole
    return role ? { sessionID: targetSessionID, role } : undefined
  }
  if (!targetRole) return
  if (targetRole === "requester") {
    return workflow.root_session_id ? { sessionID: workflow.root_session_id, role: targetRole } : undefined
  }
  if (targetRole === "main_pm") {
    return workflow.pm_session_id ? { sessionID: workflow.pm_session_id, role: targetRole } : undefined
  }
  if (targetRole === "tester" && workflow.tester_session_id) {
    return { sessionID: workflow.tester_session_id, role: targetRole }
  }
  const member = Database.use((db) =>
    db
      .select()
      .from(WorkflowMemberTable)
      .where(
        and(
          eq(WorkflowMemberTable.workflow_id, workflow.id),
          eq(WorkflowMemberTable.role, targetRole),
          eq(WorkflowMemberTable.status, "active"),
        ),
      )
      .orderBy(desc(WorkflowMemberTable.time_updated))
      .all(),
  )
    .toSorted((a, b) => {
      const specialty = Number(b.specialty === targetSpecialty) - Number(a.specialty === targetSpecialty)
      if (specialty !== 0) return specialty
      return b.time_updated - a.time_updated
    })[0]
  return member ? { sessionID: member.session_id, role: member.role } : undefined
}

function ack(workflow: WorkflowRow, sessionID: SessionID, messageID: string) {
  const consultation = findConsultation(workflow.id, messageID)
  const message = findWorkflowMessage(workflow.id, messageID)
  if (consultation || message?.kind === "consultation") {
    return Effect.succeed({
      title: "Consultation needs an answer",
      metadata: {
        action: "ack" as const,
        workflowID: workflow.id,
        messageID,
        kind: "consultation" as const,
        status: consultation?.status ?? message?.status,
        updated: false,
      },
      output: "Consultation messages require action=answer with an answer. Use ack for requester interventions.",
    })
  }
  return updateInterventionResponse(workflow, sessionID, messageID, "Acknowledged.", "ack")
}

function answer(workflow: WorkflowRow, sessionID: SessionID, messageID: string, answer?: string) {
  if (!answer) {
    return Effect.succeed({
      title: "Missing workflow message answer",
      metadata: { action: "answer" as const, workflowID: workflow.id, messageID, updated: false },
      output: "answer is required when action=answer.",
    })
  }
  const consultation = findConsultation(workflow.id, messageID)
  if (consultation) return updateConsultationAnswer(workflow, sessionID, messageID, answer)
  if (findWorkflowMessage(workflow.id, messageID)?.kind === "consultation") {
    return updateWorkflowMessageResponse(workflow, sessionID, messageID, answer, "answer")
  }
  return updateInterventionResponse(workflow, sessionID, messageID, answer, "answer")
}

function findConsultation(workflowID: WorkflowID, messageID: string) {
  return Database.use((db) =>
    db
      .select()
      .from(WorkflowConsultationTable)
      .where(and(eq(WorkflowConsultationTable.workflow_id, workflowID), eq(WorkflowConsultationTable.id, messageID)))
      .get(),
  )
}

function findWorkflowMessage(workflowID: WorkflowID, messageID: string) {
  return Database.use((db) =>
    db
      .select()
      .from(WorkflowMessageTable)
      .where(and(eq(WorkflowMessageTable.workflow_id, workflowID), eq(WorkflowMessageTable.id, messageID)))
      .get(),
  )
}

function updateConsultationAnswer(workflow: WorkflowRow, sessionID: SessionID, messageID: string, answer: string) {
  return Effect.sync(() => {
    const item = findConsultation(workflow.id, messageID)
    if (!item) return missingMessage("answer", workflow.id, messageID)
    if (item.to_session_id !== sessionID) return unauthorizedMessage("answer", workflow.id, messageID, "consultation")
    Database.use((db) =>
      db
        .update(WorkflowConsultationTable)
        .set({ answer, status: "answered", time_updated: Date.now() })
        .where(and(eq(WorkflowConsultationTable.workflow_id, workflow.id), eq(WorkflowConsultationTable.id, messageID)))
        .run(),
    )
    upsertWorkflowMessageRecord({
      workflowID: workflow.id,
      id: messageID,
      kind: "consultation",
      fromSessionID: item.from_session_id,
      fromRole: item.from_role,
      toSessionID: sessionID,
      toRole: item.to_role,
      milestoneID: item.milestone_id ?? undefined,
      timing: item.timing ?? undefined,
      body: item.question,
      response: answer,
      status: "answered",
      timeCreated: item.time_created,
      timeClosed: Date.now(),
      timeUpdated: Date.now(),
    })
    appendWorkflowMessageJournal(workflow, {
      action: "answer",
      kind: "consultation",
      messageID,
      sessionID,
      status: "answered",
      response: answer,
    })
    return {
      title: "Workflow consultation answered",
      metadata: {
        action: "answer" as const,
        workflowID: workflow.id,
        messageID,
        kind: "consultation" as const,
        status: "answered",
        updated: true,
      },
      output: `Consultation ${messageID} was answered and recorded in the workflow journal.`,
    }
  })
}

function updateInterventionResponse(
  workflow: WorkflowRow,
  sessionID: SessionID,
  messageID: string,
  response: string,
  action: "ack" | "answer",
) {
  const item = Database.use((db) =>
    db
      .select()
      .from(WorkflowInterventionTable)
      .where(and(eq(WorkflowInterventionTable.workflow_id, workflow.id), eq(WorkflowInterventionTable.id, messageID)))
      .get(),
  )
  if (!item) return updateWorkflowMessageResponse(workflow, sessionID, messageID, response, action)
  return Effect.sync(() => {
    if (item.target_session_id !== sessionID) return unauthorizedMessage(action, workflow.id, messageID, "intervention")
    const existing = Database.use((db) =>
      db
        .select()
        .from(WorkflowMessageTable)
        .where(and(eq(WorkflowMessageTable.workflow_id, workflow.id), eq(WorkflowMessageTable.id, messageID)))
        .get(),
    )
    Database.use((db) =>
      db
        .update(WorkflowInterventionTable)
        .set({ response, status: "acked", time_updated: Date.now() })
        .where(and(eq(WorkflowInterventionTable.workflow_id, workflow.id), eq(WorkflowInterventionTable.id, messageID)))
        .run(),
    )
    upsertWorkflowMessageRecord({
      workflowID: workflow.id,
      id: messageID,
      kind: workflowMessageKind(existing?.kind) ?? "intervention",
      fromSessionID: item.from_session_id ?? undefined,
      fromRole: existing?.from_role ?? undefined,
      toSessionID: sessionID,
      toRole: item.target_role,
      timing: item.timing,
      body: item.message,
      response,
      attachments: existing?.attachments ?? undefined,
      status: "acked",
      timeCreated: item.time_created,
      timeClosed: Date.now(),
      timeUpdated: Date.now(),
    })
    appendWorkflowMessageJournal(workflow, {
      action,
      kind: workflowMessageKind(existing?.kind) ?? "intervention",
      messageID,
      sessionID,
      status: "acked",
      response,
    })
    return {
      title: action === "ack" ? "Workflow intervention acknowledged" : "Workflow intervention answered",
      metadata: {
        action,
        workflowID: workflow.id,
        messageID,
        kind: workflowMessageKind(existing?.kind) ?? "intervention",
        status: "acked",
        updated: true,
      },
      output: `Intervention ${messageID} was acknowledged and recorded in the workflow journal.`,
    }
  })
}

function updateWorkflowMessageResponse(
  workflow: WorkflowRow,
  sessionID: SessionID,
  messageID: string,
  response: string,
  action: "ack" | "answer",
) {
  return Effect.sync(() => {
    const item = findWorkflowMessage(workflow.id, messageID)
    const kind = workflowMessageKind(item?.kind)
    if (!item || !kind) return missingMessage(action, workflow.id, messageID)
    if (item.to_session_id !== sessionID) return unauthorizedMessage(action, workflow.id, messageID, kind)
    const status = kind === "consultation" ? "answered" : "acked"
    upsertWorkflowMessageRecord({
      workflowID: workflow.id,
      id: messageID,
      kind,
      fromSessionID: item.from_session_id,
      fromRole: workflowRole(item.from_role),
      toSessionID: sessionID,
      toRole: workflowRole(item.to_role),
      milestoneID: item.milestone_id ?? undefined,
      timing: workflowTiming(item.timing),
      body: item.body,
      response,
      attachments: item.attachments ?? undefined,
      status,
      timeCreated: item.time_created,
      timeDelivered: item.time_delivered,
      timeClosed: Date.now(),
      timeUpdated: Date.now(),
    })
    appendWorkflowMessageJournal(workflow, {
      action,
      kind,
      messageID,
      sessionID,
      status,
      response,
    })
    return {
      title: kind === "consultation" ? "Workflow consultation answered" : "Workflow intervention acknowledged",
      metadata: {
        action,
        workflowID: workflow.id,
        messageID,
        kind,
        status,
        updated: true,
      },
      output: `Workflow message ${messageID} was ${status} and recorded in the workflow journal.`,
    }
  })
}

function upsertWorkflowMessageRecord(input: {
  workflowID: WorkflowID
  id: string
  kind: WorkflowMessageKind
  fromSessionID?: SessionID | null
  fromRole?: typeof WorkflowRole.Type | null
  toSessionID?: SessionID | null
  toRole?: typeof WorkflowRole.Type | null
  milestoneID?: WorkflowMilestoneID | null
  timing?: typeof WorkflowCommunicationTiming.Type | null
  body: string
  response?: string | null
  attachments?: readonly string[]
  status: string
  timeCreated: number
  timeDelivered?: number | null
  timeClosed?: number | null
  timeUpdated: number
}) {
  Database.use((db) =>
    db
      .insert(WorkflowMessageTable)
      .values({
        workflow_id: input.workflowID,
        id: input.id,
        kind: input.kind,
        from_session_id: input.fromSessionID ?? null,
        from_role: input.fromRole ?? null,
        to_session_id: input.toSessionID ?? null,
        to_role: input.toRole ?? null,
        milestone_id: input.milestoneID ?? null,
        timing: input.timing ?? null,
        body: input.body,
        response: input.response ?? null,
        attachments: input.attachments ?? null,
        status: input.status,
        time_created: input.timeCreated,
        time_delivered: input.timeDelivered ?? null,
        time_closed: input.timeClosed ?? null,
        time_updated: input.timeUpdated,
      })
      .onConflictDoUpdate({
        target: [WorkflowMessageTable.workflow_id, WorkflowMessageTable.id],
        set: {
          kind: input.kind,
          from_session_id: input.fromSessionID ?? null,
          from_role: input.fromRole ?? null,
          to_session_id: input.toSessionID ?? null,
          to_role: input.toRole ?? null,
          milestone_id: input.milestoneID ?? null,
          timing: input.timing ?? null,
          body: input.body,
          response:
            input.status === "queued"
              ? sql`case when ${WorkflowMessageTable.status} <> 'queued' then ${WorkflowMessageTable.response} else excluded.response end`
              : input.response ?? null,
          attachments: input.attachments ?? null,
          status:
            input.status === "queued"
              ? sql`case when ${WorkflowMessageTable.status} <> 'queued' then ${WorkflowMessageTable.status} else excluded.status end`
              : input.status,
          time_delivered:
            input.status === "queued"
              ? sql`case when ${WorkflowMessageTable.status} <> 'queued' then ${WorkflowMessageTable.time_delivered} else excluded.time_delivered end`
              : input.timeDelivered ?? null,
          time_closed:
            input.status === "queued"
              ? sql`case when ${WorkflowMessageTable.status} <> 'queued' then ${WorkflowMessageTable.time_closed} else excluded.time_closed end`
              : input.timeClosed ?? null,
          time_updated:
            input.status === "queued"
              ? sql`case when ${WorkflowMessageTable.status} <> 'queued' then ${WorkflowMessageTable.time_updated} else excluded.time_updated end`
              : input.timeUpdated,
        },
      })
      .run(),
  )
}

function workflowMessageKind(value: unknown): WorkflowMessageKind | undefined {
  return workflowMessageKinds.find((item) => item === value)
}

const workflowRoles = ["requester", "main_pm", "department_pm", "executor", "reviewer", "tester", "expert"] as const

function workflowRole(value: unknown): typeof WorkflowRole.Type | undefined {
  return workflowRoles.find((item) => item === value)
}

const workflowTimings = ["after-task", "interrupt", "temporary-interrupt"] as const

function workflowTiming(value: unknown): typeof WorkflowCommunicationTiming.Type | undefined {
  return workflowTimings.find((item) => item === value)
}

function missingMessage(action: typeof WorkflowMessageAction.Type, workflowID: WorkflowID, messageID: string) {
  return {
    title: "Workflow message not found",
    metadata: { action, workflowID, messageID, updated: false },
    output: `No workflow consultation or intervention with id ${messageID} exists in workflow ${workflowID}.`,
  }
}

function unauthorizedMessage(
  action: typeof WorkflowMessageAction.Type,
  workflowID: WorkflowID,
  messageID: string,
  kind: WorkflowMessageKind,
) {
  return {
    title: "Workflow message belongs to another session",
    metadata: { action, workflowID, messageID, kind, updated: false },
    output: `Message ${messageID} is a ${kind} for another session. Open the target session or ask the workflow owner to reroute it.`,
  }
}

function appendWorkflowMessageJournal(
  workflow: WorkflowRow,
  event: {
    action: "send" | "ack" | "answer"
    kind: WorkflowMessageKind
    messageID: string
    sessionID: SessionID
    targetSessionID?: SessionID
    status: string
    response: string
    attachments?: readonly string[]
  },
) {
  const file = path.join(workflow.directory, workflow.path, "journal", "messages.jsonl")
  mkdirSync(path.dirname(file), { recursive: true })
  const existing = existsSync(file) ? readFileSync(file, "utf8") : ""
  appendFileSync(
    file,
    `${JSON.stringify({
      seq: existing.trim() ? existing.trim().split(/\r?\n/).filter(Boolean).length + 1 : 1,
      ts: new Date().toISOString(),
      workflowID: workflow.id,
      ...event,
    })}\n`,
  )
}
