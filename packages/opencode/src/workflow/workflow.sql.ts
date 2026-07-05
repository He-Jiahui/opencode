import { integer, index, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"

import type { ProjectID } from "@/project/schema"
import type { SessionID } from "@/session/schema"
import { Timestamps } from "@opencode-ai/core/database/schema.sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import type {
  WorkflowConsultationInfo,
  WorkflowGraphEdge,
  WorkflowInfo,
  WorkflowInterventionInfo,
  WorkflowMemberInfo,
  WorkflowMilestoneInfo,
  WorkflowModelWhitelistConfig,
  WorkflowSchedulingConfig,
  WorkflowSessionRef,
} from "./schema"
import { WorkflowID, WorkflowMilestoneID } from "./schema"

export const WorkflowTable = sqliteTable(
  "workflow",
  {
    id: text().$type<WorkflowID>().primaryKey(),
    project_id: text().$type<ProjectID>().notNull(),
    root_session_id: text()
      .$type<SessionID>()
      .references(() => SessionTable.id, { onDelete: "set null" }),
    pm_session_id: text()
      .$type<SessionID>()
      .references(() => SessionTable.id, { onDelete: "set null" }),
    tester_session_id: text()
      .$type<SessionID>()
      .references(() => SessionTable.id, { onDelete: "set null" }),
    request: text().notNull(),
    title: text().notNull(),
    directory: text().notNull(),
    path: text().notNull(),
    xml: text().notNull(),
    status: text().$type<WorkflowInfo["status"]>().notNull(),
    staffing: text({ mode: "json" }).$type<WorkflowInfo["staffing"]>(),
    scheduling: text({ mode: "json" }).$type<WorkflowSchedulingConfig>(),
    model: text({ mode: "json" }).$type<WorkflowInfo["model"]>(),
    model_whitelist: text({ mode: "json" }).$type<WorkflowModelWhitelistConfig>(),
    agent: text(),
    test_path: text(),
    error: text(),
    ...Timestamps,
    time_completed: integer(),
  },
  (table) => [
    index("workflow_project_idx").on(table.project_id),
    index("workflow_root_session_idx").on(table.root_session_id),
    index("workflow_status_idx").on(table.status),
  ],
)

export const WorkflowMemberTable = sqliteTable(
  "workflow_member",
  {
    workflow_id: text()
      .$type<WorkflowID>()
      .notNull()
      .references(() => WorkflowTable.id, { onDelete: "cascade" }),
    id: text().notNull(),
    role: text().$type<WorkflowMemberInfo["role"]>().notNull(),
    specialty: text().notNull(),
    title: text().notNull(),
    session_id: text()
      .$type<SessionID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    capacity: integer().notNull().default(1),
    status: text().$type<WorkflowMemberInfo["status"]>().notNull(),
    availability: text().$type<WorkflowMemberInfo["availability"]>(),
    current_focus: text(),
    blockers: text({ mode: "json" }).$type<ReadonlyArray<string>>(),
    progress_note: text(),
    model: text({ mode: "json" }).$type<WorkflowInfo["model"]>(),
    model_weight: integer(),
    model_cache_until: integer(),
    ...Timestamps,
  },
  (table) => [
    primaryKey({ columns: [table.workflow_id, table.id] }),
    index("workflow_member_workflow_role_idx").on(table.workflow_id, table.role),
    index("workflow_member_session_idx").on(table.session_id),
  ],
)

export const WorkflowMilestoneTable = sqliteTable(
  "workflow_milestone",
  {
    workflow_id: text()
      .$type<WorkflowID>()
      .notNull()
      .references(() => WorkflowTable.id, { onDelete: "cascade" }),
    id: text().$type<WorkflowMilestoneID>().notNull(),
    title: text(),
    department: text(),
    review: text().$type<WorkflowMilestoneInfo["review"]>(),
    waiting_for: text().$type<WorkflowMilestoneInfo["waitingFor"]>(),
    prompt: text().notNull(),
    depends_on: text({ mode: "json" }).notNull().$type<ReadonlyArray<WorkflowMilestoneID>>(),
    status: text().$type<WorkflowMilestoneInfo["status"]>().notNull(),
    attempt: integer().notNull().default(0),
    plan_path: text(),
    review_path: text(),
    session: text({ mode: "json" }).notNull().$type<ReadonlyArray<WorkflowSessionRef>>(),
    ...Timestamps,
  },
  (table) => [
    primaryKey({ columns: [table.workflow_id, table.id] }),
    index("workflow_milestone_workflow_status_idx").on(table.workflow_id, table.status),
  ],
)

export const WorkflowEdgeTable = sqliteTable(
  "workflow_edge",
  {
    workflow_id: text()
      .$type<WorkflowID>()
      .notNull()
      .references(() => WorkflowTable.id, { onDelete: "cascade" }),
    from_id: text().$type<WorkflowMilestoneID>().notNull(),
    to_id: text().$type<WorkflowMilestoneID>().notNull(),
    data: text({ mode: "json" }).$type<WorkflowGraphEdge>(),
  },
  (table) => [
    primaryKey({ columns: [table.workflow_id, table.from_id, table.to_id] }),
    index("workflow_edge_workflow_idx").on(table.workflow_id),
  ],
)

export const WorkflowConsultationTable = sqliteTable(
  "workflow_consultation",
  {
    workflow_id: text()
      .$type<WorkflowID>()
      .notNull()
      .references(() => WorkflowTable.id, { onDelete: "cascade" }),
    id: text().notNull(),
    from_session_id: text()
      .$type<SessionID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    to_session_id: text()
      .$type<SessionID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    from_role: text().$type<WorkflowSessionRef["role"]>().notNull(),
    to_role: text().$type<WorkflowSessionRef["role"]>().notNull(),
    milestone_id: text().$type<WorkflowMilestoneID>(),
    reason: text(),
    timing: text().$type<WorkflowConsultationInfo["timing"]>(),
    question: text().notNull(),
    answer: text().notNull(),
    status: text().$type<WorkflowConsultationInfo["status"]>().notNull(),
    ...Timestamps,
  },
  (table) => [
    primaryKey({ columns: [table.workflow_id, table.id] }),
    index("workflow_consultation_workflow_idx").on(table.workflow_id),
    index("workflow_consultation_from_session_idx").on(table.from_session_id),
    index("workflow_consultation_to_session_idx").on(table.to_session_id),
  ],
)

export const WorkflowInterventionTable = sqliteTable(
  "workflow_intervention",
  {
    workflow_id: text()
      .$type<WorkflowID>()
      .notNull()
      .references(() => WorkflowTable.id, { onDelete: "cascade" }),
    id: text().notNull(),
    from_session_id: text()
      .$type<SessionID>()
      .references(() => SessionTable.id, { onDelete: "set null" }),
    target_session_id: text()
      .$type<SessionID>()
      .references(() => SessionTable.id, { onDelete: "set null" }),
    target_role: text().$type<WorkflowInterventionInfo["targetRole"]>().notNull(),
    timing: text().$type<WorkflowInterventionInfo["timing"]>().notNull(),
    message: text().notNull(),
    response: text(),
    path: text().notNull(),
    status: text().$type<WorkflowInterventionInfo["status"]>().notNull(),
    ...Timestamps,
  },
  (table) => [
    primaryKey({ columns: [table.workflow_id, table.id] }),
    index("workflow_intervention_workflow_idx").on(table.workflow_id),
    index("workflow_intervention_target_session_idx").on(table.target_session_id),
  ],
)

export const WorkflowMessageTable = sqliteTable(
  "workflow_message",
  {
    workflow_id: text()
      .$type<WorkflowID>()
      .notNull()
      .references(() => WorkflowTable.id, { onDelete: "cascade" }),
    id: text().notNull(),
    kind: text().notNull(),
    from_session_id: text()
      .$type<SessionID>()
      .references(() => SessionTable.id, { onDelete: "set null" }),
    from_role: text().$type<WorkflowSessionRef["role"]>(),
    to_session_id: text()
      .$type<SessionID>()
      .references(() => SessionTable.id, { onDelete: "set null" }),
    to_role: text().$type<WorkflowSessionRef["role"]>(),
    milestone_id: text().$type<WorkflowMilestoneID>(),
    timing: text().$type<WorkflowConsultationInfo["timing"]>(),
    body: text().notNull(),
    response: text(),
    attachments: text({ mode: "json" }).$type<ReadonlyArray<string>>(),
    status: text().notNull(),
    time_created: integer().notNull(),
    time_delivered: integer(),
    time_closed: integer(),
    time_updated: integer().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.workflow_id, table.id] }),
    index("workflow_message_workflow_idx").on(table.workflow_id),
    index("workflow_message_to_session_idx").on(table.to_session_id),
    index("workflow_message_from_session_idx").on(table.from_session_id),
    index("workflow_message_workflow_status_idx").on(table.workflow_id, table.status),
  ],
)
