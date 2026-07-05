// @ts-nocheck
import path from "path"
import { createHash } from "crypto"
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "fs"
import { appendFile, cp, mkdir, open, readFile, readdir, rename, rm, stat } from "fs/promises"

import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { Bus } from "@/bus"
import { GlobalBus } from "@/bus/global"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { EffectBridge } from "@/effect/bridge"
import { InstanceState } from "@/effect/instance-state"
import { FileWatcher } from "@/file/watcher"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { ProjectID } from "@/project/schema"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { MessageV2 } from "@/session/message-v2"
import { SessionPrompt } from "@/session/prompt"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Database, and, asc, eq, inArray, or, sql } from "@/storage/db"
import { BackgroundJob } from "@/background/job"
import { Cause, Effect, Context, Layer, Schema, Stream } from "effect"
import {
  MessageTable,
  PartTable,
  SessionContextEpochTable,
  SessionInputTable,
  SessionMessageTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { InstallationVersion } from "@opencode-ai/core/installation/version"

import { parseWorkflowXml, workflowPipelineItemsMissing } from "./parse"
import { dependencyBlockedMilestones, dependencyUnblockedMilestones, readyMilestones } from "./scheduler"
import {
  WorkflowToolCommandEvent,
  WorkflowToolCommandResultEvent,
  registerWorkflowMessageSendDispatcher,
  registerWorkflowToolCommandDispatcher,
  type WorkflowMessageSendCommand,
  type WorkflowMessageSendResult,
  type WorkflowToolCommand,
  type WorkflowToolCommandResult,
  type WorkflowToolCommandRejection,
} from "./command"
import {
  WorkflowGraph,
  WorkflowGraphEdge,
  WorkflowGraphNode,
  WorkflowID,
  WorkflowInfo,
  WorkflowCommunicationTiming,
  WorkflowRole,
  WorkflowMilestone,
  WorkflowMilestoneID,
  WorkflowModelWhitelistConfig,
  type WorkflowModelWhitelistItem,
  WorkflowSchedulingConfig,
  WorkflowStaffingConfig,
  type WorkflowConsultationInfo,
  type WorkflowDefinition,
  type WorkflowInterventionInfo,
  type WorkflowMemberInfo,
  type WorkflowMilestoneInfo,
  type WorkflowSessionRef,
} from "./schema"
import {
  WorkflowConsultationTable,
  WorkflowEdgeTable,
  WorkflowInterventionTable,
  WorkflowMessageTable,
  WorkflowMemberTable,
  WorkflowMilestoneTable,
  WorkflowTable,
} from "./workflow.sql"

const workflowDir = path.join(".opencode", "workflows")
const workflowManifestFileName = "manifest.json"
const workflowStateFileName = "workflow-state.json"
const workflowStateSchemaVersion = 2
const workflowMessageKinds = ["consultation", "intervention", "handoff", "standup", "report"] as const
type WorkflowMessageKind = (typeof workflowMessageKinds)[number]
type WorkflowToolCommandRuntimeResult = {
  input: WorkflowToolCommand
  result: {
    workflowID?: WorkflowID
    applied: boolean
    message: string
    rejection?: WorkflowToolCommandRejection
  }
}
const defaultXml = `<workflow>
  <ordered>
    <milestone id="requirements" title="Clarify requirements" department="product">Clarify the user request, constraints, and acceptance criteria.</milestone>
    <milestone id="implementation" title="Implement solution" department="engineering">Implement the requested change end to end.</milestone>
    <milestone id="verification" title="Verify solution" department="quality">Verify behavior with focused tests and checks.</milestone>
  </ordered>
</workflow>`
const workflowConsultationTimeoutMillis = 30 * 60 * 1000
const workflowInterventionTimeoutMillis = 30 * 60 * 1000
const workflowWaitingTimeoutMillis = 30 * 60 * 1000
const workflowMilestoneAttemptLimit = 3
const workflowResumeDoctorBlockingCodes = new Set([
  "duplicate_directory",
  "noncanonical_path",
  "invalid_state_json",
  "unsupported_state_version",
  "missing_manifest",
  "invalid_manifest_json",
  "unsupported_manifest_schema",
  "manifest_workflow_mismatch",
  "manifest_project_mismatch",
  "session_state_hash_mismatch",
])
const workflowStateDoctorFixCodes = new Set([
  "missing_state",
  "invalid_state_json",
  "unsupported_state_version",
  "state_workflow_mismatch",
  "state_status_mismatch",
  "state_missing_milestone",
  "milestone_status_mismatch",
  "missing_session_state",
  "invalid_session_state_json",
  "session_state_hash_mismatch",
])
const ensureSchemaSql = `
CREATE TABLE IF NOT EXISTS workflow (
  id text PRIMARY KEY NOT NULL,
  project_id text NOT NULL,
  root_session_id text,
  pm_session_id text,
  tester_session_id text,
  request text NOT NULL,
  title text NOT NULL,
  directory text NOT NULL,
  path text NOT NULL,
  xml text NOT NULL,
  status text NOT NULL,
  staffing text,
  scheduling text,
  model text,
  model_whitelist text,
  agent text,
  test_path text,
  error text,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  time_completed integer,
  FOREIGN KEY (root_session_id) REFERENCES session(id) ON DELETE set null,
  FOREIGN KEY (pm_session_id) REFERENCES session(id) ON DELETE set null,
  FOREIGN KEY (tester_session_id) REFERENCES session(id) ON DELETE set null
);
CREATE INDEX IF NOT EXISTS workflow_project_idx ON workflow (project_id);
CREATE INDEX IF NOT EXISTS workflow_root_session_idx ON workflow (root_session_id);
CREATE INDEX IF NOT EXISTS workflow_status_idx ON workflow (status);
CREATE TABLE IF NOT EXISTS workflow_member (
  workflow_id text NOT NULL,
  id text NOT NULL,
  role text NOT NULL,
  specialty text NOT NULL,
  title text NOT NULL,
  session_id text NOT NULL,
  capacity integer DEFAULT 1 NOT NULL,
  status text NOT NULL,
  availability text,
  current_focus text,
  blockers text,
  progress_note text,
  model text,
  model_weight integer,
  model_cache_until integer,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  PRIMARY KEY (workflow_id, id),
  FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE cascade,
  FOREIGN KEY (session_id) REFERENCES session(id) ON DELETE cascade
);
CREATE INDEX IF NOT EXISTS workflow_member_workflow_role_idx ON workflow_member (workflow_id, role);
CREATE INDEX IF NOT EXISTS workflow_member_session_idx ON workflow_member (session_id);
CREATE TABLE IF NOT EXISTS workflow_milestone (
  workflow_id text NOT NULL,
  id text NOT NULL,
  title text,
  department text,
  review text,
  waiting_for text,
  prompt text NOT NULL,
  depends_on text NOT NULL,
  status text NOT NULL,
  attempt integer DEFAULT 0 NOT NULL,
  plan_path text,
  review_path text,
  session text NOT NULL,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  PRIMARY KEY (workflow_id, id),
  FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE cascade
);
CREATE INDEX IF NOT EXISTS workflow_milestone_workflow_status_idx ON workflow_milestone (workflow_id, status);
CREATE TABLE IF NOT EXISTS workflow_edge (
  workflow_id text NOT NULL,
  from_id text NOT NULL,
  to_id text NOT NULL,
  data text,
  PRIMARY KEY (workflow_id, from_id, to_id),
  FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE cascade
);
CREATE INDEX IF NOT EXISTS workflow_edge_workflow_idx ON workflow_edge (workflow_id);
CREATE TABLE IF NOT EXISTS workflow_consultation (
  workflow_id text NOT NULL,
  id text NOT NULL,
  from_session_id text NOT NULL,
  to_session_id text NOT NULL,
  from_role text NOT NULL,
  to_role text NOT NULL,
  milestone_id text,
  reason text,
  timing text,
  question text NOT NULL,
  answer text NOT NULL,
  status text NOT NULL,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  PRIMARY KEY (workflow_id, id),
  FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE cascade,
  FOREIGN KEY (from_session_id) REFERENCES session(id) ON DELETE cascade,
  FOREIGN KEY (to_session_id) REFERENCES session(id) ON DELETE cascade
);
CREATE INDEX IF NOT EXISTS workflow_consultation_workflow_idx ON workflow_consultation (workflow_id);
CREATE INDEX IF NOT EXISTS workflow_consultation_from_session_idx ON workflow_consultation (from_session_id);
CREATE INDEX IF NOT EXISTS workflow_consultation_to_session_idx ON workflow_consultation (to_session_id);
CREATE TABLE IF NOT EXISTS workflow_intervention (
  workflow_id text NOT NULL,
  id text NOT NULL,
  from_session_id text,
  target_session_id text,
  target_role text NOT NULL,
  timing text NOT NULL,
  message text NOT NULL,
  response text,
  path text NOT NULL,
  status text NOT NULL,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  PRIMARY KEY (workflow_id, id),
  FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE cascade,
  FOREIGN KEY (from_session_id) REFERENCES session(id) ON DELETE set null,
  FOREIGN KEY (target_session_id) REFERENCES session(id) ON DELETE set null
);
CREATE INDEX IF NOT EXISTS workflow_intervention_workflow_idx ON workflow_intervention (workflow_id);
CREATE INDEX IF NOT EXISTS workflow_intervention_target_session_idx ON workflow_intervention (target_session_id);
CREATE TABLE IF NOT EXISTS workflow_message (
  workflow_id text NOT NULL,
  id text NOT NULL,
  kind text NOT NULL,
  from_session_id text,
  from_role text,
  to_session_id text,
  to_role text,
  milestone_id text,
  timing text,
  body text NOT NULL,
  response text,
  attachments text,
  status text NOT NULL,
  time_created integer NOT NULL,
  time_delivered integer,
  time_closed integer,
  time_updated integer NOT NULL,
  PRIMARY KEY (workflow_id, id),
  FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE cascade,
  FOREIGN KEY (from_session_id) REFERENCES session(id) ON DELETE set null,
  FOREIGN KEY (to_session_id) REFERENCES session(id) ON DELETE set null
);
CREATE INDEX IF NOT EXISTS workflow_message_workflow_idx ON workflow_message (workflow_id);
CREATE INDEX IF NOT EXISTS workflow_message_to_session_idx ON workflow_message (to_session_id);
CREATE INDEX IF NOT EXISTS workflow_message_from_session_idx ON workflow_message (from_session_id);
CREATE INDEX IF NOT EXISTS workflow_message_workflow_status_idx ON workflow_message (workflow_id, status);
`
const ensureWorkflowOwnershipSql = `
PRAGMA foreign_keys = OFF;
CREATE TABLE IF NOT EXISTS workflow_next (
  id text PRIMARY KEY NOT NULL,
  project_id text NOT NULL,
  root_session_id text,
  pm_session_id text,
  tester_session_id text,
  request text NOT NULL,
  title text NOT NULL,
  directory text NOT NULL,
  path text NOT NULL,
  xml text NOT NULL,
  status text NOT NULL,
  staffing text,
  model text,
  model_whitelist text,
  agent text,
  test_path text,
  error text,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  time_completed integer,
  FOREIGN KEY (root_session_id) REFERENCES session(id) ON DELETE set null,
  FOREIGN KEY (pm_session_id) REFERENCES session(id) ON DELETE set null,
  FOREIGN KEY (tester_session_id) REFERENCES session(id) ON DELETE set null
);
INSERT INTO workflow_next
SELECT id, project_id, root_session_id, pm_session_id, tester_session_id, request, title, directory, path, xml, status, NULL, model, NULL, agent, test_path, error, time_created, time_updated, time_completed
FROM workflow
WHERE EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'workflow');
DROP TABLE workflow;
ALTER TABLE workflow_next RENAME TO workflow;
CREATE INDEX IF NOT EXISTS workflow_project_idx ON workflow (project_id);
CREATE INDEX IF NOT EXISTS workflow_root_session_idx ON workflow (root_session_id);
CREATE INDEX IF NOT EXISTS workflow_status_idx ON workflow (status);
PRAGMA foreign_keys = ON;
`
const ensureWorkflowMigrationSql = `
INSERT INTO __drizzle_migrations (hash, created_at, name, applied_at)
SELECT '', 1779883200000, '20260527120000_agent_workflow', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE NOT EXISTS (SELECT 1 FROM __drizzle_migrations WHERE name = '20260527120000_agent_workflow');
`
let schemaEnsuredClient: unknown

function ensureColumn(table: string, column: string, sql: string) {
  const client = Database.Client().$client
  const existing = client.prepare(`SELECT 1 FROM pragma_table_info(?) WHERE name = ?`).get(table, column)
  if (existing) return
  client.exec(sql)
}

type WorkflowArchiveSessionRef = {
  role: WorkflowSessionRef["role"]
  sessionID: SessionID
  milestoneID?: WorkflowMilestoneID
  attempt?: number
}

type WorkflowConsultRequest = {
  targetSessionID?: SessionID
  targetRole?: WorkflowSessionRef["role"]
  targetSpecialty?: string
  timing?: WorkflowConsultationInfo["timing"]
  reason?: string
  modelWeight?: number
  question: string
}

type WorkflowPromptExpectation = {
  description: string
  reminder: string
  matches: (text: string) => boolean
  maxAttempts?: number
}

export const StartInput = Schema.Struct({
  sessionID: Schema.optional(SessionID),
  prompt: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  variant: Schema.optional(Schema.String),
  agent: Schema.optional(Schema.String),
  title: Schema.optional(Schema.String),
  staffing: Schema.optional(WorkflowStaffingConfig),
  scheduling: Schema.optional(WorkflowSchedulingConfig),
  modelWhitelist: Schema.optional(WorkflowModelWhitelistConfig),
}).annotate({ identifier: "WorkflowStartInput" })
export type StartInput = typeof StartInput.Type

export const UpdateXmlInput = Schema.Struct({
  workflowID: WorkflowID,
  xml: Schema.String,
}).annotate({ identifier: "WorkflowUpdateXmlInput" })
export type UpdateXmlInput = typeof UpdateXmlInput.Type

export const UpdateStaffingInput = Schema.Struct({
  workflowID: WorkflowID,
  staffing: WorkflowStaffingConfig,
  modelWhitelist: Schema.optional(WorkflowModelWhitelistConfig),
}).annotate({ identifier: "WorkflowUpdateStaffingInput" })
export type UpdateStaffingInput = typeof UpdateStaffingInput.Type

export const InterveneInput = Schema.Struct({
  workflowID: WorkflowID,
  message: Schema.String,
  sourceSessionID: Schema.optional(SessionID),
  timing: Schema.optional(WorkflowCommunicationTiming),
  targetRole: Schema.optional(WorkflowRole),
  targetSpecialty: Schema.optional(Schema.String),
  targetSessionID: Schema.optional(SessionID),
}).annotate({ identifier: "WorkflowInterveneInput" })
export type InterveneInput = typeof InterveneInput.Type

export const ListInput = Schema.Struct({
  sessionID: Schema.optional(SessionID),
}).annotate({ identifier: "WorkflowListInput" })
export type ListInput = typeof ListInput.Type

export const DoctorInput = Schema.Struct({
  workflowID: Schema.optional(WorkflowID),
  fix: Schema.optional(Schema.Boolean),
  migrate: Schema.optional(Schema.Boolean),
}).annotate({ identifier: "WorkflowDoctorInput" })
export type DoctorInput = typeof DoctorInput.Type

export const DoctorIssue = Schema.Struct({
  severity: Schema.Literals(["error", "warning"]),
  code: Schema.String,
  message: Schema.String,
  workflowID: Schema.optional(WorkflowID),
  path: Schema.optional(Schema.String),
}).annotate({ identifier: "WorkflowDoctorIssue" })
export type DoctorIssue = typeof DoctorIssue.Type

export const DoctorReport = Schema.Struct({
  ok: Schema.Boolean,
  checked: Schema.Number,
  issues: Schema.Array(DoctorIssue),
}).annotate({ identifier: "WorkflowDoctorReport" })
export type DoctorReport = typeof DoctorReport.Type

const CreatedPayloadFields = {
  workflowID: WorkflowID,
  info: WorkflowInfo,
}
const CreatedPayload = Schema.Struct(CreatedPayloadFields).annotate({ identifier: "WorkflowCreatedEvent" })

const UpdatedPayloadFields = {
  workflowID: WorkflowID,
  info: WorkflowInfo,
}
const UpdatedPayload = Schema.Struct(UpdatedPayloadFields).annotate({ identifier: "WorkflowUpdatedEvent" })

const NodeUpdatedPayloadFields = {
  workflowID: WorkflowID,
  milestone: WorkflowMilestone,
}
const NodeUpdatedPayload = Schema.Struct(NodeUpdatedPayloadFields).annotate({ identifier: "WorkflowNodeUpdatedEvent" })

const GraphUpdatedPayloadFields = {
  workflowID: WorkflowID,
  graph: Schema.optional(WorkflowGraph),
}
const GraphUpdatedPayload = Schema.Struct(GraphUpdatedPayloadFields).annotate({ identifier: "WorkflowGraphUpdatedEvent" })

export const Event = {
  Created: EventV2.define({ type: "workflow.created", schema: CreatedPayloadFields }),
  Updated: EventV2.define({ type: "workflow.updated", schema: UpdatedPayloadFields }),
  NodeUpdated: EventV2.define({ type: "workflow.node.updated", schema: NodeUpdatedPayloadFields }),
  GraphUpdated: EventV2.define({ type: "workflow.graph.updated", schema: GraphUpdatedPayloadFields }),
}

export class Error extends Schema.TaggedErrorClass<Error>()("WorkflowError", {
  message: Schema.String,
}) {}

export interface Interface {
  readonly start: (input: StartInput) => Effect.Effect<WorkflowInfo, Error>
  readonly get: (workflowID: WorkflowID) => Effect.Effect<WorkflowInfo, Error>
  readonly list: (input?: ListInput) => Effect.Effect<WorkflowInfo[]>
  readonly doctor: (input?: DoctorInput) => Effect.Effect<DoctorReport, Error>
  readonly graph: (workflowID: WorkflowID) => Effect.Effect<WorkflowGraph, Error>
  readonly updateXml: (input: UpdateXmlInput) => Effect.Effect<WorkflowGraph, Error>
  readonly updateStaffing: (input: UpdateStaffingInput) => Effect.Effect<WorkflowInfo, Error>
  readonly dispatchCommand: (input: WorkflowToolCommand) => Effect.Effect<WorkflowToolCommandResult>
  readonly intervene: (input: InterveneInput) => Effect.Effect<WorkflowInfo, Error>
  readonly continueFromSession: (input: { sessionID: SessionID; message?: string }) => Effect.Effect<WorkflowInfo, Error>
  readonly resume: (workflowID: WorkflowID) => Effect.Effect<WorkflowInfo, Error>
  readonly cancel: (workflowID: WorkflowID) => Effect.Effect<WorkflowInfo, Error>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Workflow") {}

export const use = serviceUse(Service)
export { WorkflowID } from "./schema"

function titleFromRequest(request: string) {
  return request
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find((line) => line.length > 0)
    ?.slice(0, 80)
}

const defaultStaffing = {
  mainPM: 1,
  departmentPM: 2,
  executor: 4,
  reviewer: 2,
  tester: 1,
  expert: 1,
} satisfies Required<WorkflowStaffingConfig>

function normalizeStaffing(input: WorkflowStaffingConfig | undefined) {
  return {
    mainPM: staffLimit(input?.mainPM, defaultStaffing.mainPM, 1),
    departmentPM: staffLimit(input?.departmentPM, defaultStaffing.departmentPM, 1),
    executor: staffLimit(input?.executor, defaultStaffing.executor, 1),
    reviewer: staffLimit(input?.reviewer, defaultStaffing.reviewer, 1),
    tester: staffLimit(input?.tester, defaultStaffing.tester, 1),
    expert: staffLimit(input?.expert, defaultStaffing.expert, 1),
  } satisfies Required<WorkflowStaffingConfig>
}

function normalizeScheduling(input: WorkflowSchedulingConfig | undefined) {
  const mode = input?.mode ?? "eager"
  if (mode === "economical") {
    return {
      mode,
      maxActive: schedulingMaxActive(input?.maxActive),
    }
  }
  return { mode }
}

function schedulingMaxActive(value: number | undefined) {
  if (value === undefined || !Number.isFinite(value)) return 2
  return Math.max(1, Math.min(64, Math.trunc(value)))
}

function workflowSchedulingMode(workflow: WorkflowInfo) {
  return workflow.scheduling?.mode ?? "eager"
}

function workflowSchedulingActiveLimit(workflow: WorkflowInfo) {
  if (workflowSchedulingMode(workflow) !== "economical") return undefined
  return schedulingMaxActive(workflow.scheduling?.maxActive)
}

function staffLimit(value: number | undefined, fallback: number, minimum: number) {
  if (value === undefined || !Number.isFinite(value)) return fallback
  return Math.max(minimum, Math.min(64, Math.trunc(value)))
}

const workflowModelRoles = ["requester", "main_pm", "department_pm", "executor", "reviewer", "tester", "expert"] as const

function workflowModelWhitelistKey(role: WorkflowSessionRef["role"]) {
  if (role === "main_pm") return "mainPM"
  if (role === "department_pm") return "departmentPM"
  return role
}

function workflowModelWeight(value: number | undefined) {
  if (value === undefined || !Number.isFinite(value)) return 50
  return Math.max(0, Math.min(100, value))
}

const workflowDefaultModelCacheMinutes = 240
const workflowStickySwitchMargin = 25

function workflowModelCacheMinutes(value: number | undefined) {
  if (value === undefined || !Number.isFinite(value)) return workflowDefaultModelCacheMinutes
  return Math.max(0, Math.min(43200, Math.trunc(value)))
}

function workflowModelCacheUntil(model: WorkflowInfo["model"] | WorkflowModelWhitelistItem | undefined, now: number) {
  if (!model) return undefined
  const minutes = workflowModelCacheMinutes(model.cacheMinutes)
  return minutes === 0 ? now : now + minutes * 60_000
}

function workflowModelSame(
  a: WorkflowInfo["model"] | WorkflowModelWhitelistItem | undefined,
  b: WorkflowInfo["model"] | WorkflowModelWhitelistItem | undefined,
) {
  return !!a && !!b && a.providerID === b.providerID && a.modelID === b.modelID && (a.variant ?? "") === (b.variant ?? "")
}

function workflowModelRef(model: WorkflowInfo["model"] | WorkflowModelWhitelistItem | undefined) {
  if (!model) return undefined
  return {
    providerID: model.providerID,
    modelID: model.modelID,
    ...(model.variant ? { variant: model.variant } : {}),
  }
}

function workflowModelWeightHint(value: string | undefined) {
  if (value === undefined) return undefined
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return undefined
  return workflowModelWeight(parsed)
}

function normalizeWorkflowModelWhitelist(input: WorkflowModelWhitelistConfig | undefined) {
  if (!input) return undefined
  const normalized = Object.fromEntries(
    workflowModelRoles.flatMap((role) => {
      const key = workflowModelWhitelistKey(role)
      const entries = (input[key] ?? [])
        .filter((item) => item.providerID && item.modelID)
        .map((item) => ({
          providerID: item.providerID,
          modelID: item.modelID,
          ...(item.variant?.trim() ? { variant: item.variant.trim() } : {}),
          weight: workflowModelWeight(item.weight),
          cacheMinutes: workflowModelCacheMinutes(item.cacheMinutes),
        }))
      return entries.length > 0 ? [[key, entries]] : []
    }),
  ) as WorkflowModelWhitelistConfig
  return Object.keys(normalized).length === 0 ? undefined : normalized
}

function workflowModelWhitelistForRole(
  workflow: Pick<WorkflowInfo, "model" | "modelWhitelist">,
  role: WorkflowSessionRef["role"],
) {
  const configured = workflow.modelWhitelist?.[workflowModelWhitelistKey(role)] ?? []
  if (configured.length > 0) return configured
  return workflow.model ? [{ ...workflow.model, weight: 50 }] : []
}

function workflowModelEntryForModel(input: {
  workflow: Pick<WorkflowInfo, "model" | "modelWhitelist">
  role: WorkflowSessionRef["role"]
  model?: WorkflowInfo["model"] | WorkflowModelWhitelistItem
  modelWeight?: number
}) {
  if (!input.model) return undefined
  return (
    workflowModelWhitelistForRole(input.workflow, input.role).find((item) => workflowModelSame(item, input.model)) ??
    (workflowModelSame(input.workflow.model, input.model)
      ? { ...input.model, weight: workflowModelWeight(input.modelWeight) }
      : undefined)
  )
}

function workflowModelRefText(model: WorkflowInfo["model"] | WorkflowModelWhitelistItem | undefined) {
  if (!model) return "default session model"
  return `${model.providerID}/${model.modelID}${model.variant ? ` (${model.variant})` : ""}`
}

function workflowModelComplexity(text: string) {
  const lower = text.toLowerCase()
  const keywords = [
    "architecture",
    "migration",
    "security",
    "performance",
    "concurrency",
    "database",
    "schema",
    "integration",
    "refactor",
    "workflow",
    "renderer",
    "protocol",
    "复杂",
    "架构",
    "迁移",
    "安全",
    "性能",
    "并发",
    "数据库",
    "集成",
    "重构",
  ]
  const keywordScore = keywords.filter((keyword) => lower.includes(keyword)).length * 8
  const lengthScore = Math.min(40, Math.floor(text.length / 180))
  const checklistScore = Math.min(20, (text.match(/\n[-*]|\n\d+\./g)?.length ?? 0) * 3)
  const codeScore = Math.min(20, (text.match(/`|\.tsx?|\.jsx?|\.sql|\.json|\.md|class |function |interface /g)?.length ?? 0) * 2)
  return Math.max(0, Math.min(100, 15 + keywordScore + lengthScore + checklistScore + codeScore))
}

function selectWorkflowModelFromWhitelist(input: {
  workflow: Pick<WorkflowInfo, "model" | "modelWhitelist">
  role: WorkflowSessionRef["role"]
  prompt: string
  modelWeight?: number
  fallback?: WorkflowInfo["model"]
  member?: WorkflowMemberInfo
  now?: number
}) {
  const whitelist = workflowModelWhitelistForRole(input.workflow, input.role)
  if (whitelist.length === 0) return input.fallback
  const selectionWeight = input.modelWeight === undefined ? workflowModelComplexity(input.prompt) : workflowModelWeight(input.modelWeight)
  const best = whitelist
    .toSorted(
      (a, b) =>
        Math.abs(workflowModelWeight(a.weight) - selectionWeight) - Math.abs(workflowModelWeight(b.weight) - selectionWeight) ||
        workflowModelWeight(b.weight) - workflowModelWeight(a.weight),
    )
    .at(0)
  const current = workflowModelEntryForModel({
    workflow: input.workflow,
    role: input.role,
    model: input.member?.model,
    modelWeight: input.member?.modelWeight,
  })
  if (!current) return best
  const now = input.now ?? Date.now()
  if ((input.member?.modelCacheUntil ?? 0) <= now || workflowModelCacheMinutes(current.cacheMinutes) === 0) return best
  const currentDistance = Math.abs(workflowModelWeight(current.weight) - selectionWeight)
  const bestDistance = Math.abs(workflowModelWeight(best?.weight) - selectionWeight)
  return bestDistance + workflowStickySwitchMargin < currentDistance ? best : current
}

function workflowModelSelectionPrompt(input: {
  workflow: Pick<WorkflowInfo, "model" | "modelWhitelist">
  role: WorkflowSessionRef["role"]
  selected?: WorkflowInfo["model"] | WorkflowModelWhitelistItem
  prompt: string
  modelWeight?: number
  member?: WorkflowMemberInfo
}) {
  const whitelist = workflowModelWhitelistForRole(input.workflow, input.role)
  if (whitelist.length === 0) return []
  const selectedWeight = input.modelWeight === undefined ? workflowModelComplexity(input.prompt) : workflowModelWeight(input.modelWeight)
  return [
    "## Workflow Model Selection",
    "",
    `Selected model for this turn: ${workflowModelRefText(input.selected)}`,
    input.modelWeight === undefined
      ? `Estimated task complexity: ${selectedWeight}/100`
      : `Upstream requested model weight: ${selectedWeight}/100`,
    input.member?.model
      ? `Current session cached model: ${workflowModelRefText(input.member.model)}${input.member.modelCacheUntil ? ` until ${new Date(input.member.modelCacheUntil).toISOString()}` : " with no active cache window"}`
      : "Current session cached model: none",
    "Prefer the cached model while its cache is valid unless task difficulty is clearly mismatched. A cache value of 0 minutes means switching cost is ignored.",
    "Role model whitelist; higher weight means stronger reasoning and usually higher token cost:",
    ...whitelist
      .toSorted((a, b) => workflowModelWeight(a.weight) - workflowModelWeight(b.weight))
      .map((item) => `- ${workflowModelRefText(item)} weight=${workflowModelWeight(item.weight)} cache=${workflowModelCacheMinutes(item.cacheMinutes)}m`),
    "When you delegate work, prefer cheaper/lower-weight models for narrow routine tasks and higher-weight models for broad, risky, architectural, or ambiguous tasks.",
  ]
}

function workflowAutorunEnabled() {
  const value = process.env.OPENCODE_WORKFLOW_AUTORUN?.toLowerCase()
  return value !== "0" && value !== "false"
}

function workflowGraphDocumentsEnabled() {
  const value = process.env.OPENCODE_WORKFLOW_GRAPH_DOCUMENTS?.toLowerCase()
  if (value === "1" || value === "true") return true
  if (value === "0" || value === "false") return false
  return process.env.NODE_ENV === "test"
}

function recordWorkflowGraphDiagnostic(graph: WorkflowGraph) {
  const dir = process.env.OPENCODE_SIDECAR_DIAGNOSTIC_DIR
  if (!dir) return
  const record = {
    at: new Date().toISOString(),
    workflowID: graph.workflow.id,
    status: graph.workflow.status,
    nodes: graph.nodes.length,
    edges: graph.edges.length,
    milestones: graph.milestones.length,
    members: graph.members.length,
    consultations: graph.consultations.length,
    interventions: graph.interventions.length,
    nodeTypes: countGraphValues(graph.nodes.map((node) => node.type)),
    edgeKinds: countGraphValues(graph.edges.map((edge) => edge.kind ?? "unknown")),
    approxTextBytes:
      byteLength(graph.workflow.title) +
      byteLength(graph.workflow.request) +
      graph.nodes.reduce((sum, node) => sum + byteLength(node.title) + byteLength(node.summary ?? ""), 0) +
      graph.edges.reduce(
        (sum, edge) =>
          sum +
          byteLength(edge.label ?? "") +
          byteLength(edge.summary ?? "") +
          byteLength(edge.question ?? "") +
          byteLength(edge.answer ?? ""),
        0,
      ),
  }
  try {
    mkdirSync(dir, { recursive: true })
    appendFileSync(path.join(dir, "workflow-graph.jsonl"), JSON.stringify(record) + "\n")
  } catch {}
  if (graph.nodes.length > 500 || graph.edges.length > 1_500 || record.approxTextBytes > 10 * 1024 * 1024) {
    console.warn("[workflow-graph]", JSON.stringify(record))
  }
}

function countGraphValues(values: string[]) {
  return values.reduce<Record<string, number>>((acc, value) => {
    acc[value] = (acc[value] ?? 0) + 1
    return acc
  }, {})
}

function byteLength(value: string) {
  return Buffer.byteLength(value)
}

function staffLimitForRole(staffing: WorkflowStaffingConfig | undefined, role: WorkflowSessionRef["role"]) {
  const config = normalizeStaffing(staffing)
  if (role === "main_pm") return config.mainPM
  if (role === "department_pm") return config.departmentPM
  if (role === "executor") return config.executor
  if (role === "reviewer") return config.reviewer
  if (role === "tester") return config.tester
  if (role === "expert") return config.expert
  return 1
}

function roleBusyForMilestoneStatus(role: WorkflowSessionRef["role"], status: WorkflowMilestoneInfo["status"]) {
  if (role === "department_pm") return status === "planning" || status === "reviewing"
  if (role === "expert") return status === "planning"
  if (role === "executor") return status === "executing" || status === "running"
  if (role === "reviewer") return status === "reviewing"
  if (role === "tester") return status === "testing"
  return false
}

function workflowMemberModelScore(input: {
  member: WorkflowMemberInfo
  workflow?: Pick<WorkflowInfo, "model" | "modelWhitelist">
  role: WorkflowSessionRef["role"]
  prompt?: string
  modelWeight?: number
  now?: number
}) {
  if (!input.workflow || !input.member.model) return 0
  const now = input.now ?? Date.now()
  const target = selectWorkflowModelFromWhitelist({
    workflow: input.workflow,
    role: input.role,
    prompt: input.prompt ?? input.member.specialty,
    modelWeight: input.modelWeight,
    fallback: input.workflow.model,
  })
  const cacheActive = (input.member.modelCacheUntil ?? 0) > now
  if (workflowModelSame(input.member.model, target) && cacheActive) return 4
  const sticky = selectWorkflowModelFromWhitelist({
    workflow: input.workflow,
    role: input.role,
    prompt: input.prompt ?? input.member.specialty,
    modelWeight: input.modelWeight,
    fallback: input.workflow.model,
    member: input.member,
    now,
  })
  if (workflowModelSame(input.member.model, sticky) && cacheActive) return 3
  return 0
}

export function selectWorkflowMember(input: {
  role: WorkflowSessionRef["role"]
  specialty: string
  members: WorkflowMemberInfo[]
  milestones: WorkflowMilestoneInfo[]
  excludeMilestoneID?: WorkflowMilestoneID
  limit?: number
  workflow?: Pick<WorkflowInfo, "model" | "modelWhitelist">
  prompt?: string
  modelWeight?: number
  now?: number
}) {
  const busySessions = new Set(
    input.milestones
      .filter((milestone) => String(milestone.id) !== String(input.excludeMilestoneID ?? ""))
      .filter((milestone) => roleBusyForMilestoneStatus(input.role, milestone.status))
      .flatMap((milestone) =>
        milestone.session.filter((ref) => ref.role === input.role).map((ref) => ref.sessionID),
      ),
  )
  const active = input.members
    .filter((member) => member.role === input.role && member.status === "active")
    .toSorted((a, b) => a.time.created - b.time.created || a.id.localeCompare(b.id))
  const available = active
    .slice(0, input.limit === undefined ? active.length : Math.max(0, Math.trunc(input.limit)))
    .filter((member) => !busySessions.has(member.sessionID))
  const continuity = input.excludeMilestoneID
    ? available.find((member) =>
        input.milestones.some(
          (milestone) =>
            milestone.id === input.excludeMilestoneID &&
            milestone.session.some((ref) => ref.role === input.role && ref.sessionID === member.sessionID),
        ),
      )
    : undefined
  if (continuity) return continuity
  const assignments = new Map<SessionID, number>()
  input.milestones
    .flatMap((milestone) => milestone.session)
    .filter((ref) => ref.role === input.role)
    .forEach((ref) => assignments.set(ref.sessionID, (assignments.get(ref.sessionID) ?? 0) + 1))
  return available.toSorted((a, b) => {
    const specialty = Number(b.specialty === input.specialty) - Number(a.specialty === input.specialty)
    if (specialty !== 0) return specialty
    const model =
      workflowMemberModelScore({
        member: b,
        workflow: input.workflow,
        role: input.role,
        prompt: input.prompt,
        modelWeight: input.modelWeight,
        now: input.now,
      }) -
      workflowMemberModelScore({
        member: a,
        workflow: input.workflow,
        role: input.role,
        prompt: input.prompt,
        modelWeight: input.modelWeight,
        now: input.now,
      })
    if (model !== 0) return model
    const load = (assignments.get(a.sessionID) ?? 0) - (assignments.get(b.sessionID) ?? 0)
    if (load !== 0) return load
    return a.time.created - b.time.created || a.id.localeCompare(b.id)
  })[0]
}

function roleSpecialty(role: WorkflowSessionRef["role"], specialty?: string) {
  const explicit = specialty?.trim()
  if (explicit) return explicit
  if (role === "main_pm") return "strategy"
  if (role === "department_pm") return "product"
  if (role === "executor") return "engineering"
  if (role === "reviewer") return "functional-review"
  if (role === "tester") return "quality"
  if (role === "expert") return "technical-advisory"
  return "request"
}

function staffSlug(value: string) {
  return (
    value
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 48) || "general"
  )
}

function workflowMessageKind(value: unknown): WorkflowMessageKind | undefined {
  return workflowMessageKinds.find((item) => item === value)
}

function workflowMemberID(role: WorkflowSessionRef["role"], specialty: string, index: number) {
  return `${role}:${staffSlug(specialty)}:${index}`
}

function workflowMemberTitle(role: WorkflowSessionRef["role"], specialty: string, index: number) {
  const suffix = index > 1 ? ` #${index}` : ""
  return `${roleSessionTitle(role)}: ${specialty}${suffix}`
}

function workflowPath(id: WorkflowID, ...segments: string[]) {
  return path.join(workflowDir, id, ...segments)
}

function workflowArtifactPath(workflow: Pick<WorkflowInfo, "id" | "path">, ...segments: string[]) {
  return path.join(workflow.path || workflowPath(workflow.id), ...segments)
}

function projectWorkflowPath(directory: string, workflow: Pick<WorkflowInfo, "id" | "path">, ...segments: string[]) {
  return path.join(directory, workflowArtifactPath(workflow, ...segments))
}

function workflowFolderPath(workflowID: WorkflowID) {
  return path.join(workflowDir, workflowFolderID(workflowID))
}

function workflowFolderID(workflowID: WorkflowID) {
  return String(workflowID)
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
}

function isLegacyWorkflowPath(workflow: Pick<WorkflowInfo, "id" | "path">) {
  return path.normalize(workflow.path) !== path.normalize(workflowPath(workflow.id))
}

function rewriteWorkflowStoredPath(workflow: Pick<WorkflowInfo, "id" | "path">, stored: string) {
  return rewriteStoredPathPrefix(stored, workflowPath(workflow.id), workflow.path)
}

function rewriteStoredPathPrefix(stored: string, fromPath: string, toPath: string) {
  const from = path.normalize(fromPath)
  const current = path.normalize(stored)
  if (current === from) return toPath
  if (current.startsWith(from + path.sep)) return path.join(toPath, path.relative(from, current))
  return stored
}

function workflowStoredPath(workflow: Pick<WorkflowInfo, "id" | "path">, stored: string | undefined, ...segments: string[]) {
  if (stored) return rewriteWorkflowStoredPath(workflow, stored)
  return workflowArtifactPath(workflow, ...segments)
}

function containedPath(root: string, file: string) {
  const rel = path.relative(path.resolve(root), path.resolve(file))
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel))
}

function normalizedRelativePath(root: string, file: string) {
  const full = path.isAbsolute(file) ? file : path.resolve(root, file)
  if (!containedPath(root, full)) return
  return path.relative(root, full)
}

function workflowRelativeFile(workflow: WorkflowInfo, file: string) {
  if (!containedPath(path.resolve(workflow.directory, workflow.path), file)) return
  return path.relative(path.resolve(workflow.directory, workflow.path), file)
}

function workflowPlanFileMilestoneID(relative: string) {
  const normalized = path.normalize(relative)
  if (path.basename(normalized).toLowerCase() !== "plan.md") return
  const parent = path.basename(path.dirname(normalized))
  if (!parent || parent === "." || parent === "planning" || parent === "reference") return
  return WorkflowMilestoneID.make(parent)
}

function workflowFileSignature(event: "add" | "change" | "unlink", file: string, size?: number, mtimeMs?: number) {
  if (event === "unlink") return "unlink"
  return `${size ?? 0}:${mtimeMs ?? 0}`
}

function codexContextFile(relative: string) {
  const normalized = path.normalize(relative)
  return (
    normalized.startsWith(path.join(".codex", "skills") + path.sep) ||
    normalized.startsWith(path.join(".codex", "plans") + path.sep)
  )
}

async function exists(file: string) {
  try {
    await stat(file)
    return true
  } catch {
    return false
  }
}

async function ensureWorkflowDirectory(directory: string, fromPath: string, toPath: string) {
  const from = path.join(directory, fromPath)
  const to = path.join(directory, toPath)
  if (path.normalize(fromPath) !== path.normalize(toPath) && (await exists(from)) && !(await exists(to))) {
    await cp(from, to, { recursive: true })
    return
  }
  await mkdir(to, { recursive: true })
}

const workflowFileWriteChains = new Map<string, Promise<void>>()
const workflowToolCommandChains = new Map<string, Promise<void>>()
const workflowMemberAssignmentChains = new Map<string, Promise<void>>()
let workflowWriteFaultKey = ""
let workflowWriteFaultCounter = 0

function workflowWriteFaultPoint(label: string) {
  const spec = process.env.OPENCODE_WORKFLOW_WRITE_FAULT_AT
  if (!spec) {
    workflowWriteFaultKey = ""
    workflowWriteFaultCounter = 0
    return
  }
  const key = `${process.env.OPENCODE_WORKFLOW_WRITE_FAULT_RUN ?? ""}:${spec}`
  if (key !== workflowWriteFaultKey) {
    workflowWriteFaultKey = key
    workflowWriteFaultCounter = 0
  }
  const index = ++workflowWriteFaultCounter
  if (!spec.split(/[,\s]+/).filter(Boolean).some((item) => item === String(index) || item === label || item === `${label}:${index}`)) {
    return
  }
  const signalFile = process.env.OPENCODE_WORKFLOW_WRITE_FAULT_SIGNAL_FILE
  if (signalFile) {
    mkdirSync(path.dirname(signalFile), { recursive: true })
    writeFileSync(signalFile, `${JSON.stringify({ label, pid: process.pid, point: index, time: Date.now() })}\n`)
    Atomics.wait(
      new Int32Array(new SharedArrayBuffer(4)),
      0,
      0,
      Number(process.env.OPENCODE_WORKFLOW_WRITE_FAULT_WAIT_MS ?? 30000),
    )
  }
  throw new globalThis.Error(`workflow write fault injected at ${label}#${index}`)
}

async function withWorkflowFileWriteQueue<T>(file: string, write: () => Promise<T>) {
  const key = path.resolve(file).toLowerCase()
  const previous = workflowFileWriteChains.get(key) ?? Promise.resolve()
  let release = () => {}
  const current = new Promise<void>((done) => {
    release = done
  })
  const chain = previous.catch(() => {}).then(() => current)
  workflowFileWriteChains.set(key, chain)
  await previous.catch(() => {})
  try {
    return await write()
  } finally {
    release()
    if (workflowFileWriteChains.get(key) === chain) workflowFileWriteChains.delete(key)
  }
}

async function writeFileEnsured(file: string, content: string) {
  return withWorkflowFileWriteQueue(file, async () => {
    await assertWorkflowArtifactWritableByEngine(file)
    await mkdir(path.dirname(file), { recursive: true })
    workflowWriteFaultPoint("write:after-mkdir")
    const temp = path.join(
      path.dirname(file),
      `.tmp-${path.basename(file)}-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    )
    const handle = await open(temp, "w")
    try {
      workflowWriteFaultPoint("write:after-open")
      await handle.writeFile(content)
      workflowWriteFaultPoint("write:after-write")
      await handle.sync()
      workflowWriteFaultPoint("write:after-sync")
    } finally {
      await handle.close().catch(() => {})
    }
    workflowWriteFaultPoint("write:before-rename")
    try {
      await rename(temp, file)
      workflowWriteFaultPoint("write:after-rename")
    } catch (error) {
      await rm(temp, { force: true }).catch(() => {})
      throw error
    }
  })
}

async function appendFileEnsured(file: string, content: string) {
  return withWorkflowFileWriteQueue(file, async () => {
    await assertWorkflowArtifactWritableByEngine(file)
    await mkdir(path.dirname(file), { recursive: true })
    workflowWriteFaultPoint("append:before")
    await appendFile(file, content)
    workflowWriteFaultPoint("append:after")
  })
}

async function writeFileEnsuredIfMissing(file: string, content: string) {
  if (await exists(file)) return
  await writeFileEnsured(file, content)
}

async function assertWorkflowArtifactWritableByEngine(file: string) {
  const location = workflowArtifactLocationFromFile(file)
  if (!location) return
  if (location.relative === workflowManifestFileName) return
  const manifest = await readWorkflowManifestFileUnchecked(path.join(location.root, workflowManifestFileName)).catch(
    (error) => {
      if (nodeErrorCode(error) === "ENOENT") return undefined
      throw new globalThis.Error(
        `Workflow engine cannot write ${location.relative}: manifest.json is not readable; run workflow doctor before continuing.`,
      )
    },
  )
  if (!manifest) return
  const owner = workflowManifestOwner(manifest, location.relative)
  if (!owner || owner === "engine" || owner === "engine-append") return
  throw new globalThis.Error(
    `Workflow engine cannot write ${location.relative}: ownership is ${owner}. Engine writes are limited to engine and engine-append paths.`,
  )
}

function workflowArtifactLocationFromFile(file: string) {
  const segments = path.resolve(file).split(path.sep)
  const opencodeIndex = segments.lastIndexOf(".opencode")
  if (opencodeIndex < 0) return
  if (segments[opencodeIndex + 1] !== "workflows") return
  const workflowIndex = opencodeIndex + 2
  if (!segments[workflowIndex]) return
  const relative = segments.slice(workflowIndex + 1).join("/")
  if (!relative) return
  return {
    root: segments.slice(0, workflowIndex + 1).join(path.sep),
    relative,
  }
}

function workflowManifestOwner(manifest: { ownership?: Record<string, string> }, relativePath: string) {
  const ownership = manifest.ownership ?? {}
  return Object.entries(ownership)
    .filter(([pattern]) => workflowOwnershipPatternMatches(pattern, relativePath))
    .toSorted((a, b) => workflowOwnershipPatternScore(b[0]) - workflowOwnershipPatternScore(a[0]))[0]?.[1]
}

function workflowOwnershipPatternMatches(pattern: string, relativePath: string) {
  const normalized = pattern.replaceAll("\\", "/")
  if (normalized === relativePath) return true
  if (!normalized.endsWith("/**")) return false
  const prefix = normalized.slice(0, -3)
  return relativePath === prefix.slice(0, -1) || relativePath.startsWith(prefix)
}

function workflowOwnershipPatternScore(pattern: string) {
  return pattern.replaceAll("*", "").length
}

function ensureSchema() {
  const client = Database.Client().$client
  if (schemaEnsuredClient === client) return
  client.exec(ensureSchemaSql)
  const rootSession = client
    .prepare("SELECT [notnull] FROM pragma_table_info('workflow') WHERE name = 'root_session_id'")
    .get() as { notnull: number } | undefined
  if (rootSession?.notnull) client.exec(ensureWorkflowOwnershipSql)
  if (client.prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '__drizzle_migrations'").get()) {
    client.exec(ensureWorkflowMigrationSql)
  }
  ensureColumn("workflow", "root_session_id", "ALTER TABLE workflow ADD COLUMN root_session_id text")
  ensureColumn("workflow", "pm_session_id", "ALTER TABLE workflow ADD COLUMN pm_session_id text")
  ensureColumn("workflow", "tester_session_id", "ALTER TABLE workflow ADD COLUMN tester_session_id text")
  ensureColumn("workflow", "directory", "ALTER TABLE workflow ADD COLUMN directory text NOT NULL DEFAULT ''")
  ensureColumn("workflow", "path", "ALTER TABLE workflow ADD COLUMN path text NOT NULL DEFAULT ''")
  ensureColumn("workflow", "xml", "ALTER TABLE workflow ADD COLUMN xml text NOT NULL DEFAULT ''")
  ensureColumn("workflow", "status", "ALTER TABLE workflow ADD COLUMN status text NOT NULL DEFAULT 'planning'")
  ensureColumn("workflow", "staffing", "ALTER TABLE workflow ADD COLUMN staffing text")
  ensureColumn("workflow", "scheduling", "ALTER TABLE workflow ADD COLUMN scheduling text")
  ensureColumn("workflow", "model", "ALTER TABLE workflow ADD COLUMN model text")
  ensureColumn("workflow", "model_whitelist", "ALTER TABLE workflow ADD COLUMN model_whitelist text")
  ensureColumn("workflow", "agent", "ALTER TABLE workflow ADD COLUMN agent text")
  ensureColumn("workflow", "test_path", "ALTER TABLE workflow ADD COLUMN test_path text")
  ensureColumn("workflow", "error", "ALTER TABLE workflow ADD COLUMN error text")
  ensureColumn("workflow", "time_created", "ALTER TABLE workflow ADD COLUMN time_created integer NOT NULL DEFAULT 0")
  ensureColumn("workflow", "time_updated", "ALTER TABLE workflow ADD COLUMN time_updated integer NOT NULL DEFAULT 0")
  ensureColumn("workflow", "time_completed", "ALTER TABLE workflow ADD COLUMN time_completed integer")
  ensureColumn("workflow_member", "capacity", "ALTER TABLE workflow_member ADD COLUMN capacity integer NOT NULL DEFAULT 1")
  ensureColumn("workflow_member", "status", "ALTER TABLE workflow_member ADD COLUMN status text NOT NULL DEFAULT 'active'")
  ensureColumn("workflow_member", "availability", "ALTER TABLE workflow_member ADD COLUMN availability text")
  ensureColumn("workflow_member", "current_focus", "ALTER TABLE workflow_member ADD COLUMN current_focus text")
  ensureColumn("workflow_member", "blockers", "ALTER TABLE workflow_member ADD COLUMN blockers text")
  ensureColumn("workflow_member", "progress_note", "ALTER TABLE workflow_member ADD COLUMN progress_note text")
  ensureColumn("workflow_member", "model", "ALTER TABLE workflow_member ADD COLUMN model text")
  ensureColumn("workflow_member", "model_weight", "ALTER TABLE workflow_member ADD COLUMN model_weight integer")
  ensureColumn("workflow_member", "model_cache_until", "ALTER TABLE workflow_member ADD COLUMN model_cache_until integer")
  ensureColumn("workflow_member", "time_created", "ALTER TABLE workflow_member ADD COLUMN time_created integer NOT NULL DEFAULT 0")
  ensureColumn("workflow_member", "time_updated", "ALTER TABLE workflow_member ADD COLUMN time_updated integer NOT NULL DEFAULT 0")
  ensureColumn("workflow_milestone", "attempt", "ALTER TABLE workflow_milestone ADD COLUMN attempt integer NOT NULL DEFAULT 0")
  ensureColumn("workflow_milestone", "review", "ALTER TABLE workflow_milestone ADD COLUMN review text")
  ensureColumn("workflow_milestone", "waiting_for", "ALTER TABLE workflow_milestone ADD COLUMN waiting_for text")
  ensureColumn("workflow_milestone", "plan_path", "ALTER TABLE workflow_milestone ADD COLUMN plan_path text")
  ensureColumn("workflow_milestone", "review_path", "ALTER TABLE workflow_milestone ADD COLUMN review_path text")
  ensureColumn("workflow_milestone", "session", "ALTER TABLE workflow_milestone ADD COLUMN session text NOT NULL DEFAULT '[]'")
  ensureColumn("workflow_milestone", "time_created", "ALTER TABLE workflow_milestone ADD COLUMN time_created integer NOT NULL DEFAULT 0")
  ensureColumn("workflow_milestone", "time_updated", "ALTER TABLE workflow_milestone ADD COLUMN time_updated integer NOT NULL DEFAULT 0")
  ensureColumn("workflow_consultation", "reason", "ALTER TABLE workflow_consultation ADD COLUMN reason text")
  ensureColumn("workflow_consultation", "timing", "ALTER TABLE workflow_consultation ADD COLUMN timing text")
  ensureColumn("workflow_intervention", "path", "ALTER TABLE workflow_intervention ADD COLUMN path text NOT NULL DEFAULT ''")
  ensureColumn("workflow_intervention", "status", "ALTER TABLE workflow_intervention ADD COLUMN status text NOT NULL DEFAULT 'queued'")
  ensureColumn("workflow_intervention", "time_created", "ALTER TABLE workflow_intervention ADD COLUMN time_created integer NOT NULL DEFAULT 0")
  ensureColumn("workflow_intervention", "time_updated", "ALTER TABLE workflow_intervention ADD COLUMN time_updated integer NOT NULL DEFAULT 0")
  schemaEnsuredClient = client
}

function toInfo(row: typeof WorkflowTable.$inferSelect): WorkflowInfo {
  return {
    id: row.id,
    projectID: row.project_id,
    rootSessionID: row.root_session_id ?? undefined,
    pmSessionID: row.pm_session_id ?? undefined,
    testerSessionID: row.tester_session_id ?? undefined,
    request: row.request,
    title: row.title,
    directory: row.directory,
    path: row.path,
    xml: row.xml,
    status: row.status,
    staffing: row.staffing ?? undefined,
    scheduling: row.scheduling ?? undefined,
    model: row.model ?? undefined,
    modelWhitelist: row.model_whitelist ?? undefined,
    agent: row.agent ?? undefined,
    testPath: row.test_path ?? undefined,
    error: row.error ?? undefined,
    time: {
      created: row.time_created,
      updated: row.time_updated,
      completed: row.time_completed ?? undefined,
    },
  }
}

function toListInfo(row: typeof WorkflowTable.$inferSelect): WorkflowInfo {
  const info = toInfo(row)
  return {
    ...info,
    request: compactMarkdown(info.request, 800),
    xml: "",
  }
}

function toMilestone(row: typeof WorkflowMilestoneTable.$inferSelect): WorkflowMilestoneInfo {
  return {
    id: row.id,
    title: row.title ?? undefined,
    department: row.department ?? undefined,
    review: row.review ?? undefined,
    waitingFor: row.waiting_for ?? undefined,
    prompt: row.prompt,
    dependsOn: row.depends_on,
    status: row.status,
    attempt: row.attempt,
    planPath: row.plan_path ?? undefined,
    reviewPath: row.review_path ?? undefined,
    session: row.session,
  }
}

function toMember(row: typeof WorkflowMemberTable.$inferSelect): WorkflowMemberInfo {
  return {
    id: row.id,
    workflowID: row.workflow_id,
    role: row.role,
    specialty: row.specialty,
    title: row.title,
    sessionID: row.session_id,
    capacity: row.capacity,
    status: row.status,
    availability: row.availability ?? undefined,
    currentFocus: row.current_focus ?? undefined,
    blockers: row.blockers ?? [],
    progressNote: row.progress_note ?? undefined,
    model: row.model ?? undefined,
    modelWeight: row.model_weight ?? undefined,
    modelCacheUntil: row.model_cache_until ?? undefined,
    time: {
      created: row.time_created,
      updated: row.time_updated,
    },
  }
}

function toIntervention(row: typeof WorkflowInterventionTable.$inferSelect): WorkflowInterventionInfo {
  return {
    id: row.id,
    workflowID: row.workflow_id,
    fromSessionID: row.from_session_id ?? undefined,
    targetSessionID: row.target_session_id ?? undefined,
    targetRole: row.target_role,
    timing: row.timing,
    message: row.message,
    response: row.response ?? undefined,
    path: row.path,
    status: row.status,
    time: {
      created: row.time_created,
      updated: row.time_updated,
    },
  }
}

function compactGraphConsultation(item: WorkflowConsultationInfo): WorkflowConsultationInfo {
  return {
    ...item,
    question: compactMarkdown(item.question, 800),
    answer: compactMarkdown(item.answer, 800),
    reason: item.reason ? compactMarkdown(item.reason, 240) : undefined,
  }
}

function compactGraphIntervention(item: WorkflowInterventionInfo): WorkflowInterventionInfo {
  return {
    ...item,
    message: compactMarkdown(item.message, 800),
    response: item.response ? compactMarkdown(item.response, 800) : undefined,
  }
}

function modelFromInput(input: string | undefined, variant: string | undefined) {
  if (!input) return undefined
  if (!input.includes("/")) throw new globalThis.Error(`Workflow model must be in provider/model format: ${input}`)
  const parsed = Provider.parseModel(input)
  if (!parsed.providerID || !parsed.modelID) throw new globalThis.Error(`Workflow model must be in provider/model format: ${input}`)
  const trimmed = variant?.trim()
  return { providerID: parsed.providerID, modelID: parsed.modelID, ...(trimmed ? { variant: trimmed } : {}) }
}

function modelRef(model: WorkflowInfo["model"] | undefined) {
  if (!model) return undefined
  return { providerID: model.providerID, modelID: model.modelID }
}

function textPart(text: string): MessageV2.TextPartInput {
  return { type: "text", text }
}

function latestText(message: MessageV2.WithParts | undefined) {
  return message?.parts.findLast((part) => part.type === "text")?.text ?? ""
}

function workflowSessionArchivePath(sessionID: SessionID) {
  return `session_${String(sessionID).replace(/^ses_?/, "")}.md`
}

function workflowSessionSummaryPath(sessionID: SessionID) {
  return path.join("reference", `session_${String(sessionID).replace(/^ses_?/, "")}-summary.md`)
}

function workflowReferenceIndexPath() {
  return path.join("reference", "index.md")
}

function workflowRequesterMemoryPath() {
  return path.join("reference", "requester.md")
}

function workflowStaffMemoryPath(member: Pick<WorkflowMemberInfo, "id">) {
  return path.join("reference", "staff", `${staffSlug(member.id)}.md`)
}

function workflowMainPlanPath() {
  return path.join("planning", "main-plan.md")
}

function workflowMilestoneArtifactsPath(milestoneID: WorkflowMilestoneID, ...segments: string[]) {
  return path.join(String(milestoneID), "artifacts", ...segments)
}

function workflowMilestoneDecompositionPath(milestoneID: WorkflowMilestoneID) {
  return workflowMilestoneArtifactsPath(milestoneID, "decomposition.md")
}

function workflowMilestoneReviewPath(milestoneID: WorkflowMilestoneID, attempt: number) {
  return path.join(String(milestoneID), "reviews", `review-${attempt}.md`)
}

function workflowConsultationIndexPath() {
  return path.join("reference", "consultations", "index.md")
}

function workflowConsultationPath(id: string) {
  return path.join("reference", "consultations", `${id}.md`)
}

function workflowInterventionIndexPath() {
  return path.join("interventions", "index.md")
}

function workflowInterventionPath(id: string) {
  return path.join("interventions", `${id}.md`)
}

function workflowStandupIndexPath() {
  return path.join("standups", "index.md")
}

function workflowStandupPath(id: string) {
  return path.join("standups", `${id}.md`)
}

function workflowAcceptancePath(role: "main_pm" | "requester") {
  return path.join("acceptance", role === "main_pm" ? "main-pm.md" : "requester.md")
}

function workflowDeliverySummaryPath() {
  return "delivery-summary.md"
}

function workflowExpertNotePath(milestoneID: WorkflowMilestoneID, attempt: number) {
  return workflowMilestoneArtifactsPath(milestoneID, `expert-${attempt}.md`)
}

function workflowTestPlanPath() {
  return path.join("final", "test-plan.md")
}

function workflowCommandJournalPath() {
  return path.join("journal", "commands.jsonl")
}

function workflowMessageJournalPath() {
  return path.join("journal", "messages.jsonl")
}

function workflowEventJournalPath() {
  return path.join("journal", "events.jsonl")
}

function workflowTechnicalAssessmentPath() {
  return path.join("final", "technical-assessment.md")
}

function workflowProjectionPaths(workflow: WorkflowInfo, staff: WorkflowMemberInfo[]) {
  return Array.from(
    new Set([
      workflowArtifactPath(workflow, "index.md"),
      workflowArtifactPath(workflow, "organization.md"),
      workflowArtifactPath(workflow, "progress.md"),
      workflowArtifactPath(workflow, workflowReferenceIndexPath()),
      workflowArtifactPath(workflow, workflowRequesterMemoryPath()),
      workflowArtifactPath(workflow, workflowConsultationIndexPath()),
      workflowArtifactPath(workflow, workflowInterventionIndexPath()),
      workflowArtifactPath(workflow, workflowStandupIndexPath()),
      workflowArtifactPath(workflow, workflowDeliverySummaryPath()),
      ...staff.map((member) => workflowArtifactPath(workflow, workflowStaffMemoryPath(member))),
    ]),
  )
}

function markdownFence(value: string, language = "") {
  const marker = value.includes("```") ? "````" : "```"
  return `${marker}${language}\n${value}\n${marker}`
}

function jsonFence(value: unknown) {
  return markdownFence(JSON.stringify(value, null, 2), "json")
}

function archivePart(part: MessageV2.Part) {
  if (part.type === "text") return part.text
  if (part.type === "reasoning") return ["#### Reasoning", "", part.text].join("\n")
  if (part.type === "file") return [`#### File: ${part.filename ?? part.url}`, "", jsonFence(part)].join("\n")
  if (part.type === "agent") return [`#### Agent: ${part.name}`, "", jsonFence(part)].join("\n")
  if (part.type === "subtask") return [`#### Subtask: ${part.description}`, "", jsonFence(part)].join("\n")
  if (part.type === "tool") return [`#### Tool: ${part.tool}`, "", jsonFence(part)].join("\n")
  return [`#### ${part.type}`, "", jsonFence(part)].join("\n")
}

function archiveSessionHeaderMarkdown(input: {
  workflow: WorkflowInfo
  session: Session.Info
  role: WorkflowSessionRef["role"]
  prompt?: string
  milestoneID?: WorkflowMilestoneID
  attempt?: number
}) {
  return [
    `# ${input.session.title}`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Workflow title: ${input.workflow.title}`,
    `Workflow path: ${input.workflow.path}`,
    `Session: ${input.session.id}`,
    `Role: ${roleSessionTitle(input.role)}`,
    ...(input.milestoneID ? [`Milestone: ${input.milestoneID}`] : []),
    ...(input.attempt !== undefined ? [`Attempt: ${input.attempt}`] : []),
    `Agent: ${input.session.agent ?? "default"}`,
    `Created: ${new Date(input.session.time.created).toISOString()}`,
    `Updated: ${new Date(input.session.time.updated).toISOString()}`,
    "",
    "## User Requirement",
    "",
    input.workflow.request,
    "",
    ...(input.prompt ? ["## Initial Prompt", "", input.prompt, ""] : []),
    "## Messages",
    "",
  ].join("\n")
}

function archiveMessageMarkdown(message: MessageV2.WithParts) {
  return [
    `### ${message.info.role} ${message.info.id}`,
    "",
    `Created: ${new Date(message.info.time.created).toISOString()}`,
    ...(message.info.role === "assistant"
      ? [
          ...(message.info.time.completed ? [`Completed: ${new Date(message.info.time.completed).toISOString()}`] : []),
          `Agent: ${message.info.agent}`,
          `Model: ${message.info.providerID}/${message.info.modelID}`,
          ...(message.info.variant ? [`Variant: ${message.info.variant}`] : []),
          `Finish: ${message.info.finish ?? "unknown"}`,
          ...(message.info.error ? ["", "#### Error", "", jsonFence(message.info.error)] : []),
        ]
      : [
          `Agent: ${message.info.agent}`,
          `Model: ${message.info.model.providerID}/${message.info.model.modelID}`,
          ...(message.info.model.variant ? [`Variant: ${message.info.model.variant}`] : []),
        ]),
    "",
    ...message.parts.flatMap((part) => [archivePart(part), ""]),
  ].join("\n")
}

const archiveWorkflowSessionMessages = Effect.fn("Workflow.archiveWorkflowSessionMessages")(function* (input: {
  workflow: WorkflowInfo
  session: Session.Info
  messages: MessageV2.WithParts[]
  role: WorkflowSessionRef["role"]
  prompt?: string
  milestoneID?: WorkflowMilestoneID
  attempt?: number
  file: string
}) {
  yield* Effect.promise(() =>
    writeFileEnsured(
      input.file,
      [
        archiveSessionHeaderMarkdown(input),
        "Message order: newest first.",
        "",
      ].join("\n"),
    ),
  )
  if (input.messages.length > 0) {
    yield* Effect.promise(() =>
      appendFileEnsured(
        input.file,
        `${input.messages
          .toReversed()
          .map(archiveMessageMarkdown)
          .join("\n")}\n`,
      ),
    )
    return
  }
  yield* Effect.promise(() => appendFileEnsured(input.file, "_No messages recorded yet._\n"))
})

function archiveSessionMarkdown(input: {
  workflow: WorkflowInfo
  session: Session.Info
  role: WorkflowSessionRef["role"]
  prompt?: string
  milestoneID?: WorkflowMilestoneID
  attempt?: number
  messages: MessageV2.WithParts[]
}) {
  return [
    archiveSessionHeaderMarkdown(input),
    input.messages.length === 0 ? "_No messages recorded yet._" : input.messages.map(archiveMessageMarkdown).join("\n"),
    "",
  ].join("\n")
}

function messageText(message: MessageV2.WithParts) {
  return message.parts
    .filter((part) => part.type === "text")
    .map((part) => part.text.trim())
    .filter(Boolean)
    .join("\n\n")
}

function compactMarkdown(text: string, limit = 2400) {
  const trimmed = text.replace(/\r\n/g, "\n").trim()
  if (trimmed.length <= limit) return trimmed
  return `${trimmed.slice(0, limit).trimEnd()}\n\n...`
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

export function extractHandoffSummary(text: string) {
  const match = /\n?#{2,6}\s+Handoff Summary\s*\n([\s\S]*?)(?=\n#{1,6}\s+\S|$)/i.exec(text.replace(/\r\n/g, "\n"))
  const summary = match?.[1]?.trim()
  return summary || undefined
}

function extractReferenceSummary(text: string) {
  const match = /\n?#{2,6}\s+Reference Summary\s*\n([\s\S]*?)(?=\n#{1,6}\s+\S|$)/i.exec(text.replace(/\r\n/g, "\n"))
  const summary = match?.[1]?.trim()
  return summary || undefined
}

function archiveSessionSummaryMarkdown(input: {
  workflow: WorkflowInfo
  session: Session.Info
  role: WorkflowSessionRef["role"]
  milestoneID?: WorkflowMilestoneID
  attempt?: number
  messages: MessageV2.WithParts[]
}) {
  const assistant = input.messages.filter((message) => message.info.role === "assistant").findLast((message) => messageText(message))
  const user = input.messages.filter((message) => message.info.role === "user").find((message) => messageText(message))
  const assistantText = assistant ? messageText(assistant) : ""
  const handoff = extractHandoffSummary(assistantText)
  return [
    `# ${input.session.title}`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Role: ${roleSessionTitle(input.role)}`,
    `Session: ${input.session.id}`,
    ...(input.milestoneID ? [`Milestone: ${input.milestoneID}`] : []),
    ...(input.attempt !== undefined ? [`Attempt: ${input.attempt}`] : []),
    `Full archive: ../${workflowSessionArchivePath(input.session.id)}`,
    "",
    "## Reference Summary",
    "",
    handoff
      ? compactMarkdown(handoff)
      : compactMarkdown(assistantText || "_No completed assistant summary is available yet._"),
    "",
    ...(user ? ["## Initial Request To This Session", "", compactMarkdown(messageText(user), 1200), ""] : []),
    "## Consultation",
    "",
    `Other workflow sessions can consult this session with: <opencode-workflow-consult target-session="${input.session.id}" reason="short reason" model-weight="0-100">question</opencode-workflow-consult>`,
    `They can also ask by role with: <opencode-workflow-message to-role="${input.role}" timing="temporary-interrupt" reason="short reason" model-weight="0-100">question</opencode-workflow-message>`,
    "",
  ].join("\n")
}

function referenceIndexMarkdown(input: {
  workflow: WorkflowInfo
  milestones: WorkflowMilestoneInfo[]
  members: WorkflowMemberInfo[]
  consultations: WorkflowConsultationInfo[]
  interventions: WorkflowInterventionInfo[]
  standupDocs: WorkflowStandupDoc[]
}) {
  const refs: WorkflowArchiveSessionRef[] = [
    ...(input.workflow.rootSessionID ? [{ role: "requester" as const, sessionID: input.workflow.rootSessionID }] : []),
    ...(input.workflow.pmSessionID ? [{ role: "main_pm" as const, sessionID: input.workflow.pmSessionID }] : []),
    ...(input.workflow.testerSessionID ? [{ role: "tester" as const, sessionID: input.workflow.testerSessionID }] : []),
    ...input.members.map((member) => ({ role: member.role, sessionID: member.sessionID })),
    ...input.milestones.flatMap((milestone) =>
      milestone.session.map((ref) => ({
        ...ref,
        milestoneID: ref.milestoneID ?? milestone.id,
      })),
    ),
  ]
  const sessionRows = Array.from(
    new Map(
      refs.map((ref) => [
        [ref.sessionID, ref.role, ref.milestoneID ?? "", ref.attempt ?? ""].join(":"),
        ref,
      ]),
    ).values(),
  )
  const advisorNotes = input.milestones.flatMap((milestone) =>
    milestone.session
      .filter((ref) => ref.role === "expert")
      .map((ref) => ({
        milestone,
        attempt: ref.attempt ?? milestone.attempt,
        sessionID: ref.sessionID,
      })),
  )
  return [
    `# ${input.workflow.title} Reference Library`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Status: ${input.workflow.status}`,
    "",
    "## Company Operating Documents",
    "",
    `- Company organization: organization.md`,
    `- Workflow progress: progress.md`,
    `- Main PM plan: ${workflowMainPlanPath()}`,
    `- Workflow XML: workflow.xml`,
    `- Local archive index: index.md`,
    `- Delivery summary: ${workflowDeliverySummaryPath()}`,
    `- Consultation archive: ${workflowConsultationIndexPath()}`,
    `- Company standups: ${workflowStandupIndexPath()}`,
    `- Requester interventions: ${workflowInterventionIndexPath()}`,
    "",
    "## Session Summaries",
    "",
    `Organization chart: organization.md`,
    "",
    ...(sessionRows.length === 0
      ? ["_No session summaries have been archived yet._"]
      : sessionRows.map(
          (ref) =>
            `- ${roleSessionTitle(ref.role)}${ref.milestoneID ? ` / ${ref.milestoneID}` : ""}${ref.attempt !== undefined ? ` / attempt ${ref.attempt}` : ""}: ${workflowSessionSummaryPath(ref.sessionID)} (full: ${workflowSessionArchivePath(ref.sessionID)})`,
        )),
    "",
    "## Strategic Owner Memory",
    "",
    `Requester memory: ${workflowRequesterMemoryPath()}`,
    "",
    "## Staff Memory",
    "",
    ...(input.members.length === 0
      ? ["_No staff memory files have been created yet._"]
      : input.members.map((member) => `- ${member.title} [${member.role}/${member.specialty}]: ${workflowStaffMemoryPath(member)}`)),
    "",
    "## Milestone Plans",
    "",
    ...(input.milestones.length === 0
      ? ["_No milestone plans have been created yet._"]
      : input.milestones.map(
          (milestone) =>
            `- ${milestone.id} [${milestone.status}]: ${workflowStoredPath(input.workflow, milestone.planPath, milestone.id, "plan.md")}`,
        )),
    "",
    "## Technical Advisor Notes",
    "",
    ...(advisorNotes.length === 0
      ? ["_No technical advisor notes have been created yet._"]
      : advisorNotes.map(
          (note) =>
            `- ${note.milestone.id} / attempt ${note.attempt} / ${note.sessionID}: ${workflowExpertNotePath(note.milestone.id, note.attempt)}`,
        )),
    "",
    "## Consultation History",
    "",
    `Company standups: ${workflowStandupIndexPath()}`,
    `Consultation archive: ${workflowConsultationIndexPath()}`,
    "",
    ...(input.consultations.length === 0
      ? ["_No cross-session consultations have been recorded yet._"]
      : input.consultations.map(
        (consultation) =>
          `- ${consultation.fromRole} ${consultation.fromSessionID} -> ${consultation.toRole} ${consultation.toSessionID}${consultation.timing ? ` [${consultation.timing}]` : ""}${consultation.reason ? ` (${consultation.reason})` : ""}: ${compactMarkdown(consultation.question, 160).replace(/\n/g, " ")}`,
      )),
    "",
    "## Main PM Supervision Notes",
    "",
    ...(input.standupDocs.length === 0
      ? ["_No main PM supervision notes have been recorded yet._"]
      : input.standupDocs.map((doc) => `- ${doc.at} [${doc.reason}]: ${doc.path}`)),
    "",
    "## Requester Interventions",
    "",
    `Intervention index: ${workflowInterventionIndexPath()}`,
    "",
    ...(input.interventions.length === 0
      ? ["_No requester interventions have been recorded yet._"]
      : input.interventions.map(
          (intervention) =>
            `- ${intervention.id} [${intervention.status}/${intervention.timing}] -> ${intervention.targetRole}${intervention.targetSessionID ? ` ${intervention.targetSessionID}` : ""}: ${intervention.path}`,
        )),
    "",
    "## Final Review",
    "",
    `Delivery summary: ${workflowDeliverySummaryPath()}`,
    `Tester completeness review: ${workflowTestPlanPath()}`,
    `Technical advisor assessment: ${workflowTechnicalAssessmentPath()}`,
    `Main PM acceptance: ${workflowAcceptancePath("main_pm")}`,
    `Requester acceptance: ${workflowAcceptancePath("requester")}`,
    "",
  ].join("\n")
}

function deliverySummaryMarkdown(input: {
  workflow: WorkflowInfo
  milestones: WorkflowMilestoneInfo[]
  members: WorkflowMemberInfo[]
  consultations: WorkflowConsultationInfo[]
  interventions: WorkflowInterventionInfo[]
  standupDocs: WorkflowStandupDoc[]
}) {
  const counts = new Map(input.milestones.map((milestone) => [milestone.status, 0]))
  input.milestones.forEach((milestone) => counts.set(milestone.status, (counts.get(milestone.status) ?? 0) + 1))
  const roleOwner = (role: WorkflowSessionRef["role"]) => input.members.find((member) => member.role === role)
  return [
    `# ${input.workflow.title} Delivery Summary`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Status: ${input.workflow.status}`,
    ...(input.workflow.time.completed ? [`Completed: ${new Date(input.workflow.time.completed).toISOString()}`] : []),
    ...(input.workflow.error ? [`Current blocker: ${input.workflow.error}`] : []),
    "",
    "## Strategic Request",
    "",
    compactMarkdown(input.workflow.request, 1600),
    "",
    "## Final State",
    "",
    `- Milestones: ${Array.from(counts.entries()).map(([status, count]) => `${status}=${count}`).join(", ") || "none"}`,
    `- Main PM: ${input.workflow.pmSessionID ?? roleOwner("main_pm")?.sessionID ?? "unassigned"}`,
    `- Technical advisor: ${roleOwner("expert")?.sessionID ?? "unassigned"}`,
    `- Tester: ${input.workflow.testerSessionID ?? roleOwner("tester")?.sessionID ?? "unassigned"}`,
    `- Requester: ${input.workflow.rootSessionID ?? "unassigned"}`,
    "",
    "## Milestone Outcomes",
    "",
    ...(input.milestones.length === 0
      ? ["_No milestones have been recorded yet._"]
      : input.milestones.map(
          (milestone) =>
            `- ${milestone.id} [${milestone.status}] attempt ${milestone.attempt}: ${milestone.title ?? milestone.prompt}`,
        )),
    "",
    "## Review And Acceptance Evidence",
    "",
    `- Tester completeness review: ${workflowTestPlanPath()}`,
    `- Technical advisor assessment: ${workflowTechnicalAssessmentPath()}`,
    `- Main PM acceptance: ${workflowAcceptancePath("main_pm")}`,
    `- Requester acceptance: ${workflowAcceptancePath("requester")}`,
    "",
    "## Coordination Evidence",
    "",
    `- Cross-session consultations: ${input.consultations.length}`,
    `- Requester interventions: ${input.interventions.length}`,
    `- Main PM supervision / standups: ${input.standupDocs.length}`,
    "",
    "## Reuse Guidance",
    "",
    "Future workflow employees should read this summary first, then inspect the linked review files, staff memory, and consultation archive before repeating investigation or reopening completed decisions.",
    "",
  ].join("\n")
}

function interventionMarkdown(input: { workflow: WorkflowInfo; intervention: WorkflowInterventionInfo }) {
  return [
    `# Requester Intervention ${input.intervention.id}`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Status: ${input.intervention.status}`,
    `Timing: ${input.intervention.timing}`,
    `Target role: ${roleSessionTitle(input.intervention.targetRole)}`,
    ...(input.intervention.fromSessionID ? [`Requester session: ${input.intervention.fromSessionID}`] : []),
    ...(input.intervention.targetSessionID ? [`Target session: ${input.intervention.targetSessionID}`] : []),
    `Created: ${new Date(input.intervention.time.created).toISOString()}`,
    `Updated: ${new Date(input.intervention.time.updated).toISOString()}`,
    "",
    "## Message",
    "",
    input.intervention.message,
    "",
    "## Response",
    "",
    input.intervention.response?.trim() || "_No response recorded yet._",
    "",
  ].join("\n")
}

function interventionIndexMarkdown(input: { workflow: WorkflowInfo; interventions: WorkflowInterventionInfo[] }) {
  return [
    `# ${input.workflow.title} Requester Interventions`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Status: ${input.workflow.status}`,
    "",
    ...(input.interventions.length === 0
      ? ["_No requester interventions have been recorded yet._"]
      : input.interventions.map(
          (intervention) =>
            `- ${new Date(intervention.time.created).toISOString()} [${intervention.status}/${intervention.timing}] ${roleSessionTitle(intervention.targetRole)}: ${intervention.path}`,
        )),
    "",
  ].join("\n")
}

function consultationIndexMarkdown(input: { workflow: WorkflowInfo; consultations: WorkflowConsultationInfo[] }) {
  return [
    `# ${input.workflow.title} Consultation Archive`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Status: ${input.workflow.status}`,
    "",
    ...(input.consultations.length === 0
      ? ["_No cross-session consultations have been recorded yet._"]
      : input.consultations.map(
          (consultation) =>
            `- ${new Date(consultation.time.created).toISOString()} ${roleSessionTitle(consultation.fromRole)} ${consultation.fromSessionID} -> ${roleSessionTitle(consultation.toRole)} ${consultation.toSessionID}${consultation.timing ? ` [${consultation.timing}]` : ""}${consultation.reason ? ` (${consultation.reason})` : ""}: ${workflowConsultationPath(consultation.id)}`,
        )),
    "",
  ].join("\n")
}

function consultationMarkdown(input: { workflow: WorkflowInfo; consultation: WorkflowConsultationInfo }) {
  return [
    `# ${input.workflow.title} Consultation`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Consultation: ${input.consultation.id}`,
    `Status: ${input.consultation.status}`,
    `Created: ${new Date(input.consultation.time.created).toISOString()}`,
    `Updated: ${new Date(input.consultation.time.updated).toISOString()}`,
    `From: ${roleSessionTitle(input.consultation.fromRole)} ${input.consultation.fromSessionID}`,
    `To: ${roleSessionTitle(input.consultation.toRole)} ${input.consultation.toSessionID}`,
    ...(input.consultation.milestoneID ? [`Milestone: ${input.consultation.milestoneID}`] : []),
    ...(input.consultation.timing ? [`Timing: ${input.consultation.timing}`] : []),
    ...(input.consultation.reason ? [`Reason: ${input.consultation.reason}`] : []),
    "",
    "## Question",
    "",
    compactMarkdown(input.consultation.question, 2400),
    "",
    "## Response",
    "",
    compactMarkdown(input.consultation.answer, 3200),
    "",
    "## Reuse Notes",
    "",
    "Other workflow employees should treat this as company memory when the same decision, risk, file, or acceptance question comes up again.",
    "",
  ].join("\n")
}

function standupIndexHeader(workflow: WorkflowInfo) {
  return [`# ${workflow.title} Company Standups`, "", `Workflow: ${workflow.id}`, `Status: ${workflow.status}`, ""].join("\n")
}

function standupMarkdown(input: { workflow: WorkflowInfo; reason: string; progress: string; output: string }) {
  return [
    `# ${input.workflow.title} Company Standup`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Reason: ${input.reason}`,
    `Created: ${new Date().toISOString()}`,
    "",
    "## Main PM Notes",
    "",
    input.output.trim() || "_No main PM standup notes were produced._",
    "",
    "## Progress Snapshot",
    "",
    input.progress,
    "",
  ].join("\n")
}

function recentStandupLines(index: string) {
  return index
    .split(/\r?\n/)
    .filter((line) => line.startsWith("- "))
    .slice(-5)
}

type WorkflowStandupDoc = {
  id: string
  at: string
  reason: string
  path: string
  title: string
  summary: string
}

function standupDocsFromIndex(workflow: WorkflowInfo, index: string): WorkflowStandupDoc[] {
  return index
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.startsWith("- "))
    .map((line) => {
      const body = line.slice(2)
      const timeEnd = body.indexOf(" ")
      if (timeEnd < 0) return
      const rest = body.slice(timeEnd + 1)
      const pathStart = rest.lastIndexOf(": ")
      if (pathStart < 0) return
      const docPath = rest.slice(pathStart + 2).trim()
      if (!docPath) return
      const reason = rest.slice(0, pathStart).trim() || "company standup"
      const name = path.basename(docPath, path.extname(docPath))
      return {
        id: `${workflow.id}:standup:${name}`,
        at: body.slice(0, timeEnd),
        reason,
        path: docPath,
        title: reason === "main PM supervision" ? "Main PM supervision" : `Company standup: ${reason}`,
        summary: `Main PM coordination note recorded for ${reason}.`,
      }
    })
    .filter((doc): doc is WorkflowStandupDoc => !!doc)
}

function progressMarkdown(input: {
  workflow: WorkflowInfo
  milestones: WorkflowMilestoneInfo[]
  members: WorkflowMemberInfo[]
  interventions: WorkflowInterventionInfo[]
  standups?: string[]
}) {
  const active = input.milestones.filter((milestone) => interruptedMilestone(milestone.status))
  return [
    `# ${input.workflow.title} Workflow Progress`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Status: ${input.workflow.status}`,
    ...(input.workflow.error ? [`Blocker: ${input.workflow.error}`] : []),
    `Updated: ${new Date(input.workflow.time.updated).toISOString()}`,
    "",
    "## Current Work",
    "",
    ...(active.length === 0
      ? ["_No active milestone is running._"]
      : active.map((milestone) => `- ${milestone.id} [${milestone.status}] attempt ${milestone.attempt}: ${milestone.title ?? milestone.prompt}`)),
    "",
    "## Milestones",
    "",
    ...(input.milestones.length === 0
      ? ["_No milestones parsed yet._"]
      : input.milestones.map(
          (milestone) =>
            `- ${milestone.id} [${milestone.status}] deps: ${milestone.dependsOn.join(", ") || "none"} plan: ${workflowStoredPath(input.workflow, milestone.planPath, milestone.id, "plan.md")}`,
        )),
    "",
    "## Staff",
    "",
    ...(input.members.length === 0
      ? ["_No company staff sessions have been created yet._"]
      : input.members.map(
          (member) =>
            `- ${member.title} [${member.status}/${member.availability ?? "unknown"}] session: ${member.sessionID} focus: ${compactMarkdown(member.currentFocus ?? "none", 120).replace(/\n/g, " ")} blockers: ${(member.blockers ?? []).join("; ") || "none"} note: ${compactMarkdown(member.progressNote ?? "none", 120).replace(/\n/g, " ")} model: ${workflowModelRefText(member.model)}${member.modelCacheUntil ? ` cached-until: ${new Date(member.modelCacheUntil).toISOString()}` : ""}`,
        )),
    "",
    "## Recent Requester Interventions",
    "",
    ...(input.interventions.length === 0
      ? ["_No requester interventions have been recorded yet._"]
      : input.interventions
          .slice(-8)
          .map(
            (intervention) =>
              `- ${new Date(intervention.time.created).toISOString()} [${intervention.status}/${intervention.timing}] ${compactMarkdown(intervention.message, 160).replace(/\n/g, " ")}`,
          )),
    "",
    "## Recent Main PM Supervision",
    "",
    ...(input.standups?.length
      ? input.standups
      : ["_No main PM supervision notes have been recorded yet._"]),
    "",
    "## Company Standups",
    "",
    `- ${workflowStandupIndexPath()}`,
    "",
  ].join("\n")
}

function organizationMarkdown(input: {
  workflow: WorkflowInfo
  members: WorkflowMemberInfo[]
  milestones: WorkflowMilestoneInfo[]
}) {
  const assignments = new Map<SessionID, string[]>()
  input.milestones.forEach((milestone) =>
    milestone.session.forEach((ref) => {
      const current = assignments.get(ref.sessionID) ?? []
      current.push(`${milestone.id} [${ref.role}] attempt ${ref.attempt ?? milestone.attempt}`)
      assignments.set(ref.sessionID, current)
    }),
  )
  const staffing = normalizeStaffing(input.workflow.staffing)
  return [
    `# ${input.workflow.title} Company Organization`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Status: ${input.workflow.status}`,
    `Requester session: ${input.workflow.rootSessionID ?? "unassigned"}`,
    "",
    "## Staffing Limits",
    "",
    `- Main PM: ${staffing.mainPM}`,
    `- Department PM: ${staffing.departmentPM}`,
    `- Executor: ${staffing.executor}`,
    `- Reviewer support: ${staffing.reviewer}`,
    `- Tester: ${staffing.tester}`,
    `- Technical advisor: ${staffing.expert}`,
    "",
    "## Model Whitelists",
    "",
    ...workflowModelRoles.flatMap((role) => {
      const items = workflowModelWhitelistForRole(input.workflow, role)
      return [
        `- ${roleSessionTitle(role)}:`,
        ...(items.length === 0
          ? ["  - default session model"]
          : items.map(
              (item) =>
                `  - ${workflowModelRefText(item)} weight=${workflowModelWeight(item.weight)} cache=${workflowModelCacheMinutes(item.cacheMinutes)}m`,
            )),
      ]
    }),
    "",
    "## Staff",
    "",
    ...(input.members.length === 0
      ? ["_No staff sessions have been created yet._"]
      : input.members.map((member) =>
          [
            `- ${member.title}`,
            `  - role: ${member.role}`,
            `  - specialty: ${member.specialty}`,
            `  - session: ${member.sessionID}`,
            `  - status: ${member.status}`,
            `  - availability: ${member.availability ?? "unknown"}`,
            `  - current focus: ${member.currentFocus ?? "none"}`,
            `  - blockers: ${(member.blockers ?? []).join("; ") || "none"}`,
            `  - progress note: ${member.progressNote ?? "none"}`,
            `  - cached model: ${workflowModelRefText(member.model)}${member.modelWeight === undefined ? "" : ` weight=${workflowModelWeight(member.modelWeight)}`}${member.modelCacheUntil ? ` until=${new Date(member.modelCacheUntil).toISOString()}` : ""}`,
            `  - current/previous assignments: ${(assignments.get(member.sessionID) ?? ["none"]).join("; ")}`,
          ].join("\n"),
        )),
    "",
    "## Operating Rules",
    "",
    "- Sessions are long-lived company members. Reuse them across tasks instead of creating disposable employees.",
    "- Requester owns strategy and may redirect the workflow during execution.",
    "- Main PM supervises scope, sequencing, and cross-team alignment.",
    "- Department PMs turn scope into executable plans and own functional review approval.",
    "- Reviewer sessions, when configured, are optional audit/support staff and do not replace Department PM approval.",
    "- Executors coordinate technical details with peers and advisors before making risky changes.",
    "- Testers perform completeness and regression review.",
    "- Technical advisors guide architecture, technology choices, performance, and optimization risk.",
    "- Every substantial session output is archived into the reference library for later staff to reuse.",
    "- Use workflow communication XML for cross-session consultation and escalation.",
    "- Prefer employees whose cached model already matches the task. Switch a long-lived employee to another model only when the task difficulty is clearly mismatched or the configured cache duration is 0 minutes.",
    "",
  ].join("\n")
}

function staffMemoryMarkdown(input: {
  workflow: WorkflowInfo
  member: WorkflowMemberInfo
  milestones: WorkflowMilestoneInfo[]
  consultations: WorkflowConsultationInfo[]
  interventions: WorkflowInterventionInfo[]
  sessionSummary?: string
  standups?: string[]
}) {
  const assignments = input.milestones.flatMap((milestone) =>
    milestone.session
      .filter((ref) => ref.sessionID === input.member.sessionID && ref.role === input.member.role)
      .map((ref) => ({
        milestone,
        attempt: ref.attempt ?? milestone.attempt,
      })),
  )
  const activeAssignments = assignments.filter((assignment) => interruptedMilestone(assignment.milestone.status))
  const relatedConsultations = input.consultations.filter(
    (consultation) =>
      consultation.fromSessionID === input.member.sessionID || consultation.toSessionID === input.member.sessionID,
  )
  const peerExecutorConsultations = relatedConsultations.filter(
    (consultation) => consultation.fromRole === "executor" && consultation.toRole === "executor",
  )
  const advisorNotes = input.member.role === "expert"
    ? assignments.map((assignment) => ({
        milestone: assignment.milestone,
        attempt: assignment.attempt,
        path: workflowExpertNotePath(assignment.milestone.id, assignment.attempt),
      }))
    : []
  const relatedInterventions = input.interventions.filter(
    (intervention) => intervention.targetSessionID === input.member.sessionID,
  )
  return [
    `# ${input.member.title} Staff Memory`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Workflow title: ${input.workflow.title}`,
    `Session: ${input.member.sessionID}`,
    `Role: ${roleSessionTitle(input.member.role)}`,
    `Specialty: ${input.member.specialty}`,
    `Capacity: ${input.member.capacity}`,
    `Status: ${input.member.status}`,
    `Updated: ${new Date().toISOString()}`,
    "",
    "## Employee Profile",
    "",
    `Role responsibility: ${workflowEmployeeResponsibility(input.member.role)}`,
    `Stable consultation specialty: ${input.member.specialty}`,
    `Direct consultation handle: ${input.member.sessionID}`,
    "",
    "## Company Context",
    "",
    `Organization chart: ../../organization.md`,
    `Progress: ../../progress.md`,
    `Requester memory: ../requester.md`,
    `Reference index: ../index.md`,
    `Consultation archive: ../consultations/index.md`,
    `Session summary: ../${workflowSessionSummaryPath(input.member.sessionID).replace(/^reference[\\/]/, "")}`,
    "",
    "## Active Responsibilities",
    "",
    ...(activeAssignments.length === 0
      ? ["_No active assignment right now; remain available for consultation in this specialty._"]
      : activeAssignments.map(
          (assignment) =>
            `- ${assignment.milestone.id} [${assignment.milestone.status}] attempt ${assignment.attempt}: ${assignment.milestone.title ?? assignment.milestone.prompt}`,
        )),
    "",
    "## Current/Previous Assignments",
    "",
    ...(assignments.length === 0
      ? ["_No assignments recorded yet._"]
      : assignments.map(
          (assignment) =>
            `- ${assignment.milestone.id} [${assignment.milestone.status}] attempt ${assignment.attempt}: ${assignment.milestone.title ?? assignment.milestone.prompt} (${workflowStoredPath(input.workflow, assignment.milestone.planPath, assignment.milestone.id, "plan.md")})`,
        )),
    "",
    ...(input.member.role === "expert"
      ? [
          "## Technical Advisor Notes",
          "",
          ...(advisorNotes.length === 0
            ? ["_No technical advisor notes recorded for this advisor yet._"]
            : advisorNotes.map(
                (note) =>
                  `- ${note.milestone.id} [${note.milestone.status}] attempt ${note.attempt}: ../../${note.path}`,
              )),
          "",
        ]
      : []),
    "## Latest Handoff Memory",
    "",
    input.sessionSummary ? compactMarkdown(input.sessionSummary, 1600) : "_No session handoff summary has been archived yet._",
    "",
    "## Recent Main PM Supervision",
    "",
    ...(input.standups?.length
      ? input.standups
      : ["_No main PM supervision notes have been recorded yet._"]),
    "",
    "## Consultation History",
    "",
    ...(relatedConsultations.length === 0
      ? ["_No consultations recorded for this employee yet._"]
      : relatedConsultations.map((consultation) =>
          [
            `- ${consultation.fromSessionID === input.member.sessionID ? "Asked" : "Answered"} ${roleSessionTitle(consultation.fromRole)} -> ${roleSessionTitle(consultation.toRole)}`,
            ...(consultation.timing ? [` [${consultation.timing}]`] : []),
            ...(consultation.reason ? [` (${consultation.reason})`] : []),
            `: ${compactMarkdown(consultation.question, 180).replace(/\n/g, " ")}`,
          ].join(""),
        )),
    "",
    "## Consultation Capabilities",
    "",
    `- Direct: <opencode-workflow-consult target-session="${input.member.sessionID}" timing="temporary-interrupt" reason="why this employee has context" model-weight="0-100">question</opencode-workflow-consult>`,
    `- Role based: <opencode-workflow-message to-role="${input.member.role}" specialty="${input.member.specialty}" timing="temporary-interrupt" reason="short reason" model-weight="0-100">question</opencode-workflow-message>`,
    "- Use after-task for normal handoff, temporary-interrupt for a quick answer before continuing, and interrupt only when work should pause.",
    "- Set model-weight from 0-100 based on task complexity; low values favor cheaper routine work, high values favor broad, risky, architectural, or ambiguous work.",
    "",
    ...(input.member.role === "executor"
      ? [
          "## Peer Executor Handoffs",
          "",
          ...(peerExecutorConsultations.length === 0
            ? ["_No executor peer handoffs recorded for this employee yet._"]
            : peerExecutorConsultations.map((consultation) =>
                [
                  `- ${consultation.fromSessionID === input.member.sessionID ? "Requested" : "Provided"} peer handoff`,
                  ...(consultation.timing ? [` [${consultation.timing}]`] : []),
                  ...(consultation.reason ? [` (${consultation.reason})`] : []),
                  `: ${compactMarkdown(consultation.question, 180).replace(/\n/g, " ")}`,
                  `\n  - answer: ${compactMarkdown(consultation.answer, 220).replace(/\n/g, " ")}`,
                ].join(""),
              )),
          "",
        ]
      : []),
    "## Requester Interventions",
    "",
    ...(relatedInterventions.length === 0
      ? ["_No requester interventions targeted this employee yet._"]
      : relatedInterventions.map(
          (intervention) =>
            `- ${intervention.id} [${intervention.status}/${intervention.timing}]: ${intervention.path}`,
        )),
    "",
    "## How Other Employees Should Use This Memory",
    "",
    `Consult this employee directly with: <opencode-workflow-consult target-session="${input.member.sessionID}" reason="why this employee has context" model-weight="0-100">question</opencode-workflow-consult>`,
    `Ask by role with: <opencode-workflow-message to-role="${input.member.role}" specialty="${input.member.specialty}" timing="temporary-interrupt" reason="short reason" model-weight="0-100">question</opencode-workflow-message>`,
    "Read this staff memory before assigning similar work, asking this employee for context, or deciding that a previous investigation needs to be repeated.",
    "",
  ].join("\n")
}

function requesterMemoryMarkdown(input: {
  workflow: WorkflowInfo
  interventions: WorkflowInterventionInfo[]
  sessionSummary?: string
  standups?: string[]
}) {
  const recentInterventions = input.interventions
    .filter((intervention) => !intervention.fromSessionID || intervention.fromSessionID === input.workflow.rootSessionID)
    .slice(-12)
  return [
    `# ${input.workflow.title} Requester Memory`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Requester session: ${input.workflow.rootSessionID ?? "unassigned"}`,
    `Status: ${input.workflow.status}`,
    `Updated: ${new Date().toISOString()}`,
    "",
    "## Company Context",
    "",
    `Organization chart: ../organization.md`,
    `Progress: ../progress.md`,
    `Reference index: index.md`,
    `Consultation archive: consultations/index.md`,
    `Company standups: ../${workflowStandupIndexPath()}`,
    `Requester interventions: ../${workflowInterventionIndexPath()}`,
    "",
    "## Strategic Direction",
    "",
    compactMarkdown(input.workflow.request, 2000),
    "",
    "## Latest Requester Handoff",
    "",
    input.sessionSummary ? compactMarkdown(input.sessionSummary, 1600) : "_No requester session handoff summary has been archived yet._",
    "",
    "## Recent Requester Interventions",
    "",
    ...(recentInterventions.length === 0
      ? ["_No requester interventions have been recorded yet._"]
      : recentInterventions.map(
          (intervention) =>
            `- ${new Date(intervention.time.created).toISOString()} [${intervention.status}/${intervention.timing}] -> ${roleSessionTitle(intervention.targetRole)}${intervention.targetSessionID ? ` ${intervention.targetSessionID}` : ""}: ${compactMarkdown(intervention.message, 220).replace(/\n/g, " ")}`,
        )),
    "",
    "## Recent Main PM Supervision",
    "",
    ...(input.standups?.length
      ? input.standups
      : ["_No main PM supervision notes have been recorded yet._"]),
    "",
    "## How Employees Should Use This Memory",
    "",
    "Treat this file as the strategic source of truth before interpreting milestone scope or final acceptance criteria.",
    `Escalate strategy questions with: <opencode-workflow-message to-role="requester" timing="temporary-interrupt" reason="strategy clarification" model-weight="0-100">question</opencode-workflow-message>`,
    "",
  ].join("\n")
}

function archiveIndexMarkdown(workflow: WorkflowInfo, milestones: WorkflowMilestoneInfo[]) {
  const sessionRows: WorkflowArchiveSessionRef[] = [
    ...(workflow.rootSessionID ? [{ role: "requester" as const, sessionID: workflow.rootSessionID }] : []),
    ...(workflow.pmSessionID ? [{ role: "main_pm" as const, sessionID: workflow.pmSessionID }] : []),
    ...(workflow.testerSessionID ? [{ role: "tester" as const, sessionID: workflow.testerSessionID }] : []),
    ...milestones.flatMap((milestone) =>
      milestone.session.map((ref) => ({
        ...ref,
        milestoneID: ref.milestoneID ?? milestone.id,
      })),
    ),
  ]
  return [
    `# ${workflow.title}`,
    "",
    `Workflow: ${workflow.id}`,
    `Status: ${workflow.status}`,
    `Path: ${workflow.path}`,
    `Created: ${new Date(workflow.time.created).toISOString()}`,
    `Updated: ${new Date(workflow.time.updated).toISOString()}`,
    "",
    "## Core Files",
    "",
    "- workflow.xml",
    "- organization.md",
    "- progress.md",
    `- ${workflowStandupIndexPath()}`,
    `- ${workflowMainPlanPath()}`,
    `- ${workflowReferenceIndexPath()}`,
    `- ${workflowRequesterMemoryPath()}`,
    `- ${workflowInterventionIndexPath()}`,
    ...(workflow.testPath ? [`- ${path.relative(workflow.path, workflow.testPath)}`] : []),
    `- ${workflowTechnicalAssessmentPath()}`,
    "",
    "## User Requirement",
    "",
    workflow.request,
    "",
    "## Sessions",
    "",
    ...(sessionRows.length === 0
      ? ["_No workflow sessions recorded yet._"]
      : sessionRows.map(
          (ref) =>
            `- ${roleSessionTitle(ref.role)}${ref.milestoneID ? ` / ${ref.milestoneID}` : ""}${ref.attempt !== undefined ? ` / attempt ${ref.attempt}` : ""}: ${workflowSessionArchivePath(ref.sessionID)}`,
        )),
    "",
    "## Milestones",
    "",
    ...(milestones.length === 0
      ? ["_No milestones parsed yet._"]
      : milestones.map(
          (milestone) =>
            `- ${milestone.id} [${milestone.status}]: ${milestone.title ?? milestone.prompt} (${workflowStoredPath(workflow, milestone.planPath, milestone.id, "plan.md")})`,
        )),
    "",
  ].join("\n")
}

function edgesFrom(definition: WorkflowDefinition): WorkflowGraphEdge[] {
  return definition.milestones.flatMap((milestone) =>
    milestone.dependsOn.map((from) => ({
      id: `${from}->${milestone.id}`,
      from: String(from),
      to: String(milestone.id),
      kind: "dependency" as const,
    })),
  )
}

function milestoneStates(milestones: WorkflowMilestoneInfo[]) {
  return milestones.map((milestone) => ({ id: milestone.id, status: milestone.status }))
}

function workflowSessionTitle(role: string, task: string) {
  return `${role}: ${task.replace(/\s+/g, " ").trim().slice(0, 72)}`
}

function workflowRequesterTitle(request: string) {
  return `Requester: ${titleFromRequest(request) ?? "Workflow request"}`
}

function workflowSessionTitles(workflow: WorkflowInfo, milestones: WorkflowMilestoneInfo[]) {
  const titles = new Map<SessionID, string>()
  if (workflow.rootSessionID) titles.set(workflow.rootSessionID, workflowRequesterTitle(workflow.request))
  if (workflow.pmSessionID) titles.set(workflow.pmSessionID, workflowSessionTitle("Main PM", workflow.title))
  if (workflow.testerSessionID) titles.set(workflow.testerSessionID, workflowSessionTitle("Tester", workflow.title))
  milestones.forEach((milestone) => {
    const task = milestone.title ?? String(milestone.id)
    milestone.session.forEach((ref) => {
      titles.set(ref.sessionID, workflowSessionTitle(roleSessionTitle(ref.role), task))
    })
  })
  return titles
}

function roleSessionTitle(role: WorkflowSessionRef["role"]) {
  if (role === "main_pm") return "Main PM"
  if (role === "department_pm") return "Department PM"
  if (role === "executor") return "Executor"
  if (role === "reviewer") return "Reviewer"
  if (role === "tester") return "Tester"
  if (role === "expert") return "Technical Advisor"
  return "Requester"
}

function workflowSessionRole(
  workflow: WorkflowInfo,
  milestones: WorkflowMilestoneInfo[],
  sessionID: SessionID,
): WorkflowSessionRef["role"] {
  if (workflow.rootSessionID === sessionID) return "requester"
  if (workflow.pmSessionID === sessionID) return "main_pm"
  if (workflow.testerSessionID === sessionID) return "tester"
  return workflowSessionAssignment(milestones, sessionID)?.ref.role ?? "requester"
}

export function workflowSessionAssignment(milestones: WorkflowMilestoneInfo[], sessionID: SessionID) {
  return milestones
    .flatMap((milestone, index) =>
      milestone.session
        .filter((ref) => ref.sessionID === sessionID)
        .map((ref) => ({
          milestone,
          ref,
          index,
        })),
    )
    .toSorted(
      (a, b) =>
        workflowSessionAssignmentRank(a.milestone.status) - workflowSessionAssignmentRank(b.milestone.status) ||
        (b.ref.attempt ?? b.milestone.attempt) - (a.ref.attempt ?? a.milestone.attempt) ||
        b.index - a.index,
    )[0]
}

function workflowSessionAssignmentRank(status: WorkflowMilestoneInfo["status"]) {
  if (interruptedMilestone(status) || status === "testing") return 0
  if (status === "blocked" || status === "rejected" || status === "failed") return 1
  if (status === "pending") return 2
  return 3
}

function workflowSessionMilestoneID(milestones: WorkflowMilestoneInfo[], sessionID: SessionID) {
  return workflowSessionAssignment(milestones, sessionID)?.milestone.id
}

function workflowSessionAttempt(milestones: WorkflowMilestoneInfo[], sessionID: SessionID) {
  const assignment = workflowSessionAssignment(milestones, sessionID)
  return assignment ? (assignment.ref.attempt ?? assignment.milestone.attempt) : undefined
}

function workflowAgentForRole(role: WorkflowSessionRef["role"]) {
  if (role === "main_pm") return "workflow-main-pm"
  if (role === "department_pm") return "workflow-department-pm"
  if (role === "executor") return "workflow-executor"
  if (role === "reviewer") return "workflow-reviewer"
  if (role === "tester") return "workflow-tester"
  if (role === "expert") return "workflow-expert"
  return "build"
}

function workflowProjectMemoryPrompt() {
  return [
    "Project-local planning and skills are part of the company memory.",
    "Before finalizing plans, implementation, review, or tests, check relevant project context under `.opencode/` and `.codex/` when present, especially `.opencode/plans/**`, `.opencode/skill/**`, `.opencode/skills/**`, `.codex/plans/**`, `.codex/skill/**`, `.codex/skills/**`, `AGENTS.md`, `.codex/AGENTS.md`, and `SKILL.md` files.",
    "Treat these files as local process knowledge and reusable plans, then link or summarize any relevant facts in your handoff summary so the workflow reference library can preserve them.",
  ].join("\n")
}

export function workflowReferencePrompt(workflow: WorkflowInfo) {
  return [
    `Workflow root: ${workflow.path}`,
    `Workflow index: ${workflowArtifactPath(workflow, "index.md")}`,
    `Reference library: ${workflowArtifactPath(workflow, workflowReferenceIndexPath())}`,
    `Company organization: ${workflowArtifactPath(workflow, "organization.md")}`,
    `Workflow progress: ${workflowArtifactPath(workflow, "progress.md")}`,
    `Requester strategic memory: ${workflowArtifactPath(workflow, workflowRequesterMemoryPath())}`,
    `Staff memory directory: ${workflowArtifactPath(workflow, "reference", "staff")}`,
    `Consultation archive: ${workflowArtifactPath(workflow, workflowConsultationIndexPath())}`,
    `Requester interventions: ${workflowArtifactPath(workflow, workflowInterventionIndexPath())}`,
    `Company standups: ${workflowArtifactPath(workflow, workflowStandupIndexPath())}`,
    `Workflow XML: ${workflowArtifactPath(workflow, "workflow.xml")}`,
    "",
    "This workflow operates like a small company, not a disposable subtask tree. Sessions are long-lived staff members with roles, specialties, and accumulated context.",
    "Read the workflow index, organization chart, and reference library when present so you know the company structure, current completion state, session ids, related plans, prior decisions, and which employee owns which function.",
    "Before making or reviewing changes, inspect the workflow memory in this order when available: progress.md for live state, organization.md for owners and capacities, reference/requester.md for strategic direction and direction changes, reference/index.md for session summaries, staff memory, and consultation history, reference/consultations/index.md and reference/consultations/*.md for prior employee consultations, reference/staff/*.md for the relevant employee's accumulated responsibilities, standups/index.md and standups/*.md for recent supervision details, interventions/index.md for requester direction changes, and the relevant milestone plan/review/advisor files.",
    workflowProjectMemoryPrompt(),
    "Use prior session summaries as company memory: do not repeat completed investigation, preserve decisions already made, and explicitly call out when new evidence supersedes earlier notes.",
    "When your work produces reusable facts, end with a `## Handoff Summary` section so the archive and reference library can feed later staff sessions.",
    "The handoff summary should list completed scope, decisions made, files or modules touched, tests or evidence, open risks, and which employee or milestone should use it next.",
    "If you need another employee's context or a technical decision, ask through workflow communication instead of guessing.",
    "For a specific known session, emit:",
    '<opencode-workflow-consult target-session="ses_xxx" reason="why this session has the answer" model-weight="0-100">question for that session</opencode-workflow-consult>',
    "For role-based communication, emit:",
    '<opencode-workflow-message to-role="expert|main_pm|department_pm|executor|reviewer|tester|requester" specialty="optional area" timing="after-task|interrupt|temporary-interrupt" reason="short reason" model-weight="0-100">message or question</opencode-workflow-message>',
    "Role-based workflow messages are consultation/notification only: they prompt an employee and archive the exchange, but they do not dispatch milestones, attach a session to a milestone, or change milestone status.",
    "When you need to receive or close collaboration messages assigned to your session, use the built-in workflow_message tool: action=inbox at the start of a workflow-owned turn, action=answer for consultations, and action=ack or action=answer for requester interventions. Do not claim a consultation/intervention is closed unless the tool confirms it.",
    "To dispatch real milestone work, update workflow.xml or emit <opencode-workflow-update>, then use the built-in workflow tool with action=update_xml or action=resume and confirm the tool result.",
    "If the built-in workflow tool is unavailable but you must close a workflow gate, emit workflow control XML such as <opencode-workflow-control action=\"plan_complete\" milestone=\"milestone-id\">...</opencode-workflow-control> or <opencode-workflow-control action=\"force_complete\" milestone=\"milestone-id\">...</opencode-workflow-control>. The workflow manager will route that through the same command bus and record the result.",
    "Use timing=\"after-task\" for normal handoff, timing=\"temporary-interrupt\" when you need a quick answer before continuing, and timing=\"interrupt\" when the current task should pause until direction changes.",
    "Set model-weight=\"0-100\" when delegating; use lower weights for routine/focused tasks and higher weights for complex, risky, architectural, or ambiguous tasks.",
    "If the only valid blocker is a requester/user decision, send it to to-role=\"requester\" with 2-3 explicit options, mark one option as Recommended, and include the tradeoff for each option. Do not stop silently after asking.",
    "Main PM and department PM sessions may revise the workflow graph directly by emitting:",
    '<opencode-workflow-update reason="why the graph changed"><workflow>...</workflow></opencode-workflow-update>',
    "Use workflow updates when a milestone is too broad, requester strategy changes, or the company needs new ordered/parallel work. Preserve completed milestone ids when they remain valid.",
    "For consultation messages only, the workflow manager will prompt the target employee session, record the exchange in the workflow graph, update the reference library, and inject the answer back here. This still does not dispatch milestone work.",
  ].join("\n")
}

export function workflowEmployeeContextPrompt(
  workflow: WorkflowInfo,
  input: {
    sessionID: SessionID
    role: WorkflowSessionRef["role"]
    milestoneID?: WorkflowMilestoneID
    attempt?: number
    member?: WorkflowMemberInfo
    milestone?: WorkflowMilestoneInfo
  },
) {
  return [
    "## Workflow Employee Context",
    "",
    "You are operating as a long-lived employee in this workflow company. Reuse your accumulated staff memory and update it through your handoff summary instead of treating this as a disposable task.",
    `Workflow: ${workflow.id}`,
    `Workflow title: ${workflow.title}`,
    `Session: ${input.sessionID}`,
    `Company role: ${roleSessionTitle(input.role)}`,
    `Role responsibility: ${workflowEmployeeResponsibility(input.role)}`,
    "At the start and end of each workflow-owned turn, use the built-in workflow tool with action=status_update to report availability=working|idle|blocked_waiting, currentFocus, blockers, and progressNote. Do not only describe your state in prose.",
    "At the start of each workflow-owned turn, also use workflow_message action=inbox. If the inbox includes a consultation, respond with workflow_message action=answer. If it includes a requester intervention, respond with workflow_message action=ack or action=answer. A message is not closed until the tool result says it was recorded.",
    ...(input.member
      ? [
          `Employee title: ${input.member.title}`,
          `Specialty: ${input.member.specialty}`,
          `Staff memory: ${workflowArtifactPath(workflow, workflowStaffMemoryPath(input.member))}`,
        ]
      : input.role === "requester"
        ? ["Employee title: Requester / strategic owner", `Requester memory: ${workflowArtifactPath(workflow, workflowRequesterMemoryPath())}`]
        : []),
    ...(input.milestoneID ? [`Current milestone: ${input.milestoneID}`] : []),
    ...(input.milestone
      ? [
          `Current milestone title: ${input.milestone.title ?? input.milestone.id}`,
          `Current department: ${input.milestone.department ?? "unspecified"}`,
          `Current plan file: ${workflowStoredPath(workflow, input.milestone.planPath, input.milestone.id, "plan.md")}`,
        ]
      : []),
    ...(input.attempt !== undefined ? [`Current attempt: ${input.attempt}`] : []),
    "",
    "Before answering, read the workflow progress, organization chart, reference index, your staff memory file when listed above, and the relevant plan/review files. If another employee has the answer, use workflow communication XML instead of guessing.",
    workflowProjectMemoryPrompt(),
    "",
    input.milestone ? workflowMilestoneFileIsolationPrompt(workflow, input.milestone) : workflowRuntimeFileIsolationPrompt(workflow),
    "",
    "Use this direct consultation XML when a specific employee session owns the missing context:",
    '<opencode-workflow-consult target-session="ses_xxx" timing="after-task|interrupt|temporary-interrupt" reason="short reason" model-weight="0-100">question</opencode-workflow-consult>',
    "Use this role-based communication XML when the workflow should route the message to an employee by function:",
    '<opencode-workflow-message to-role="expert|main_pm|department_pm|executor|reviewer|tester|requester" specialty="optional area" timing="after-task|interrupt|temporary-interrupt" reason="short reason" model-weight="0-100">message or question</opencode-workflow-message>',
    "Set model-weight=\"0-100\" to choose the target role's model from its whitelist; higher weights select stronger reasoning profiles.",
    "When asking the requester/user to decide, include 2-3 concrete options in the message body, label the recommended option, and state the impact of each option.",
  ].join("\n")
}

export function workflowCompanySnapshotPrompt(input: {
  workflow?: Pick<WorkflowInfo, "rootSessionID" | "request">
  members: WorkflowMemberInfo[]
  milestones: WorkflowMilestoneInfo[]
  staffing?: WorkflowStaffingConfig
  currentSessionID?: SessionID
}) {
  const memberBySession = new Map(input.members.map((member) => [member.sessionID, member]))
  const roleOrder = ["requester", "main_pm", "department_pm", "expert", "executor", "reviewer", "tester"]
  const staffingRoles = ["main_pm", "department_pm", "expert", "executor", "reviewer", "tester"] as const
  const staffingLines = staffingRoles.map((role) => {
    const staffed = input.members.filter((member) => member.role === role).length
    const busy = new Set(
      input.milestones
        .filter((milestone) => roleBusyForMilestoneStatus(role, milestone.status))
        .flatMap((milestone) =>
          milestone.session.filter((ref) => ref.role === role).map((ref) => ref.sessionID),
        ),
    ).size
    return `- ${roleSessionTitle(role)}: staffed ${staffed}/${staffLimitForRole(input.staffing, role)}, busy ${busy}`
  })
  const strategicOwnerLines = input.workflow?.rootSessionID
    ? [
        `- ${input.workflow.rootSessionID === input.currentSessionID ? "(you) " : ""}${workflowRequesterTitle(input.workflow.request)} [requester/strategy] session=${input.workflow.rootSessionID} status=active capacity=1 memory=${workflowRequesterMemoryPath()} responsibility=strategic direction, intervention, and final acceptance request="${compactMarkdown(input.workflow.request, 180).replace(/\n/g, " ")}"`,
      ]
    : ["- Requester strategic owner session is unassigned."]
  const visibleMembers = input.members
    .toSorted(
      (a, b) =>
        roleOrder.indexOf(a.role) - roleOrder.indexOf(b.role) ||
        a.time.created - b.time.created ||
        a.id.localeCompare(b.id),
    )
    .slice(0, 24)
  const visibleMilestones = input.milestones
    .toSorted((a, b) => String(a.id).localeCompare(String(b.id)))
    .slice(0, 40)
  const memberLines =
    visibleMembers.length === 0
      ? ["- No company employee sessions have been assigned yet."]
      : [
          ...visibleMembers.map((member) => {
            const assignments = input.milestones
              .filter((milestone) => milestone.session.some((ref) => ref.sessionID === member.sessionID))
              .map((milestone) => {
                const roles = milestone.session
                  .filter((ref) => ref.sessionID === member.sessionID)
                  .map((ref) => `${roleSessionTitle(ref.role)} attempt ${ref.attempt ?? milestone.attempt}`)
                  .join(", ")
                return `${milestone.id} [${milestone.status}] as ${roles}`
              })
            const focus = compactMarkdown(member.currentFocus ?? "none", 140).replace(/\n/g, " ")
            const blockers = (member.blockers ?? []).map((blocker) => compactMarkdown(blocker, 80).replace(/\n/g, " ")).join("; ") || "none"
            const note = compactMarkdown(member.progressNote ?? "none", 140).replace(/\n/g, " ")
            return `- ${member.sessionID === input.currentSessionID ? "(you) " : ""}${member.title} [${member.role}/${member.specialty}] session=${member.sessionID} status=${member.status} capacity=${member.capacity} assignments=${assignments.join("; ") || "none"}`
              + ` availability=${member.availability ?? "unknown"} focus="${focus}" blockers="${blockers}" progress="${note}"`
          }),
          ...(input.members.length > visibleMembers.length
            ? [`- ... ${input.members.length - visibleMembers.length} more employee session(s) omitted; read organization.md for the full company chart.`]
            : []),
        ]
  const milestoneLines =
    visibleMilestones.length === 0
      ? ["- No milestones have been parsed yet."]
      : [
          ...visibleMilestones.map((milestone) => {
            const owners = milestone.session
              .toSorted((a, b) => roleOrder.indexOf(a.role) - roleOrder.indexOf(b.role) || (a.attempt ?? 0) - (b.attempt ?? 0))
              .map((ref) => {
                const member = memberBySession.get(ref.sessionID)
                return `${roleSessionTitle(ref.role)}=${member?.title ?? ref.sessionID}#${ref.attempt ?? milestone.attempt}`
              })
              .join("; ")
            return `- ${milestone.id} [${milestone.status}] department=${milestone.department ?? "unspecified"} deps=${milestone.dependsOn.join(", ") || "none"} owners=${owners || "unassigned"}`
          }),
          ...(input.milestones.length > visibleMilestones.length
            ? [`- ... ${input.milestones.length - visibleMilestones.length} more milestone(s) omitted; read progress.md for the full workflow state.`]
            : []),
        ]
  return [
    "## Company Operating Snapshot",
    "",
    "Use this live snapshot to understand who is in the workflow company, what they own, and which tasks still need coordination. Treat session ids as stable employee channels.",
    "",
    "### Staffing Limits",
    "",
    ...staffingLines,
    "",
    "### Strategic Owner",
    "",
    ...strategicOwnerLines,
    "",
    "### Review Boundaries",
    "",
    "- Department PM owns milestone functional approval or rejection.",
    "- Tester owns completeness, regression, and feedback-loop review.",
    "- Reviewer staff, when present, is optional audit/support context and does not replace Department PM approval.",
    "",
    "### Employees",
    "",
    ...memberLines,
    "",
    "### Milestones",
    "",
    ...milestoneLines,
  ].join("\n")
}

export function workflowEmployeeTaskPrompt(
  workflow: WorkflowInfo,
  input: Parameters<typeof workflowEmployeeContextPrompt>[1],
  task: string,
  company?: {
    members: WorkflowMemberInfo[]
    milestones: WorkflowMilestoneInfo[]
  },
) {
  return [
    workflowEmployeeContextPrompt(workflow, input),
    ...(company
      ? [
          "",
          workflowCompanySnapshotPrompt({
            workflow,
            ...company,
            staffing: workflow.staffing,
            currentSessionID: input.sessionID,
          }),
        ]
      : []),
    "",
    "## Assigned Workflow Task",
    "",
    task,
  ].join("\n")
}

function workflowEmployeeResponsibility(role: WorkflowSessionRef["role"]) {
  if (role === "requester") return "Own strategic goals, acceptance direction, and mid-workflow interventions."
  if (role === "main_pm") return "Supervise scope, sequencing, progress, risk, and company coordination."
  if (role === "department_pm") return "Convert milestone scope into executable plans and functional review."
  if (role === "executor") return "Implement assigned milestone work and coordinate technical decisions with peers."
  if (role === "reviewer") return "Support PM-owned functional review with optional audit context; Department PM approval remains authoritative."
  if (role === "tester") return "Perform completeness, regression, and feedback-loop review."
  return "Advise on architecture, technology choices, performance, integration risk, and optimization."
}

function workflowRuntimeFileIsolationPrompt(workflow: WorkflowInfo) {
  return [
    "## Workflow File Isolation",
    "",
    "The workflow root contains opencode-managed runtime files. Do not delete, rename, or overwrite these unless this prompt names the exact file as your deliverable.",
    `Runtime root: ${workflow.path}`,
    `Opencode-managed examples: ${workflowArtifactPath(workflow, "progress.md")}, ${workflowArtifactPath(workflow, "organization.md")}, ${workflowArtifactPath(workflow, "index.md")}, ${workflowArtifactPath(workflow, workflowStateFileName)}, session_*.md, ${workflowArtifactPath(workflow, workflowReferenceIndexPath())}, ${workflowArtifactPath(workflow, workflowStandupIndexPath())}, ${workflowArtifactPath(workflow, workflowInterventionIndexPath())}.`,
    `Main PM planning workspace: ${workflowArtifactPath(workflow, "planning")}.`,
    `Final review workspace: ${workflowArtifactPath(workflow, "final")}.`,
    "If a requester or milestone mentions shared paths such as implementation/*.md, verification/*.md, or requirements/*.md, treat them as logical names and write the real file inside the owning milestone's artifacts/ directory unless the path is already inside that milestone directory.",
  ].join("\n")
}

function workflowMilestoneFileIsolationPrompt(workflow: WorkflowInfo, milestone: WorkflowMilestoneInfo) {
  return [
    workflowRuntimeFileIsolationPrompt(workflow),
    "",
    `Milestone-owned directory: ${workflowArtifactPath(workflow, milestone.id)}.`,
    `Milestone plan file: ${workflowArtifactPath(workflow, milestone.id, "plan.md")}.`,
    `Agent-authored deliverables for this milestone must go under: ${workflowArtifactPath(workflow, workflowMilestoneArtifactsPath(milestone.id))}.`,
    "When the milestone text names a shared relative output path outside this milestone directory, preserve that logical path under artifacts/. Example: implementation/audit-core.md becomes <milestone>/artifacts/implementation/audit-core.md.",
    "Record every logical-path to artifacts-path mapping in your Handoff Summary so downstream milestones can find the files.",
    "Do not write into sibling milestone directories unless this prompt explicitly asks for cross-milestone synthesis.",
  ].join("\n")
}

function consultationAttribute(attributes: string, name: string) {
  return new RegExp(`${name}=["']([^"']+)["']`, "i").exec(attributes)?.[1]
}

export function parseConsultRequests(text: string): WorkflowConsultRequest[] {
  const direct = Array.from(text.matchAll(/<opencode-workflow-consult\b([^>]*)>([\s\S]*?)<\/opencode-workflow-consult>/gi))
    .map((match) => ({
      target: consultationAttribute(match[1] ?? "", "target-session") ?? consultationAttribute(match[1] ?? "", "targetSessionID"),
      reason: consultationAttribute(match[1] ?? "", "reason"),
      timing: consultationAttribute(match[1] ?? "", "timing"),
      modelWeight: workflowModelWeightHint(
        consultationAttribute(match[1] ?? "", "model-weight") ?? consultationAttribute(match[1] ?? "", "modelWeight"),
      ),
      question: (match[2] ?? "").trim(),
    }))
    .flatMap((item) => {
      if (!item.target || !item.question) return []
      try {
        return [
          {
            targetSessionID: SessionID.make(item.target),
            ...(item.reason ? { reason: item.reason } : {}),
            ...(isWorkflowTiming(item.timing) ? { timing: item.timing } : {}),
            ...(item.modelWeight !== undefined ? { modelWeight: item.modelWeight } : {}),
            question: item.question,
          },
        ]
      } catch {
        return []
      }
    })
  const roleBased = Array.from(text.matchAll(/<opencode-workflow-message\b([^>]*)>([\s\S]*?)<\/opencode-workflow-message>/gi))
    .map((match) => ({
      role: consultationAttribute(match[1] ?? "", "to-role") ?? consultationAttribute(match[1] ?? "", "role"),
      specialty: consultationAttribute(match[1] ?? "", "specialty"),
      reason: consultationAttribute(match[1] ?? "", "reason"),
      timing: consultationAttribute(match[1] ?? "", "timing"),
      modelWeight: workflowModelWeightHint(
        consultationAttribute(match[1] ?? "", "model-weight") ?? consultationAttribute(match[1] ?? "", "modelWeight"),
      ),
      question: (match[2] ?? "").trim(),
    }))
    .flatMap((item) => {
      if (!isWorkflowRole(item.role) || !item.question) return []
      return [
        {
          targetRole: item.role,
          ...(item.specialty ? { targetSpecialty: item.specialty } : {}),
          ...(item.reason ? { reason: item.reason } : {}),
          ...(isWorkflowTiming(item.timing) ? { timing: item.timing } : {}),
          ...(item.modelWeight !== undefined ? { modelWeight: item.modelWeight } : {}),
          question: item.question,
        },
      ]
    })
  return [...direct, ...roleBased]
}

export function parseWorkflowUpdateXml(text: string) {
  const body = /<opencode-workflow-update\b[^>]*>([\s\S]*?)<\/opencode-workflow-update>/i.exec(text)?.[1]?.trim()
  if (!body) return undefined
  const unfenced = /^```(?:xml)?\s*\n([\s\S]*?)\n```$/i.exec(body)?.[1]?.trim() ?? body
  const workflow = /<workflow\b[\s\S]*<\/workflow>/i.exec(unfenced)?.[0]?.trim()
  return workflow || undefined
}

function isWorkflowRole(value: string | undefined): value is WorkflowSessionRef["role"] {
  return (
    value === "requester" ||
    value === "main_pm" ||
    value === "department_pm" ||
    value === "executor" ||
    value === "reviewer" ||
    value === "tester" ||
    value === "expert"
  )
}

function isWorkflowTiming(value: string | undefined): value is WorkflowConsultationInfo["timing"] {
  return value === "after-task" || value === "interrupt" || value === "temporary-interrupt"
}

function milestoneJobID(workflowID: WorkflowID, milestoneID: WorkflowMilestoneID, attempt: number) {
  return `${workflowID}:${milestoneID}:${attempt}`
}

function interruptedMilestone(status: WorkflowMilestoneInfo["status"]) {
  return status === "planning" || status === "executing" || status === "reviewing" || status === "running"
}

function retryableMilestone(status: WorkflowMilestoneInfo["status"]) {
  return (
    status === "pending" ||
    status === "planning" ||
    status === "executing" ||
    status === "reviewing" ||
    status === "running" ||
    status === "failed" ||
    status === "rejected" ||
    status === "blocked"
  )
}

function terminalMilestone(status: WorkflowMilestoneInfo["status"]) {
  return (
    status === "approved" ||
    status === "done" ||
    status === "completed" ||
    status === "skipped" ||
    status === "cancelled"
  )
}

const workflowMilestoneTransitions: Record<WorkflowMilestoneInfo["status"], WorkflowMilestoneInfo["status"][]> = {
  pending: ["planning", "skipped", "cancelled"],
  planning: ["executing", "blocked", "cancelled"],
  executing: ["reviewing", "blocked", "failed", "cancelled"],
  reviewing: ["testing", "rejected", "cancelled"],
  testing: ["approved", "rejected", "cancelled"],
  rejected: ["planning", "executing", "skipped", "cancelled"],
  approved: ["done"],
  blocked: ["planning", "executing", "cancelled"],
  done: [],
  skipped: [],
  failed: ["pending", "planning", "cancelled"],
  running: ["reviewing", "blocked", "failed", "cancelled"],
  completed: [],
  cancelled: [],
}

function canonicalMilestoneStatus(status: WorkflowMilestoneInfo["status"]) {
  if (status === "completed") return "done"
  if (status === "running") return "executing"
  return status
}

function legalMilestoneTransitions(status: WorkflowMilestoneInfo["status"]) {
  return workflowMilestoneTransitions[status] ?? []
}

function workflowCommandRejection(
  code:
    | "illegal_transition"
    | "not_authorized"
    | "invalid_xml"
    | "unknown_milestone"
    | "workflow_not_active"
    | "precondition_failed",
  reason: string,
  allowedTransitions?: string[],
) {
  return {
    applied: false,
    message: reason,
    rejection: {
      code,
      reason,
      ...(allowedTransitions?.length ? { allowedTransitions } : {}),
    },
  }
}

function milestoneTransitionGuidance(input: {
  currentStatus: WorkflowMilestoneInfo["status"]
  targetStatus: WorkflowMilestoneInfo["status"]
}) {
  if (
    canonicalMilestoneStatus(input.currentStatus) === "planning" &&
    ["approved", "done"].includes(canonicalMilestoneStatus(input.targetStatus))
  ) {
    return " Planning gates do not close through milestone_status done/approved from non-owner sessions. Use action=plan_complete to continue this milestone into executor/review work, or requester/main_pm action=force_complete when this is a gate-only milestone that should unblock dependents. The owning department PM session may re-issue milestone_status=done/approved as a compatibility alias for plan_complete."
  }
  return ""
}

export function temporaryInterruptPauseStatus(status: WorkflowMilestoneInfo["status"] | undefined) {
  if (status === "planning" || status === "executing" || status === "reviewing" || status === "running") return status
  return undefined
}

function errorFromCause(cause: Cause.Cause<unknown>) {
  const error = Cause.squash(cause)
  return error instanceof globalThis.Error ? error.message : String(error)
}

function workflowCommandDurabilityFailure(cause: Cause.Cause<unknown>) {
  const message = errorFromCause(cause)
  return (
    message.includes("workflow write fault injected") ||
    message.includes("Workflow engine cannot write") ||
    /\b(EACCES|EPERM|ENOSPC|EBUSY|EIO)\b/i.test(message)
  )
}

function graphFrom(
  info: WorkflowInfo,
  milestones: WorkflowMilestoneInfo[],
  edges: WorkflowGraphEdge[],
  consultations: WorkflowConsultationInfo[],
  members: WorkflowMemberInfo[],
  interventions: WorkflowInterventionInfo[],
  standupDocs: WorkflowStandupDoc[],
): WorkflowGraph {
  const includeDocumentGraph = workflowGraphDocumentsEnabled()
  const memberNodeID = (member: WorkflowMemberInfo) => `${info.id}:member:${member.id}`
  const mainMember = members.find((member) => member.role === "main_pm" && member.sessionID === info.pmSessionID)
  const testerMember = members.find((member) => member.role === "tester" && member.sessionID === info.testerSessionID)
  const expertMember =
    members.find((member) => member.role === "expert" && member.specialty === "performance-and-architecture") ??
    members.find((member) => member.role === "expert")
  const mainPMID = mainMember ? memberNodeID(mainMember) : `${info.id}:main_pm`
  const testerID = testerMember ? memberNodeID(testerMember) : `${info.id}:tester`
  const expertID = expertMember ? memberNodeID(expertMember) : `${info.id}:expert`
  const libraryID = `${info.id}:reference`
  const operationDocs = includeDocumentGraph
    ? [
        {
          id: `${info.id}:organization`,
          title: "Company organization",
          path: workflowArtifactPath(info, "organization.md"),
          summary: "Company staffing limits, long-lived employee roles, responsibilities, and current assignments.",
        },
        {
          id: `${info.id}:progress`,
          title: "Workflow progress",
          path: workflowArtifactPath(info, "progress.md"),
          summary: "Live workflow state, active milestones, staff assignments, interventions, and supervision notes.",
        },
        {
          id: `${info.id}:main-plan`,
          title: "Main PM plan",
          path: workflowArtifactPath(info, workflowMainPlanPath()),
          summary: "Main product manager high-level plan and linked workflow file index.",
        },
        {
          id: `${info.id}:workflow-xml`,
          title: "Workflow XML",
          path: workflowArtifactPath(info, "workflow.xml"),
          summary: "Canonical ordered and parallel workflow scheduling definition.",
        },
        {
          id: `${info.id}:archive-index`,
          title: "Local archive index",
          path: workflowArtifactPath(info, "index.md"),
          summary: "Local workflow artifact index with archived session records.",
        },
        {
          id: `${info.id}:delivery-summary`,
          title: "Delivery summary",
          path: workflowArtifactPath(info, workflowDeliverySummaryPath()),
          summary: "Workflow final state, milestone outcomes, review gates, acceptance, and reuse guidance.",
        },
      ]
    : []
  const requesterMemoryID = `${info.id}:requester-memory`
  const interventionIndexID = `${info.id}:interventions`
  const standupIndexID = `${info.id}:standups`
  const requesterMemoryDoc = {
    id: requesterMemoryID,
    title: "Requester strategic memory",
    path: workflowArtifactPath(info, workflowRequesterMemoryPath()),
    summary: "Requester strategic direction, latest handoff, intervention history, and acceptance context.",
  }
  const staffMemoryDocs = includeDocumentGraph
    ? members.map((member) => ({
        id: `${info.id}:staff:${member.id}:memory`,
        member,
        title: `${member.title} memory`,
        path: workflowArtifactPath(info, workflowStaffMemoryPath(member)),
        summary: `Long-lived staff memory for ${roleSessionTitle(member.role)} ${member.specialty}.`,
      }))
    : []
  const finalReviewDocs =
    includeDocumentGraph &&
    (info.testPath || info.status === "testing" || info.status === "accepting" || info.status === "completed")
      ? [
          {
            id: `${info.id}:test-plan`,
            title: "Tester completeness review",
            path: info.testPath ?? workflowArtifactPath(info, workflowTestPlanPath()),
            summary: "Tester-created targeted tests, regression checks, and completeness review.",
          },
          {
            id: `${info.id}:technical-assessment`,
            title: "Technical advisor assessment",
            path: workflowArtifactPath(info, workflowTechnicalAssessmentPath()),
            summary: "Final architecture, integration, and performance assessment before acceptance.",
          },
          {
            id: `${info.id}:main-pm-acceptance`,
            title: "Main PM acceptance",
            path: workflowArtifactPath(info, workflowAcceptancePath("main_pm")),
            summary: "Main product manager final approve/reject decision.",
          },
          {
            id: `${info.id}:requester-acceptance`,
            title: "Requester acceptance",
            path: workflowArtifactPath(info, workflowAcceptancePath("requester")),
            summary: "Requester final strategic approve/reject decision.",
          },
        ]
      : []
  const expertNoteDocs = includeDocumentGraph
    ? milestones.flatMap((milestone) =>
        milestone.session
          .filter((ref) => ref.role === "expert")
          .map((ref) => {
            const attempt = ref.attempt ?? milestone.attempt
            return {
              id: `${milestone.id}:expert:${attempt}:document`,
              milestone,
              ref,
              attempt,
              title: `${milestone.title ?? milestone.id} technical advisor note #${attempt}`,
              path: workflowArtifactPath(info, workflowExpertNotePath(milestone.id, attempt)),
              summary: `Technical advisor milestone note for ${milestone.id} attempt ${attempt}.`,
            }
          }),
      )
    : []
  const sessionNodeID = (milestone: WorkflowMilestoneInfo, session: WorkflowSessionRef) =>
    members.find((member) => member.sessionID === session.sessionID && member.role === session.role)
      ? memberNodeID(members.find((member) => member.sessionID === session.sessionID && member.role === session.role)!)
      : `${milestone.id}:${session.role}:${session.attempt ?? 0}`
  const sessionNodeIDs = new Map<SessionID, string>([
    ...(info.rootSessionID ? [[info.rootSessionID, info.id] as const] : []),
    ...(info.pmSessionID ? [[info.pmSessionID, mainPMID] as const] : []),
    ...(info.testerSessionID ? [[info.testerSessionID, testerID] as const] : []),
    ...members.map((member) => [member.sessionID, memberNodeID(member)] as const),
    ...milestones.flatMap((milestone) =>
      milestone.session.map((session) => [session.sessionID, sessionNodeID(milestone, session)] as const),
    ),
  ])
  const sessionRefs: WorkflowArchiveSessionRef[] = includeDocumentGraph
    ? [
        ...(info.rootSessionID ? [{ role: "requester" as const, sessionID: info.rootSessionID }] : []),
        ...(info.pmSessionID ? [{ role: "main_pm" as const, sessionID: info.pmSessionID }] : []),
        ...(info.testerSessionID ? [{ role: "tester" as const, sessionID: info.testerSessionID }] : []),
        ...members.map((member) => ({ role: member.role, sessionID: member.sessionID })),
        ...milestones.flatMap((milestone) =>
          milestone.session.map((ref) => ({
            ...ref,
            milestoneID: ref.milestoneID ?? milestone.id,
          })),
        ),
      ]
    : []
  const consultationDocs = includeDocumentGraph
    ? consultations.map((consultation) => ({
        id: `${consultation.id}:document`,
        consultation,
        title: `${roleSessionTitle(consultation.fromRole)} to ${roleSessionTitle(consultation.toRole)} consultation`,
        path: workflowArtifactPath(info, workflowConsultationPath(consultation.id)),
        summary: compactMarkdown(consultation.question, 240),
        fromNodeID: sessionNodeIDs.get(consultation.fromSessionID),
        toNodeID: sessionNodeIDs.get(consultation.toSessionID),
      }))
    : []
  const sessionByRole = (
    milestone: WorkflowMilestoneInfo,
    attempt: number,
    role: WorkflowSessionRef["role"],
  ) => milestone.session.find((session) => (session.attempt ?? 0) === attempt && session.role === role)
  const milestoneSession = (milestone: WorkflowMilestoneInfo) =>
    milestone.session
      .toSorted(
        (a, b) =>
          (b.attempt ?? 0) - (a.attempt ?? 0) ||
          ["reviewer", "executor", "department_pm"].indexOf(b.role) -
            ["reviewer", "executor", "department_pm"].indexOf(a.role),
      )
      .find((session) =>
        milestone.status === "planning"
          ? session.role === "department_pm"
          : milestone.status === "executing"
            ? session.role === "executor"
            : milestone.status === "reviewing" ||
                milestone.status === "approved" ||
                milestone.status === "rejected" ||
                milestone.status === "done"
              ? session.role === "department_pm" || session.role === "reviewer"
              : true,
      )
  const graphEdges = [
    ...edges,
    ...(info.pmSessionID
      ? [
          {
            id: `${info.id}->${mainPMID}`,
            from: info.id,
            to: mainPMID,
            kind: "entry" as const,
          },
        ]
      : []),
    ...(includeDocumentGraph
      ? [
          {
            id: `${libraryID}->${requesterMemoryID}:document`,
            from: libraryID,
            to: requesterMemoryID,
            kind: "document" as const,
            label: "requester memory",
            path: requesterMemoryDoc.path,
            summary: requesterMemoryDoc.summary,
          },
          {
            id: `${info.id}->${requesterMemoryID}:document`,
            from: info.id,
            to: requesterMemoryID,
            kind: "document" as const,
            label: "memory",
            path: requesterMemoryDoc.path,
            summary: "Strategic owner memory for workflow direction and requester interventions.",
          },
          ...operationDocs.flatMap((doc) => [
      {
        id: `${libraryID}->${doc.id}:document`,
        from: libraryID,
        to: doc.id,
        kind: "document" as const,
        label: "operations",
        path: doc.path,
        summary: doc.summary,
      },
      {
        id: `${info.id}->${doc.id}:document`,
        from: info.id,
        to: doc.id,
        kind: "document" as const,
        label: doc.id.endsWith(":progress") ? "progress" : doc.id.endsWith(":organization") ? "organization" : "artifact",
        path: doc.path,
        summary: doc.summary,
      },
          ]),
          {
            id: `${libraryID}->${interventionIndexID}:document`,
            from: libraryID,
            to: interventionIndexID,
            kind: "document" as const,
            label: "interventions",
            path: workflowArtifactPath(info, workflowInterventionIndexPath()),
            summary: "Requester intervention index",
          },
          {
            id: `${libraryID}->${standupIndexID}:document`,
            from: libraryID,
            to: standupIndexID,
            kind: "document" as const,
            label: "standups",
            path: workflowArtifactPath(info, workflowStandupIndexPath()),
            summary: "Main PM workflow standup notes",
          },
          ...standupDocs.flatMap((doc) => [
      {
        id: `${standupIndexID}->${doc.id}:document`,
        from: standupIndexID,
        to: doc.id,
        kind: "document" as const,
        label: doc.reason === "main PM supervision" ? "supervision note" : "standup note",
        path: doc.path,
        summary: doc.summary,
      },
      {
        id: `${libraryID}->${doc.id}:document`,
        from: libraryID,
        to: doc.id,
        kind: "document" as const,
        label: "standup note",
        path: doc.path,
        summary: doc.summary,
      },
      ...(info.pmSessionID
        ? [
            {
              id: `${mainPMID}->${doc.id}:document-owner`,
              from: mainPMID,
              to: doc.id,
              kind: "document" as const,
              label: "supervises",
              path: doc.path,
              summary: "Main PM supervision and company coordination evidence.",
            },
          ]
        : []),
          ]),
          ...staffMemoryDocs.flatMap((doc) => [
      {
        id: `${libraryID}->${doc.id}:document`,
        from: libraryID,
        to: doc.id,
        kind: "document" as const,
        label: "staff memory",
        path: doc.path,
        summary: doc.summary,
      },
      {
        id: `${memberNodeID(doc.member)}->${doc.id}:document`,
        from: memberNodeID(doc.member),
        to: doc.id,
        kind: "document" as const,
        label: "memory",
        path: doc.path,
        summary: `Personal workflow memory for ${doc.member.title}.`,
      },
          ]),
          ...expertNoteDocs.flatMap((doc) => [
      {
        id: `${libraryID}->${doc.id}:document`,
        from: libraryID,
        to: doc.id,
        kind: "document" as const,
        label: "advisor note",
        path: doc.path,
        summary: doc.summary,
      },
      {
        id: `${doc.milestone.id}->${doc.id}:document`,
        from: String(doc.milestone.id),
        to: doc.id,
        kind: "document" as const,
        label: "advisor note",
        path: doc.path,
        summary: doc.summary,
      },
      ...(sessionNodeIDs.get(doc.ref.sessionID)
        ? [
            {
              id: `${sessionNodeIDs.get(doc.ref.sessionID)}->${doc.id}:document-owner`,
              from: sessionNodeIDs.get(doc.ref.sessionID)!,
              to: doc.id,
              kind: "document" as const,
              label: "owns",
              path: doc.path,
              summary: "Technical advisor owns milestone architecture and performance guidance.",
            },
          ]
        : []),
          ]),
          ...finalReviewDocs.map((doc) => ({
            id: `${libraryID}->${doc.id}:document`,
            from: libraryID,
            to: doc.id,
            kind: "document" as const,
            label: "final review",
            path: doc.path,
            summary: doc.summary,
          })),
          ...finalReviewDocs.flatMap((doc) => {
      if (doc.id.endsWith(":test-plan") && info.testerSessionID) {
        return [
          {
            id: `${testerID}->${doc.id}:document-owner`,
            from: testerID,
            to: doc.id,
            kind: "document" as const,
            label: "owns",
            path: doc.path,
            summary: "Tester owns completeness and regression review evidence.",
          },
        ]
      }
      if (doc.id.endsWith(":technical-assessment") && expertMember) {
        return [
          {
            id: `${expertID}->${doc.id}:document-owner`,
            from: expertID,
            to: doc.id,
            kind: "document" as const,
            label: "owns",
            path: doc.path,
            summary: "Technical advisor owns architecture and performance assessment evidence.",
          },
        ]
      }
      if (doc.id.endsWith(":main-pm-acceptance") && info.pmSessionID) {
        return [
          {
            id: `${mainPMID}->${doc.id}:document-owner`,
            from: mainPMID,
            to: doc.id,
            kind: "document" as const,
            label: "owns",
            path: doc.path,
            summary: "Main PM owns product acceptance evidence.",
          },
        ]
      }
      if (doc.id.endsWith(":requester-acceptance") && info.rootSessionID) {
        return [
          {
            id: `${info.id}->${doc.id}:document-owner`,
            from: info.id,
            to: doc.id,
            kind: "document" as const,
            label: "owns",
            path: doc.path,
            summary: "Requester owns final strategic acceptance evidence.",
          },
        ]
      }
      return []
          }),
          ...consultationDocs.flatMap((doc) => [
      {
        id: `${libraryID}->${doc.id}:document`,
        from: libraryID,
        to: doc.id,
        kind: "document" as const,
        label: "consultation",
        path: doc.path,
        summary: doc.summary,
      },
      ...(doc.fromNodeID
        ? [
            {
              id: `${doc.fromNodeID}->${doc.id}:document-asked`,
              from: doc.fromNodeID,
              to: doc.id,
              kind: "document" as const,
              label: "asked",
              path: doc.path,
              summary: `Question: ${compactMarkdown(doc.consultation.question, 180).replace(/\n/g, " ")}`,
            },
          ]
        : []),
      ...(doc.toNodeID
        ? [
            {
              id: `${doc.id}->${doc.toNodeID}:document-answered`,
              from: doc.id,
              to: doc.toNodeID,
              kind: "document" as const,
              label: "answered by",
              path: doc.path,
              summary: `Answer: ${compactMarkdown(doc.consultation.answer, 180).replace(/\n/g, " ")}`,
            },
          ]
        : []),
          ]),
        ]
      : []),
    ...milestones
      .filter((milestone) => milestone.dependsOn.length === 0)
      .map((milestone) => ({
        id: `${info.pmSessionID ? mainPMID : info.id}->${milestone.id}`,
        from: info.pmSessionID ? mainPMID : info.id,
        to: String(milestone.id),
        kind: "entry" as const,
      })),
    ...milestones.flatMap((milestone) => [
      ...milestone.session.map((session) => ({
        id: `${milestone.id}->${sessionNodeID(milestone, session)}`,
        from: String(milestone.id),
        to: sessionNodeID(milestone, session),
        kind: "session" as const,
      })),
      ...(info.pmSessionID
        ? milestone.session
            .filter((session) => session.role === "department_pm")
            .map((session) => ({
              id: `${mainPMID}->${sessionNodeID(milestone, session)}`,
              from: mainPMID,
              to: sessionNodeID(milestone, session),
              kind: "session" as const,
            }))
        : []),
      ...Array.from(new Set(milestone.session.map((session) => session.attempt ?? 0))).flatMap((attempt) => {
        const departmentPM = sessionByRole(milestone, attempt, "department_pm")
        const executor = sessionByRole(milestone, attempt, "executor")
        const reviewer = sessionByRole(milestone, attempt, "reviewer")
        const functionalReviewer = reviewer ?? departmentPM
        return [
          ...(departmentPM && executor
            ? [
                {
                  id: `${sessionNodeID(milestone, departmentPM)}->${sessionNodeID(milestone, executor)}`,
                  from: sessionNodeID(milestone, departmentPM),
                  to: sessionNodeID(milestone, executor),
                  kind: "session" as const,
                },
              ]
            : []),
          ...(executor && functionalReviewer
            ? [
                {
                  id: `${sessionNodeID(milestone, executor)}->${sessionNodeID(milestone, functionalReviewer)}:functional-review`,
                  from: sessionNodeID(milestone, executor),
                  to: sessionNodeID(milestone, functionalReviewer),
                  kind: "session" as const,
                },
              ]
            : []),
        ]
      }),
    ]),
    ...(info.testerSessionID
      ? [
          ...milestones
            .filter((milestone) => milestone.status === "done" || milestone.status === "completed")
            .map((milestone) => ({
              id: `${milestone.id}->${info.id}:tester`,
              from: String(milestone.id),
              to: testerID,
              kind: "tester" as const,
            })),
        ]
      : []),
    ...(includeDocumentGraph
      ? sessionRefs.flatMap((ref) => {
          const nodeID = sessionNodeIDs.get(ref.sessionID)
          const docID = `${String(ref.sessionID)}:summary`
          if (!nodeID) return []
          return [
            {
              id: `${libraryID}->${docID}:document`,
              from: libraryID,
              to: docID,
              kind: "document" as const,
              label: "reference",
              path: workflowArtifactPath(info, workflowSessionSummaryPath(ref.sessionID)),
              summary: `${roleSessionTitle(ref.role)} reference summary`,
            },
            {
              id: `${nodeID}->${docID}:document`,
              from: nodeID,
              to: docID,
              kind: "document" as const,
              label: "summary",
              path: workflowArtifactPath(info, workflowSessionSummaryPath(ref.sessionID)),
              summary: "Session summary document",
            },
          ]
        })
      : []),
    ...consultations.flatMap((consultation) => {
      const from = sessionNodeIDs.get(consultation.fromSessionID)
      const to = sessionNodeIDs.get(consultation.toSessionID)
      if (!from || !to) return []
      return [
        {
          id: `${consultation.id}:consultation`,
          from,
          to,
          kind: "consultation" as const,
          label: consultation.timing ?? "consult",
          question: compactMarkdown(consultation.question, 800),
          answer: compactMarkdown(consultation.answer, 800),
          summary: [
            `${roleSessionTitle(consultation.fromRole)} consulted ${roleSessionTitle(consultation.toRole)}`,
            ...(consultation.reason ? [`Reason: ${consultation.reason}`] : []),
          ].join("\n"),
        },
      ]
    }),
    ...interventions.flatMap((intervention) => {
      const from = intervention.fromSessionID ? sessionNodeIDs.get(intervention.fromSessionID) : undefined
      const to = intervention.targetSessionID ? sessionNodeIDs.get(intervention.targetSessionID) : undefined
      return [
        {
          id: `${intervention.id}:intervention`,
          from: from ?? info.id,
          to: to ?? (intervention.targetRole === "main_pm" ? mainPMID : info.id),
          kind: "consultation" as const,
          label: intervention.timing,
          question: compactMarkdown(intervention.message, 800),
          answer: intervention.response ? compactMarkdown(intervention.response, 800) : undefined,
          summary: `Requester intervention to ${roleSessionTitle(intervention.targetRole)} [${intervention.status}]`,
        },
        ...(includeDocumentGraph
          ? [
              {
                id: `${interventionIndexID}->${intervention.id}:document`,
                from: interventionIndexID,
                to: `${intervention.id}:document`,
                kind: "document" as const,
                label: "intervention",
                path: intervention.path,
                summary: compactMarkdown(intervention.message, 240),
              },
            ]
          : []),
      ]
    }),
  ].filter(
    (edge, index, all) =>
      all.findIndex(
        (item) =>
          item.id === edge.id ||
          (edge.kind !== "consultation" && item.from === edge.from && item.to === edge.to && item.kind === edge.kind),
      ) === index,
  )
  const nodes: WorkflowGraphNode[] = [
    {
      id: info.id,
      type: "workflow" as const,
      title: info.title,
      role: "requester" as const,
      status: info.status,
      ...(info.rootSessionID ? { sessionID: info.rootSessionID } : {}),
    },
    ...(includeDocumentGraph
      ? [
          {
            id: libraryID,
            type: "document" as const,
            title: "Reference library",
            path: workflowArtifactPath(info, workflowReferenceIndexPath()),
            summary: "Workflow reference library with session summaries, plans, and consultation history.",
          },
          ...operationDocs.map((doc) => ({
            id: doc.id,
            type: "document" as const,
            title: doc.title,
            path: doc.path,
            summary: doc.summary,
          })),
          {
            id: requesterMemoryID,
            type: "document" as const,
            title: requesterMemoryDoc.title,
            role: "requester" as const,
            ...(info.rootSessionID ? { sessionID: info.rootSessionID } : {}),
            path: requesterMemoryDoc.path,
            summary: requesterMemoryDoc.summary,
          },
          {
            id: interventionIndexID,
            type: "document" as const,
            title: "Requester interventions",
            path: workflowArtifactPath(info, workflowInterventionIndexPath()),
            summary: "Requester intervention history and direction changes.",
          },
          {
            id: standupIndexID,
            type: "document" as const,
            title: "Company standups",
            path: workflowArtifactPath(info, workflowStandupIndexPath()),
            summary: "Main PM standups and workflow-triggered coordination notes.",
          },
          ...standupDocs.map((doc) => ({
            id: doc.id,
            type: "document" as const,
            title: doc.title,
            role: "main_pm" as const,
            ...(info.pmSessionID ? { sessionID: info.pmSessionID } : {}),
            path: doc.path,
            summary: doc.summary,
          })),
          ...staffMemoryDocs.map((doc) => ({
            id: doc.id,
            type: "document" as const,
            title: doc.title,
            role: doc.member.role,
            sessionID: doc.member.sessionID,
            path: doc.path,
            summary: doc.summary,
          })),
          ...expertNoteDocs.map((doc) => ({
            id: doc.id,
            type: "document" as const,
            title: doc.title,
            role: "expert" as const,
            sessionID: doc.ref.sessionID,
            milestoneID: doc.milestone.id,
            path: doc.path,
            summary: doc.summary,
          })),
          ...finalReviewDocs.map((doc) => ({
            id: doc.id,
            type: "document" as const,
            title: doc.title,
            path: doc.path,
            summary: doc.summary,
          })),
        ]
      : []),
    ...(info.pmSessionID
      ? mainMember
        ? []
        : [
          {
            id: mainPMID,
            type: "session" as const,
            title: "Main product manager",
            role: "main_pm" as const,
            sessionID: info.pmSessionID,
          },
        ]
      : []),
    ...members.map((member) => ({
      id: memberNodeID(member),
      type: "session" as const,
      title: member.title,
      role: member.role,
      sessionID: member.sessionID,
      status: member.status === "paused" ? ("blocked" as const) : undefined,
      summary: `${roleSessionTitle(member.role)} / ${member.specialty} / capacity ${member.capacity} / model ${workflowModelRefText(member.model)}`,
    })),
    ...milestones.flatMap((milestone) => [
      {
        id: String(milestone.id),
        type: "milestone" as const,
        title: milestone.title ?? String(milestone.id),
        status: milestone.status,
        milestoneID: milestone.id,
        sessionID: milestoneSession(milestone)?.sessionID,
        path: milestone.planPath,
      },
      ...milestone.session
        .filter((session) => !members.some((member) => member.sessionID === session.sessionID && member.role === session.role))
        .map((session) => ({
          id: sessionNodeID(milestone, session),
          type: "session" as const,
          title: `${session.role} ${milestone.title ?? milestone.id}`,
          role: session.role,
          sessionID: session.sessionID,
          milestoneID: milestone.id,
        })),
    ]),
    ...(info.testerSessionID
      ? testerMember
        ? []
        : [
          {
            id: `${info.id}:tester`,
            type: "session" as const,
            title: "Workflow tester",
            role: "tester" as const,
            sessionID: info.testerSessionID,
            path: info.testPath,
          },
        ]
      : []),
    ...(includeDocumentGraph
      ? [
          ...sessionRefs.map((ref) => ({
            id: `${String(ref.sessionID)}:summary`,
            type: "document" as const,
            title: `${roleSessionTitle(ref.role)} summary`,
            role: ref.role,
            sessionID: ref.sessionID,
            ...(ref.milestoneID ? { milestoneID: ref.milestoneID } : {}),
            path: workflowArtifactPath(info, workflowSessionSummaryPath(ref.sessionID)),
            summary: "Short reference document for other workflow sessions.",
          })),
          ...consultationDocs.map((doc) => ({
            id: doc.id,
            type: "document" as const,
            title: doc.title,
            role: doc.consultation.fromRole,
            sessionID: doc.consultation.fromSessionID,
            ...(doc.consultation.milestoneID ? { milestoneID: doc.consultation.milestoneID } : {}),
            path: doc.path,
            summary: doc.summary,
          })),
          ...interventions.map((intervention) => ({
            id: `${intervention.id}:document`,
            type: "document" as const,
            title: `Intervention ${new Date(intervention.time.created).toISOString()}`,
            role: "requester" as const,
            sessionID: intervention.fromSessionID,
            path: intervention.path,
            summary: compactMarkdown(intervention.message, 240),
          })),
        ]
      : []),
  ].filter((node, index, all) => all.findIndex((item) => item.id === node.id) === index)
  return {
    workflow: info,
    milestones,
    members,
    consultations: consultations.map(compactGraphConsultation),
    interventions: interventions.map(compactGraphIntervention),
    nodes,
    edges: graphEdges,
  }
}

function pmPermission(): Permission.Ruleset {
  return Permission.fromConfig({
    "*": "deny",
    read: "allow",
    list: "allow",
    glob: "allow",
    grep: "allow",
    edit: {
      "*": "deny",
      [path.join(workflowDir, "**")]: "allow",
    },
    bash: "deny",
    task: "deny",
    todowrite: "allow",
    question: "allow",
    workflow: "allow",
  })
}

function reviewerPermission(): Permission.Ruleset {
  return Permission.fromConfig({
    "*": "deny",
    read: "allow",
    list: "allow",
    glob: "allow",
    grep: "allow",
    bash: "allow",
    edit: {
      "*": "deny",
      [path.join(workflowDir, "**")]: "allow",
    },
    task: "deny",
    todowrite: "allow",
    workflow: "allow",
  })
}

function promptMainPm(input: { workflow: WorkflowInfo }) {
  return [
    "You are the main product manager of a small long-lived agent company inside an automated opencode workflow.",
    "",
    "Create and supervise an implementation-scale workflow plan for this request. Treat requester strategy as adjustable during the project, and keep the organization aligned when direction changes.",
    "Do not treat staff sessions as disposable. The requester, main PM, department PMs, executors, testers, and technical advisors are long-lived employees who should accumulate context and collaborate across milestones.",
    "When assigning or consulting staff, prefer the employee whose cached model and specialty already fit the task. Increase model-weight only when the remaining work is clearly harder than the employee's current cached model profile.",
    "Do not hide a complex feature behind a single broad milestone. Every milestone must be small enough for one executor session to finish, one department PM session to functionally review, and the tester to verify for completeness.",
    "PM roles must not edit implementation code or perform code changes. PMs may write workflow, planning, decomposition, reference, and review documents under the workflow directory; implementation belongs to executor sessions.",
    `Write the canonical XML to ${workflowArtifactPath(input.workflow, "workflow.xml")}.`,
    `Write the high-level plan to ${workflowArtifactPath(input.workflow, workflowMainPlanPath())}.`,
    `Treat ${workflowArtifactPath(input.workflow, "organization.md")}, ${workflowArtifactPath(input.workflow, "progress.md")}, and ${workflowArtifactPath(input.workflow, "index.md")} as runtime-generated status files. Read them, but do not rewrite them directly.`,
    `Maintain planning notes under ${workflowArtifactPath(input.workflow, "planning")}; link important child documents from ${workflowMainPlanPath()}.`,
    "Do not claim that dispatch has started unless you have written workflow.xml or emitted an <opencode-workflow-update> block. The workflow runtime creates department PM, executor, reviewer, and tester sessions after it validates the XML.",
    "Use the built-in workflow tool to inspect and control runtime state: call action=status before supervising, action=update_xml after creating or replacing the canonical XML, and action=resume when dispatch should continue. Do not rely only on natural-language claims of dispatch.",
    "Do not use <opencode-workflow-message> as a dispatch mechanism. It only asks or notifies another employee; it does not create a milestone job or unblock ordered dependencies.",
    "If a PM-only planning gate is intentionally complete and should unblock downstream milestones, requester or main PM must use workflow tool action=force_complete. Do not use milestone_status done/approved from planning.",
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "Use this XML structure only:",
    "<workflow><ordered|parallel><milestone id=\"slug\" title=\"title\" department=\"team\">milestone scope</milestone></ordered|parallel></workflow>",
    "Use <ordered> when milestones must run sequentially and <parallel> when they can run concurrently. Nest groups when useful.",
    "Milestone content must include concrete deliverables, likely files or modules, acceptance checks, and handoff constraints.",
    "For complex plugin or engine work, split by real subsystems instead of naming the whole system. Examples: contracts and architecture, manager lifecycle, CPU simulation, GPU rendering path, materials and shaders, textures and atlases, asset importers, emitter shapes, particle types or modules, editor authoring UI, serialization/runtime API, docs, tests, and build integration.",
    `Write ${workflowMainPlanPath()} with a linked file index for workflow.xml, each milestone plan.md path, and any future decomposition files.`,
    "",
    "User requirement:",
    input.workflow.request,
  ].join("\n")
}

function promptDepartmentPm(input: { workflow: WorkflowInfo; milestone: WorkflowMilestoneInfo }) {
  return [
    `You are a long-lived department product manager for milestone ${input.milestone.id}.`,
    "",
    `Main request: ${input.workflow.request}`,
    `Milestone title: ${input.milestone.title ?? input.milestone.id}`,
    `Department: ${input.milestone.department ?? "unspecified"}`,
    `Milestone scope: ${input.milestone.prompt}`,
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "Act like a department lead: turn the milestone into concrete work, coordinate with executors, consult the technical advisor for architecture or performance-sensitive choices, and keep main PM strategy visible.",
    "Do not edit implementation code from the department PM session. You may write or revise workflow plan, decomposition, reference, and review documents; implementation belongs to executor sessions.",
    `Save a detailed execution plan to ${workflowArtifactPath(input.workflow, input.milestone.id, "plan.md")}.`,
    `If this milestone is still too broad, update ${workflowArtifactPath(input.workflow, "workflow.xml")} before writing a broad plan.`,
    `When splitting, replace this milestone with ordered/parallel child milestones whose ids are prefixed with "${input.milestone.id}-". Preserve the dependency intent and do not leave dependencies pointing at a removed milestone id.`,
    `Write the split rationale and child mapping to ${workflowArtifactPath(input.workflow, workflowMilestoneDecompositionPath(input.milestone.id))}.`,
    "Each child milestone must name concrete files or modules, deliverables, acceptance checks, and whether it can run in parallel.",
    "For plugin or graphics work, split independent concerns such as manager lifecycle, CPU/GPU paths, materials, textures, importers, emitter shapes, particle types, editor UI, serialization/runtime API, docs, tests, and build integration.",
    "Only keep this milestone executable when it is already small enough for one executor session to complete without guessing.",
    "Keep every plan file linked to the parent workflow and to any related child plan files.",
    "Include a `## Handoff Summary` section describing the executor-ready scope, assumptions, risks, and expected evidence.",
    "If you need strategy clarification, emit an opencode workflow message to main_pm or requester. If you need technical guidance, emit one to expert.",
    "Use the built-in workflow tool with action=status at the start. After writing plan.md and the Handoff Summary, call action=plan_complete for this milestone so the runtime can continue. If you split this milestone, call action=update_xml with the full revised workflow XML after writing it. If a real blocker prevents dispatch, call action=block with the blocker reason.",
    "Do not close planning by calling milestone_status done/approved. That command is intentionally rejected from planning; use plan_complete, or ask requester/main PM to force_complete a gate-only milestone.",
    "Do not tell the company that executors were dispatched unless the workflow tool confirms update_xml/resume or this milestone is already running in the workflow status.",
  ].join("\n")
}

function promptExecutor(input: {
  workflow: WorkflowInfo
  milestone: WorkflowMilestoneInfo
  expertPath?: string
  peerContext?: string
}) {
  return [
    `You are a long-lived executor employee assigned to workflow milestone ${input.milestone.id}.`,
    "",
    `Main request: ${input.workflow.request}`,
    `Detailed plan file: ${workflowArtifactPath(input.workflow, input.milestone.id, "plan.md")}`,
    ...(input.expertPath ? [`Technical advisor notes: ${input.expertPath}`] : []),
    ...(input.peerContext ? ["", "Workflow-triggered executor peer sync:", input.peerContext] : []),
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "Before editing, read the linked workflow and plan files for this milestone.",
    "Use the built-in workflow tool with action=status at the start so you know the current workflow and neighboring milestone states.",
    "Coordinate like an engineer in a small company. If another executor, PM, tester, requester, or technical advisor has context you need, emit workflow communication XML instead of guessing.",
    "If the plan is still a broad umbrella for multiple independent subsystems, do not mark it complete by doing a shallow slice. Explain the required split in the milestone plan or output so the department PM can reject it back to planning.",
    "Execute the plan in continuous plan execution mode until this milestone is complete.",
    "At task end, include a `## Handoff Summary` section with decisions, changed files, tests/evidence, remaining risks, and facts other staff should reuse; this summary is archived into the workflow reference library.",
    "At the end, include this exact XML marker:",
    `<opencode-workflow-result milestone="${input.milestone.id}" status="complete">`,
    "summary of completed work",
    "</opencode-workflow-result>",
  ].join("\n")
}

function promptReviewer(input: { workflow: WorkflowInfo; milestone: WorkflowMilestoneInfo; executorOutput: string }) {
  return [
    `You are the department PM acting as functional reviewer for workflow milestone ${input.milestone.id}.`,
    "",
    `Main request: ${input.workflow.request}`,
    `Detailed plan file: ${workflowArtifactPath(input.workflow, input.milestone.id, "plan.md")}`,
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "Use the built-in workflow tool with action=status at the start so your review uses the current workflow state.",
    "Review whether the executor completed the functional scope of this milestone. Check the repository state, linked workflow files, plan files, advisor notes, staff summaries, and relevant diffs.",
    "Reject work that treats a broad subsystem as complete without handling the concrete child concerns called out by the plan.",
    "Reject work that should have been decomposed into smaller workflow milestones but was implemented as a vague single pass.",
    "Include a `## Handoff Summary` section that other PMs, testers, and the main PM can reuse.",
    "If acceptable, respond with:",
    `<opencode-workflow-review milestone="${input.milestone.id}" decision="approve">approved reason</opencode-workflow-review>`,
    "If not acceptable, respond with:",
    `<opencode-workflow-review milestone="${input.milestone.id}" decision="reject">specific feedback</opencode-workflow-review>`,
    "",
    "Executor output:",
    input.executorOutput,
  ].join("\n")
}

function promptTester(input: { workflow: WorkflowInfo; milestones: WorkflowMilestoneInfo[] }) {
  return [
    "You are the long-lived workflow tester responsible for completeness review and regression feedback.",
    "",
    `Main request: ${input.workflow.request}`,
    `Write targeted test notes to ${workflowArtifactPath(input.workflow, workflowTestPlanPath())}.`,
    workflowReferencePrompt(input.workflow),
    "",
    "Use the built-in workflow tool with action=status at the start. If all work is truly complete after your required XML gate, action=complete may be used as an explicit final workflow signal.",
    "Create or update focused tests for the approved milestones, then run the most relevant checks.",
    "Your review is about completeness, regression coverage, and whether feedback loops should reopen PM/executor work. Functional acceptance remains with PM/requester.",
    "Test by concrete subsystem and linked plan file, not only by top-level feature wording.",
    "If you need product clarification, ask the main product manager session explicitly.",
    "End with exactly one test gate XML block so the workflow manager can decide whether to proceed to technical assessment and acceptance.",
    "Also include a `## Handoff Summary` section for future PM, requester, executor, and advisor sessions.",
    "If the workflow is complete enough for acceptance, respond with:",
    `<opencode-workflow-test decision="pass">tested scope, commands, evidence, and residual risks</opencode-workflow-test>`,
    "If tests fail, coverage is incomplete, or work must reopen, respond with:",
    `<opencode-workflow-test decision="fail" milestones="comma-separated affected milestone ids">specific failing checks, affected milestones, and required feedback loop</opencode-workflow-test>`,
    "",
    "Approved milestones:",
    ...input.milestones.map((milestone) => `- ${milestone.id}: ${milestone.title ?? milestone.prompt}`),
  ].join("\n")
}

function promptExpert(input: { workflow: WorkflowInfo; milestone: WorkflowMilestoneInfo }) {
  return [
    `You are the technical advisor for workflow milestone ${input.milestone.id}.`,
    "",
    `Main request: ${input.workflow.request}`,
    `Milestone title: ${input.milestone.title ?? input.milestone.id}`,
    `Department: ${input.milestone.department ?? "unspecified"}`,
    `Detailed plan file: ${workflowArtifactPath(input.workflow, input.milestone.id, "plan.md")}`,
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "Use the built-in workflow tool with action=status at the start so your advice matches the current workflow graph.",
    "Advise on technical selection, architecture risks, integration boundaries, performance, maintainability, and test strategy before the executor starts.",
    "Be specific enough for the executor, department PM, and tester to reuse your answer. Name files, modules, commands, and risk gates when possible.",
    "If the milestone is too broad, recommend concrete decomposition rather than allowing a vague implementation pass.",
    "Include a `## Handoff Summary` section for future executor, PM, tester, and final technical assessment sessions.",
    `Write a concise advisor note suitable for ${workflowArtifactPath(input.workflow, workflowExpertNotePath(input.milestone.id, input.milestone.attempt))}.`,
  ].join("\n")
}

function promptAcceptance(input: {
  workflow: WorkflowInfo
  role: "main_pm" | "requester"
  testerOutput: string
  expertOutput: string
  mainPmOutput?: string
}) {
  const roleName = input.role === "main_pm" ? "main product manager" : "requester and strategic owner"
  return [
    `You are the ${roleName} of this workflow.`,
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "The workflow implementation and completeness testing have finished. Review whether the outcome satisfies the strategic goal. You may accept, redirect, or reopen work.",
    "Respond with exactly one acceptance XML block so the workflow manager can close or reopen the workflow.",
    "If acceptable, respond with:",
    `<opencode-workflow-acceptance role="${input.role}" decision="approve">approved reason</opencode-workflow-acceptance>`,
    ...(input.role === "requester"
      ? [
          "When approving final workflow completion, also include this exact completion marker:",
          '<opencode-workflow-complete status="complete">final accepted outcome</opencode-workflow-complete>',
        ]
      : []),
    "If not acceptable, respond with:",
    `<opencode-workflow-acceptance role="${input.role}" decision="reject" milestones="comma-separated affected milestone ids">specific feedback and what must reopen</opencode-workflow-acceptance>`,
    "",
    "Tester output:",
    input.testerOutput,
    "",
    "Technical advisor final assessment:",
    input.expertOutput,
    ...(input.mainPmOutput
      ? [
          "",
          "Main PM final acceptance notes:",
          input.mainPmOutput,
        ]
      : []),
  ].join("\n")
}

function mainPlanningExpectation(): WorkflowPromptExpectation {
  return {
    description: "dispatchable workflow XML and main plan",
    reminder: [
      "Finish the main PM planning task now.",
      `Write workflow.xml and ${workflowMainPlanPath()}, or emit:`,
      '<opencode-workflow-update reason="initial dispatch"><workflow>...</workflow></opencode-workflow-update>',
      "After writing files, call the built-in workflow tool with action=update_xml or action=resume so the runtime can validate and dispatch.",
      "Do not say dispatch has started unless the workflow runtime has received workflow XML.",
    ].join("\n"),
    matches: (text) =>
      parseWorkflowUpdateXml(text) !== undefined ||
      /opencode-workflow-control\b[^>]*\baction=["']resume["']/i.test(text) ||
      (/workflow\.xml/i.test(text) && /main-plan\.md/i.test(text)),
  }
}

function handoffExpectation(label: string): WorkflowPromptExpectation {
  return {
    description: `${label} with a reusable ## Handoff Summary section`,
    reminder: "Finish the role task now and include a `## Handoff Summary` section with completed scope, evidence, risks, and next owner.",
    matches: hasHandoffSummary,
    maxAttempts: 2,
  }
}

function executorExpectation(milestoneID: WorkflowMilestoneID): WorkflowPromptExpectation {
  return {
    description: `executor completion XML for milestone ${milestoneID}`,
    reminder: [
      "Continue executing the milestone until the planned work is complete.",
      "End with:",
      `<opencode-workflow-result milestone="${milestoneID}" status="complete">summary of completed work</opencode-workflow-result>`,
    ].join("\n"),
    matches: workflowResultComplete,
  }
}

function reviewExpectation(milestoneID: WorkflowMilestoneID): WorkflowPromptExpectation {
  return {
    description: `functional review XML for milestone ${milestoneID}`,
    reminder: [
      "Complete the functional review now.",
      "End with exactly one of:",
      `<opencode-workflow-review milestone="${milestoneID}" decision="approve">approved reason</opencode-workflow-review>`,
      `<opencode-workflow-review milestone="${milestoneID}" decision="reject">specific feedback</opencode-workflow-review>`,
    ].join("\n"),
    matches: (text) => approved(text) || rejected(text),
  }
}

function testExpectation(): WorkflowPromptExpectation {
  return {
    description: "tester gate XML with pass/fail decision",
    reminder:
      'Complete the test review now and end with `<opencode-workflow-test decision="pass">...</opencode-workflow-test>` or `<opencode-workflow-test decision="fail" milestones="...">...</opencode-workflow-test>`.',
    matches: (text) => parseTestDecision(text) !== undefined,
  }
}

function technicalExpectation(): WorkflowPromptExpectation {
  return {
    description: "technical assessment XML with pass/fail decision",
    reminder:
      'Complete the technical assessment now and end with `<opencode-workflow-technical decision="pass">...</opencode-workflow-technical>` or `<opencode-workflow-technical decision="fail" milestones="...">...</opencode-workflow-technical>`.',
    matches: (text) => parseTechnicalDecision(text) !== undefined,
  }
}

function acceptanceExpectation(role: "main_pm" | "requester"): WorkflowPromptExpectation {
  return {
    description:
      role === "requester"
        ? `${roleSessionTitle(role)} acceptance XML plus workflow completion marker`
        : `${roleSessionTitle(role)} acceptance XML with approve/reject decision`,
    reminder:
      role === "requester"
        ? `Complete final acceptance now. Reject with <opencode-workflow-acceptance role="${role}" decision="reject">...</opencode-workflow-acceptance>, or approve with both <opencode-workflow-acceptance role="${role}" decision="approve">...</opencode-workflow-acceptance> and <opencode-workflow-complete status="complete">...</opencode-workflow-complete>.`
        : `Complete acceptance now and end with exactly one <opencode-workflow-acceptance role="${role}" decision="approve|reject">...</opencode-workflow-acceptance> block.`,
    matches: (text) => {
      const decision = parseAcceptanceDecision(text)
      if (role !== "requester") return decision !== undefined
      return decision === "reject" || (decision === "approve" && workflowComplete(text))
    },
  }
}

function workflowControlExpectation(): WorkflowPromptExpectation {
  return {
    description: "workflow control XML with resume/block or command action",
    reminder:
      'Decide whether the workflow should resume or remain blocked and end with `<opencode-workflow-control action="resume">...</opencode-workflow-control>` or `<opencode-workflow-control action="block">...</opencode-workflow-control>`. If you are closing a specific gate, use action="plan_complete", action="force_complete", action="force_skip", or action="milestone_status" with milestone="...".',
    matches: (text) => parseWorkflowControlCommand(text) !== undefined,
  }
}

function hasHandoffSummary(text: string) {
  return /(^|\n)#{2,6}\s+Handoff Summary\b/i.test(text)
}

function workflowResultComplete(text: string) {
  return /<opencode-workflow-result\b[^>]*\bstatus=["']complete["'][^>]*>/i.test(text)
}

function workflowComplete(text: string) {
  return /<opencode-workflow-complete\b[^>]*\bstatus=["']complete["'][^>]*>/i.test(text)
}

function workflowSessionExpectation(input: {
  role: WorkflowSessionRef["role"]
  milestoneID?: WorkflowMilestoneID
  milestoneStatus?: WorkflowMilestoneInfo["status"]
  workflowStatus: WorkflowInfo["status"]
}) {
  if (input.role === "main_pm") {
    if (input.workflowStatus === "accepting") return acceptanceExpectation("main_pm")
    return mainPlanningExpectation()
  }
  if (input.role === "department_pm" && input.milestoneID) {
    if (input.milestoneStatus === "reviewing") return reviewExpectation(input.milestoneID)
    return handoffExpectation("department PM execution plan")
  }
  if (input.role === "reviewer" && input.milestoneID) return reviewExpectation(input.milestoneID)
  if (input.role === "executor" && input.milestoneID) return executorExpectation(input.milestoneID)
  if (input.role === "tester") return testExpectation()
  if (input.role === "expert") {
    if (input.workflowStatus === "testing" || input.workflowStatus === "accepting") return technicalExpectation()
    return handoffExpectation("technical advisor note")
  }
  if (input.role === "requester" && input.workflowStatus === "accepting") return acceptanceExpectation("requester")
  return undefined
}

function workflowExpectedOutputPrompt(expectation: WorkflowPromptExpectation, attempt: number, maxAttempts: number) {
  return [
    "Continue this workflow session. The previous response stopped before the workflow manager could recognize the required completion output.",
    "",
    `Required output: ${expectation.description}`,
    "",
    expectation.reminder,
    "",
    "Do not request product clarification unless the original request is genuinely impossible to interpret. If another workflow employee has needed context, use workflow communication XML and then continue after the answer.",
    "Workflow communication XML is not a dispatch signal. If the missing required output is a workflow status transition, use the built-in workflow tool and confirm it applied before saying the transition happened.",
    "If a requester/user decision is truly required, emit `<opencode-workflow-message to-role=\"requester\" timing=\"interrupt\" reason=\"decision required\">...Options: 1. ... (Recommended) ... 2. ...</opencode-workflow-message>` with concrete options and impacts.",
    `This is automatic continuation attempt ${attempt} of ${maxAttempts}.`,
  ].join("\n")
}

function workflowDispatchCorrectionExpectation(role: WorkflowSessionRef["role"]): WorkflowPromptExpectation {
  return {
    description: "real workflow dispatch/control output, not workflow-message assignment",
    reminder: [
      "Your previous response claimed dispatch, routing, or queueing without a confirmed workflow control result.",
      "Workflow communication or natural-language assignment only asks or notifies staff. It does not create a milestone job, attach a session to a milestone, unblock ordered dependencies, or close the current planning gate.",
      "Now produce the real control output:",
      "- If the graph needs more or different milestone jobs, emit <opencode-workflow-update> with the full valid workflow XML, then use the workflow tool action=update_xml.",
      "- If the graph is already valid and should continue, use the workflow tool action=resume or end with <opencode-workflow-control action=\"resume\">...</opencode-workflow-control>.",
      "- If you own a department PM planning gate and the plan is ready, use workflow tool action=plan_complete for the milestone, or emit <opencode-workflow-control action=\"plan_complete\" milestone=\"milestone-id\">...</opencode-workflow-control>.",
      "- If a requester/main PM gate should unblock downstream work without executor/review work, use workflow tool action=force_complete, or emit <opencode-workflow-control action=\"force_complete\" milestone=\"milestone-id\">...</opencode-workflow-control>.",
      "- If this was only a question to another employee, rewrite it as a question and do not describe it as an assignment, dispatch, wave, or executor startup.",
    ].join("\n"),
    matches: (text) =>
      parseWorkflowUpdateXml(text) !== undefined ||
      parseWorkflowControlCommand(text) !== undefined ||
      (parseConsultRequests(text).length > 0 && !workflowMessageDispatchMisuse(text, role)),
    maxAttempts: 2,
  }
}

function workflowDispatchCorrectionPrompt(input: {
  workflow: WorkflowInfo
  role: WorkflowSessionRef["role"]
  milestoneID?: WorkflowMilestoneID
  previous: string
}) {
  return [
    "Workflow dispatch correction required.",
    "",
    `Workflow: ${input.workflow.id}`,
    `Role: ${roleSessionTitle(input.role)}`,
    ...(input.milestoneID ? [`Milestone: ${input.milestoneID}`] : []),
    "",
    "The previous response looked like it claimed milestone dispatch, routing, or queueing without a confirmed workflow control result.",
    "Workflow messages and natural-language assignments are only consultation or notification. They cannot start executor/reviewer/tester sessions and cannot satisfy ordered/parallel DAG scheduling.",
    "",
    "Correct the workflow state now using the real dispatch path:",
    "1. For new or revised work items, emit <opencode-workflow-update> containing the full valid workflow XML and then use workflow tool action=update_xml.",
    "2. If the XML is already correct, use workflow tool action=resume or end with <opencode-workflow-control action=\"resume\">continue scheduling</opencode-workflow-control>.",
    "3. If this is a department PM planning handoff, finish the plan with ## Handoff Summary and use workflow tool action=plan_complete, or emit <opencode-workflow-control action=\"plan_complete\" milestone=\"milestone-id\">...</opencode-workflow-control>.",
    "4. If this is a PM-only gate that should unblock downstream work, requester/main PM must use workflow tool action=force_complete, or emit <opencode-workflow-control action=\"force_complete\" milestone=\"milestone-id\">...</opencode-workflow-control>.",
    "5. If you only need information from another employee, ask a question; do not call it assignment, dispatch, wave, direct routing, or executor startup.",
    "",
    "Previous response excerpt:",
    compactMarkdown(input.previous, 1200),
  ].join("\n")
}

function defaultMilestonePlan(workflow: WorkflowInfo, milestone: WorkflowMilestoneInfo) {
  return [
    `# ${milestone.title ?? milestone.id}`,
    "",
    `Workflow: ${workflow.id}`,
    `Department: ${milestone.department ?? "unspecified"}`,
    "",
    "## User Requirement",
    workflow.request,
    "",
    "## Initial Scope",
    milestone.prompt,
    "",
    "This file is pre-created by the workflow manager. The department product manager session will replace it with a detailed execution plan.",
    "",
  ].join("\n")
}

function approved(text: string) {
  return /<opencode-workflow-review\b[^>]*\bdecision=["']approve["'][^>]*>/i.test(text)
}

function rejected(text: string) {
  return /<opencode-workflow-review\b[^>]*\bdecision=["']reject["'][^>]*>/i.test(text)
}

export function parseAcceptanceDecision(text: string) {
  const decision = /<opencode-workflow-acceptance\b[^>]*\bdecision=["'](approve|reject)["'][^>]*>/i.exec(text)?.[1]
  if (decision === "approve" || decision === "reject") return decision
  return undefined
}

function parseRequesterAcceptanceDecision(text: string) {
  const attributes = /<opencode-workflow-acceptance\b([^>]*)>/i.exec(text)?.[1] ?? ""
  if (consultationAttribute(attributes, "role") !== "requester") return undefined
  return parseAcceptanceDecision(text)
}

export function parseTestDecision(text: string) {
  const decision = /<opencode-workflow-test\b[^>]*\bdecision=["'](pass|fail)["'][^>]*>/i.exec(text)?.[1]
  if (decision === "pass" || decision === "fail") return decision
  return undefined
}

export function parseTechnicalDecision(text: string) {
  const decision = /<opencode-workflow-technical\b[^>]*\bdecision=["'](pass|fail)["'][^>]*>/i.exec(text)?.[1]
  if (decision === "pass" || decision === "fail") return decision
  return undefined
}

export function parseGateMilestoneIDs(text: string, gate: "test" | "technical" | "acceptance") {
  const attributes = new RegExp(`<opencode-workflow-${gate}\\b([^>]*)>`, "i").exec(text)?.[1] ?? ""
  const value =
    consultationAttribute(attributes, "milestones") ??
    consultationAttribute(attributes, "milestone") ??
    consultationAttribute(attributes, "reopen")
  return Array.from(new Set((value ?? "").split(/[,\s;]+/).map((item) => item.trim()).filter(Boolean))).map((item) =>
    WorkflowMilestoneID.make(item),
  )
}

const workflowControlActions = new Set([
  "resume",
  "block",
  "milestone_status",
  "plan_complete",
  "force_complete",
  "force_skip",
  "scheduling",
  "workflow_status",
  "complete",
])
const workflowMilestoneStatusValues = new Set([
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
const workflowStatusValues = new Set([
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
const workflowSchedulingModeValues = new Set(["eager", "staged", "economical"])

export function parseWorkflowControlCommand(text: string) {
  const paired = /<opencode-workflow-control\b([^>]*)>([\s\S]*?)<\/opencode-workflow-control>/i.exec(text)
  const standalone = paired ? undefined : /<opencode-workflow-control\b([^>]*)\/?>/i.exec(text)
  const attributes = paired?.[1] ?? standalone?.[1]
  if (!attributes) return undefined
  const action = (workflowControlAttribute(attributes, ["action"]) ?? "").replace(/-/g, "_").toLowerCase()
  if (!workflowControlActions.has(action)) return undefined
  const milestoneID = workflowControlAttribute(attributes, ["milestoneID", "milestone-id", "milestone", "id"])
  const milestoneStatus = workflowMilestoneStatusAttribute(
    workflowControlAttribute(attributes, ["milestoneStatus", "milestone-status", "status"]),
  )
  const workflowStatus = workflowControlAttribute(attributes, ["workflowStatus", "workflow-status", "workflow"])?.toLowerCase()
  const schedulingMode = workflowControlAttribute(attributes, ["schedulingMode", "scheduling-mode", "mode"])?.toLowerCase()
  const schedulingMaxActive = Number(workflowControlAttribute(attributes, ["schedulingMaxActive", "scheduling-max-active", "maxActive", "max-active"]))
  const message = paired?.[2]?.trim()
  return {
    action,
    ...(milestoneID ? { milestoneID: WorkflowMilestoneID.make(milestoneID) } : {}),
    ...(milestoneStatus && workflowMilestoneStatusValues.has(milestoneStatus) ? { milestoneStatus } : {}),
    ...(workflowStatus && workflowStatusValues.has(workflowStatus) ? { workflowStatus } : {}),
    ...(schedulingMode && workflowSchedulingModeValues.has(schedulingMode) ? { schedulingMode } : {}),
    ...(Number.isFinite(schedulingMaxActive) ? { schedulingMaxActive } : {}),
    ...(message ? { message } : {}),
  } as WorkflowToolCommand
}

function workflowMilestoneStatusAttribute(value: string | undefined) {
  const status = value?.replace(/[-\s]+/g, "_").toLowerCase()
  if (!status) return undefined
  if (status === "approval" || status === "approve" || status === "accepted") return "approved"
  if (status === "complete" || status === "finished" || status === "finish") return "done"
  if (status === "in_progress" || status === "inprogress") return "executing"
  return workflowMilestoneStatusValues.has(status) ? status : undefined
}

export function parseWorkflowControlAction(text: string) {
  const action = parseWorkflowControlCommand(text)?.action
  if (action === "resume" || action === "block") return action
  return undefined
}

function workflowControlAttribute(attributes: string, names: string[]) {
  return names
    .map((name) => new RegExp(`\\b${name}\\s*=\\s*["']([^"']+)["']`, "i").exec(attributes)?.[1]?.trim())
    .find((value) => value)
}

export function workflowMessageDispatchMisuse(text: string, role?: WorkflowSessionRef["role"]) {
  if (role !== "requester" && role !== "main_pm" && role !== "department_pm") return false
  const requests = parseConsultRequests(text)
  if (
    !requests.some(
      (request) =>
        request.targetRole === "department_pm" ||
        request.targetRole === "executor" ||
        request.targetRole === "reviewer" ||
        request.targetRole === "tester",
    )
  ) {
    return false
  }
  return requests.some((request) =>
    workflowTextClaimsDispatch([request.reason ?? "", request.question, text].filter(Boolean).join("\n")),
  )
}

export function workflowDispatchClaimWithoutControl(text: string, role?: WorkflowSessionRef["role"]) {
  if (role !== "requester" && role !== "main_pm" && role !== "department_pm") return false
  if (parseWorkflowUpdateXml(text) || parseWorkflowControlCommand(text)) return false
  if (workflowMessageDispatchMisuse(text, role)) return true
  return workflowTextClaimsDispatch(text) || workflowTextClaimsControlQueue(text)
}

function workflowPlanClaimsClosedGate(text: string) {
  return (
    /\bqueued\b[\s\S]{0,120}\bmilestone_status\s*=\s*(done|approved|approval|complete|completed)\b/i.test(text) ||
    /\bmilestone_status\s*=\s*(done|approved|approval|complete|completed)\b[\s\S]{0,120}\bqueued\b/i.test(text) ||
    /\bgate[-\s]?only\b[\s\S]{0,120}\bcomplete\b/i.test(text) ||
    /\brequirements[-\s]?milestone work is complete\b/i.test(text)
  )
}

function workflowTextClaimsDispatch(text: string) {
  const cleaned = text.replace(/do not use <opencode-workflow-message> as a dispatch mechanism/gi, "")
  return /(\byour assignment\b|\bassign(?:ing|ed|ment)?\b[\s\S]{0,120}\b(executor|reviewer|tester|department[_\s-]*pm|sessions?)\b|\bdispatch(?:ed|ing)?\b[\s\S]{0,120}\b(executor|reviewer|tester|session|wave)\b|\broute\b[\s\S]{0,120}\bexecutor\b|\bprompt\b[\s\S]{0,120}\bexecutor sessions\b|\bwave\s*\d\b|第一波|第[一二三四五六七八九十]+波|直接继续到执行者|直接(?:派发|分配|启动)|开始派发|派发(?:执行者|会话|任务|第一波)|启动(?:执行者|后续流程)|路由到执行者|分配给执行者)/i.test(
    cleaned,
  )
}

function workflowCommandMessageClaimsDispatch(text: string | undefined, role?: WorkflowSessionRef["role"]) {
  if (!text || (role !== "main_pm" && role !== "department_pm")) return false
  return workflowTextClaimsDispatch(text)
}

function workflowTextClaimsControlQueue(text: string) {
  const controlTerm =
    /\b(milestone_status|plan_complete|force_complete|resume|workflow manager|control[-_\s]?plane|scheduler|dispatch|dispatching|queue|调度|派发|后续流程)\b/i
  return (
    /(\bqueued\b|\bqueue(?:d|ing)?\b|\bre-issue(?:d)?\b|\bissued\b|已(?:排队|提交|发送|触发)|重新(?:排队|提交|发送)|再次(?:排队|提交|发送))[\s\S]{0,180}\b(milestone_status|plan_complete|force_complete|resume|workflow manager|control[-_\s]?plane|scheduler|dispatch|dispatching|调度|派发|后续流程)\b/i.test(
      text,
    ) ||
    (controlTerm.test(text) &&
      /\b(hasn['’]?t|has not|not|still|genuinely|appears|is)\b[\s\S]{0,120}\b(drained?|draining|transitioned|stalled|stuck|blocked)\b|\b(drained?|draining|transitioned|stalled|stuck|blocked)\b[\s\S]{0,120}\b(milestone_status|plan_complete|force_complete|resume|control[-_\s]?plane|queue|scheduler)\b|没有(?:执行|生效|转换|派发)|未(?:执行|生效|转换|派发)/i.test(
        text,
      ))
  )
}

function implicitWorkflowResume(text: string) {
  const toolingBlocker = /Bun is not defined/i.test(text) && /(stale|runner|tooling|运行器|工具|不是产品|不需要产品澄清|不需要修订\s*XML)/i.test(text)
  const dispatching = /Status\s*:\s*dispatching/i.test(text) || /状态\s*[:：]?\s*dispatching/i.test(text)
  const handoff = /(继续派发|开始派发|接手\s*M\d+|M\d+[-_\w]*\s*[:：]\s*(in_progress|pending)|下一步\s*[:：]?.*M\d+)/i.test(text)
  return (dispatching && (handoff || toolingBlocker)) || (toolingBlocker && handoff)
}

function requesterDirectExecutionOverride(text: string) {
  return (
    /(忽视|跳过|绕过|不要等|不用等|不需要等)[\s\S]{0,40}(planning|计划|规划|需求|requirements)/i.test(text) ||
    /直接[\s\S]{0,40}(继续|进入|到|派发|分配|启动)[\s\S]{0,40}(执行者|执行|executor|审计|audit|后续)/i.test(text) ||
    /(?:ignore|skip|bypass)[\s\S]{0,40}(planning|requirements)[\s\S]{0,60}(executor|execution|dispatch|audit|downstream)/i.test(
      text,
    )
  )
}

function workflowInferredDispatchControl(input: {
  text: string
  role?: WorkflowSessionRef["role"]
  milestoneID?: WorkflowMilestoneID
  milestoneStatus?: WorkflowMilestoneInfo["status"]
  milestones?: WorkflowMilestoneInfo[]
}) {
  if (!workflowDispatchClaimWithoutControl(input.text, input.role)) return
  const currentStatus = input.milestoneStatus ? canonicalMilestoneStatus(input.milestoneStatus) : undefined
  const gateClosed =
    workflowPlanClaimsClosedGate(input.text) || requesterDirectExecutionOverride(input.text) || hasHandoffSummary(input.text)
  if (
    input.milestoneID &&
    gateClosed &&
    (currentStatus === "pending" || currentStatus === "planning" || currentStatus === "blocked") &&
    (input.role === "requester" || input.role === "main_pm" || input.role === "department_pm")
  ) {
    return {
      action: input.role === "department_pm" ? "plan_complete" : "force_complete",
      milestoneID: input.milestoneID,
      message:
        "Runtime converted a completed planning-gate handoff that claimed dispatch without confirmed workflow control into the real workflow control command.",
    } satisfies Partial<WorkflowToolCommand>
  }
  if (!gateClosed || (input.role !== "requester" && input.role !== "main_pm")) return
  const activeGates = (input.milestones ?? []).filter((item) =>
    ["planning", "blocked"].includes(canonicalMilestoneStatus(item.status)),
  )
  const planningGates =
    activeGates.length > 0
      ? activeGates
      : (input.milestones ?? []).filter((item) => canonicalMilestoneStatus(item.status) === "pending")
  if (planningGates.length !== 1) return
  return {
    action: "force_complete",
    milestoneID: planningGates[0].id,
    message:
      "Runtime converted a requester/main PM planning-gate handoff that claimed dispatch without confirmed workflow control into force_complete.",
  } satisfies Partial<WorkflowToolCommand>
}

function feedbackMilestoneIDs(items: WorkflowMilestoneInfo[], ids: WorkflowMilestoneID[]) {
  const known = new Set(items.map((item) => String(item.id)))
  const affected = new Set(ids.map(String).filter((id) => known.has(id)))
  for (let changed = true; changed;) {
    changed = false
    for (const item of items) {
      if (affected.has(String(item.id))) continue
      if (!item.dependsOn.some((id) => affected.has(String(id)))) continue
      affected.add(String(item.id))
      changed = true
    }
  }
  return items.filter((item) => affected.has(String(item.id))).map((item) => item.id)
}

function parseXmlDefinition(xml: string, workflow?: Pick<WorkflowInfo, "directory" | "id" | "path">) {
  try {
    return parseWorkflowXml(xml, workflow ? { readPipelineItems: workflowPipelineItemReader(workflow) } : {})
  } catch (error) {
    throw new globalThis.Error(error instanceof globalThis.Error ? error.message : String(error))
  }
}

function workflowPipelineItemReader(workflow: Pick<WorkflowInfo, "directory" | "id" | "path">) {
  return (itemsPath: string) => {
    const normalized = normalizedWorkflowItemPath(itemsPath)
    if (!normalized) throw new globalThis.Error(`invalid workflow pipeline items path: ${itemsPath}`)
    try {
      return JSON.parse(readFileSync(projectWorkflowPath(workflow.directory, workflow, normalized), "utf8"))
    } catch (error) {
      if (nodeErrorCode(error) === "ENOENT") return workflowPipelineItemsMissing(normalized)
      throw error
    }
  }
}

function nodeErrorCode(error: unknown) {
  return typeof error === "object" && error !== null && "code" in error
    ? String((error as { code?: unknown }).code)
    : undefined
}

function normalizedWorkflowItemPath(itemsPath: string) {
  const trimmed = itemsPath.trim()
  if (!trimmed || path.isAbsolute(trimmed)) return undefined
  const parts = trimmed.split(/[\\/]+/).filter(Boolean)
  if (parts.length === 0 || parts.some((part) => part === "..")) return undefined
  return path.join(...parts)
}

function workflowPipelineItemPaths(xml: string) {
  return Array.from(xml.matchAll(/<pipeline\b[^>]*\bitems\s*=\s*["']([^"']+)["']/gi))
    .map((match) => match[1])
    .filter((value): value is string => typeof value === "string")
    .map(normalizedWorkflowItemPath)
    .filter((value): value is string => typeof value === "string")
}

function workflowXmlFromText(text: string) {
  const trimmed = text.trim()
  const candidate = /^<workflow\b/i.test(trimmed)
    ? trimmed
    : /<workflow\b[\s\S]*<\/workflow>/i.exec(trimmed)?.[0]?.trim()
  if (!candidate) return undefined
  try {
    parseXmlDefinition(candidate)
    return candidate
  } catch {
    return undefined
  }
}

function toConsultation(row: typeof WorkflowConsultationTable.$inferSelect): WorkflowConsultationInfo {
  return {
    id: row.id,
    workflowID: row.workflow_id,
    fromSessionID: row.from_session_id,
    toSessionID: row.to_session_id,
    fromRole: row.from_role,
    toRole: row.to_role,
    milestoneID: row.milestone_id ?? undefined,
    reason: row.reason ?? undefined,
    timing: row.timing ?? undefined,
    question: row.question,
    answer: row.answer,
    status: row.status,
    time: {
      created: row.time_created,
      updated: row.time_updated,
    },
  }
}

function workflowStatePath(workflow: Pick<WorkflowInfo, "id" | "path">) {
  return workflowArtifactPath(workflow, workflowStateFileName)
}

function workflowStateSessionSnapshotPath(sessionID: SessionID) {
  return path.join("state", "sessions", `${String(sessionID).replace(/[^a-zA-Z0-9._-]+/g, "_")}.json`)
}

function workflowStateRelativePath(value: string | undefined) {
  if (!value || path.isAbsolute(value)) return undefined
  const normalized = path.normalize(value)
  if (normalized === ".." || normalized.startsWith(".." + path.sep)) return undefined
  return normalized
}

function isWorkflowStatePath(file: string) {
  const normalized = path.normalize(file)
  return path.basename(normalized) === workflowStateFileName && path.dirname(path.dirname(normalized)) === workflowDir
}

function workflowInstanceSessionPath(worktree: string, directory: string) {
  return path.relative(path.resolve(worktree), directory).replaceAll("\\", "/")
}

function workflowModelSessionRef(model: WorkflowInfo["model"] | undefined) {
  if (!model) return undefined
  return { id: model.modelID, providerID: model.providerID, ...(model.variant ? { variant: model.variant } : {}) }
}

function workflowStateDirectory(workflow: WorkflowInfo) {
  return { ...workflow, directory: "." }
}

function workflowStateFileDirectory(ctx: { directory: string }, file: string) {
  const relative = normalizedRelativePath(ctx.directory, path.dirname(file))
  if (!relative) return
  const normalized = path.normalize(relative)
  if (path.dirname(normalized) === workflowDir) return normalized.replaceAll("\\", "/")
}

function workflowStateStoredPath(pathname: string | undefined, fallback: string | undefined) {
  const normalized = path.normalize(pathname ?? "")
  if (normalized === workflowDir || normalized.startsWith(workflowDir + path.sep)) return pathname
  return fallback
}

function workflowStateSessionRefs(input: {
  workflow: WorkflowInfo
  milestones: WorkflowMilestoneInfo[]
  members: WorkflowMemberInfo[]
  consultations: WorkflowConsultationInfo[]
  interventions: WorkflowInterventionInfo[]
}) {
  const titles = workflowSessionTitles(input.workflow, input.milestones)
  const refs = new Map<SessionID, { sessionID: SessionID; role: WorkflowSessionRef["role"]; title?: string }>()
  const add = (sessionID: SessionID | undefined, role: WorkflowSessionRef["role"], title?: string) => {
    if (!sessionID || refs.has(sessionID)) return
    refs.set(sessionID, { sessionID, role, title: title ?? titles.get(sessionID) })
  }
  add(input.workflow.rootSessionID, "requester", workflowRequesterTitle(input.workflow.request))
  add(input.workflow.pmSessionID, "main_pm", workflowSessionTitle("Main PM", input.workflow.title))
  add(input.workflow.testerSessionID, "tester", workflowSessionTitle("Tester", input.workflow.title))
  input.members.forEach((member) => add(member.sessionID, member.role, member.title))
  input.milestones.forEach((milestone) => milestone.session.forEach((ref) => add(ref.sessionID, ref.role, titles.get(ref.sessionID))))
  input.consultations.forEach((consultation) => {
    add(consultation.fromSessionID, consultation.fromRole)
    add(consultation.toSessionID, consultation.toRole)
  })
  input.interventions.forEach((intervention) => {
    add(intervention.fromSessionID, "requester")
    add(intervention.targetSessionID, intervention.targetRole)
  })
  return [...refs.values()]
}

function workflowStateSessionSnapshot(input: {
  workflow: WorkflowInfo
  ref: { sessionID: SessionID; role: WorkflowSessionRef["role"]; title?: string }
  info?: Session.Info
}) {
  return {
    id: input.ref.sessionID,
    role: input.ref.role,
    title: input.info?.title ?? input.ref.title ?? roleSessionTitle(input.ref.role),
    parentID:
      input.info?.parentID ??
      (input.workflow.rootSessionID && input.ref.sessionID !== input.workflow.rootSessionID
        ? input.workflow.rootSessionID
        : undefined),
    agent: input.info?.agent ?? workflowAgentForRole(input.ref.role),
    model: input.info?.model ?? workflowModelSessionRef(input.workflow.model),
    metadata: {
      ...(input.info?.metadata ?? {}),
      restoredWorkflow: {
        id: input.workflow.id,
        path: input.workflow.path,
        role: input.ref.role,
        archive: workflowSessionArchivePath(input.ref.sessionID),
        summary: workflowSessionSummaryPath(input.ref.sessionID),
      },
    },
    permission: input.info?.permission,
    time: input.info?.time ?? {
      created: input.workflow.time.created,
      updated: input.workflow.time.updated,
    },
  }
}

function workflowStateSessionIndexEntry(session: ReturnType<typeof workflowStateSessionSnapshot>) {
  const { messages, durableMessages, durableInputs, contextEpoch, ...summary } = session
  return {
    ...summary,
    path: workflowStateSessionSnapshotPath(session.id),
    contentHash: workflowStateContentHash(session),
    messageCount: Array.isArray(messages) ? messages.length : 0,
    durableMessageCount: Array.isArray(durableMessages) ? durableMessages.length : 0,
    durableInputCount: Array.isArray(durableInputs) ? durableInputs.length : 0,
    contextEpoch: contextEpoch ? { baselineSeq: contextEpoch.baseline_seq } : undefined,
  }
}

async function readWorkflowStateSessionSnapshot(directory: string, workflow: WorkflowInfo, sessionState: Record<string, unknown>) {
  const relative = workflowStateRelativePath(typeof sessionState.path === "string" ? sessionState.path : undefined)
  if (!relative) return sessionState
  const sidecar = await readFile(projectWorkflowPath(directory, workflow, relative), "utf8")
    .then((text) => JSON.parse(text))
    .catch(() => undefined)
  if (!sidecar || typeof sidecar !== "object") return sessionState
  if (typeof sessionState.contentHash === "string" && workflowStateContentHash(sidecar) !== sessionState.contentHash) {
    throw new globalThis.Error(`session state content hash mismatch: ${relative}`)
  }
  return {
    ...sidecar,
    ...sessionState,
    messages: Array.isArray(sidecar.messages) ? sidecar.messages : sessionState.messages,
    durableMessages: Array.isArray(sidecar.durableMessages) ? sidecar.durableMessages : sessionState.durableMessages,
    durableInputs: Array.isArray(sidecar.durableInputs) ? sidecar.durableInputs : sessionState.durableInputs,
    contextEpoch: sidecar.contextEpoch ?? sessionState.contextEpoch,
  }
}

function workflowStateContentHash(value: unknown) {
  return createHash("sha256").update(workflowStateStableJson(value)).digest("hex")
}

function workflowStateStableJson(value: unknown): string {
  if (value === undefined) return "null"
  if (value === null || typeof value !== "object") return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(workflowStateStableJson).join(",")}]`
  return `{${Object.entries(value as Record<string, unknown>)
    .filter((entry) => entry[1] !== undefined)
    .toSorted((a, b) => a[0].localeCompare(b[0]))
    .map((entry) => `${JSON.stringify(entry[0])}:${workflowStateStableJson(entry[1])}`)
    .join(",")}}`
}

function workflowStateSessionRow(input: {
  ctx: { directory: string; worktree: string; project: { id: string } }
  workflow: WorkflowInfo
  session: ReturnType<typeof workflowStateSessionSnapshot>
}) {
  return {
    id: input.session.id,
    project_id: input.ctx.project.id,
    workspace_id: null,
    parent_id: input.session.parentID ?? null,
    slug: `restored-${String(input.session.id).replace(/[^a-zA-Z0-9._-]+/g, "-")}`,
    directory: input.ctx.directory,
    path: workflowInstanceSessionPath(input.ctx.worktree, input.ctx.directory),
    title: input.session.title,
    version: InstallationVersion,
    share_url: null,
    summary_additions: null,
    summary_deletions: null,
    summary_files: null,
    summary_diffs: null,
    metadata: input.session.metadata,
    cost: 0,
    tokens_input: 0,
    tokens_output: 0,
    tokens_reasoning: 0,
    tokens_cache_read: 0,
    tokens_cache_write: 0,
    revert: null,
    permission: input.session.permission ?? null,
    agent: input.session.agent ?? null,
    model: input.session.model ?? null,
    time_created: input.session.time?.created ?? input.workflow.time.created,
    time_updated: input.session.time?.updated ?? input.workflow.time.updated,
    time_compacting: input.session.time?.compacting ?? null,
    time_archived: input.session.time?.archived ?? null,
  }
}

function workflowRestoreMessageID(sessionID: SessionID) {
  return MessageID.ascending(`msg_workflow_restore_${String(sessionID).replace(/[^a-zA-Z0-9._-]+/g, "_")}`)
}

function workflowRestorePartID(sessionID: SessionID) {
  return PartID.ascending(`prt_workflow_restore_${String(sessionID).replace(/[^a-zA-Z0-9._-]+/g, "_")}`)
}

function workflowRestoreMessageExcerpts(session: ReturnType<typeof workflowStateSessionSnapshot>) {
  const messages = Array.isArray(session.messages) ? session.messages : []
  if (messages.length === 0) return []
  const excerpts = messages
    .flatMap((message) => (Array.isArray(message?.parts) ? message.parts : []))
    .flatMap((part) => (part?.type === "text" && typeof part.text === "string" ? [part.text] : []))
    .map((text) => text.replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .slice(0, 3)
  return [
    "",
    `Recovered snapshot messages: ${messages.length}.`,
    ...excerpts.map((text, index) => `Recovered message excerpt ${index + 1}: ${text.slice(0, 500)}`),
  ]
}

function workflowRestoreMessageText(workflow: WorkflowInfo, session: ReturnType<typeof workflowStateSessionSnapshot>) {
  return [
    "This session was restored from a project-local workflow snapshot.",
    "",
    `Workflow: ${workflow.id}`,
    `Workflow root: ${workflow.path}`,
    `Session: ${session.id}`,
    `Role: ${roleSessionTitle(session.role)}`,
    `Full session archive: ${workflowArtifactPath(workflow, workflowSessionArchivePath(session.id))}`,
    `Session summary: ${workflowArtifactPath(workflow, workflowSessionSummaryPath(session.id))}`,
    ...workflowRestoreMessageExcerpts(session),
    "",
    "Use the workflow root, reference library, and archived session files as the recovered conversation history before continuing work.",
  ].join("\n")
}

function workflowRestoreMessageRows(workflow: WorkflowInfo, sessions: ReturnType<typeof workflowStateSessionSnapshot>[]) {
  const restoredAt = Date.now()
  return sessions.map((session, index) => {
    const id = workflowRestoreMessageID(session.id)
    const time = Math.max(
      restoredAt + index,
      session.time?.updated ?? 0,
      session.time?.created ?? 0,
      workflow.time.updated ?? 0,
      workflow.time.created,
    )
    const model = session.model ?? workflowModelSessionRef(workflow.model) ?? { providerID: "opencode", id: "workflow-restore" }
    return {
      message: {
        id,
        session_id: session.id,
        time_created: time,
        time_updated: time,
        data: {
          role: "user" as const,
          time: { created: time },
          agent: session.agent ?? workflowAgentForRole(session.role),
          model: {
            providerID: model.providerID,
            modelID: model.id ?? model.modelID,
            ...(model.variant ? { variant: model.variant } : {}),
          },
        },
      },
      part: {
        id: workflowRestorePartID(session.id),
        message_id: id,
        session_id: session.id,
        time_created: time,
        time_updated: time,
        data: {
          type: "text" as const,
          text: workflowRestoreMessageText(workflow, session),
          synthetic: true,
          metadata: {
            restoredWorkflow: {
              id: workflow.id,
              path: workflow.path,
              archive: workflowSessionArchivePath(session.id),
              summary: workflowSessionSummaryPath(session.id),
            },
          },
        },
      },
    }
  })
}

function workflowStateMessageRows(sessions: ReturnType<typeof workflowStateSessionSnapshot>[]) {
  return sessions.flatMap((session) =>
    (Array.isArray(session.messages) ? session.messages : []).flatMap((message) => {
      if (!message?.info?.id || !message.info.sessionID || !Array.isArray(message.parts)) return []
      const messageData = { ...message.info }
      delete messageData.id
      delete messageData.sessionID
      const row = {
        message: {
          id: message.info.id,
          session_id: message.info.sessionID,
          time_created: message.info.time?.created ?? session.time?.created ?? Date.now(),
          time_updated: message.info.time?.completed ?? message.info.time?.created ?? session.time?.updated ?? Date.now(),
          data: messageData,
        },
        parts: message.parts
          .filter((part) => part?.id && part.messageID && part.sessionID)
          .map((part) => {
            const partData = { ...part }
            delete partData.id
            delete partData.messageID
            delete partData.sessionID
            return {
              id: part.id,
              message_id: part.messageID,
              session_id: part.sessionID,
              time_created: part.time?.start ?? message.info.time?.created ?? session.time?.created ?? Date.now(),
              time_updated: part.time?.end ?? part.time?.start ?? message.info.time?.completed ?? message.info.time?.created ?? Date.now(),
              data: partData,
            }
          }),
      }
      return [row]
    }),
  )
}

function workflowStateDurableMessageRows(sessions: ReturnType<typeof workflowStateSessionSnapshot>[]) {
  return sessions.flatMap((session) =>
    (Array.isArray(session.durableMessages) ? session.durableMessages : []).flatMap((row) => {
      if (!row?.id || !row.type || row.seq === undefined || !row.data) return []
      return [
        {
          id: row.id,
          session_id: session.id,
          type: row.type,
          seq: row.seq,
          time_created: row.time_created ?? session.time?.created ?? Date.now(),
          time_updated: row.time_updated ?? row.time_created ?? session.time?.updated ?? Date.now(),
          data: row.data,
        },
      ]
    }),
  )
}

function workflowStateDurableInputRows(sessions: ReturnType<typeof workflowStateSessionSnapshot>[]) {
  return sessions.flatMap((session) =>
    (Array.isArray(session.durableInputs) ? session.durableInputs : []).flatMap((row) => {
      if (!row?.id || !row.prompt || !row.delivery || row.admitted_seq === undefined) return []
      return [
        {
          id: row.id,
          session_id: session.id,
          prompt: row.prompt,
          delivery: row.delivery,
          admitted_seq: row.admitted_seq,
          promoted_seq: row.promoted_seq ?? null,
          time_created: row.time_created ?? session.time?.created ?? Date.now(),
        },
      ]
    }),
  )
}

function legacyQueuedWorkflowCommands(input: {
  workflow: WorkflowInfo
  sessions: ReturnType<typeof workflowStateSessionSnapshot>[]
  legacy: boolean
  existingCommandIDs: Set<string>
}) {
  if (!input.legacy) return []
  const commands = input.sessions.flatMap((session) =>
    (Array.isArray(session.messages) ? session.messages : []).flatMap((message) =>
      (Array.isArray(message?.parts) ? message.parts : []).flatMap((part) =>
        legacyQueuedWorkflowCommandFromPart({
          workflow: input.workflow,
          sessionID: session.id,
          part,
        }),
      ),
    ),
  )
  const seen = new Set<string>()
  return commands
    .filter((command) => {
      const key = command.id ?? `${command.sourceSessionID}:${command.action}:${command.milestoneID ?? ""}:${command.milestoneStatus ?? ""}`
      if (seen.has(key) || input.existingCommandIDs.has(key)) return false
      seen.add(key)
      return true
    })
    .toSorted(
      (left, right) =>
        legacyQueuedWorkflowCommandPriority(input.workflow, left) - legacyQueuedWorkflowCommandPriority(input.workflow, right),
    )
}

function legacyQueuedWorkflowCommandFromPart(input: {
  workflow: WorkflowInfo
  sessionID: SessionID
  part: Record<string, unknown>
}) {
  if (!input.part || input.part.type !== "tool" || input.part.tool !== "workflow") return []
  const state = typeof input.part.state === "object" && input.part.state ? input.part.state : {}
  if (state.status !== "completed") return []
  const metadata = typeof state.metadata === "object" && state.metadata ? state.metadata : {}
  const output = typeof state.output === "string" ? state.output : ""
  if (metadata.workflowID && metadata.workflowID !== input.workflow.id) return []
  if (metadata.queued !== true && !/Queued workflow command/i.test(output)) return []
  const commandInput = typeof state.input === "object" && state.input ? state.input : {}
  const action = typeof commandInput.action === "string" ? commandInput.action : undefined
  if (!action || !["milestone_status", "plan_complete", "force_complete", "force_skip"].includes(action)) return []
  const milestoneID = typeof commandInput.milestoneID === "string" ? commandInput.milestoneID : undefined
  if (!milestoneID) return []
  const milestoneStatus = typeof commandInput.milestoneStatus === "string" ? commandInput.milestoneStatus : undefined
  const sourceSessionID =
    typeof commandInput.sourceSessionID === "string" ? SessionID.make(commandInput.sourceSessionID) : input.sessionID
  const message = typeof commandInput.message === "string" ? commandInput.message : "Recovered legacy queued workflow command."
  return [
    {
      ...commandInput,
      id: `legacy-queued:${String(input.part.id ?? `${sourceSessionID}:${action}:${milestoneID}`)}`,
      action,
      workflowID: input.workflow.id,
      sourceSessionID,
      sourceAgent: "workflow-legacy-queued-recovery",
      milestoneID: WorkflowMilestoneID.make(milestoneID),
      ...(milestoneStatus ? { milestoneStatus } : {}),
      message,
    },
  ]
}

function legacyQueuedWorkflowCommandStillRelevant(command: WorkflowToolCommand, milestone: WorkflowMilestoneInfo | undefined) {
  if (!milestone) return false
  const currentStatus = canonicalMilestoneStatus(milestone.status)
  if (command.action === "milestone_status") {
    const targetStatus = command.milestoneStatus ? canonicalMilestoneStatus(command.milestoneStatus) : undefined
    return currentStatus === "planning" && (targetStatus === "done" || targetStatus === "approved")
  }
  if (command.action === "plan_complete") return currentStatus === "planning" || currentStatus === "blocked"
  if (command.action === "force_complete" || command.action === "force_skip") {
    return ["pending", "planning", "blocked", "rejected", "failed"].includes(currentStatus)
  }
  return false
}

function legacyQueuedWorkflowCommandPriority(workflow: WorkflowInfo, command: WorkflowToolCommand) {
  const sourceIsManager = command.sourceSessionID === workflow.rootSessionID || command.sourceSessionID === workflow.pmSessionID
  const targetStatus = command.milestoneStatus ? canonicalMilestoneStatus(command.milestoneStatus) : undefined
  if (sourceIsManager && command.action === "milestone_status" && (targetStatus === "done" || targetStatus === "approved")) return 0
  if (command.action === "force_complete" || command.action === "force_skip") return 1
  if (command.action === "plan_complete") return 2
  return 3
}

function workflowStateContextEpochRows(sessions: ReturnType<typeof workflowStateSessionSnapshot>[]) {
  return sessions.flatMap((session) => {
    const row = session.contextEpoch
    if (!row?.baseline || !row.snapshot || row.baseline_seq === undefined) return []
    return [
      {
        session_id: session.id,
        baseline: row.baseline,
        snapshot: row.snapshot,
        baseline_seq: row.baseline_seq,
      },
    ]
  })
}

function workflowDurableSessionSnapshot(sessionID: SessionID) {
  return Effect.sync(() =>
    Database.use((db) => ({
      durableMessages: db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.seq))
        .all(),
      durableInputs: db
        .select()
        .from(SessionInputTable)
        .where(eq(SessionInputTable.session_id, sessionID))
        .orderBy(asc(SessionInputTable.admitted_seq))
        .all(),
      contextEpoch: db
        .select()
        .from(SessionContextEpochTable)
        .where(eq(SessionContextEpochTable.session_id, sessionID))
        .get(),
    })),
  ).pipe(
    Effect.catchCause(() =>
      Effect.succeed({
        durableMessages: [],
        durableInputs: [],
        contextEpoch: undefined,
      }),
    ),
  )
}

function workflowStateWorkflowRow(workflow: WorkflowInfo) {
  return {
    id: workflow.id,
    project_id: workflow.projectID,
    root_session_id: workflow.rootSessionID ?? null,
    pm_session_id: workflow.pmSessionID ?? null,
    tester_session_id: workflow.testerSessionID ?? null,
    request: workflow.request,
    title: workflow.title,
    directory: workflow.directory,
    path: workflow.path,
    xml: workflow.xml,
    status: normalizeWorkflowStateWorkflowStatus(workflow.status),
    staffing: workflow.staffing ?? null,
    model: workflow.model ?? null,
    model_whitelist: workflow.modelWhitelist ?? null,
    agent: workflow.agent ?? null,
    test_path: workflow.testPath ?? null,
    error: workflow.error ?? null,
    time_created: workflow.time.created,
    time_updated: workflow.time.updated,
    time_completed: workflow.time.completed ?? null,
  }
}

function workflowStateMilestoneRow(workflow: WorkflowInfo, milestone: WorkflowMilestoneInfo) {
  return {
    workflow_id: workflow.id,
    id: milestone.id,
    title: milestone.title,
    department: milestone.department,
    review: milestone.review,
    waiting_for: milestone.waitingFor,
    prompt: milestone.prompt,
    depends_on: milestone.dependsOn ?? [],
    status: normalizeWorkflowStateMilestoneStatus(milestone.status),
    attempt: milestone.attempt ?? 0,
    plan_path: workflowStoredPath(workflow, milestone.planPath, milestone.id, "plan.md"),
    review_path: milestone.reviewPath ? rewriteWorkflowStoredPath(workflow, milestone.reviewPath) : undefined,
    session: milestone.session ?? [],
    time_created: workflow.time.created,
    time_updated: workflow.time.updated,
  }
}

function workflowStateMemberRow(member: WorkflowMemberInfo) {
  return {
    workflow_id: member.workflowID,
    id: member.id,
    role: member.role,
    specialty: member.specialty,
    title: member.title,
    session_id: member.sessionID,
    capacity: member.capacity,
    status: member.status,
    availability: member.availability ?? null,
    current_focus: member.currentFocus ?? null,
    blockers: member.blockers ?? [],
    progress_note: member.progressNote ?? null,
    model: member.model ?? null,
    model_weight: member.modelWeight ?? null,
    model_cache_until: member.modelCacheUntil ?? null,
    time_created: member.time.created,
    time_updated: member.time.updated,
  }
}

function workflowStateConsultationRow(consultation: WorkflowConsultationInfo) {
  return {
    workflow_id: consultation.workflowID,
    id: consultation.id,
    from_session_id: consultation.fromSessionID,
    to_session_id: consultation.toSessionID,
    from_role: consultation.fromRole,
    to_role: consultation.toRole,
    milestone_id: consultation.milestoneID,
    reason: consultation.reason,
    timing: consultation.timing,
    question: consultation.question,
    answer: consultation.answer,
    status: consultation.status,
    time_created: consultation.time.created,
    time_updated: consultation.time.updated,
  }
}

function workflowStateConsultationMessageRow(consultation: WorkflowConsultationInfo) {
  return {
    workflow_id: consultation.workflowID,
    id: consultation.id,
    kind: "consultation",
    from_session_id: consultation.fromSessionID,
    from_role: consultation.fromRole,
    to_session_id: consultation.toSessionID,
    to_role: consultation.toRole,
    milestone_id: consultation.milestoneID ?? null,
    timing: consultation.timing ?? null,
    body: consultation.question,
    response: consultation.answer,
    attachments: null,
    status: consultation.status,
    time_created: consultation.time.created,
    time_delivered: null,
    time_closed: ["answered", "expired", "failed"].includes(consultation.status) ? consultation.time.updated : null,
    time_updated: consultation.time.updated,
  }
}

function workflowStateInterventionRow(intervention: WorkflowInterventionInfo) {
  return {
    workflow_id: intervention.workflowID,
    id: intervention.id,
    from_session_id: intervention.fromSessionID ?? null,
    target_session_id: intervention.targetSessionID ?? null,
    target_role: intervention.targetRole,
    timing: intervention.timing,
    message: intervention.message,
    response: intervention.response ?? null,
    path: intervention.path,
    status: intervention.status,
    time_created: intervention.time.created,
    time_updated: intervention.time.updated,
  }
}

function workflowStateInterventionMessageRow(intervention: WorkflowInterventionInfo) {
  return {
    workflow_id: intervention.workflowID,
    id: intervention.id,
    kind: intervention.message.includes("Attachments:") ? "handoff" : "intervention",
    from_session_id: intervention.fromSessionID ?? null,
    from_role: null,
    to_session_id: intervention.targetSessionID ?? null,
    to_role: intervention.targetRole,
    milestone_id: null,
    timing: intervention.timing,
    body: intervention.message,
    response: intervention.response ?? null,
    attachments: null,
    status: intervention.status,
    time_created: intervention.time.created,
    time_delivered: intervention.status === "delivered" || intervention.status === "acked" ? intervention.time.updated : null,
    time_closed: ["acked", "expired", "failed"].includes(intervention.status) ? intervention.time.updated : null,
    time_updated: intervention.time.updated,
  }
}

function workflowStateEdgeRows(workflowID: WorkflowID, edges: WorkflowGraphEdge[]) {
  return edges.map((edge) => ({
    workflow_id: workflowID,
    from_id: WorkflowMilestoneID.make(edge.from),
    to_id: WorkflowMilestoneID.make(edge.to),
    data: edge,
  }))
}

async function writeWorkflowStateFile(file: string, state: unknown) {
  const content = `${JSON.stringify(state, null, 2)}\n`
  const existing = await readFile(file, "utf8").catch(() => undefined)
  if (existing === content) return
  await writeFileEnsured(file, content)
}

function workflowManifest(workflow: WorkflowInfo) {
  return {
    schema: 2,
    workflowID: workflow.id,
    projectID: workflow.projectID,
    title: workflow.title,
    created: workflow.time.created,
    ownership: {
      "views/**": "engine",
      "archive/**": "engine",
      "inbox/**": "engine",
      "journal/**": "engine-append",
      "graph/**": "engine-append",
      "work/**": "agent",
      "manifest.json": "engine",
      "workflow.xml": "engine",
      "workflow-state.json": "engine",
      "planning/**": "engine",
      "organization.md": "engine",
      "progress.md": "engine",
      "reference/**": "engine",
      "sessions/**": "engine",
      "interventions/**": "engine",
      "standups/**": "engine-append",
      "final/**": "engine",
    },
  }
}

async function readWorkflowManifestFileUnchecked(file: string) {
  return JSON.parse(await readFile(file, "utf8"))
}

function workflowStateFileVersion(state) {
  const version = state?.schema ?? state?.version ?? 1
  return Number.isInteger(version) ? version : undefined
}

function normalizeWorkflowStateWorkflowStatus(status) {
  if (status === "running") return "executing"
  return status
}

function normalizeWorkflowStateMilestoneStatus(status) {
  if (status === "completed" || status === "complete" || status === "finished") return "done"
  if (status === "running" || status === "in_progress" || status === "inprogress") return "executing"
  if (status === "approval" || status === "approve" || status === "accepted") return "approved"
  return status
}

function normalizeWorkflowStateFile(state) {
  return {
    ...state,
    schema: workflowStateSchemaVersion,
    version: workflowStateSchemaVersion,
    workflow: {
      ...state.workflow,
      status: normalizeWorkflowStateWorkflowStatus(state.workflow?.status),
      directory: ".",
    },
    milestones: Array.isArray(state.milestones)
      ? state.milestones.map((milestone) => ({
          ...milestone,
          status: normalizeWorkflowStateMilestoneStatus(milestone?.status),
        }))
      : [],
    journal: state.journal ?? {},
  }
}

function workflowStateSnapshot(state) {
  const version = workflowStateFileVersion(state)
  if (!version || version > workflowStateSchemaVersion || !state?.workflow?.id) return
  return {
    state: normalizeWorkflowStateFile(state),
    legacy:
      version < workflowStateSchemaVersion ||
      state.schema !== workflowStateSchemaVersion ||
      state.version !== workflowStateSchemaVersion,
  }
}

async function readWorkflowStateFile(file: string) {
  const parsed = JSON.parse(await readFile(file, "utf8"))
  return workflowStateSnapshot(parsed)
}

async function readWorkflowStateFileUnchecked(file: string) {
  return JSON.parse(await readFile(file, "utf8"))
}

async function workflowStateFiles(directory: string) {
  const root = path.join(directory, workflowDir)
  const entries = await readdir(root, { withFileTypes: true }).catch(() => [])
  return entries
    .filter((entry) => entry.isDirectory() && !entry.name.includes(".orphaned-"))
    .map((entry) => path.join(root, entry.name, workflowStateFileName))
}

async function workflowDirectoryEntries(directory: string) {
  return readdir(path.join(directory, workflowDir), { withFileTypes: true }).catch(() => [])
}

async function workflowDirectoriesForID(directory: string, workflowID: WorkflowID) {
  const root = path.join(directory, workflowDir)
  const entries = await workflowDirectoryEntries(directory)
  const matches = await Promise.all(
    entries
      .filter((entry) => entry.isDirectory() && !entry.name.includes(".orphaned-"))
      .map(async (entry) => {
        const relative = path.join(workflowDir, entry.name)
        if (entry.name === workflowFolderID(workflowID)) return relative
        const manifest = await readWorkflowManifestFileUnchecked(path.join(root, entry.name, workflowManifestFileName)).catch(() => undefined)
        if (manifest?.workflowID === workflowID) return relative
        const state = await readWorkflowStateFileUnchecked(path.join(root, entry.name, workflowStateFileName)).catch(() => undefined)
        return state?.workflow?.id === workflowID ? relative : undefined
      }),
  )
  return matches.filter((item): item is string => Boolean(item))
}

async function workflowDirectoryJournalScore(directory: string, relative: string) {
  const highWater = async (name: string) => {
    const file = path.join(directory, relative, "journal", name)
    const text = await readFile(file, "utf8").catch((error) => {
      if (nodeErrorCode(error) === "ENOENT") return ""
      throw error
    })
    return Math.max(
      0,
      ...text
        .split(/\r?\n/)
        .filter((line) => line.trim().length > 0)
        .flatMap((line) => {
          try {
            const row = JSON.parse(line)
            return Number.isInteger(row?.seq) ? [row.seq] : []
          } catch {
            return []
          }
        }),
    )
  }
  const updated = await stat(path.join(directory, relative))
    .then((info) => info.mtimeMs)
    .catch(() => 0)
  const [commands, messages, events] = await Promise.all([
    highWater("commands.jsonl"),
    highWater("messages.jsonl"),
    highWater("events.jsonl"),
  ])
  return {
    path: relative,
    commands,
    messages,
    events,
    highWater: commands + messages + events,
    maxHighWater: Math.max(commands, messages, events),
    updated,
  }
}

function workflowDirectoryJournalDescription(score: Awaited<ReturnType<typeof workflowDirectoryJournalScore>>) {
  return `${score.path} (highWater=${score.highWater}, commands=${score.commands}, messages=${score.messages}, events=${score.events})`
}

function compareWorkflowDirectoryScore(
  left: Awaited<ReturnType<typeof workflowDirectoryJournalScore>>,
  right: Awaited<ReturnType<typeof workflowDirectoryJournalScore>>,
  canonicalPath: string,
  currentPath: string,
) {
  const highWater = right.highWater - left.highWater
  if (highWater !== 0) return highWater
  const maxHighWater = right.maxHighWater - left.maxHighWater
  if (maxHighWater !== 0) return maxHighWater
  const canonical = Number(path.normalize(right.path) === path.normalize(canonicalPath)) - Number(path.normalize(left.path) === path.normalize(canonicalPath))
  if (canonical !== 0) return canonical
  const current = Number(path.normalize(right.path) === path.normalize(currentPath)) - Number(path.normalize(left.path) === path.normalize(currentPath))
  if (current !== 0) return current
  const updated = right.updated - left.updated
  if (updated !== 0) return updated
  return left.path.localeCompare(right.path)
}

async function workflowTempFiles(directory: string, workflow: WorkflowInfo) {
  const entries = await readdir(path.join(directory, workflow.path), { withFileTypes: true }).catch(() => [])
  return entries.filter((entry) => entry.isFile() && entry.name.startsWith(".tmp-")).map((entry) => path.join(workflow.path, entry.name))
}

async function removeWorkflowTempFiles(directory: string, workflow: WorkflowInfo) {
  await Promise.all((await workflowTempFiles(directory, workflow)).map((file) => rm(path.join(directory, file), { force: true })))
}

async function repairWorkflowJournalTails(directory: string, workflow: WorkflowInfo) {
  await Promise.all(
    ["commands.jsonl", "messages.jsonl", "events.jsonl"].map((name) =>
      repairWorkflowJournalTail({ directory, workflow, name }),
    ),
  )
}

async function repairWorkflowJournalSequences(directory: string, workflow: WorkflowInfo) {
  await Promise.all(
    ["commands.jsonl", "messages.jsonl", "events.jsonl"].map((name) =>
      repairWorkflowJournalSequence({ directory, workflow, name }),
    ),
  )
}

async function repairWorkflowJournalTail(input: {
  workflow: WorkflowInfo
  directory: string
  name: string
}) {
  const file = path.join(input.directory, input.workflow.path, "journal", input.name)
  const text = await readFile(file, "utf8").catch((error) => {
    if (nodeErrorCode(error) === "ENOENT") return undefined
    throw error
  })
  if (!text) return
  const entries = text
    .split(/\r?\n/)
    .map((line, index) => ({ line, index }))
    .filter((item) => item.line.trim().length > 0)
    .map((item) => {
      try {
        JSON.parse(item.line)
        return { ...item, valid: true }
      } catch {
        return { ...item, valid: false }
      }
    })
  const firstInvalid = entries.findIndex((item) => !item.valid)
  if (firstInvalid === -1) return
  if (entries.slice(firstInvalid + 1).some((item) => item.valid)) return
  const repaired = entries
    .slice(0, firstInvalid)
    .map((item) => item.line)
    .join("\n")
  await writeFileEnsured(file, repaired ? `${repaired}\n` : "")
}

async function repairWorkflowJournalSequence(input: {
  workflow: WorkflowInfo
  directory: string
  name: string
}) {
  const file = path.join(input.directory, input.workflow.path, "journal", input.name)
  const text = await readFile(file, "utf8").catch((error) => {
    if (nodeErrorCode(error) === "ENOENT") return undefined
    throw error
  })
  if (!text) return
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0)
  const rows: Record<string, unknown>[] = []
  for (const line of lines) {
    const data = (() => {
      try {
        return JSON.parse(line)
      } catch {
        return undefined
      }
    })()
    if (data === undefined) return
    if (typeof data !== "object" || data === null || Array.isArray(data)) return
    rows.push(data as Record<string, unknown>)
  }
  const repaired = rows.map((row, index) => JSON.stringify({ ...row, seq: index + 1 })).join("\n")
  const content = repaired ? `${repaired}\n` : ""
  if (content === text) return
  await writeFileEnsured(file, content)
}

async function workflowJournalIssues(input: {
  workflow: WorkflowInfo
  directory: string
  name: string
}) {
  const file = path.join(input.directory, input.workflow.path, "journal", input.name)
  const relative = path.join(input.workflow.path, "journal", input.name)
  const text = await readFile(file, "utf8").catch((error) => {
    if (nodeErrorCode(error) === "ENOENT") return undefined
    throw error
  })
  if (!text) return []
  const parsed = text
    .split(/\r?\n/)
    .map((line, index) => ({ line, index }))
    .filter((item) => item.line.trim().length > 0)
    .map((item) => {
      try {
        return { ...item, data: JSON.parse(item.line) }
      } catch (error) {
        return {
          ...item,
          issue: {
            severity: "error" as const,
            code: "invalid_journal_json",
            workflowID: input.workflow.id,
            path: relative,
            message: `${input.name} line ${item.index + 1} is not valid JSON: ${
              error instanceof globalThis.Error ? error.message : String(error)
            }`,
          },
        }
      }
    })
  const issues = parsed.flatMap((item) => (item.issue ? [item.issue] : []))
  const rows = parsed.filter((item) => item.data !== undefined)
  if (rows.length === 0) return issues
  const seqRows = rows.filter((item) => Object.prototype.hasOwnProperty.call(item.data, "seq"))
  if (seqRows.length === 0) {
    return [
      ...issues,
      {
        severity: "warning" as const,
        code: "missing_journal_seq",
        workflowID: input.workflow.id,
        path: relative,
        message: `${input.name} has entries without seq; journal order cannot be used as a recovery high-water mark`,
      },
    ]
  }
  return [
    ...issues,
    ...rows.flatMap((item, index) => {
      const seq = item.data.seq
      if (seq === undefined) {
        return [
          {
            severity: "warning" as const,
            code: "missing_journal_seq",
            workflowID: input.workflow.id,
            path: relative,
            message: `${input.name} line ${item.index + 1} is missing seq`,
          },
        ]
      }
      if (!Number.isInteger(seq) || seq <= 0) {
        return [
          {
            severity: "error" as const,
            code: "invalid_journal_seq",
            workflowID: input.workflow.id,
            path: relative,
            message: `${input.name} line ${item.index + 1} has invalid seq ${String(seq)}`,
          },
        ]
      }
      if (seq !== index + 1) {
        return [
          {
            severity: "error" as const,
            code: "journal_seq_gap",
            workflowID: input.workflow.id,
            path: relative,
            message: `${input.name} line ${item.index + 1} has seq ${seq}; expected ${index + 1}`,
          },
        ]
      }
      return []
    }),
  ]
}

async function writeWorkflowGraphRevision(directory: string, workflow: WorkflowInfo, xml: string) {
  const graphRoot = path.join(directory, workflowArtifactPath(workflow, "graph"))
  const revisions = (await readdir(graphRoot, { withFileTypes: true }).catch(() => []))
    .filter((entry) => entry.isFile())
    .flatMap((entry) => {
      const match = /^rev-(\d+)\.xml$/.exec(entry.name)
      return match ? [{ name: entry.name, revision: Number(match[1]) }] : []
    })
  const latest = revisions.toSorted((a, b) => b.revision - a.revision)[0]
  if ((await readFile(path.join(graphRoot, latest?.name ?? ""), "utf8").catch(() => undefined)) === xml) return
  const relative = workflowArtifactPath(
    workflow,
    "graph",
    `rev-${String((latest?.revision ?? 0) + 1).padStart(3, "0")}.xml`,
  )
  await writeFileEnsured(path.join(directory, relative), xml)
  return relative
}

async function workflowJournalExists(input: {
  workflow: WorkflowInfo
  directory: string
  name: string
}) {
  const info = await stat(path.join(input.directory, input.workflow.path, "journal", input.name)).catch((error) => {
    if (nodeErrorCode(error) === "ENOENT") return undefined
    throw error
  })
  return info?.isFile() === true
}

async function workflowJournalSnapshot(input: {
  workflow: WorkflowInfo
  directory: string
  name: string
  useLineSequence?: boolean
}) {
  const file = path.join(input.directory, input.workflow.path, "journal", input.name)
  const text = await readFile(file, "utf8").catch((error) => {
    if (nodeErrorCode(error) === "ENOENT") return ""
    throw error
  })
  const rows = text
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0)
    .flatMap((line, index) => {
      try {
        const row = JSON.parse(line)
        if (input.useLineSequence && !Number.isInteger(row?.seq)) return [{ ...row, seq: index + 1 }]
        return [row]
      } catch {
        return []
      }
    })
  const seqs = rows.map((row) => row?.seq).filter((seq) => Number.isInteger(seq))
  return {
    path: path.join("journal", input.name).replaceAll("\\", "/"),
    entries: rows.length,
    highWater: seqs.length > 0 ? Math.max(...seqs) : 0,
  }
}

async function workflowJournalState(directory: string, workflow: WorkflowInfo) {
  return {
    commands: await workflowJournalSnapshot({ workflow, directory, name: "commands.jsonl" }),
    messages: await workflowJournalSnapshot({ workflow, directory, name: "messages.jsonl", useLineSequence: true }),
    events: await workflowJournalSnapshot({ workflow, directory, name: "events.jsonl" }),
  }
}

async function workflowCommandJournalReplayRows(input: { directory: string; workflow: WorkflowInfo; highWater: number }) {
  const text = await readFile(projectWorkflowPath(input.directory, input.workflow, workflowCommandJournalPath()), "utf8").catch(
    (error) => {
      if (nodeErrorCode(error) === "ENOENT") return ""
      throw error
    },
  )
  const rows = text
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
    .flatMap((line) => {
      try {
        return [JSON.parse(line)]
      } catch {
        return []
      }
    })
    .filter((row) => Number.isInteger(row?.seq))
    .toSorted((a, b) => a.seq - b.seq)
  return {
    highWater: rows.length > 0 ? Math.max(...rows.map((row) => row.seq)) : 0,
    rows: rows.filter((row) => row.seq > input.highWater && row.outcome === "applied"),
  }
}

async function workflowCommandJournalIDs(input: { directory: string; workflow: WorkflowInfo }) {
  const text = await readFile(projectWorkflowPath(input.directory, input.workflow, workflowCommandJournalPath()), "utf8").catch(
    (error) => {
      if (nodeErrorCode(error) === "ENOENT") return ""
      throw error
    },
  )
  return new Set(
    text
      .trim()
      .split(/\r?\n/)
      .filter(Boolean)
      .flatMap((line) => {
        try {
          const row = JSON.parse(line)
          return typeof row?.id === "string" ? [row.id] : []
        } catch {
          return []
        }
      }),
  )
}

async function workflowMessageJournalReplayRows(input: { directory: string; workflow: WorkflowInfo; highWater: number }) {
  const text = await readFile(projectWorkflowPath(input.directory, input.workflow, workflowMessageJournalPath()), "utf8").catch(
    (error) => {
      if (nodeErrorCode(error) === "ENOENT") return ""
      throw error
    },
  )
  const rows = text
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
    .flatMap((line, index) => {
      try {
        const row = JSON.parse(line)
        return [{ ...row, seq: Number.isInteger(row?.seq) ? row.seq : index + 1 }]
      } catch {
        return []
      }
    })
    .filter((row) => Number.isInteger(row?.seq))
    .toSorted((a, b) => a.seq - b.seq)
  return {
    highWater: rows.length > 0 ? Math.max(...rows.map((row) => row.seq)) : 0,
    rows: rows.filter((row) => row.seq > input.highWater && typeof row.messageID === "string"),
  }
}

async function workflowEventJournalReplayRows(input: { directory: string; workflow: WorkflowInfo; highWater: number }) {
  const text = await readFile(projectWorkflowPath(input.directory, input.workflow, workflowEventJournalPath()), "utf8").catch(
    (error) => {
      if (nodeErrorCode(error) === "ENOENT") return ""
      throw error
    },
  )
  const rows = text
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
    .flatMap((line) => {
      try {
        return [JSON.parse(line)]
      } catch {
        return []
      }
    })
    .filter((row) => Number.isInteger(row?.seq))
    .toSorted((a, b) => a.seq - b.seq)
  return {
    highWater: rows.length > 0 ? Math.max(...rows.map((row) => row.seq)) : 0,
    rows: rows.filter((row) => row.seq > input.highWater && typeof row.action === "string"),
  }
}

function workflowJournalRowTime(row: { ts?: unknown }, fallback: number) {
  if (typeof row.ts !== "string") return fallback
  const parsed = Date.parse(row.ts)
  return Number.isFinite(parsed) ? parsed : fallback
}

function workflowJournalRowResponse(row: { response?: unknown; answer?: unknown }) {
  if (typeof row.response === "string") return row.response
  if (typeof row.answer === "string") return row.answer
}

function workflowHasControlHistory(workflow: WorkflowInfo, milestones: WorkflowMilestoneInfo[]) {
  return (
    !["pending", "running", "planning"].includes(workflow.status) ||
    milestones.some((milestone) => milestone.attempt > 0 || milestone.session.length > 0 || milestone.status !== "pending")
  )
}

export const layer: Layer.Layer<
  Service,
  never,
  BackgroundJob.Service | Bus.Service | EventV2Bridge.Service | Session.Service | SessionPrompt.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    ensureSchema()
    const bus = yield* Bus.Service
    const events = yield* EventV2Bridge.Service
    const session = yield* Session.Service
    const prompt = yield* SessionPrompt.Service
    const background = yield* BackgroundJob.Service
    const workflowManagedMessageKeys = new Set<string>()
    const observedWorkflowMessageKeys = new Set<string>()
    const observedRequesterMessageKeys = new Set<string>()
    const workflowToolCommandResults = new Map<string, WorkflowToolCommandRuntimeResult>()
    const workflowToolCommandInflight = new Map<
      string,
      {
        promise: Promise<WorkflowToolCommandRuntimeResult>
        resolve: (value: WorkflowToolCommandRuntimeResult) => void
        reject: (reason: unknown) => void
      }
    >()
    const workflowToolCommandDeferred = () => {
      let resolve = (_value: WorkflowToolCommandRuntimeResult) => {}
      let reject = (_reason: unknown) => {}
      const promise = new Promise<WorkflowToolCommandRuntimeResult>((done, fail) => {
        resolve = done
        reject = fail
      })
      promise.catch(() => {})
      return { promise, resolve, reject }
    }
    const rememberWorkflowToolCommandResult = (id: string, value: WorkflowToolCommandRuntimeResult) => {
      workflowToolCommandResults.set(id, value)
      if (workflowToolCommandResults.size > 500) {
        workflowToolCommandResults.delete(workflowToolCommandResults.keys().next().value)
      }
    }
    const withWorkflowToolCommandQueue = (workflowID: WorkflowID, effect: Effect.Effect<void>) => {
      const previous = workflowToolCommandChains.get(workflowID) ?? Promise.resolve()
      let release = () => {}
      const current = new Promise<void>((done) => {
        release = done
      })
      const chain = previous.catch(() => {}).then(() => current)
      workflowToolCommandChains.set(workflowID, chain)
      return Effect.gen(function* () {
        yield* Effect.promise(() => previous.catch(() => {}))
        return yield* effect
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            release()
            if (workflowToolCommandChains.get(workflowID) === chain) workflowToolCommandChains.delete(workflowID)
          }),
        ),
      )
    }
    const withWorkflowMemberAssignmentQueue = <A, E, R>(workflowID: WorkflowID, effect: Effect.Effect<A, E, R>) => {
      const previous = workflowMemberAssignmentChains.get(workflowID) ?? Promise.resolve()
      let release = () => {}
      const current = new Promise<void>((done) => {
        release = done
      })
      const chain = previous.catch(() => {}).then(() => current)
      workflowMemberAssignmentChains.set(workflowID, chain)
      return Effect.gen(function* () {
        yield* Effect.promise(() => previous.catch(() => {}))
        return yield* effect
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            release()
            if (workflowMemberAssignmentChains.get(workflowID) === chain) workflowMemberAssignmentChains.delete(workflowID)
          }),
        ),
      )
    }
    const observedWorkflowFiles = new Map<string, string>()
    const workflowMessageKey = (sessionID: SessionID, messageID: MessageID) => `${sessionID}:${messageID}`

    const recoverLegacyQueuedWorkflowCommands = Effect.fn("Workflow.recoverLegacyQueuedWorkflowCommands")(function* (
      workflowID: WorkflowID,
      commands: WorkflowToolCommand[],
    ) {
      if (commands.length === 0) return false
      let attempted = false
      for (const command of commands) {
        const current = command.milestoneID
          ? (yield* milestones(workflowID)).find((item) => item.id === command.milestoneID)
          : undefined
        if (!legacyQueuedWorkflowCommandStillRelevant(command, current)) continue
        attempted = true
        yield* handleWorkflowToolCommand({
          ...command,
          workflowID,
        }).pipe(Effect.ignore)
      }
      if (attempted) yield* publishUpdated(workflowID).pipe(Effect.ignore)
      return attempted
    })

    const syncWorkflowStateFile = Effect.fn("Workflow.syncWorkflowStateFile")(function* (file: string) {
      const ctx = yield* InstanceState.context
      const snapshot = yield* Effect.promise(() => readWorkflowStateFile(file)).pipe(
        Effect.catchCause(() => Effect.succeed(undefined)),
      )
      if (!snapshot) return false
      const state = snapshot.state
      const storedPath = workflowStateStoredPath(state.workflow.path, workflowStateFileDirectory(ctx, file))
      if (!storedPath) return false
      const stateTimeCreated = state.workflow.time?.created ?? Date.now()
      const stateTimeUpdated = state.workflow.time?.updated ?? stateTimeCreated
      const workflow: WorkflowInfo = {
        ...state.workflow,
        projectID: ProjectID.make(ctx.project.id),
        directory: ctx.directory,
        path: storedPath,
        time: {
          created: stateTimeCreated,
          updated: stateTimeUpdated,
          completed: state.workflow.time?.completed,
        },
      }
      const snapshotCommandHighWater = state.journal?.commands?.highWater ?? 0
      const commandJournalReplay = yield* Effect.promise(() =>
        workflowCommandJournalReplayRows({ directory: ctx.directory, workflow, highWater: snapshotCommandHighWater }),
      )
      const commandJournalIDs = yield* Effect.promise(() => workflowCommandJournalIDs({ directory: ctx.directory, workflow }))
      const snapshotMessageHighWater = state.journal?.messages?.highWater ?? 0
      const messageJournalReplay = yield* Effect.promise(() =>
        workflowMessageJournalReplayRows({ directory: ctx.directory, workflow, highWater: snapshotMessageHighWater }),
      )
      const snapshotEventHighWater = state.journal?.events?.highWater ?? 0
      const eventJournalReplay = yield* Effect.promise(() =>
        workflowEventJournalReplayRows({ directory: ctx.directory, workflow, highWater: snapshotEventHighWater }),
      )
      const milestoneItems = (Array.isArray(state.milestones) ? state.milestones : []).map((milestone) => ({
        ...milestone,
        planPath: workflowStateStoredPath(milestone.planPath, workflowArtifactPath(workflow, milestone.id, "plan.md")),
        reviewPath: workflowStateStoredPath(milestone.reviewPath, undefined),
        session: Array.isArray(milestone.session) ? milestone.session : [],
      }))
      const memberItems = (Array.isArray(state.members) ? state.members : []).map((member) => ({
        ...member,
        workflowID: workflow.id,
        time: member.time ?? { created: workflow.time.created, updated: workflow.time.updated },
      }))
      const consultationItems = (Array.isArray(state.consultations) ? state.consultations : []).map((consultation) => ({
        ...consultation,
        workflowID: workflow.id,
        time: consultation.time ?? { created: workflow.time.created, updated: workflow.time.updated },
      }))
      const interventionItems = (Array.isArray(state.interventions) ? state.interventions : []).map((intervention) => ({
        ...intervention,
        workflowID: workflow.id,
        path: workflowStateStoredPath(intervention.path, workflowArtifactPath(workflow, workflowInterventionPath(intervention.id))),
        time: intervention.time ?? { created: workflow.time.created, updated: workflow.time.updated },
      }))
      const sessionStatesFromDisk = yield* Effect.all(
        (Array.isArray(state.sessions) ? state.sessions : [])
          .filter((item) => item?.id)
          .map((item) => Effect.promise(() => readWorkflowStateSessionSnapshot(ctx.directory, workflow, item))),
        { concurrency: 4 },
      )
      const sessionByID = new Map(sessionStatesFromDisk.filter((item) => item?.id).map((item) => [item.id, item]))
      const sessionRefs = workflowStateSessionRefs({
        workflow,
        milestones: milestoneItems,
        members: memberItems,
        consultations: consultationItems,
        interventions: interventionItems,
      })
      const sessionRows = sessionRefs.map((ref) =>
        workflowStateSessionRow({
          ctx,
          workflow,
          session: {
            ...workflowStateSessionSnapshot({ workflow, ref }),
            ...(sessionByID.get(ref.sessionID) ?? {}),
            id: ref.sessionID,
            role: ref.role,
          },
        }),
      )
      const sessionStates = sessionRows.map((row, index) => {
        const state = sessionByID.get(row.id) ?? {}
        const ref = sessionRefs[index]
        return {
          id: row.id,
          role: state.role ?? ref?.role ?? "requester",
          title: row.title,
          parentID: row.parent_id ?? undefined,
          agent: row.agent ?? undefined,
          model: row.model ?? undefined,
          metadata: row.metadata ?? undefined,
          permission: row.permission ?? undefined,
          time: {
            created: row.time_created,
            updated: row.time_updated,
            compacting: row.time_compacting ?? undefined,
            archived: row.time_archived ?? undefined,
          },
          messages: Array.isArray(state.messages) ? state.messages : [],
          durableMessages: Array.isArray(state.durableMessages) ? state.durableMessages : [],
          durableInputs: Array.isArray(state.durableInputs) ? state.durableInputs : [],
          contextEpoch: state.contextEpoch,
        }
      })
      const legacyQueuedCommands = legacyQueuedWorkflowCommands({
        workflow,
        sessions: sessionStates,
        legacy: snapshot.legacy,
        existingCommandIDs: commandJournalIDs,
      })
      const storedSessionIDs = new Set(
        sessionRows.length === 0
          ? []
          : Database.use((db) =>
              db
                .select({ id: SessionTable.id })
                .from(SessionTable)
                .where(inArray(SessionTable.id, sessionRows.map((row) => row.id)))
                .all(),
            ).map((row) => row.id),
      )
      const persistedMessages = workflowStateMessageRows(sessionStates)
      const persistedParts = persistedMessages.flatMap((item) => item.parts)
      const restoreMessages = workflowRestoreMessageRows(
        workflow,
        sessionStates.filter((sessionState) => !storedSessionIDs.has(sessionState.id)),
      )
      const durableMessages = workflowStateDurableMessageRows(sessionStates)
      const durableInputs = workflowStateDurableInputRows(sessionStates)
      const contextEpochs = workflowStateContextEpochRows(sessionStates)
      const messageIDs = [...persistedMessages.map((item) => item.message.id), ...restoreMessages.map((item) => item.message.id)]
      const partIDs = [...persistedParts.map((item) => item.id), ...restoreMessages.map((item) => item.part.id)]
      const durableMessageIDs = durableMessages.map((row) => row.id)
      const durableInputIDs = durableInputs.map((row) => row.id)
      const contextEpochSessionIDs = contextEpochs.map((row) => row.session_id)
      const storedMessages = new Map(
        messageIDs.length === 0
          ? []
          : Database.use((db) =>
              db
                .select({ id: MessageTable.id, timeUpdated: MessageTable.time_updated, data: MessageTable.data })
                .from(MessageTable)
                .where(inArray(MessageTable.id, messageIDs))
                .all(),
            ).map((row) => [row.id, { timeUpdated: row.timeUpdated, hash: workflowStateContentHash(row.data) }]),
      )
      const storedParts = new Map(
        partIDs.length === 0
          ? []
          : Database.use((db) =>
              db
                .select({ id: PartTable.id, timeUpdated: PartTable.time_updated, data: PartTable.data })
                .from(PartTable)
                .where(inArray(PartTable.id, partIDs))
                .all(),
            ).map((row) => [row.id, { timeUpdated: row.timeUpdated, hash: workflowStateContentHash(row.data) }]),
      )
      const storedDurableMessages = new Map(
        durableMessageIDs.length === 0
          ? []
          : Database.use((db) =>
              db
                .select({ id: SessionMessageTable.id, timeUpdated: SessionMessageTable.time_updated, data: SessionMessageTable.data })
                .from(SessionMessageTable)
                .where(inArray(SessionMessageTable.id, durableMessageIDs))
                .all(),
            ).map((row) => [row.id, { timeUpdated: row.timeUpdated, hash: workflowStateContentHash(row.data) }]),
      )
      const storedDurableInputs = new Map(
        durableInputIDs.length === 0
          ? []
          : Database.use((db) =>
              db
                .select({
                  id: SessionInputTable.id,
                  prompt: SessionInputTable.prompt,
                  delivery: SessionInputTable.delivery,
                  admittedSeq: SessionInputTable.admitted_seq,
                  promotedSeq: SessionInputTable.promoted_seq,
                  timeCreated: SessionInputTable.time_created,
                })
                .from(SessionInputTable)
                .where(inArray(SessionInputTable.id, durableInputIDs))
                .all(),
            ).map((row) => [row.id, row]),
      )
      const storedContextEpochs = new Map(
        contextEpochSessionIDs.length === 0
          ? []
          : Database.use((db) =>
              db
                .select({
                  sessionID: SessionContextEpochTable.session_id,
                  baseline: SessionContextEpochTable.baseline,
                  snapshot: SessionContextEpochTable.snapshot,
                  baselineSeq: SessionContextEpochTable.baseline_seq,
                })
                .from(SessionContextEpochTable)
                .where(inArray(SessionContextEpochTable.session_id, contextEpochSessionIDs))
                .all(),
            ).map((row) => [row.sessionID, row]),
      )
      const edgeRows = workflowStateEdgeRows(
        workflow.id,
        (Array.isArray(state.edges) ? state.edges : []).filter((edge) => edge?.from && edge?.to),
      )
      const existing = Database.use((db) => db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflow.id)).get())
      const stateMilestoneSignature = JSON.stringify(
        milestoneItems
          .map((item) => ({
            id: item.id,
            status: item.status,
            attempt: item.attempt,
            waitingFor: item.waitingFor,
            planPath: item.planPath,
            reviewPath: item.reviewPath,
            session: item.session,
          }))
          .toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
      )
      const storedMilestoneSignature = existing
        ? JSON.stringify(
            Database.use((db) =>
              db
                .select()
                .from(WorkflowMilestoneTable)
                .where(eq(WorkflowMilestoneTable.workflow_id, workflow.id))
                .all(),
            )
              .map((row) => ({
                id: row.id,
                status: row.status,
                attempt: row.attempt,
                waitingFor: row.waiting_for ?? undefined,
                planPath: row.plan_path ?? undefined,
                reviewPath: row.review_path ?? undefined,
                session: row.session,
              }))
              .toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
          )
        : ""
      const stateMemberSignature = JSON.stringify(
        memberItems
          .map((item) => ({
            id: item.id,
            role: item.role,
            specialty: item.specialty,
            sessionID: item.sessionID,
            status: item.status,
            availability: item.availability,
          }))
          .toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
      )
      const storedMemberSignature = existing
        ? JSON.stringify(
            Database.use((db) =>
              db.select().from(WorkflowMemberTable).where(eq(WorkflowMemberTable.workflow_id, workflow.id)).all(),
            )
              .map((row) => ({
                id: row.id,
                role: row.role,
                specialty: row.specialty,
                sessionID: row.session_id,
                status: row.status,
                availability: row.availability ?? undefined,
              }))
              .toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
          )
        : ""
      const stateConsultationSignature = JSON.stringify(
        consultationItems
          .map((item) => ({
            id: item.id,
            fromSessionID: item.fromSessionID,
            toSessionID: item.toSessionID,
            fromRole: item.fromRole,
            toRole: item.toRole,
            milestoneID: item.milestoneID,
            question: item.question,
            answer: item.answer,
            status: item.status,
          }))
          .toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
      )
      const storedConsultationSignature = existing
        ? JSON.stringify(
            Database.use((db) =>
              db.select().from(WorkflowConsultationTable).where(eq(WorkflowConsultationTable.workflow_id, workflow.id)).all(),
            )
              .map((row) => ({
                id: row.id,
                fromSessionID: row.from_session_id,
                toSessionID: row.to_session_id,
                fromRole: row.from_role,
                toRole: row.to_role,
                milestoneID: row.milestone_id ?? undefined,
                question: row.question,
                answer: row.answer,
                status: row.status,
              }))
              .toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
          )
        : ""
      const stateInterventionSignature = JSON.stringify(
        interventionItems
          .map((item) => ({
            id: item.id,
            fromSessionID: item.fromSessionID,
            targetSessionID: item.targetSessionID,
            targetRole: item.targetRole,
            timing: item.timing,
            message: item.message,
            response: item.response,
            status: item.status,
          }))
          .toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
      )
      const storedInterventionSignature = existing
        ? JSON.stringify(
            Database.use((db) =>
              db.select().from(WorkflowInterventionTable).where(eq(WorkflowInterventionTable.workflow_id, workflow.id)).all(),
            )
              .map((row) => ({
                id: row.id,
                fromSessionID: row.from_session_id ?? undefined,
                targetSessionID: row.target_session_id ?? undefined,
                targetRole: row.target_role,
                timing: row.timing,
                message: row.message,
                response: row.response ?? undefined,
                status: row.status,
              }))
              .toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
          )
        : ""
      const stateEdgeSignature = JSON.stringify(
        edgeRows
          .map((row) => ({
            from: row.from_id,
            to: row.to_id,
            data: row.data,
          }))
          .toSorted((a, b) => `${a.from}->${a.to}`.localeCompare(`${b.from}->${b.to}`)),
      )
      const storedEdgeSignature = existing
        ? JSON.stringify(
            Database.use((db) => db.select().from(WorkflowEdgeTable).where(eq(WorkflowEdgeTable.workflow_id, workflow.id)).all())
              .map((row) => ({
                from: row.from_id,
                to: row.to_id,
                data: row.data,
              }))
              .toSorted((a, b) => `${a.from}->${a.to}`.localeCompare(`${b.from}->${b.to}`)),
          )
        : ""
      const staleWorkflowTables =
        stateMilestoneSignature !== storedMilestoneSignature ||
        stateMemberSignature !== storedMemberSignature ||
        stateConsultationSignature !== storedConsultationSignature ||
        stateInterventionSignature !== storedInterventionSignature ||
        stateEdgeSignature !== storedEdgeSignature
      const missingSessions = sessionRows.filter((row) => !storedSessionIDs.has(row.id)).length
      const staleMessages =
        persistedMessages.filter((item) => {
          const stored = storedMessages.get(item.message.id)
          if (!stored) return true
          if (stored.timeUpdated < item.message.time_updated) return true
          return stored.hash !== workflowStateContentHash(item.message.data)
        }).length + restoreMessages.filter((item) => !storedMessages.has(item.message.id)).length
      const staleParts =
        persistedParts.filter((row) => {
          const stored = storedParts.get(row.id)
          if (!stored) return true
          if (stored.timeUpdated < row.time_updated) return true
          return stored.hash !== workflowStateContentHash(row.data)
        }).length + restoreMessages.filter((item) => !storedParts.has(item.part.id)).length
      const staleDurableMessages = durableMessages.filter((row) => {
        const stored = storedDurableMessages.get(row.id)
        if (!stored) return true
        if (stored.timeUpdated < row.time_updated) return true
        return stored.hash !== workflowStateContentHash(row.data)
      }).length
      const staleDurableInputs = durableInputs.filter((row) => {
        const stored = storedDurableInputs.get(row.id)
        if (!stored) return true
        if (stored.delivery !== row.delivery || stored.admittedSeq !== row.admitted_seq) return true
        if ((stored.promotedSeq ?? null) !== (row.promoted_seq ?? null)) return true
        if (stored.timeCreated < row.time_created) return true
        return JSON.stringify(stored.prompt) !== JSON.stringify(row.prompt)
      }).length
      const staleContextEpochs = contextEpochs.filter(
        (row) => {
          const stored = storedContextEpochs.get(row.session_id)
          if (!stored) return true
          if (stored.baselineSeq < row.baseline_seq) return true
          if (stored.baseline !== row.baseline) return true
          return workflowStateContentHash(stored.snapshot) !== workflowStateContentHash(row.snapshot)
        },
      ).length
      if (
        existing &&
        missingSessions === 0 &&
        staleMessages === 0 &&
        staleParts === 0 &&
        staleDurableMessages === 0 &&
        staleDurableInputs === 0 &&
        staleContextEpochs === 0 &&
        !staleWorkflowTables &&
        existing.time_updated >= workflow.time.updated &&
        existing.directory === ctx.directory &&
        path.normalize(existing.path) === path.normalize(workflow.path) &&
        commandJournalReplay.rows.length === 0 &&
        messageJournalReplay.rows.length === 0 &&
        eventJournalReplay.rows.length === 0
      ) {
        if (snapshot.legacy) {
          if (yield* recoverLegacyQueuedWorkflowCommands(workflow.id, legacyQueuedCommands)) return true
          yield* Effect.promise(() => writeWorkflowStateFile(file, state)).pipe(Effect.ignore)
          return true
        }
        if (
          commandJournalReplay.highWater > snapshotCommandHighWater ||
          messageJournalReplay.highWater > snapshotMessageHighWater ||
          eventJournalReplay.highWater > snapshotEventHighWater
        ) {
          if (eventJournalReplay.highWater > snapshotEventHighWater) {
            yield* events.publish(Event.GraphUpdated, { workflowID: workflow.id }).pipe(Effect.ignore)
          }
          yield* publishUpdated(workflow.id).pipe(Effect.ignore)
          return true
        }
        return false
      }
      Database.transaction((tx) => {
        if (sessionRows.length > 0) {
          tx.insert(SessionTable)
            .values(sessionRows)
            .onConflictDoUpdate({
              target: SessionTable.id,
              set: {
                project_id: sql`excluded.project_id`,
                workspace_id: sql`excluded.workspace_id`,
                parent_id: sql`excluded.parent_id`,
                slug: sql`excluded.slug`,
                directory: sql`excluded.directory`,
                path: sql`excluded.path`,
                title: sql`excluded.title`,
                version: sql`excluded.version`,
                share_url: sql`excluded.share_url`,
                summary_additions: sql`excluded.summary_additions`,
                summary_deletions: sql`excluded.summary_deletions`,
                summary_files: sql`excluded.summary_files`,
                summary_diffs: sql`excluded.summary_diffs`,
                metadata: sql`excluded.metadata`,
                cost: sql`excluded.cost`,
                tokens_input: sql`excluded.tokens_input`,
                tokens_output: sql`excluded.tokens_output`,
                tokens_reasoning: sql`excluded.tokens_reasoning`,
                tokens_cache_read: sql`excluded.tokens_cache_read`,
                tokens_cache_write: sql`excluded.tokens_cache_write`,
                revert: sql`excluded.revert`,
                permission: sql`excluded.permission`,
                agent: sql`excluded.agent`,
                model: sql`excluded.model`,
                time_created: sql`excluded.time_created`,
                time_updated: sql`excluded.time_updated`,
                time_compacting: sql`excluded.time_compacting`,
                time_archived: sql`excluded.time_archived`,
              },
            })
            .run()
        }
        if (persistedMessages.length > 0) {
          tx.insert(MessageTable)
            .values(persistedMessages.map((item) => item.message))
            .onConflictDoUpdate({
              target: MessageTable.id,
              set: {
                session_id: sql`excluded.session_id`,
                time_created: sql`excluded.time_created`,
                time_updated: sql`excluded.time_updated`,
                data: sql`excluded.data`,
              },
            })
            .run()
          if (persistedParts.length > 0) {
            tx.insert(PartTable)
              .values(persistedParts)
              .onConflictDoUpdate({
                target: PartTable.id,
                set: {
                  message_id: sql`excluded.message_id`,
                  session_id: sql`excluded.session_id`,
                  time_created: sql`excluded.time_created`,
                  time_updated: sql`excluded.time_updated`,
                  data: sql`excluded.data`,
                },
              })
              .run()
          }
        }
        if (restoreMessages.length > 0) {
          tx.insert(MessageTable).values(restoreMessages.map((item) => item.message)).onConflictDoNothing().run()
          tx.insert(PartTable).values(restoreMessages.map((item) => item.part)).onConflictDoNothing().run()
        }
        if (durableMessages.length > 0) {
          tx.insert(SessionMessageTable)
            .values(durableMessages)
            .onConflictDoUpdate({
              target: SessionMessageTable.id,
              set: {
                session_id: sql`excluded.session_id`,
                type: sql`excluded.type`,
                seq: sql`excluded.seq`,
                time_created: sql`excluded.time_created`,
                time_updated: sql`excluded.time_updated`,
                data: sql`excluded.data`,
              },
            })
            .run()
        }
        if (durableInputs.length > 0) {
          tx.insert(SessionInputTable)
            .values(durableInputs)
            .onConflictDoUpdate({
              target: SessionInputTable.id,
              set: {
                session_id: sql`excluded.session_id`,
                prompt: sql`excluded.prompt`,
                delivery: sql`excluded.delivery`,
                admitted_seq: sql`excluded.admitted_seq`,
                promoted_seq: sql`excluded.promoted_seq`,
                time_created: sql`excluded.time_created`,
              },
            })
            .run()
        }
        if (contextEpochs.length > 0) {
          tx.insert(SessionContextEpochTable)
            .values(contextEpochs)
            .onConflictDoUpdate({
              target: SessionContextEpochTable.session_id,
              set: {
                baseline: sql`excluded.baseline`,
                snapshot: sql`excluded.snapshot`,
                baseline_seq: sql`excluded.baseline_seq`,
              },
            })
            .run()
        }
        tx.insert(WorkflowTable)
          .values(workflowStateWorkflowRow(workflow))
          .onConflictDoUpdate({
            target: WorkflowTable.id,
            set: workflowStateWorkflowRow(workflow),
          })
          .run()
        tx.delete(WorkflowMilestoneTable).where(eq(WorkflowMilestoneTable.workflow_id, workflow.id)).run()
        tx.delete(WorkflowMemberTable).where(eq(WorkflowMemberTable.workflow_id, workflow.id)).run()
        tx.delete(WorkflowConsultationTable).where(eq(WorkflowConsultationTable.workflow_id, workflow.id)).run()
        tx.delete(WorkflowInterventionTable).where(eq(WorkflowInterventionTable.workflow_id, workflow.id)).run()
        tx.delete(WorkflowMessageTable).where(eq(WorkflowMessageTable.workflow_id, workflow.id)).run()
        tx.delete(WorkflowEdgeTable).where(eq(WorkflowEdgeTable.workflow_id, workflow.id)).run()
        if (milestoneItems.length > 0) {
          tx.insert(WorkflowMilestoneTable).values(milestoneItems.map((item) => workflowStateMilestoneRow(workflow, item))).run()
        }
        if (memberItems.length > 0) tx.insert(WorkflowMemberTable).values(memberItems.map(workflowStateMemberRow)).run()
        if (consultationItems.length > 0) {
          tx.insert(WorkflowConsultationTable).values(consultationItems.map(workflowStateConsultationRow)).run()
        }
        if (interventionItems.length > 0) {
          tx.insert(WorkflowInterventionTable).values(interventionItems.map(workflowStateInterventionRow)).run()
        }
        const messageRows = [
          ...consultationItems.map(workflowStateConsultationMessageRow),
          ...interventionItems.map(workflowStateInterventionMessageRow),
        ]
        if (messageRows.length > 0) tx.insert(WorkflowMessageTable).values(messageRows).run()
        if (edgeRows.length > 0) tx.insert(WorkflowEdgeTable).values(edgeRows).run()
        for (const row of commandJournalReplay.rows) {
          const updated = workflowJournalRowTime(row, Date.now())
          const workflowStatus = typeof row.to?.workflowStatus === "string" ? row.to.workflowStatus : undefined
          if (workflowStatus) {
            const terminal = workflowStatus === "completed" || workflowStatus === "failed" || workflowStatus === "cancelled"
            tx.update(WorkflowTable)
              .set({
                status: workflowStatus,
                time_updated: updated,
                ...(terminal ? { time_completed: updated } : { time_completed: null }),
              })
              .where(eq(WorkflowTable.id, workflow.id))
              .run()
          }
          const milestoneID = typeof row.milestoneID === "string" ? row.milestoneID : undefined
          const milestoneStatus = typeof row.to?.milestoneStatus === "string" ? row.to.milestoneStatus : undefined
          if (milestoneID && milestoneStatus) {
            tx.update(WorkflowMilestoneTable)
              .set({ status: milestoneStatus, time_updated: updated })
              .where(and(eq(WorkflowMilestoneTable.workflow_id, workflow.id), eq(WorkflowMilestoneTable.id, milestoneID)))
              .run()
          }
        }
        for (const row of messageJournalReplay.rows) {
          const messageID = typeof row.messageID === "string" ? row.messageID : undefined
          const updated = workflowJournalRowTime(row, Date.now())
          const status = typeof row.status === "string" ? row.status : undefined
          const response = workflowJournalRowResponse(row)
          if (messageID && row.kind === "consultation") {
            const consultationStatus = status && ["pending", "answered", "expired", "failed"].includes(status) ? status : undefined
            const answer = row.action === "send" ? undefined : response
            const set = {
              time_updated: updated,
              ...(consultationStatus ? { status: consultationStatus } : {}),
              ...(answer !== undefined ? { answer } : {}),
            }
            tx.update(WorkflowConsultationTable)
              .set(set)
              .where(and(eq(WorkflowConsultationTable.workflow_id, workflow.id), eq(WorkflowConsultationTable.id, messageID)))
              .run()
          }
          if (messageID && (row.kind === "intervention" || row.kind === "handoff")) {
            const interventionStatus = status && ["queued", "delivered", "acked", "blocked", "expired", "failed"].includes(status) ? status : undefined
            const interventionResponse = row.action === "send" ? undefined : response
            const set = {
              time_updated: updated,
              ...(interventionStatus ? { status: interventionStatus } : {}),
              ...(interventionResponse !== undefined ? { response: interventionResponse } : {}),
            }
            tx.update(WorkflowInterventionTable)
              .set(set)
              .where(and(eq(WorkflowInterventionTable.workflow_id, workflow.id), eq(WorkflowInterventionTable.id, messageID)))
              .run()
          }
        }
      })
      if (restoreMessages.length > 0) {
        for (const item of restoreMessages) {
          yield* session
            .updateMessage({
              ...item.message.data,
              id: item.message.id,
              sessionID: item.message.session_id,
            })
            .pipe(Effect.catchCause(() => Effect.void))
          yield* session
            .updatePart({
              ...item.part.data,
              id: item.part.id,
              messageID: item.part.message_id,
              sessionID: item.part.session_id,
            })
            .pipe(Effect.catchCause(() => Effect.void))
        }
      }
      if (
        commandJournalReplay.rows.length > 0 ||
        commandJournalReplay.highWater > snapshotCommandHighWater ||
        messageJournalReplay.rows.length > 0 ||
        messageJournalReplay.highWater > snapshotMessageHighWater ||
        eventJournalReplay.rows.length > 0 ||
        eventJournalReplay.highWater > snapshotEventHighWater
      ) {
        if (eventJournalReplay.rows.some((row) => row.action === "graph.revised")) {
          yield* events.publish(Event.GraphUpdated, { workflowID: workflow.id }).pipe(Effect.ignore)
        }
        yield* publishUpdated(workflow.id).pipe(Effect.ignore)
      } else {
        yield* events.publish(Event.Created, { workflowID: workflow.id, info: workflow }).pipe(Effect.ignore)
      }
      if (snapshot.legacy) {
        if (yield* recoverLegacyQueuedWorkflowCommands(workflow.id, legacyQueuedCommands)) return true
        yield* Effect.promise(() => writeWorkflowStateFile(file, state)).pipe(Effect.ignore)
      }
      return true
    })

    const syncWorkflowStatesFromDisk = Effect.fn("Workflow.syncWorkflowStatesFromDisk")(function* () {
      const ctx = yield* InstanceState.context
      const files = yield* Effect.promise(() => workflowStateFiles(ctx.directory))
      const results = yield* Effect.all(
        files.map((file) => syncWorkflowStateFile(file).pipe(Effect.catchCause(() => Effect.succeed(false)))),
        { concurrency: 1 },
      )
      return results.some(Boolean)
    })

    const syncWorkflowStateFromDisk = Effect.fn("Workflow.syncWorkflowStateFromDisk")(function* (workflowID: WorkflowID) {
      const ctx = yield* InstanceState.context
      const directories = yield* Effect.promise(() => workflowDirectoriesForID(ctx.directory, workflowID))
      const results = yield* Effect.all(
        directories.map((directory) =>
          syncWorkflowStateFile(path.join(ctx.directory, directory, workflowStateFileName)).pipe(
            Effect.catchCause(() => Effect.succeed(false)),
          ),
        ),
        { concurrency: 1 },
      )
      return results.some(Boolean)
    })

    const publishUpdated = Effect.fn("Workflow.publishUpdated")(function* (workflowID: WorkflowID) {
      const info = yield* get(workflowID)
      yield* writeWorkflowState(workflowID, info).pipe(Effect.ignore)
      yield* events.publish(Event.Updated, { workflowID, info })
      yield* events.publish(Event.GraphUpdated, { workflowID })
      return info
    })

    const ensureAuditablePath = Effect.fn("Workflow.ensureAuditablePath")(function* (workflow: WorkflowInfo) {
      if (!isLegacyWorkflowPath(workflow)) return workflow
      const ctx = yield* InstanceState.context
      const oldPath = workflow.path
      const nextPath = workflowFolderPath(workflow.id)
      yield* Effect.promise(() => ensureWorkflowDirectory(ctx.directory, workflow.path, nextPath))
      const rewrite = (value: string | undefined) => (value ? rewriteStoredPathPrefix(value, oldPath, nextPath) : value)
      const now = Date.now()
      Database.transaction((tx) => {
        tx.update(WorkflowTable)
          .set({
            path: nextPath,
            ...(workflow.testPath ? { test_path: rewrite(workflow.testPath) } : {}),
            time_updated: now,
          })
          .where(eq(WorkflowTable.id, workflow.id))
          .run()
        for (const milestone of tx
          .select()
          .from(WorkflowMilestoneTable)
          .where(eq(WorkflowMilestoneTable.workflow_id, workflow.id))
          .all()) {
          tx.update(WorkflowMilestoneTable)
            .set({
              ...(milestone.plan_path ? { plan_path: rewrite(milestone.plan_path) } : {}),
              ...(milestone.review_path ? { review_path: rewrite(milestone.review_path) } : {}),
              time_updated: now,
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, workflow.id), eq(WorkflowMilestoneTable.id, milestone.id)))
            .run()
        }
        for (const intervention of tx
          .select()
          .from(WorkflowInterventionTable)
          .where(eq(WorkflowInterventionTable.workflow_id, workflow.id))
          .all()) {
          tx.update(WorkflowInterventionTable)
            .set({ path: rewrite(intervention.path) ?? intervention.path, time_updated: now })
            .where(and(eq(WorkflowInterventionTable.workflow_id, workflow.id), eq(WorkflowInterventionTable.id, intervention.id)))
            .run()
        }
      })
      return { ...workflow, path: nextPath, testPath: rewrite(workflow.testPath), time: { ...workflow.time, updated: now } }
    })

    const get = Effect.fn("Workflow.get")(function* (workflowID: WorkflowID) {
      const row = Database.use((db) => db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflowID)).get())
      if (row) return yield* ensureAuditablePath(toInfo(row))
      yield* syncWorkflowStatesFromDisk().pipe(Effect.ignore)
      const restored = Database.use((db) => db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflowID)).get())
      if (!restored) return yield* new Error({ message: `Workflow not found: ${workflowID}` })
      return yield* ensureAuditablePath(toInfo(restored))
    })

    const ensureRequesterSession = Effect.fn("Workflow.ensureRequesterSession")(function* (workflow: WorkflowInfo) {
      if (workflow.rootSessionID) {
        yield* session.setTitle({ sessionID: workflow.rootSessionID, title: workflowRequesterTitle(workflow.request) }).pipe(
          Effect.ignore,
        )
        return workflow
      }
      const root = yield* session.create({
        title: workflowRequesterTitle(workflow.request),
        agent: workflow.agent,
        model: workflow.model
          ? { id: workflow.model.modelID, providerID: workflow.model.providerID, variant: workflow.model.variant }
          : undefined,
      })
      Database.use((db) =>
        db
          .update(WorkflowTable)
          .set({ root_session_id: root.id, time_updated: Date.now() })
          .where(eq(WorkflowTable.id, workflow.id))
          .run(),
      )
      return { ...workflow, rootSessionID: root.id, time: { ...workflow.time, updated: Date.now() } }
    })

    const normalizeWorkflowSessions = Effect.fn("Workflow.normalizeWorkflowSessions")(function* (input: WorkflowInfo) {
      const workflow = yield* ensureRequesterSession(input)
      if (!workflow.rootSessionID) return
      const items = yield* milestones(workflow.id)
      const staff = yield* members(workflow.id)
      const sessionIDs = [
        workflow.pmSessionID,
        workflow.testerSessionID,
        ...staff.map((member) => member.sessionID),
        ...items.flatMap((milestone) => milestone.session.map((ref) => ref.sessionID)),
      ].filter((id): id is SessionID => !!id && id !== workflow.rootSessionID)
      const titles = workflowSessionTitles(workflow, items)
      if (sessionIDs.length === 0) return
      yield* Effect.all(
        Array.from(new Set(sessionIDs)).map((sessionID) =>
          Effect.all([
            session.setParent({ sessionID, parentID: workflow.rootSessionID! }),
            session.setTitle({
              sessionID,
              title: staff.find((member) => member.sessionID === sessionID)?.title ?? titles.get(sessionID) ?? "Workflow agent",
            }),
          ]).pipe(Effect.ignore),
        ),
      )
    })

    const writeArchiveIndex = Effect.fn("Workflow.writeArchiveIndex")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      yield* writeNote(workflowArtifactPath(workflow, "index.md"), archiveIndexMarkdown(workflow, yield* milestones(workflowID)))
    })

    const writeRequesterMemory = Effect.fn("Workflow.writeRequesterMemory")(function* (
      workflow: WorkflowInfo,
      interventionItems: WorkflowInterventionInfo[],
    ) {
      const ctx = yield* InstanceState.context
      const standupIndex = yield* Effect.promise(() =>
        readFile(projectWorkflowPath(ctx.directory, workflow, workflowStandupIndexPath()), "utf8"),
      ).pipe(Effect.catchCause(() => Effect.succeed("")))
      const requesterSessionID = workflow.rootSessionID
      const summaryText = requesterSessionID
        ? yield* Effect.promise(() =>
            readFile(projectWorkflowPath(ctx.directory, workflow, workflowSessionSummaryPath(requesterSessionID)), "utf8"),
          ).pipe(Effect.catchCause(() => Effect.succeed("")))
        : ""
      yield* writeNote(
        workflowArtifactPath(workflow, workflowRequesterMemoryPath()),
        requesterMemoryMarkdown({
          workflow,
          interventions: interventionItems,
          sessionSummary: extractReferenceSummary(summaryText),
          standups: recentStandupLines(standupIndex),
        }),
      )
    })

    const writeStaffMemory = Effect.fn("Workflow.writeStaffMemory")(function* (
      workflow: WorkflowInfo,
      items: WorkflowMilestoneInfo[],
      staff: WorkflowMemberInfo[],
      consultationItems: WorkflowConsultationInfo[],
      interventionItems: WorkflowInterventionInfo[],
    ) {
      const ctx = yield* InstanceState.context
      const standupIndex = yield* Effect.promise(() =>
        readFile(projectWorkflowPath(ctx.directory, workflow, workflowStandupIndexPath()), "utf8"),
      ).pipe(Effect.catchCause(() => Effect.succeed("")))
      const standups = recentStandupLines(standupIndex)
      yield* Effect.all(
        staff.map((member) =>
          Effect.gen(function* () {
            const summaryText = yield* Effect.promise(() =>
              readFile(projectWorkflowPath(ctx.directory, workflow, workflowSessionSummaryPath(member.sessionID)), "utf8"),
            ).pipe(Effect.catchCause(() => Effect.succeed("")))
            yield* writeNote(
              workflowArtifactPath(workflow, workflowStaffMemoryPath(member)),
              staffMemoryMarkdown({
                workflow,
                member,
                milestones: items,
                consultations: consultationItems,
                interventions: interventionItems,
                sessionSummary: extractReferenceSummary(summaryText),
                standups,
              }),
            )
          }),
        ),
        { discard: true },
      )
    })

    const writeReferenceIndex = Effect.fn("Workflow.writeReferenceIndex")(function* (
      workflowID: WorkflowID,
      workflowOverride?: WorkflowInfo,
    ) {
      const workflow = workflowOverride ?? (yield* get(workflowID))
      yield* writeProgress(workflowID, workflow).pipe(Effect.ignore)
      yield* ensureStandupIndex(workflowID).pipe(Effect.ignore)
      const ctx = yield* InstanceState.context
      const standupIndex = yield* Effect.promise(() =>
        readFile(projectWorkflowPath(ctx.directory, workflow, workflowStandupIndexPath()), "utf8"),
      ).pipe(Effect.catchCause(() => Effect.succeed("")))
      const items = yield* milestones(workflowID)
      const staff = yield* members(workflowID)
      const consultationItems = yield* consultations(workflowID)
      const interventionItems = yield* interventions(workflowID)
      yield* writeConsultationArtifacts(workflow, consultationItems)
      yield* writeRequesterMemory(workflow, interventionItems)
      yield* writeStaffMemory(workflow, items, staff, consultationItems, interventionItems)
      yield* writeDeliverySummary(workflowID, workflow, standupIndex)
      yield* writeNote(
        workflowArtifactPath(workflow, workflowReferenceIndexPath()),
        referenceIndexMarkdown({
          workflow,
          milestones: items,
          members: staff,
          consultations: consultationItems,
          interventions: interventionItems,
          standupDocs: standupDocsFromIndex(workflow, standupIndex),
        }),
      )
      yield* writeWorkflowState(workflowID, workflow).pipe(Effect.ignore)
    })

    const writeDeliverySummary = Effect.fn("Workflow.writeDeliverySummary")(function* (
      workflowID: WorkflowID,
      workflow: WorkflowInfo,
      standupIndex?: string,
    ) {
      const ctx = yield* InstanceState.context
      const index = standupIndex ?? (yield* Effect.promise(() =>
        readFile(projectWorkflowPath(ctx.directory, workflow, workflowStandupIndexPath()), "utf8"),
      ).pipe(Effect.catchCause(() => Effect.succeed(""))))
      yield* writeNote(
        workflowArtifactPath(workflow, workflowDeliverySummaryPath()),
        deliverySummaryMarkdown({
          workflow,
          milestones: yield* milestones(workflowID),
          members: yield* members(workflowID),
          consultations: yield* consultations(workflowID),
          interventions: yield* interventions(workflowID),
          standupDocs: standupDocsFromIndex(workflow, index),
        }),
      )
    })

    const list = Effect.fn("Workflow.list")(function* (input?: ListInput) {
      const ctx = yield* InstanceState.context
      yield* syncWorkflowStatesFromDisk().pipe(Effect.ignore)
      const rows = Database.use((db) =>
        db
          .select()
          .from(WorkflowTable)
          .where(eq(WorkflowTable.project_id, ctx.project.id))
          .orderBy(asc(WorkflowTable.time_created))
          .all(),
      )
      const workflows = rows.map(toListInfo)
      if (!input?.sessionID) return workflows
      const sessionID = input.sessionID
      const milestoneWorkflowIDs =
        rows.length === 0
          ? new Set<WorkflowID>()
          : new Set(
              Database.use((db) =>
                db
                  .select({
                    workflow_id: WorkflowMilestoneTable.workflow_id,
                    session: WorkflowMilestoneTable.session,
                  })
                  .from(WorkflowMilestoneTable)
                  .where(inArray(WorkflowMilestoneTable.workflow_id, rows.map((row) => row.id)))
                  .all(),
              )
                .filter((row) => row.session.some((ref) => ref.sessionID === sessionID))
                .map((row) => row.workflow_id),
            )
      const memberWorkflowIDs =
        rows.length === 0
          ? new Set<WorkflowID>()
          : new Set(
              Database.use((db) =>
                db
                  .select({
                    workflow_id: WorkflowMemberTable.workflow_id,
                  })
                  .from(WorkflowMemberTable)
                  .where(
                    and(
                      inArray(WorkflowMemberTable.workflow_id, rows.map((row) => row.id)),
                      eq(WorkflowMemberTable.session_id, sessionID),
                    ),
                  )
                  .all(),
              ).map((row) => row.workflow_id),
            )
      return workflows.filter(
        (workflow) =>
          workflow.rootSessionID === sessionID ||
          workflow.pmSessionID === sessionID ||
          workflow.testerSessionID === sessionID ||
          memberWorkflowIDs.has(workflow.id) ||
          milestoneWorkflowIDs.has(workflow.id),
      )
    })

    const milestones = Effect.fn("Workflow.milestones")(function* (workflowID: WorkflowID) {
      return Database.use((db) =>
        db
          .select()
          .from(WorkflowMilestoneTable)
          .where(eq(WorkflowMilestoneTable.workflow_id, workflowID))
          .orderBy(asc(WorkflowMilestoneTable.time_created))
          .all()
          .map(toMilestone),
      )
    })

    const consultations = Effect.fn("Workflow.consultations")(function* (workflowID: WorkflowID) {
      return Database.use((db) =>
        db
          .select()
          .from(WorkflowConsultationTable)
          .where(eq(WorkflowConsultationTable.workflow_id, workflowID))
          .orderBy(asc(WorkflowConsultationTable.time_created))
          .all()
          .map(toConsultation),
      )
    })

    const interventions = Effect.fn("Workflow.interventions")(function* (workflowID: WorkflowID) {
      return Database.use((db) =>
        db
          .select()
          .from(WorkflowInterventionTable)
          .where(eq(WorkflowInterventionTable.workflow_id, workflowID))
          .orderBy(asc(WorkflowInterventionTable.time_created))
          .all()
          .map(toIntervention),
      )
    })

    const members = Effect.fn("Workflow.members")(function* (workflowID: WorkflowID) {
      return Database.use((db) =>
        db
          .select()
          .from(WorkflowMemberTable)
          .where(eq(WorkflowMemberTable.workflow_id, workflowID))
          .orderBy(asc(WorkflowMemberTable.time_created))
          .all()
          .map(toMember),
      )
    })

    const fixWorkflowDirectories = Effect.fn("Workflow.fixWorkflowDirectories")(function* (workflow: WorkflowInfo) {
      const ctx = yield* InstanceState.context
      const directories = yield* Effect.promise(() => workflowDirectoriesForID(ctx.directory, workflow.id))
      if (directories.length < 2) return
      const canonical = workflowFolderPath(workflow.id)
      const winner = (yield* Effect.promise(() =>
        Promise.all(directories.map((item) => workflowDirectoryJournalScore(ctx.directory, item))),
      )).toSorted((a, b) => compareWorkflowDirectoryScore(a, b, canonical, workflow.path))[0]
      if (!winner) return
      const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "")
      const moved = new Set<string>()
      const orphanPath = (item: string, suffix: string) =>
        path.join(ctx.directory, `${item}.orphaned-${stamp}${suffix}`)
      if (path.normalize(winner.path) !== path.normalize(canonical)) {
        const canonicalFullPath = path.join(ctx.directory, canonical)
        const winnerFullPath = path.join(ctx.directory, winner.path)
        if (yield* Effect.promise(() => exists(canonicalFullPath))) {
          yield* Effect.promise(() => rename(canonicalFullPath, orphanPath(canonical, "-replaced")))
          moved.add(path.normalize(canonical))
        }
        yield* Effect.promise(async () => {
          await mkdir(path.dirname(canonicalFullPath), { recursive: true })
          await rename(winnerFullPath, canonicalFullPath)
        })
        moved.add(path.normalize(winner.path))
      }
      const extras = directories.filter((item) => {
        const normalized = path.normalize(item)
        if (moved.has(normalized)) return false
        if (normalized === path.normalize(canonical)) return false
        if (normalized === path.normalize(winner.path)) return false
        return true
      })
      if (extras.length === 0) return
      yield* Effect.all(
        extras.map((item, index) =>
          Effect.promise(() =>
            rename(
              path.join(ctx.directory, item),
              path.join(ctx.directory, `${item}.orphaned-${stamp}${index === 0 ? "" : `-${index + 1}`}`),
            ),
          ),
        ),
        { concurrency: 1 },
      )
    })

    const fixWorkflowOrphanActiveMilestones = Effect.fn("Workflow.fixWorkflowOrphanActiveMilestones")(function* (
      workflow: WorkflowInfo,
    ) {
      const milestoneItems = yield* milestones(workflow.id)
      const activeJobIDs = new Set(
        (yield* background.list()).filter((job) => job.status === "running").map((job) => job.id),
      )
      const orphanIDs = milestoneItems
        .filter((milestone) => ["planning", "executing", "reviewing", "running"].includes(milestone.status))
        .filter((milestone) => !activeJobIDs.has(milestoneJobID(workflow.id, milestone.id, milestone.attempt)))
        .map((milestone) => milestone.id)
      if (orphanIDs.length === 0) return false
      const now = Date.now()
      const message = `workflow doctor --fix blocked orphan active milestone(s): ${orphanIDs.join(", ")}. Resume explicitly after inspecting the owning session archive.`
      Database.transaction((tx) => {
        tx.update(WorkflowMilestoneTable)
          .set({ status: "blocked", time_updated: now })
          .where(and(eq(WorkflowMilestoneTable.workflow_id, workflow.id), inArray(WorkflowMilestoneTable.id, orphanIDs)))
          .run()
        tx.update(WorkflowTable)
          .set({ status: "blocked", error: message, time_updated: now })
          .where(eq(WorkflowTable.id, workflow.id))
          .run()
      })
      yield* publishUpdated(workflow.id).pipe(Effect.ignore)
      return true
    })

    const doctorWorkflows = Effect.fn("Workflow.doctorWorkflows")(function* (input?: DoctorInput) {
      const ctx = yield* InstanceState.context
      yield* syncWorkflowStatesFromDisk().pipe(Effect.ignore)
      const rows = Database.use((db) => {
        if (input?.workflowID) {
          const row = db
            .select()
            .from(WorkflowTable)
            .where(and(eq(WorkflowTable.project_id, ctx.project.id), eq(WorkflowTable.id, input.workflowID)))
            .get()
          return row ? [row] : []
        }
        return db
          .select()
          .from(WorkflowTable)
          .where(eq(WorkflowTable.project_id, ctx.project.id))
          .orderBy(asc(WorkflowTable.time_created))
          .all()
      })
      if (input?.workflowID && rows.length === 0) return yield* new Error({ message: `Workflow not found: ${input.workflowID}` })
      return rows.map(toInfo)
    })

    const workflowEdges = Effect.fn("Workflow.workflowEdges")(function* (workflowID: WorkflowID) {
      return Database.use((db) =>
        db
          .select()
          .from(WorkflowEdgeTable)
          .where(eq(WorkflowEdgeTable.workflow_id, workflowID))
          .all()
          .map((row) => row.data ?? { id: `${row.from_id}->${row.to_id}`, from: String(row.from_id), to: String(row.to_id) }),
      )
    })

    const doctorWorkflow = Effect.fn("Workflow.doctorWorkflow")(function* (workflow: WorkflowInfo) {
      const ctx = yield* InstanceState.context
      const issues: DoctorIssue[] = []
      const workflowRoot = path.join(ctx.directory, workflow.path)
      const expectedPath = workflowPath(workflow.id)
      const milestoneItems = yield* milestones(workflow.id)
      const staffItems = yield* members(workflow.id)
      const consultationItems = yield* consultations(workflow.id)
      const interventionItems = yield* interventions(workflow.id)
      if (path.normalize(workflow.path) !== path.normalize(expectedPath)) {
        issues.push({
          severity: "error",
          code: "noncanonical_path",
          workflowID: workflow.id,
          path: workflow.path,
          message: `workflow path is ${workflow.path}; expected ${expectedPath}`,
        })
      }
      const directories = yield* Effect.promise(() => workflowDirectoriesForID(ctx.directory, workflow.id))
      if (directories.length > 1) {
        const directoryScores = yield* Effect.promise(() =>
          Promise.all(directories.map((item) => workflowDirectoryJournalScore(ctx.directory, item))),
        )
        issues.push({
          severity: "error",
          code: "duplicate_directory",
          workflowID: workflow.id,
          message: `workflow has multiple local directories: ${directoryScores
            .toSorted((a, b) => compareWorkflowDirectoryScore(a, b, expectedPath, workflow.path))
            .map(workflowDirectoryJournalDescription)
            .join(", ")}`,
        })
      }
      const rootStat = yield* Effect.tryPromise({
        try: () => stat(workflowRoot),
        catch: (error) => error,
      }).pipe(
        Effect.catch((error: unknown) =>
          nodeErrorCode(error) === "ENOENT" ? Effect.succeed(undefined) : Effect.fail(error as globalThis.Error),
        ),
      )
      if (!rootStat?.isDirectory()) {
        issues.push({
          severity: "error",
          code: "missing_directory",
          workflowID: workflow.id,
          path: workflow.path,
          message: `workflow directory does not exist: ${workflow.path}`,
        })
        return issues
      }
      const manifestPath = path.join(workflowRoot, workflowManifestFileName)
      const manifest = yield* Effect.tryPromise({
        try: () => readWorkflowManifestFileUnchecked(manifestPath),
        catch: (error) => error,
      }).pipe(
        Effect.catch((error: unknown) => {
          issues.push({
            severity: "error",
            code: nodeErrorCode(error) === "ENOENT" ? "missing_manifest" : "invalid_manifest_json",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowManifestFileName),
            message:
              nodeErrorCode(error) === "ENOENT"
                ? `manifest.json is missing for ${workflow.id}`
                : `manifest.json is not valid JSON: ${
                    error instanceof globalThis.Error ? error.message : String(error)
                  }`,
          })
          return Effect.succeed(undefined)
        }),
      )
      if (manifest) {
        if (manifest.schema !== 2) {
          issues.push({
            severity: "warning",
            code: "unsupported_manifest_schema",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowManifestFileName),
            message: `manifest.json schema is ${manifest.schema ?? "<missing>"}; expected 2`,
          })
        }
        if (manifest.workflowID !== workflow.id) {
          issues.push({
            severity: "error",
            code: "manifest_workflow_mismatch",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowManifestFileName),
            message: `manifest.json belongs to ${manifest.workflowID ?? "<missing>"} instead of ${workflow.id}`,
          })
        }
        if (manifest.projectID && manifest.projectID !== workflow.projectID) {
          issues.push({
            severity: "error",
            code: "manifest_project_mismatch",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowManifestFileName),
            message: `manifest.json project is ${manifest.projectID} but DB project is ${workflow.projectID}`,
          })
        }
        const ownership = manifest.ownership && typeof manifest.ownership === "object" ? manifest.ownership : {}
        ;["workflow.xml", "workflow-state.json", "journal/**", "work/**"].forEach((key) => {
          if (Object.prototype.hasOwnProperty.call(ownership, key)) return
          issues.push({
            severity: "warning",
            code: "manifest_missing_ownership",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowManifestFileName),
            message: `manifest.json ownership is missing ${key}`,
          })
        })
      }
      const statePath = path.join(workflowRoot, workflowStateFileName)
      const state = yield* Effect.tryPromise({
        try: () => readWorkflowStateFileUnchecked(statePath),
        catch: (error) => error,
      }).pipe(
        Effect.catch((error: unknown) => {
          issues.push({
            severity: "error",
            code: nodeErrorCode(error) === "ENOENT" ? "missing_state" : "invalid_state_json",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowStateFileName),
            message:
              nodeErrorCode(error) === "ENOENT"
                ? `workflow-state.json is missing for ${workflow.id}`
                : `workflow-state.json is not valid JSON: ${
                    error instanceof globalThis.Error ? error.message : String(error)
                  }`,
          })
          return Effect.succeed(undefined)
        }),
      )
      if (state) {
        const stateVersion = workflowStateFileVersion(state)
        if (!stateVersion || stateVersion > workflowStateSchemaVersion) {
          issues.push({
            severity: "warning",
            code: "unsupported_state_version",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowStateFileName),
            message: `workflow-state.json version is ${stateVersion ?? "<missing>"}; supported version is ${workflowStateSchemaVersion}`,
          })
        }
        if (state.workflow?.id !== workflow.id) {
          issues.push({
            severity: "error",
            code: "state_workflow_mismatch",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowStateFileName),
            message: `workflow-state.json belongs to ${state.workflow?.id ?? "<missing>"} instead of ${workflow.id}`,
          })
        }
        if (state.workflow?.status && state.workflow.status !== workflow.status) {
          issues.push({
            severity: "error",
            code: "state_status_mismatch",
            workflowID: workflow.id,
            path: path.join(workflow.path, workflowStateFileName),
            message: `DB status is ${workflow.status} but workflow-state.json status is ${state.workflow.status}`,
          })
        }
        const stateMilestones = new Map(
          (Array.isArray(state.milestones) ? state.milestones : [])
            .filter((item) => item?.id)
            .map((item) => [String(item.id), item]),
        )
        milestoneItems.forEach((milestone) => {
          const snapshot = stateMilestones.get(String(milestone.id))
          if (!snapshot) {
            issues.push({
              severity: "warning",
              code: "state_missing_milestone",
              workflowID: workflow.id,
              message: `workflow-state.json is missing milestone ${milestone.id}`,
            })
            return
          }
          if (snapshot.status && snapshot.status !== milestone.status) {
            issues.push({
              severity: "error",
              code: "milestone_status_mismatch",
              workflowID: workflow.id,
              path: path.join(workflow.path, workflowStateFileName),
              message: `milestone ${milestone.id} DB status is ${milestone.status} but state status is ${snapshot.status}`,
            })
          }
        })
        for (const stateSession of Array.isArray(state.sessions) ? state.sessions : []) {
          const sessionPath = workflowStateRelativePath(stateSession?.path)
          if (!sessionPath) continue
          yield* Effect.tryPromise({
            try: () => readFile(projectWorkflowPath(ctx.directory, workflow, sessionPath), "utf8").then((text) => JSON.parse(text)),
            catch: (error) => error,
          }).pipe(
            Effect.map((sessionState) => {
              if (
                typeof stateSession?.contentHash === "string" &&
                workflowStateContentHash(sessionState) !== stateSession.contentHash
              ) {
                issues.push({
                  severity: "error",
                  code: "session_state_hash_mismatch",
                  workflowID: workflow.id,
                  path: path.join(workflow.path, sessionPath),
                  message: `session state content hash does not match workflow-state.json index: ${sessionPath}`,
                })
              }
            }),
            Effect.catch((error: unknown) => {
              issues.push({
                severity: "error",
                code: nodeErrorCode(error) === "ENOENT" ? "missing_session_state" : "invalid_session_state_json",
                workflowID: workflow.id,
                path: path.join(workflow.path, sessionPath),
                message:
                  nodeErrorCode(error) === "ENOENT"
                    ? `session state is missing: ${sessionPath}`
                    : `session state is not valid JSON: ${
                        error instanceof globalThis.Error ? error.message : String(error)
                      }`,
              })
              return Effect.succeed(undefined)
            }),
          )
        }
      }
      const jobs = yield* background.list()
      const activeJobIDs = new Set(jobs.filter((job) => job.status === "running").map((job) => job.id))
      milestoneItems
        .filter((milestone) => ["planning", "executing", "reviewing", "running"].includes(milestone.status))
        .filter((milestone) => !activeJobIDs.has(milestoneJobID(workflow.id, milestone.id, milestone.attempt)))
        .forEach((milestone) =>
          issues.push({
            severity: "warning",
            code: "orphan_active_milestone",
            workflowID: workflow.id,
            message: `milestone ${milestone.id} is ${milestone.status} with sessions but no running workflow.milestone job`,
          }),
        )
      if (
        workflowHasControlHistory(workflow, milestoneItems) &&
        !(yield* Effect.promise(() => workflowJournalExists({ workflow, directory: ctx.directory, name: "commands.jsonl" })))
      ) {
        issues.push({
          severity: "warning",
          code: "missing_command_journal",
          workflowID: workflow.id,
          path: path.join(workflow.path, "journal", "commands.jsonl"),
          message:
            "active workflow has control-plane history but no journal/commands.jsonl; queued/applied/rejected command claims cannot be audited",
        })
      }
      if (
        (consultationItems.length > 0 || interventionItems.length > 0) &&
        !(yield* Effect.promise(() => workflowJournalExists({ workflow, directory: ctx.directory, name: "messages.jsonl" })))
      ) {
        issues.push({
          severity: "warning",
          code: "missing_message_journal",
          workflowID: workflow.id,
          path: path.join(workflow.path, "journal", "messages.jsonl"),
          message:
            "workflow has consultations or interventions but no journal/messages.jsonl; message delivery claims cannot be audited",
        })
      }
      const now = Date.now()
      interventionItems
        .filter((intervention) => ["queued", "delivered"].includes(intervention.status))
        .filter((intervention) => now - intervention.time.updated >= workflowInterventionTimeoutMillis)
        .forEach((intervention) =>
          issues.push({
            severity: "warning",
            code: "stale_intervention",
            workflowID: workflow.id,
            path: intervention.path,
            message: `intervention ${intervention.id} is ${intervention.status} and has not advanced for ${Math.round(
              (now - intervention.time.updated) / 60000,
            )} minutes`,
          }),
        )
      const projectionIssues = yield* Effect.all(
        workflowProjectionPaths(workflow, staffItems).map((projectionPath) =>
          Effect.tryPromise({
            try: () => stat(path.join(ctx.directory, projectionPath)),
            catch: (error) => error,
          }).pipe(
            Effect.map((fileStat) =>
              fileStat.isFile() && fileStat.size > 0
                ? undefined
                : {
                    severity: "warning" as const,
                    code: "empty_projection",
                    workflowID: workflow.id,
                    path: projectionPath,
                    message: `workflow projection is empty or not a file and can be rebuilt with workflow doctor --fix: ${projectionPath}`,
                  },
            ),
            Effect.catch((error: unknown) =>
              nodeErrorCode(error) === "ENOENT"
                ? Effect.succeed({
                    severity: "warning" as const,
                    code: "missing_projection",
                    workflowID: workflow.id,
                    path: projectionPath,
                    message: `workflow projection is missing and can be rebuilt with workflow doctor --fix: ${projectionPath}`,
                  })
                : Effect.fail(error as globalThis.Error),
            ),
          ),
        ),
        { concurrency: 4 },
      )
      issues.push(...projectionIssues.filter((issue): issue is DoctorIssue => !!issue))
      issues.push(...(yield* Effect.promise(() => workflowTempFiles(ctx.directory, workflow))).map((file) => ({
        severity: "warning" as const,
        code: "temporary_file",
        workflowID: workflow.id,
        path: file,
        message: `temporary workflow write file remains: ${file}`,
      })))
      issues.push(...(yield* Effect.promise(() => workflowJournalIssues({ workflow, directory: ctx.directory, name: "commands.jsonl" }))))
      issues.push(...(yield* Effect.promise(() => workflowJournalIssues({ workflow, directory: ctx.directory, name: "messages.jsonl" }))))
      issues.push(...(yield* Effect.promise(() => workflowJournalIssues({ workflow, directory: ctx.directory, name: "events.jsonl" }))))
      return issues
    })

    const doctor = Effect.fn("Workflow.doctor")(function* (input?: DoctorInput) {
      yield* InstanceState.get(initState)
      const workflows = yield* doctorWorkflows(input)
      const migrated = input?.migrate
        ? yield* Effect.all(workflows.map((workflow) => ensureAuditablePath(workflow)), { concurrency: 1 })
        : workflows
      if (input?.migrate) {
        yield* Effect.all(migrated.map((workflow) => writeWorkflowState(workflow.id)), { concurrency: 1 })
      }
      if (input?.fix || input?.migrate) {
        const ctx = yield* InstanceState.context
        const fixPass = (targets: WorkflowInfo[]) =>
          Effect.gen(function* () {
            yield* Effect.all(targets.map((workflow) => fixWorkflowDirectories(workflow)), { concurrency: 1 })
            yield* Effect.all(targets.map((workflow) => Effect.promise(() => removeWorkflowTempFiles(ctx.directory, workflow))), { concurrency: 1 })
            yield* Effect.all(targets.map((workflow) => Effect.promise(() => repairWorkflowJournalTails(ctx.directory, workflow))), { concurrency: 1 })
            yield* Effect.all(targets.map((workflow) => Effect.promise(() => repairWorkflowJournalSequences(ctx.directory, workflow))), { concurrency: 1 })
            const orphanTargets = yield* doctorWorkflows(input)
            const fixedOrphans = yield* Effect.all(orphanTargets.map((workflow) => fixWorkflowOrphanActiveMilestones(workflow)), { concurrency: 1 })
            if (fixedOrphans.some(Boolean)) {
              yield* Effect.all(orphanTargets.map((workflow) => writeWorkflowState(workflow.id)), { concurrency: 1 })
            }
            const stateRepairTargets = yield* doctorWorkflows(input)
            const stateRepairIssues = (
              yield* Effect.all(stateRepairTargets.map((workflow) => doctorWorkflow(workflow)), { concurrency: 1 })
            ).flat()
            const workflowsNeedingStateRewrite = new Set(
              stateRepairIssues
                .filter((issue) => issue.workflowID && workflowStateDoctorFixCodes.has(issue.code))
                .map((issue) => issue.workflowID),
            )
            if (workflowsNeedingStateRewrite.size > 0) {
              yield* Effect.all(
                stateRepairTargets
                  .filter((workflow) => workflowsNeedingStateRewrite.has(workflow.id))
                  .map((workflow) => writeWorkflowState(workflow.id)),
                { concurrency: 1 },
              )
            }
            const projectionRepairTargets = yield* doctorWorkflows(input)
            yield* Effect.all(projectionRepairTargets.map((workflow) => rebuildWorkflowProjections(workflow.id)), {
              concurrency: 1,
            })
            return yield* doctorWorkflows(input)
          })
        const afterFirstPass = yield* fixPass(migrated)
        if (input?.fix) yield* fixPass(afterFirstPass)
      }
      const checked = input?.fix || input?.migrate ? yield* doctorWorkflows(input) : migrated
      const issues = (yield* Effect.all(checked.map((workflow) => doctorWorkflow(workflow)), { concurrency: 4 })).flat()
      return {
        ok: !issues.some((issue) => issue.severity === "error"),
        checked: checked.length,
        issues,
      }
    })

    const blockWorkflowResumeForDoctorIssues = Effect.fn("Workflow.blockWorkflowResumeForDoctorIssues")(function* (
      workflowID: WorkflowID,
      reason: string,
    ) {
      const report = yield* doctor({ workflowID })
      const blockers = report.issues.filter(
        (issue) => issue.severity === "error" && workflowResumeDoctorBlockingCodes.has(issue.code),
      )
      if (blockers.length === 0) return
      const workflow = yield* get(workflowID)
      return yield* blockWorkflow(
        workflow,
        [
          `Workflow doctor blocked ${reason}: ${blockers.map((issue) => `${issue.code}${issue.path ? ` at ${issue.path}` : ""}`).join("; ")}.`,
          "Run `opencode workflow doctor <workflowID> --fix` or `opencode workflow doctor <workflowID> --migrate` before resuming so the scheduler does not continue from contradictory workflow files.",
        ].join(" "),
      )
    })

    const writeWorkflowState = Effect.fn("Workflow.writeWorkflowState")(function* (
      workflowID: WorkflowID,
      workflowOverride?: WorkflowInfo,
    ) {
      const ctx = yield* InstanceState.context
      const workflow = workflowOverride ?? (yield* get(workflowID))
      const milestoneItems = yield* milestones(workflowID)
      const memberItems = yield* members(workflowID)
      const consultationItems = yield* consultations(workflowID)
      const interventionItems = yield* interventions(workflowID)
      const sessionRefs = workflowStateSessionRefs({
        workflow,
        milestones: milestoneItems,
        members: memberItems,
        consultations: consultationItems,
        interventions: interventionItems,
      })
      const sessions = yield* Effect.all(
        sessionRefs.map((ref) =>
          session.get(ref.sessionID).pipe(
            Effect.flatMap((info) =>
              Effect.all({
                messages: session.messages({ sessionID: ref.sessionID }).pipe(
                  Effect.catchCause(() => Effect.succeed([])),
                ),
                durable: workflowDurableSessionSnapshot(ref.sessionID),
              }).pipe(
                Effect.map(({ messages, durable }) => ({
                  ...workflowStateSessionSnapshot({ workflow, ref, info }),
                  messages,
                  ...durable,
                })),
              ),
            ),
            Effect.catchCause(() => Effect.succeed(workflowStateSessionSnapshot({ workflow, ref }))),
          ),
        ),
        { concurrency: 4 },
      )
      const edges = yield* workflowEdges(workflowID)
      const manifest = workflowManifest(workflow)
      const journal = yield* Effect.promise(() => workflowJournalState(ctx.directory, workflow))
      const sessionIndex = sessions.map(workflowStateSessionIndexEntry)
      yield* Effect.promise(() =>
        Promise.all([
          writeWorkflowStateFile(
            path.join(ctx.directory, workflowArtifactPath(workflow, workflowManifestFileName)),
            manifest,
          ),
          ...sessions.map((snapshot) =>
            writeWorkflowStateFile(
              path.join(ctx.directory, workflowArtifactPath(workflow, workflowStateSessionSnapshotPath(snapshot.id))),
              snapshot,
            ),
          ),
          writeWorkflowStateFile(
            path.join(ctx.directory, workflowStatePath(workflow)),
            {
              schema: workflowStateSchemaVersion,
              version: workflowStateSchemaVersion,
              manifest,
              journal,
              workflow: workflowStateDirectory(workflow),
              milestones: milestoneItems,
              members: memberItems,
              consultations: consultationItems,
              interventions: interventionItems,
              edges,
              sessions: sessionIndex,
            },
          ),
        ]),
      )
    })

    const writeOrganization = Effect.fn("Workflow.writeOrganization")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      yield* writeNote(
        workflowArtifactPath(workflow, "organization.md"),
        organizationMarkdown({
          workflow,
          members: yield* members(workflowID),
          milestones: yield* milestones(workflowID),
        }),
      )
    })

    const writeProgress = Effect.fn("Workflow.writeProgress")(function* (
      workflowID: WorkflowID,
      workflowOverride?: WorkflowInfo,
    ) {
      const ctx = yield* InstanceState.context
      const workflow = workflowOverride ?? (yield* get(workflowID))
      const standupIndex = yield* Effect.promise(() =>
        readFile(projectWorkflowPath(ctx.directory, workflow, workflowStandupIndexPath()), "utf8"),
      ).pipe(Effect.catchCause(() => Effect.succeed("")))
      yield* writeNote(
        workflowArtifactPath(workflow, "progress.md"),
        progressMarkdown({
          workflow,
          milestones: yield* milestones(workflowID),
          members: yield* members(workflowID),
          interventions: yield* interventions(workflowID),
          standups: recentStandupLines(standupIndex),
        }),
      )
    })

    const writeInterventionArtifacts = Effect.fn("Workflow.writeInterventionArtifacts")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      const items = yield* interventions(workflowID)
      yield* writeNote(workflowArtifactPath(workflow, workflowInterventionIndexPath()), interventionIndexMarkdown({ workflow, interventions: items }))
      yield* Effect.all(
        items.map((intervention) => writeNote(intervention.path, interventionMarkdown({ workflow, intervention }))),
        { discard: true },
      )
    })

    const writeConsultationArtifacts = Effect.fn("Workflow.writeConsultationArtifacts")(function* (
      workflow: WorkflowInfo,
      items: WorkflowConsultationInfo[],
    ) {
      yield* writeNote(
        workflowArtifactPath(workflow, workflowConsultationIndexPath()),
        consultationIndexMarkdown({ workflow, consultations: items }),
      )
      yield* Effect.all(
        items.map((consultation) =>
          writeNote(workflowArtifactPath(workflow, workflowConsultationPath(consultation.id)), consultationMarkdown({ workflow, consultation })),
        ),
        { discard: true },
      )
    })

    const rebuildWorkflowProjections = Effect.fn("Workflow.rebuildWorkflowProjections")(function* (workflowID: WorkflowID) {
      yield* writeInterventionArtifacts(workflowID).pipe(Effect.ignore)
      yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
      yield* writeOrganization(workflowID).pipe(Effect.ignore)
    })

    const upsertWorkflowMessage = Effect.fn("Workflow.upsertWorkflowMessage")(function* (input: {
      workflowID: WorkflowID
      id: string
      kind: WorkflowMessageKind
      fromSessionID?: SessionID | null
      fromRole?: WorkflowSessionRef["role"] | null
      toSessionID?: SessionID | null
      toRole?: WorkflowSessionRef["role"] | null
      milestoneID?: WorkflowMilestoneID | null
      timing?: WorkflowCommunicationTiming | null
      body: string
      response?: string | null
      attachments?: readonly string[] | null
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
              response: input.response ?? null,
              attachments: input.attachments ?? null,
              status: input.status,
              time_delivered: input.timeDelivered ?? null,
              time_closed: input.timeClosed ?? null,
              time_updated: input.timeUpdated,
            },
          })
          .run(),
      )
    })

    const mirrorWorkflowMessageJournalEvent = Effect.fn("Workflow.mirrorWorkflowMessageJournalEvent")(function* (
      workflowID: WorkflowID,
      event: Record<string, unknown>,
    ) {
      const messageID = typeof event.messageID === "string" ? event.messageID : undefined
      const kind = workflowMessageKind(event.kind)
      const status = typeof event.status === "string" ? event.status : undefined
      if (!messageID || !kind || !status) return
      const existing = Database.use((db) =>
        db
          .select()
          .from(WorkflowMessageTable)
          .where(and(eq(WorkflowMessageTable.workflow_id, workflowID), eq(WorkflowMessageTable.id, messageID)))
          .get(),
      )
      const now = Date.now()
      const body =
        existing?.body ??
        (typeof event.request === "string" ? event.request : undefined) ??
        (typeof event.response === "string" ? event.response : undefined) ??
        `${kind} ${messageID}`
      const attachments = Array.isArray(event.attachments)
        ? event.attachments.filter((item) => typeof item === "string")
        : existing?.attachments
      const sessionID = typeof event.sessionID === "string" ? event.sessionID : undefined
      const sourceSessionID = typeof event.sourceSessionID === "string" ? event.sourceSessionID : undefined
      const targetSessionID = typeof event.targetSessionID === "string" ? event.targetSessionID : undefined
      const closedStatuses = ["acked", "answered", "expired", "failed", "rejected"]
      yield* upsertWorkflowMessage({
        workflowID,
        id: messageID,
        kind: workflowMessageKind(existing?.kind) ?? kind,
        fromSessionID: existing?.from_session_id ?? sourceSessionID ?? null,
        fromRole: existing?.from_role ?? (event.sourceRole as WorkflowSessionRef["role"] | undefined) ?? null,
        toSessionID: existing?.to_session_id ?? targetSessionID ?? sessionID ?? null,
        toRole: existing?.to_role ?? (event.targetRole as WorkflowSessionRef["role"] | undefined) ?? null,
        milestoneID: existing?.milestone_id ?? null,
        timing: existing?.timing ?? null,
        body,
        response: typeof event.response === "string" ? event.response : existing?.response ?? null,
        attachments,
        status,
        timeCreated: existing?.time_created ?? now,
        timeDelivered: status === "delivered" ? existing?.time_delivered ?? now : existing?.time_delivered ?? null,
        timeClosed: closedStatuses.includes(status) ? now : existing?.time_closed ?? null,
        timeUpdated: now,
      })
    })

    const appendWorkflowMessageRuntimeJournal = Effect.fn("Workflow.appendWorkflowMessageRuntimeJournal")(function* (
      workflowID: WorkflowID,
      event: Record<string, unknown>,
    ) {
      const workflow = yield* get(workflowID)
      const ctx = yield* InstanceState.context
      const file = projectWorkflowPath(ctx.directory, workflow, workflowMessageJournalPath())
      const existing = yield* Effect.promise(() => readFile(file, "utf8")).pipe(
        Effect.catchCause(() => Effect.succeed("")),
      )
      yield* Effect.promise(() =>
        appendFileEnsured(
          file,
          `${JSON.stringify({
            seq: existing.trim() ? existing.trim().split(/\r?\n/).filter(Boolean).length + 1 : 1,
            ts: new Date().toISOString(),
            workflowID,
            ...event,
          })}\n`,
        ),
      )
      yield* mirrorWorkflowMessageJournalEvent(workflowID, event).pipe(Effect.ignore)
    })

    const appendWorkflowEventRuntimeJournal = Effect.fn("Workflow.appendWorkflowEventRuntimeJournal")(function* (
      workflowID: WorkflowID,
      event: Record<string, unknown>,
    ) {
      const workflow = yield* get(workflowID)
      const ctx = yield* InstanceState.context
      const file = projectWorkflowPath(ctx.directory, workflow, workflowEventJournalPath())
      const existing = yield* Effect.promise(() => readFile(file, "utf8")).pipe(
        Effect.catchCause(() => Effect.succeed("")),
      )
      yield* Effect.promise(() =>
        appendFileEnsured(
          file,
          `${JSON.stringify({
            seq: existing.trim() ? existing.trim().split(/\r?\n/).filter(Boolean).length + 1 : 1,
            ts: new Date().toISOString(),
            workflowID,
            ...event,
          })}\n`,
        ),
      )
    })

    const rejectWorkflowMessageDispatchMisuse = Effect.fn("Workflow.rejectWorkflowMessageDispatchMisuse")(function* (input: {
      workflowID: WorkflowID
      sourceSessionID: SessionID
      sourceRole: WorkflowSessionRef["role"]
      sourceMilestoneID?: WorkflowMilestoneID
      sourceAttempt?: number
      text: string
      response?: string
    }) {
      yield* appendWorkflowMessageRuntimeJournal(input.workflowID, {
        action: "reject",
        kind: "consultation",
        sourceSessionID: input.sourceSessionID,
        sourceRole: input.sourceRole,
        milestoneID: input.sourceMilestoneID,
        attempt: input.sourceAttempt,
        status: "rejected",
        request: compactMarkdown(input.text, 700),
        response:
          input.response ??
          "workflow-message is consultation/notification only; use workflow update_xml, resume, plan_complete, force_complete, or scheduler-created milestone sessions for dispatch.",
      }).pipe(Effect.ignore)
    })

    const queueExpiredWorkflowMessageIntervention = Effect.fn("Workflow.queueExpiredWorkflowMessageIntervention")(function* (input: {
      workflow: WorkflowInfo
      targetSessionID?: SessionID
      targetRole: WorkflowSessionRef["role"]
      message: string
      sourceMessageID: string
      sourceKind: "consultation" | "intervention"
    }) {
      if (!input.targetSessionID) return
      const id = `expired_${input.sourceKind}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
      const interventionPath = workflowArtifactPath(input.workflow, workflowInterventionPath(id))
      const now = Date.now()
      Database.use((db) =>
        db
          .insert(WorkflowInterventionTable)
          .values({
            workflow_id: input.workflow.id,
            id,
            target_session_id: input.targetSessionID,
            target_role: input.targetRole,
            timing: "temporary-interrupt" as const,
            message: input.message,
            path: interventionPath,
            status: "queued" as const,
            time_created: now,
            time_updated: now,
          })
          .run(),
      )
      yield* appendWorkflowMessageRuntimeJournal(input.workflow.id, {
        action: "escalate",
        kind: "intervention",
        messageID: id,
        sourceKind: input.sourceKind,
        sourceMessageID: input.sourceMessageID,
        targetSessionID: input.targetSessionID,
        targetRole: input.targetRole,
        status: "queued",
        response: input.message,
      }).pipe(Effect.ignore)
      const jobID = `${input.workflow.id}:intervention:${id}`
      yield* background.start({
        id: jobID,
        type: "workflow.intervention",
        title: `${input.workflow.title} expired message escalation`,
        metadata: { workflowID: input.workflow.id, interventionID: id },
        run: deliverIntervention(input.workflow.id, id, jobID).pipe(
          Effect.delay("10 millis"),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : updateIntervention(input.workflow.id, id, {
                  status: "failed",
                  response: `Workflow expired-message escalation failed: ${errorFromCause(cause)}`,
                }).pipe(Effect.asVoid),
          ),
          Effect.as("workflow expired message escalation delivered"),
        ),
      })
      return id
    })

    const recordMainPMSystemReport = Effect.fn("Workflow.recordMainPMSystemReport")(function* (
      workflow: WorkflowInfo,
      message: string,
    ) {
      const mainPMSessionID = workflow.pmSessionID ?? (yield* members(workflow.id)).find((member) => member.role === "main_pm")?.sessionID
      if (!mainPMSessionID) return
      const id = `system_report_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
      const interventionPath = workflowArtifactPath(workflow, workflowInterventionPath(id))
      const now = Date.now()
      Database.use((db) =>
        db
          .insert(WorkflowInterventionTable)
          .values({
            workflow_id: workflow.id,
            id,
            target_session_id: mainPMSessionID,
            target_role: "main_pm" as const,
            timing: "temporary-interrupt" as const,
            message,
            path: interventionPath,
            status: "delivered" as const,
            time_created: now,
            time_updated: now,
          })
          .run(),
      )
      yield* appendWorkflowMessageRuntimeJournal(workflow.id, {
        action: "deliver",
        kind: "intervention",
        messageID: id,
        targetSessionID: mainPMSessionID,
        targetRole: "main_pm",
        status: "delivered",
        response: message,
      }).pipe(Effect.ignore)
      yield* writeInterventionArtifacts(workflow.id).pipe(Effect.ignore)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflow.id).pipe(Effect.ignore)
      return id
    })

    const expireWorkflowMessages = Effect.fn("Workflow.expireWorkflowMessages")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      const now = Date.now()
      const staleConsultations = Database.use((db) =>
        db
          .select()
          .from(WorkflowConsultationTable)
          .where(and(eq(WorkflowConsultationTable.workflow_id, workflowID), eq(WorkflowConsultationTable.status, "pending")))
          .all()
          .filter((item) => now - item.time_created >= workflowConsultationTimeoutMillis),
      )
      const staleInterventions = Database.use((db) =>
        db
          .select()
          .from(WorkflowInterventionTable)
          .where(eq(WorkflowInterventionTable.workflow_id, workflowID))
          .all()
          .filter(
            (item) =>
              ["queued", "delivered", "blocked"].includes(item.status) &&
              now - item.time_created >= workflowInterventionTimeoutMillis,
          ),
      )
      for (const item of staleConsultations) {
        const response = `Consultation ${item.id} expired without an answer from ${roleSessionTitle(item.to_role)} after 30 minutes.`
        Database.use((db) =>
          db
            .update(WorkflowConsultationTable)
            .set({ status: "expired", answer: response, time_updated: now })
            .where(
              and(
                eq(WorkflowConsultationTable.workflow_id, workflowID),
                eq(WorkflowConsultationTable.id, item.id),
                eq(WorkflowConsultationTable.status, "pending"),
              ),
            )
            .run(),
        )
        yield* appendWorkflowMessageRuntimeJournal(workflowID, {
          action: "expire",
          kind: "consultation",
          messageID: item.id,
          sessionID: item.to_session_id,
          targetSessionID: item.from_session_id,
          status: "expired",
          response,
        }).pipe(Effect.ignore)
        yield* queueExpiredWorkflowMessageIntervention({
          workflow,
          targetSessionID: item.from_session_id,
          targetRole: item.from_role,
          sourceKind: "consultation",
          sourceMessageID: item.id,
          message: [
            response,
            "",
            `Original reason: ${item.reason ?? "not provided"}`,
            `Original question: ${compactMarkdown(item.question, 700)}`,
            "",
            "Decide whether to reroute this question, proceed with documented assumptions, or block the workflow for requester direction.",
          ].join("\n"),
        }).pipe(Effect.ignore)
      }
      for (const item of staleInterventions) {
        const response = `Intervention ${item.id} expired without acknowledgement from ${roleSessionTitle(item.target_role)} after 30 minutes.`
        Database.use((db) =>
          db
            .update(WorkflowInterventionTable)
            .set({ status: "expired", response, time_updated: now })
            .where(
              and(
                eq(WorkflowInterventionTable.workflow_id, workflowID),
                eq(WorkflowInterventionTable.id, item.id),
                or(
                  eq(WorkflowInterventionTable.status, "queued"),
                  eq(WorkflowInterventionTable.status, "delivered"),
                  eq(WorkflowInterventionTable.status, "blocked"),
                ),
              ),
            )
            .run(),
        )
        const sourceSessionID = item.from_session_id ?? workflow.rootSessionID
        const sourceRole = sourceSessionID
          ? yield* workflowToolCommandSourceRole(workflow, sourceSessionID).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
          : undefined
        yield* appendWorkflowMessageRuntimeJournal(workflowID, {
          action: "expire",
          kind: "intervention",
          messageID: item.id,
          sessionID: item.target_session_id,
          targetSessionID: sourceSessionID,
          status: "expired",
          response,
        }).pipe(Effect.ignore)
        if (item.target_role === "requester") {
          yield* blockWorkflow(
            workflow,
            [
              `Workflow message escalation reached requester and expired: ${response}`,
              "",
              `Original intervention: ${compactMarkdown(item.message, 700)}`,
              "",
              "The requester did not close the escalated message in time. Human direction is required before the workflow can continue.",
            ].join("\n"),
          ).pipe(Effect.ignore)
          continue
        }
        yield* queueExpiredWorkflowMessageIntervention({
          workflow,
          targetSessionID: sourceSessionID,
          targetRole: sourceRole ?? "requester",
          sourceKind: "intervention",
          sourceMessageID: item.id,
          message: [
            response,
            "",
            `Original intervention: ${compactMarkdown(item.message, 700)}`,
            "",
            "The target session did not close this intervention in time. Decide whether to retry, reroute, or block for human direction.",
          ].join("\n"),
        }).pipe(Effect.ignore)
      }
      if (staleConsultations.length > 0) {
        yield* writeConsultationArtifacts(workflow, yield* consultations(workflowID)).pipe(Effect.ignore)
      }
      if (staleConsultations.length > 0 || staleInterventions.length > 0) {
        yield* writeInterventionArtifacts(workflowID).pipe(Effect.ignore)
        yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
        yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
        yield* publishUpdated(workflowID).pipe(Effect.ignore)
      }
      return staleConsultations.length + staleInterventions.length
    })

    const ensureStandupIndex = Effect.fn("Workflow.ensureStandupIndex")(function* (workflowID: WorkflowID) {
      const ctx = yield* InstanceState.context
      const workflow = yield* get(workflowID)
      yield* Effect.promise(() =>
        writeFileEnsuredIfMissing(
          projectWorkflowPath(ctx.directory, workflow, workflowStandupIndexPath()),
          `${standupIndexHeader(workflow)}_No company standups have been recorded yet._\n`,
        ),
      )
    })

    const appendStandupIndex = Effect.fn("Workflow.appendStandupIndex")(function* (
      workflow: WorkflowInfo,
      standupPath: string,
      reason: string,
    ) {
      const ctx = yield* InstanceState.context
      const indexPath = projectWorkflowPath(ctx.directory, workflow, workflowStandupIndexPath())
      const existing = yield* Effect.promise(() => readFile(indexPath, "utf8")).pipe(
        Effect.catchCause(() => Effect.succeed(standupIndexHeader(workflow))),
      )
      const normalized = existing.includes("_No company standups") ? standupIndexHeader(workflow) : existing.trimEnd()
      yield* writeNote(
        workflowArtifactPath(workflow, workflowStandupIndexPath()),
        `${normalized}\n- ${new Date().toISOString()} ${reason}: ${standupPath}\n`,
      )
    })

    const writeMainPMSupervisionNote = Effect.fn("Workflow.writeMainPMSupervisionNote")(function* (input: {
      workflowID: WorkflowID
      message: string
      output: string
    }) {
      const workflow = yield* get(input.workflowID)
      const standupPath = workflowArtifactPath(
        workflow,
        workflowStandupPath(`supervision_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`),
      )
      yield* writeNote(
        standupPath,
        standupMarkdown({
          workflow,
          reason: "main PM supervision",
          progress: progressMarkdown({
            workflow,
            milestones: yield* milestones(input.workflowID),
            members: yield* members(input.workflowID),
            interventions: yield* interventions(input.workflowID),
          }),
          output: [
            "## Triggering Progress Update",
            "",
            input.message,
            "",
            "## Main PM Response",
            "",
            input.output,
          ].join("\n"),
        }),
      )
      yield* appendStandupIndex(workflow, standupPath, "main PM supervision")
      yield* writeReferenceIndex(input.workflowID).pipe(Effect.ignore)
    })

    const graph = Effect.fn("Workflow.graph")(function* (workflowID: WorkflowID) {
      const ctx = yield* InstanceState.context
      const info = yield* get(workflowID)
      yield* normalizeWorkflowSessions(info)
      yield* expireWorkflowMessages(workflowID).pipe(Effect.ignore)
      const next = yield* get(workflowID)
      const items = yield* milestones(workflowID)
      const staff = yield* members(workflowID)
      const standupIndex = yield* Effect.promise(() =>
        readFile(projectWorkflowPath(ctx.directory, next, workflowStandupIndexPath()), "utf8"),
      ).pipe(Effect.catchCause(() => Effect.succeed("")))
      const edges = Database.use((db) =>
        db
          .select()
          .from(WorkflowEdgeTable)
          .where(eq(WorkflowEdgeTable.workflow_id, workflowID))
          .all()
          .map((row) => row.data ?? { id: `${row.from_id}->${row.to_id}`, from: String(row.from_id), to: String(row.to_id) }),
      )
      const result = graphFrom(
        next,
        items,
        edges,
        yield* consultations(workflowID),
        staff,
        yield* interventions(workflowID),
        standupDocsFromIndex(next, standupIndex),
      )
      recordWorkflowGraphDiagnostic(result)
      return result
    })

    const setStatus = Effect.fn("Workflow.setStatus")(function* (
      workflowID: WorkflowID,
      status: WorkflowInfo["status"],
      extra?: { error?: string; testerSessionID?: SessionID; testPath?: string },
    ) {
      const current = yield* get(workflowID)
      const now = Date.now()
      const terminal = status === "completed" || status === "failed" || status === "cancelled"
      const next: WorkflowInfo = {
        ...current,
        status,
        ...(extra?.error !== undefined ? { error: extra.error } : {}),
        ...(extra?.testerSessionID !== undefined ? { testerSessionID: extra.testerSessionID } : {}),
        ...(extra?.testPath !== undefined ? { testPath: extra.testPath } : {}),
        time: terminal
          ? { ...current.time, updated: now, completed: now }
          : { created: current.time.created, updated: now },
      }
      if (terminal) yield* writeReferenceIndex(workflowID, next).pipe(Effect.ignore)
      const patch = {
        status,
        ...(extra?.error !== undefined ? { error: extra.error } : {}),
        ...(extra?.testerSessionID !== undefined ? { tester_session_id: extra.testerSessionID } : {}),
        ...(extra?.testPath !== undefined ? { test_path: extra.testPath } : {}),
        time_updated: now,
        ...(terminal ? { time_completed: now } : { time_completed: null }),
      }
      Database.use((db) =>
        db
          .update(WorkflowTable)
          .set(patch)
          .where(eq(WorkflowTable.id, workflowID))
          .run(),
      )
      return yield* publishUpdated(workflowID)
    })

    const setMainProductManagerSession = Effect.fn("Workflow.setMainProductManagerSession")(function* (
      workflowID: WorkflowID,
      sessionID: SessionID,
    ) {
      Database.use((db) =>
        db
          .update(WorkflowTable)
          .set({ pm_session_id: sessionID, time_updated: Date.now() })
          .where(eq(WorkflowTable.id, workflowID))
          .run(),
      )
      return yield* publishUpdated(workflowID)
    })

    const updateMilestone = Effect.fn("Workflow.updateMilestone")(function* (
      workflowID: WorkflowID,
      milestoneID: WorkflowMilestoneID,
      patch: Partial<WorkflowMilestoneInfo>,
    ) {
      const workflow = yield* get(workflowID)
      const row = {
        ...(patch.title !== undefined ? { title: patch.title } : {}),
        ...(patch.department !== undefined ? { department: patch.department } : {}),
        ...(patch.review !== undefined ? { review: patch.review } : {}),
        ...(patch.waitingFor !== undefined ? { waiting_for: patch.waitingFor } : {}),
        ...(patch.prompt !== undefined ? { prompt: patch.prompt } : {}),
        ...(patch.dependsOn !== undefined ? { depends_on: patch.dependsOn } : {}),
        ...(patch.status !== undefined ? { status: patch.status } : {}),
        ...(patch.attempt !== undefined ? { attempt: patch.attempt } : {}),
        ...(patch.planPath !== undefined ? { plan_path: rewriteWorkflowStoredPath(workflow, patch.planPath) } : {}),
        ...(patch.reviewPath !== undefined ? { review_path: rewriteWorkflowStoredPath(workflow, patch.reviewPath) } : {}),
        ...(patch.session !== undefined ? { session: patch.session } : {}),
        time_updated: Date.now(),
      }
      Database.use((db) =>
        db
          .update(WorkflowMilestoneTable)
          .set(row)
          .where(and(eq(WorkflowMilestoneTable.workflow_id, workflowID), eq(WorkflowMilestoneTable.id, milestoneID)))
          .run(),
      )
      const milestone = (yield* milestones(workflowID)).find((item) => item.id === milestoneID)
      if (milestone) yield* events.publish(Event.NodeUpdated, { workflowID, milestone })
      yield* publishUpdated(workflowID)
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
      yield* writeOrganization(workflowID).pipe(Effect.ignore)
      yield* writeProgress(workflowID).pipe(Effect.ignore)
      yield* deliverReadyInterventions(workflowID).pipe(Effect.ignore)
      return milestone
    })

    const cancelMilestoneBackgroundRuns = Effect.fn("Workflow.cancelMilestoneBackgroundRuns")(function* (
      workflowID: WorkflowID,
      milestoneID: WorkflowMilestoneID,
      attempt: number,
    ) {
      for (const job of (yield* background.list()).filter(
        (job) =>
          job.status === "running" &&
          job.type === "workflow.milestone" &&
          job.metadata?.workflowID === workflowID &&
          job.metadata?.milestoneID === milestoneID,
      )) {
        yield* background.cancel(job.id).pipe(Effect.ignore)
      }
      for (const item of Array.from({ length: attempt + 3 }, (_, index) => index + 1)) {
        yield* background.cancel(milestoneJobID(workflowID, milestoneID, item)).pipe(Effect.ignore)
      }
    })

    const saveDefinition = Effect.fn("Workflow.saveDefinition")(function* (
      workflowID: WorkflowID,
      xml: string,
      definition: WorkflowDefinition,
      status: WorkflowInfo["status"] = "dispatching",
    ) {
      const ctx = yield* InstanceState.context
      const workflow = yield* get(workflowID)
      const edges = edgesFrom(definition)
      const now = Date.now()
      const existing = new Map((yield* milestones(workflowID)).map((milestone) => [milestone.id, milestone]))
      const definitionIDs = new Set(definition.milestones.map((milestone) => milestone.id))
      const retained = [...existing.values()].filter(
        (milestone) => milestone.session.length > 0 && !definitionIDs.has(milestone.id),
      )
      for (const milestone of retained.filter((milestone) => interruptedMilestone(milestone.status))) {
        yield* cancelMilestoneBackgroundRuns(workflowID, milestone.id, milestone.attempt)
      }
      const rows = [
        ...definition.milestones.map((milestone) => ({
          workflow_id: workflowID,
          id: milestone.id,
          title: milestone.title,
          department: milestone.department,
          review: milestone.review,
          waiting_for: milestone.waitingFor,
          prompt: milestone.prompt,
          depends_on: milestone.dependsOn,
          status: existing.get(milestone.id)?.status ?? ("pending" as const),
          attempt: existing.get(milestone.id)?.attempt ?? 0,
          plan_path: workflowStoredPath(workflow, existing.get(milestone.id)?.planPath, milestone.id, "plan.md"),
          review_path: existing.get(milestone.id)?.reviewPath
            ? rewriteWorkflowStoredPath(workflow, existing.get(milestone.id)!.reviewPath!)
            : undefined,
          session: existing.get(milestone.id)?.session ?? [],
          time_created: now,
          time_updated: now,
        })),
        ...retained.map((milestone) => ({
          workflow_id: workflowID,
          id: milestone.id,
          title: milestone.title,
          department: milestone.department,
          review: milestone.review,
          waiting_for: milestone.waitingFor,
          prompt: milestone.prompt,
          depends_on: milestone.dependsOn.filter((id) => definitionIDs.has(id)),
          status: "cancelled" as const,
          attempt: milestone.attempt,
          plan_path: workflowStoredPath(workflow, milestone.planPath, milestone.id, "plan.md"),
          review_path: milestone.reviewPath ? rewriteWorkflowStoredPath(workflow, milestone.reviewPath) : undefined,
          session: milestone.session,
          time_created: now,
          time_updated: now,
        })),
      ]
      Database.transaction((tx) => {
        tx.update(WorkflowTable)
          .set({ xml, status, error: null, time_updated: now })
          .where(eq(WorkflowTable.id, workflowID))
          .run()
        tx.delete(WorkflowMilestoneTable).where(eq(WorkflowMilestoneTable.workflow_id, workflowID)).run()
        tx.delete(WorkflowEdgeTable).where(eq(WorkflowEdgeTable.workflow_id, workflowID)).run()
        if (rows.length > 0) tx.insert(WorkflowMilestoneTable).values(rows).run()
        if (edges.length > 0) {
          tx.insert(WorkflowEdgeTable)
            .values(
              edges.map((edge) => ({
                workflow_id: workflowID,
                from_id: WorkflowMilestoneID.make(edge.from),
                to_id: WorkflowMilestoneID.make(edge.to),
                data: edge,
              })),
            )
          .run()
        }
      })
      const revisionPath = yield* Effect.promise(() => writeWorkflowGraphRevision(ctx.directory, workflow, xml))
      if (revisionPath) {
        yield* appendWorkflowEventRuntimeJournal(workflowID, {
          action: "graph.revised",
          path: revisionPath,
          milestones: rows.length,
          edges: edges.length,
        }).pipe(Effect.ignore)
      }
      yield* events.publish(Event.GraphUpdated, { workflowID })
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
      yield* writeOrganization(workflowID).pipe(Effect.ignore)
      yield* writeProgress(workflowID).pipe(Effect.ignore)
      return yield* graph(workflowID)
    })

    const refreshWorkflowXml = Effect.fn("Workflow.refreshWorkflowXml")(function* (
      workflowID: WorkflowID,
      status: WorkflowInfo["status"],
    ) {
      const ctx = yield* InstanceState.context
      const workflow = yield* get(workflowID)
      const xml = yield* Effect.promise(() => readFile(projectWorkflowPath(ctx.directory, workflow, "workflow.xml"), "utf8")).pipe(
        Effect.catchCause(() => Effect.succeed("")),
      )
      if (!xml.trim()) return false
      if (xml === workflow.xml) return false
      const definition = yield* Effect.try({
        try: () => parseXmlDefinition(xml, workflow),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      })
      yield* saveDefinition(workflowID, xml, definition, status)
      yield* writePrecreatedPlans(yield* get(workflowID), yield* milestones(workflowID))
      yield* publishUpdated(workflowID)
      return true
    })

    const queueWorkflowArtifactRefresh = Effect.fn("Workflow.queueWorkflowArtifactRefresh")(function* (
      workflowID: WorkflowID,
      reason: string,
    ) {
      const jobID = `${workflowID}:artifact-refresh`
      if ((yield* background.list()).some((job) => job.id === jobID && job.status === "running")) return
      const workflow = yield* get(workflowID)
      yield* background.start({
        id: jobID,
        type: "workflow.artifacts",
        title: `${workflow.title} artifacts`,
        metadata: { workflowID, reason },
        run: Effect.gen(function* () {
          yield* Effect.sleep("25 millis")
          yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
          yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
          yield* writeOrganization(workflowID).pipe(Effect.ignore)
        }).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : appendWorkflowEventRuntimeJournal(workflowID, {
                  action: "artifact_refresh.failed",
                  message: errorFromCause(cause),
                }).pipe(Effect.ignore),
          ),
          Effect.as("workflow artifacts refreshed"),
        ),
      })
    })

    const updateMilestoneSession = Effect.fn("Workflow.updateMilestoneSession")(function* (
      workflowID: WorkflowID,
      milestoneID: WorkflowMilestoneID,
      sessionRefs: WorkflowSessionRef[],
    ) {
      Database.use((db) =>
        db
          .update(WorkflowMilestoneTable)
          .set({ session: sessionRefs, time_updated: Date.now() })
          .where(and(eq(WorkflowMilestoneTable.workflow_id, workflowID), eq(WorkflowMilestoneTable.id, milestoneID)))
          .run(),
      )
      const milestone = (yield* milestones(workflowID)).find((item) => item.id === milestoneID)
      if (milestone) yield* events.publish(Event.NodeUpdated, { workflowID, milestone }).pipe(Effect.ignore)
      yield* events.publish(Event.GraphUpdated, { workflowID }).pipe(Effect.ignore)
      yield* queueWorkflowArtifactRefresh(workflowID, "milestone session assignment").pipe(Effect.ignore)
      return milestone
    })

    const recordConsultation = Effect.fn("Workflow.recordConsultation")(function* (input: {
      workflow: WorkflowInfo
      fromSessionID: SessionID
      toSessionID: SessionID
      fromRole: WorkflowSessionRef["role"]
      toRole: WorkflowSessionRef["role"]
      milestoneID?: WorkflowMilestoneID
      reason?: string
      timing?: WorkflowConsultationInfo["timing"]
      question: string
      answer: string
      status: WorkflowConsultationInfo["status"]
    }) {
      const id = `consult_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
      const now = Date.now()
      Database.use((db) =>
        db
          .insert(WorkflowConsultationTable)
          .values({
            workflow_id: input.workflow.id,
            id,
            from_session_id: input.fromSessionID,
            to_session_id: input.toSessionID,
            from_role: input.fromRole,
            to_role: input.toRole,
            milestone_id: input.milestoneID,
            reason: input.reason,
            timing: input.timing,
            question: input.question,
            answer: input.answer,
            status: input.status,
            time_created: now,
            time_updated: now,
          })
          .run(),
      )
      yield* upsertWorkflowMessage({
        workflowID: input.workflow.id,
        id,
        kind: "consultation",
        fromSessionID: input.fromSessionID,
        fromRole: input.fromRole,
        toSessionID: input.toSessionID,
        toRole: input.toRole,
        milestoneID: input.milestoneID,
        timing: input.timing,
        body: input.question,
        response: input.answer,
        status: input.status,
        timeCreated: now,
        timeClosed: input.status === "answered" || input.status === "expired" || input.status === "failed" ? now : undefined,
        timeUpdated: now,
      })
      yield* queueWorkflowArtifactRefresh(input.workflow.id, "consultation recorded").pipe(Effect.ignore)
      yield* events.publish(Event.GraphUpdated, { workflowID: input.workflow.id })
    })

    const answerPendingRequesterConsultations = Effect.fn("Workflow.answerPendingRequesterConsultations")(function* (
      workflow: WorkflowInfo,
      requesterSessionID: SessionID,
      answer: string,
    ) {
      const pending = Database.use((db) =>
        db
          .select()
          .from(WorkflowConsultationTable)
          .where(
            and(
              eq(WorkflowConsultationTable.workflow_id, workflow.id),
              eq(WorkflowConsultationTable.to_session_id, requesterSessionID),
              eq(WorkflowConsultationTable.to_role, "requester"),
              eq(WorkflowConsultationTable.status, "pending"),
            ),
          )
          .all(),
      )
      if (pending.length === 0) return false
      const items = yield* milestones(workflow.id)
      Database.use((db) =>
        db
          .update(WorkflowConsultationTable)
          .set({
            answer,
            status: "answered" as const,
            time_updated: Date.now(),
          })
          .where(
            and(
              eq(WorkflowConsultationTable.workflow_id, workflow.id),
              eq(WorkflowConsultationTable.to_session_id, requesterSessionID),
              eq(WorkflowConsultationTable.to_role, "requester"),
              eq(WorkflowConsultationTable.status, "pending"),
            ),
          )
          .run(),
      )
      yield* Effect.all(
        pending.map((consultation) =>
          Effect.gen(function* () {
            yield* upsertWorkflowMessage({
              workflowID: workflow.id,
              id: consultation.id,
              kind: "consultation",
              fromSessionID: consultation.from_session_id,
              fromRole: consultation.from_role,
              toSessionID: requesterSessionID,
              toRole: "requester",
              milestoneID: consultation.milestone_id ?? undefined,
              timing: consultation.timing ?? undefined,
              body: consultation.question,
              response: answer,
              status: "answered",
              timeCreated: consultation.time_created,
              timeClosed: Date.now(),
              timeUpdated: Date.now(),
            })
            const milestoneID = consultation.milestone_id ?? workflowSessionMilestoneID(items, consultation.from_session_id)
            const milestone = milestoneID ? items.find((item) => item.id === milestoneID) : undefined
            yield* runPrompt(
              consultation.from_session_id,
              workflowAgentForRole(consultation.from_role),
              workflow.model,
              [
                "Requester clarification response received.",
                "",
                workflowReferencePrompt(workflow),
                "",
                `Requester session: ${requesterSessionID}`,
                ...(consultation.reason ? [`Original reason: ${consultation.reason}`] : []),
                ...(consultation.timing ? [`Original timing: ${consultation.timing}`] : []),
                "",
                "Original question:",
                consultation.question,
                "",
                "Requester response:",
                answer,
                "",
                "Use this requester answer to continue or revise your assigned workflow work. If the answer changes scope or sequencing, notify the main PM with workflow communication XML.",
              ].join("\n"),
              {
                workflowID: workflow.id,
                role: consultation.from_role,
                milestoneID,
                attempt: workflowSessionAttempt(items, consultation.from_session_id),
              },
              {
                consult: false,
                expect: workflowSessionExpectation({
                  role: consultation.from_role,
                  milestoneID,
                  milestoneStatus: milestone?.status,
                  workflowStatus: workflow.status,
                }),
              },
            ).pipe(Effect.ignore)
          }),
        ),
        { concurrency: 1, discard: true },
      )
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* writeOrganization(workflow.id).pipe(Effect.ignore)
      yield* writeProgress(workflow.id).pipe(Effect.ignore)
      yield* events.publish(Event.GraphUpdated, { workflowID: workflow.id })
      return true
    })

    const applyRequesterAcceptance = Effect.fn("Workflow.applyRequesterAcceptance")(function* (
      workflow: WorkflowInfo,
      text: string,
    ) {
      const decision = parseRequesterAcceptanceDecision(text)
      if (!decision) return false
      if (workflow.status !== "accepting" && workflow.status !== "blocked") return false
      yield* writeNote(workflowArtifactPath(workflow, workflowAcceptancePath("requester")), text)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      if (decision === "reject") {
        if (
          yield* reopenFeedbackMilestones({
            workflow,
            source: "Requester final acceptance",
            output: text,
            milestoneIDs: parseGateMilestoneIDs(text, "acceptance"),
          })
        ) {
          return true
        }
        yield* blockWorkflow(workflow, `Requester rejected final acceptance: ${compactMarkdown(text, 240).replace(/\n/g, " ")}`)
        return true
      }
      if (!workflowComplete(text)) return false
      for (const item of yield* milestones(workflow.id)) {
        if (item.status === "testing" || item.status === "approved") {
          yield* updateMilestone(workflow.id, item.id, { status: "done" })
        }
      }
      const latest = yield* get(workflow.id)
      yield* setStatus(workflow.id, "completed", {
        error: "",
        ...(latest.testerSessionID ? { testerSessionID: latest.testerSessionID } : {}),
        ...(latest.testPath ? { testPath: latest.testPath } : {}),
      })
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* writeProgress(workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflow.id).pipe(Effect.ignore)
      yield* events.publish(Event.GraphUpdated, { workflowID: workflow.id })
      return true
    })

    const resolveConsultRequests: (
      workflowID: WorkflowID,
      sourceSessionID: SessionID,
      sourceAgent: string,
      model: WorkflowInfo["model"] | undefined,
      sourceRole: WorkflowSessionRef["role"],
      sourceMilestoneID: WorkflowMilestoneID | undefined,
      sourceAttempt: number | undefined,
      text: string,
      depth?: number,
    ) => Effect.Effect<MessageV2.WithParts | undefined, unknown> = Effect.fn("Workflow.resolveConsultRequests")(function* (
      workflowID,
      sourceSessionID,
      sourceAgent,
      model,
      sourceRole,
      sourceMilestoneID,
      sourceAttempt,
      text,
      depth = 0,
    ) {
      const requests = parseConsultRequests(text)
      if (requests.length === 0) return
      if (workflowMessageDispatchMisuse(text, sourceRole)) {
        yield* rejectWorkflowMessageDispatchMisuse({
          workflowID,
          sourceSessionID,
          sourceRole,
          sourceMilestoneID,
          sourceAttempt,
          text,
        })
        return
      }
      const workflow = yield* get(workflowID)
      const items = yield* milestones(workflowID)
      const sourceResponses: MessageV2.WithParts[] = []
      for (const request of requests) {
        const targetSessionID =
          request.targetSessionID ??
          (request.targetRole
            ? request.targetRole === "requester"
              ? workflow.rootSessionID
              : (yield* ensureCompanyMember({
                  workflow,
                  role: request.targetRole,
                  specialty: request.targetSpecialty,
                  prompt: request.question,
                  modelWeight: request.modelWeight,
                }))?.sessionID
            : undefined)
        if (!targetSessionID || targetSessionID === sourceSessionID) continue
        const targetRole = request.targetRole ?? workflowSessionRole(workflow, items, targetSessionID)
        const timing = request.timing ?? "after-task"
        if (targetRole === "requester") {
          yield* recordConsultation({
            workflow,
            fromSessionID: sourceSessionID,
            toSessionID: targetSessionID,
            fromRole: sourceRole,
            toRole: "requester",
            milestoneID: sourceMilestoneID,
            reason: request.reason,
            timing,
            question: request.question,
            answer: "_Pending requester response in the requester session._",
            status: "pending",
          })
          if (timing === "after-task") continue
          if (sourceMilestoneID) {
            yield* updateMilestone(workflowID, sourceMilestoneID, { status: "blocked" }).pipe(Effect.ignore)
          }
          yield* blockWorkflow(
            yield* get(workflowID),
            `Requester clarification requested by ${roleSessionTitle(sourceRole)}${request.reason ? `: ${request.reason}` : ""}`,
          ).pipe(Effect.ignore)
          return
        }
        const targetMilestoneID = workflowSessionMilestoneID(items, targetSessionID)
        const targetAttempt = workflowSessionAttempt(items, targetSessionID)
        const targetInfo = yield* session
          .get(targetSessionID)
          .pipe(Effect.mapError((error) => new Error({ message: error.message })))
        const pauseStatus =
          timing === "temporary-interrupt" && sourceMilestoneID
            ? temporaryInterruptPauseStatus((yield* milestones(workflowID)).find((item) => item.id === sourceMilestoneID)?.status)
            : undefined
        if (pauseStatus && sourceMilestoneID) {
          yield* updateMilestone(workflowID, sourceMilestoneID, { status: "blocked" }).pipe(Effect.ignore)
        }
        const answerPrompt = runPrompt(
          targetSessionID,
          targetInfo.agent ?? workflowAgentForRole(targetRole),
          model,
          [
            "A peer employee session in the same workflow company is asking for consultation.",
            "",
            workflowReferencePrompt(workflow),
            "",
            `Asking session: ${sourceSessionID}`,
            `Asking role: ${roleSessionTitle(sourceRole)}`,
            `Requested timing: ${timing}`,
            ...(request.reason ? [`Reason: ${request.reason}`] : []),
            ...(request.modelWeight !== undefined ? [`Requested model weight: ${request.modelWeight}/100`] : []),
            "",
            "Question:",
            request.question,
            "",
            "Answer concisely with the facts, decisions, files, risks, and acceptance notes this peer needs. If this should change company direction or workflow XML, say so explicitly and notify main_pm with workflow communication XML.",
          ].join("\n"),
          {
            workflowID,
            role: targetRole,
            milestoneID: targetMilestoneID,
          },
          { consult: false, control: false, modelWeight: request.modelWeight },
        )
        const answer =
          pauseStatus && sourceMilestoneID
            ? yield* answerPrompt.pipe(
                Effect.catchCause((cause) =>
                  Effect.gen(function* () {
                    yield* blockWorkflow(
                      yield* get(workflowID),
                      `Temporary workflow consultation failed for milestone ${sourceMilestoneID}: ${errorFromCause(cause)}`,
                    )
                    return undefined
                  }),
                ),
              )
            : yield* answerPrompt
        if (!answer) return
        const answerText = latestText(answer)
        if (depth < 2 && parseConsultRequests(answerText).length > 0) {
          yield* resolveConsultRequests(
            workflowID,
            targetSessionID,
            targetInfo.agent ?? workflowAgentForRole(targetRole),
            model,
            targetRole,
            targetMilestoneID,
            targetAttempt,
            answerText,
            depth + 1,
          ).pipe(Effect.ignore)
        }
        yield* recordConsultation({
          workflow,
          fromSessionID: sourceSessionID,
          toSessionID: targetSessionID,
          fromRole: sourceRole,
          toRole: targetRole,
          milestoneID: sourceMilestoneID,
          reason: request.reason,
          timing,
          question: request.question,
          answer: answerText,
          status: "answered",
        })
        if (targetRole === "main_pm") {
          yield* applyWorkflowControl(workflowID, answerText, `consultation response to ${roleSessionTitle(sourceRole)}`, {
            exceptJobID:
              sourceMilestoneID && sourceAttempt !== undefined
                ? milestoneJobID(workflowID, sourceMilestoneID, sourceAttempt)
                : undefined,
            sourceSessionID: targetSessionID,
            sourceAgent: targetInfo.agent ?? workflowAgentForRole(targetRole),
          }).pipe(Effect.ignore)
          if (yield* workflowIsBlocked(workflowID)) return
        }
        const sourceExpectation = workflowSessionExpectation({
          role: sourceRole,
          milestoneID: sourceMilestoneID,
          milestoneStatus: sourceMilestoneID
            ? (yield* milestones(workflowID)).find((item) => item.id === sourceMilestoneID)?.status
            : undefined,
          workflowStatus: (yield* get(workflowID)).status,
        })
        sourceResponses.push(
          yield* runPrompt(
            sourceSessionID,
            sourceAgent,
            model,
            [
              "Workflow consultation response received.",
              "",
              `Consulted session: ${targetSessionID}`,
              `Consulted role: ${roleSessionTitle(targetRole)}`,
              `Requested timing: ${timing}`,
              ...(request.reason ? [`Original reason: ${request.reason}`] : []),
              "",
              "Original question:",
              request.question,
              "",
              "Response:",
              answerText,
              "",
              "Use this answer to continue your assigned workflow task. Do not repeat the consultation unless something remains unclear.",
              ...(sourceExpectation
                ? [
                    "",
                    `Required output: ${sourceExpectation.description}`,
                    "",
                    sourceExpectation.reminder,
                  ]
                : []),
            ].join("\n"),
            {
              workflowID,
              role: sourceRole,
              milestoneID: sourceMilestoneID,
              attempt: sourceAttempt,
            },
            {
              consult: false,
              expect: sourceExpectation,
            },
          ),
        )
        if (pauseStatus && sourceMilestoneID && !(yield* workflowIsBlocked(workflowID))) {
          yield* updateMilestone(workflowID, sourceMilestoneID, { status: pauseStatus }).pipe(Effect.ignore)
        }
        if (sourceExpectation?.matches(latestText(sourceResponses.at(-1)!))) return sourceResponses.at(-1)
        if (timing === "interrupt") {
          if (sourceMilestoneID) {
            yield* updateMilestone(workflowID, sourceMilestoneID, { status: "blocked" }).pipe(Effect.ignore)
          }
          yield* blockWorkflow(
            yield* get(workflowID),
            `Workflow communication interrupt from ${roleSessionTitle(sourceRole)}${request.reason ? `: ${request.reason}` : ""}`,
          ).pipe(Effect.ignore)
          return
        }
      }
      return sourceResponses.at(-1)
    })

    const continuePlanning = Effect.fn("Workflow.continuePlanning")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      if (!["planning", "dispatching", "blocked"].includes(workflow.status)) return workflow
      const currentItems = yield* milestones(workflowID)
      if (currentItems.some((item) => item.session.length > 0)) return yield* schedule(workflowID)
      const ctx = yield* InstanceState.context
      const xml = yield* Effect.promise(() => readFile(projectWorkflowPath(ctx.directory, workflow, "workflow.xml"), "utf8")).pipe(
        Effect.catchCause(() => Effect.succeed(defaultXml)),
      )
      const definition = yield* Effect.try({
        try: () => parseXmlDefinition(xml, workflow),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      })
      if (xml === defaultXml) yield* writeNote(workflowArtifactPath(workflow, "workflow.xml"), xml)
      yield* saveDefinition(workflowID, xml, definition)
      const next = yield* get(workflowID)
      yield* writePrecreatedPlans(next, yield* milestones(workflowID))
      yield* publishUpdated(workflowID)
      return yield* schedule(workflowID)
    })

    const queueContinuePlanning = Effect.fn("Workflow.queueContinuePlanning")(function* (
      workflow: WorkflowInfo,
      reason: string,
    ) {
      if (!["planning", "dispatching"].includes(workflow.status)) return false
      const items = yield* milestones(workflow.id)
      if (!items.every((item) => item.session.length === 0 && item.status === "pending")) return false
      yield* background.cancel(workflow.id).pipe(Effect.ignore)
      yield* background.start({
        id: workflow.id,
        type: "workflow",
        title: workflow.title,
        metadata: { workflowID: workflow.id },
        run: continuePlanning(workflow.id).pipe(
          Effect.delay("10 millis"),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : blockPlanning(workflow.id, `Workflow planning failed after ${reason}: ${errorFromCause(cause)}`).pipe(
                  Effect.asVoid,
                ),
          ),
          Effect.as("workflow planning continued"),
        ),
      })
      return true
    })

    const advanceWorkflowAfterSession = Effect.fn("Workflow.advanceWorkflowAfterSession")(function* (
      workflowID: WorkflowID,
      reason: string,
    ) {
      const workflow = yield* get(workflowID)
      if (["blocked", "cancelled", "completed", "failed"].includes(workflow.status)) return
      if (yield* queueContinuePlanning(workflow, reason)) return
      yield* schedule(workflowID).pipe(Effect.ignore)
    })

    const workflowPromptText = Effect.fn("Workflow.workflowPromptText")(function* (
      sessionID: SessionID,
      text: string,
      archive: {
        workflowID: WorkflowID
        role: WorkflowSessionRef["role"]
        milestoneID?: WorkflowMilestoneID
        attempt?: number
      },
    ) {
      const workflow = yield* get(archive.workflowID)
      const staff = yield* members(archive.workflowID)
      const items = yield* milestones(archive.workflowID)
      const member =
        staff.find((item) => item.sessionID === sessionID && item.role === archive.role) ??
        staff.find((item) => item.sessionID === sessionID)
      const milestone = archive.milestoneID ? items.find((item) => item.id === archive.milestoneID) : undefined
      return workflowEmployeeTaskPrompt(
        workflow,
        {
          sessionID,
          role: archive.role,
          milestoneID: archive.milestoneID,
          attempt: archive.attempt,
          ...(member ? { member } : {}),
          ...(milestone ? { milestone } : {}),
        },
        text,
        { members: staff, milestones: items },
      )
    })

    const workflowMemberForSession = Effect.fn("Workflow.workflowMemberForSession")(function* (
      workflowID: WorkflowID,
      sessionID: SessionID,
      role: WorkflowSessionRef["role"],
    ) {
      const row = Database.use((db) =>
        db
          .select()
          .from(WorkflowMemberTable)
          .where(
            and(
              eq(WorkflowMemberTable.workflow_id, workflowID),
              eq(WorkflowMemberTable.session_id, sessionID),
              eq(WorkflowMemberTable.role, role),
            ),
          )
          .orderBy(asc(WorkflowMemberTable.time_created))
          .all()
          .at(-1),
      )
      return row ? toMember(row) : undefined
    })

    const rememberWorkflowMemberModel = Effect.fn("Workflow.rememberWorkflowMemberModel")(function* (input: {
      workflowID: WorkflowID
      sessionID: SessionID
      role: WorkflowSessionRef["role"]
      model?: WorkflowInfo["model"] | WorkflowModelWhitelistItem
      modelWeight?: number
    }) {
      if (!input.model) return
      const now = Date.now()
      Database.use((db) =>
        db
          .update(WorkflowMemberTable)
          .set({
            model: workflowModelRef(input.model),
            model_weight: Math.trunc(workflowModelWeight(input.model.weight ?? input.modelWeight)),
            model_cache_until: workflowModelCacheUntil(input.model, now) ?? null,
            time_updated: now,
          })
          .where(
            and(
              eq(WorkflowMemberTable.workflow_id, input.workflowID),
              eq(WorkflowMemberTable.session_id, input.sessionID),
              eq(WorkflowMemberTable.role, input.role),
            ),
          )
          .run(),
      )
    })

    const runPrompt = Effect.fn("Workflow.runPrompt")(function* (
      sessionID: SessionID,
      agent: string,
      model: WorkflowInfo["model"] | undefined,
      text: string,
      archive?: {
        workflowID: WorkflowID
        role: WorkflowSessionRef["role"]
        milestoneID?: WorkflowMilestoneID
        attempt?: number
      },
      options?: { consult?: boolean; control?: boolean; expect?: WorkflowPromptExpectation; modelWeight?: number },
    ) {
      const runOnce = Effect.fn("Workflow.runPromptOnce")(function* (nextText: string) {
        const basePromptText = archive ? yield* workflowPromptText(sessionID, nextText, archive) : nextText
        const selectionWorkflow = archive ? yield* get(archive.workflowID) : undefined
        const selectionMember = selectionWorkflow
          ? yield* workflowMemberForSession(archive!.workflowID, sessionID, archive!.role)
          : undefined
        const selectionWeight =
          options?.modelWeight === undefined ? workflowModelComplexity(basePromptText) : workflowModelWeight(options.modelWeight)
        const selectedModel = selectionWorkflow
          ? selectWorkflowModelFromWhitelist({
              workflow: selectionWorkflow,
              role: archive!.role,
              prompt: basePromptText,
              modelWeight: options?.modelWeight,
              fallback: model,
              member: selectionMember,
            })
          : model
        if (selectionWorkflow) {
          yield* rememberWorkflowMemberModel({
            workflowID: archive!.workflowID,
            sessionID,
            role: archive!.role,
            model: selectedModel,
            modelWeight: selectionWeight,
          }).pipe(Effect.ignore)
        }
        const promptText = selectionWorkflow
          ? [
              basePromptText,
              "",
              ...workflowModelSelectionPrompt({
                workflow: selectionWorkflow,
                role: archive!.role,
                selected: selectedModel,
                prompt: basePromptText,
                modelWeight: options?.modelWeight,
                member: selectionMember,
              }),
            ].join("\n")
          : basePromptText
        const messageID = MessageID.ascending()
        workflowManagedMessageKeys.add(workflowMessageKey(sessionID, messageID))
        return {
          promptText,
          model: selectedModel,
          result: yield* prompt.prompt({
            sessionID,
            agent,
            model: modelRef(selectedModel),
            variant: selectedModel?.variant,
            messageID,
            parts: [textPart(promptText)],
          }),
        }
      })
      const archiveResult = Effect.fn("Workflow.archivePromptResult")(function* (input: {
        promptText: string
        model?: WorkflowInfo["model"]
        result: MessageV2.WithParts
      }) {
        if (!archive) return
        yield* archiveWorkflowSession({
          workflowID: archive.workflowID,
          sessionID,
          role: archive.role,
          prompt: input.promptText,
          milestoneID: archive.milestoneID,
          attempt: archive.attempt,
        }).pipe(Effect.ignore)
        yield* queueWorkflowArtifactRefresh(archive.workflowID, "session prompt archived").pipe(Effect.ignore)
        const output = latestText(input.result)
        const dispatchClaim = workflowDispatchClaimWithoutControl(output, archive.role)
        const dispatchMessageMisuse = workflowMessageDispatchMisuse(output, archive.role)
        const inferredControlItems = dispatchClaim ? yield* milestones(archive.workflowID) : []
        const inferredControl = dispatchClaim
          ? workflowInferredDispatchControl({
              text: output,
              role: archive.role,
              milestoneID: archive.milestoneID,
              milestoneStatus: archive.milestoneID
                ? inferredControlItems.find((item) => item.id === archive.milestoneID)?.status
                : undefined,
              milestones: inferredControlItems,
            })
          : undefined
        const inferredControlReplay = inferredControl
          ? yield* handleWorkflowToolCommand(
              {
                ...inferredControl,
                id: Bus.createID(),
                workflowID: archive.workflowID,
                sourceSessionID: sessionID,
                sourceAgent: agent,
              },
              {
                currentJobID:
                  archive.milestoneID && archive.attempt !== undefined
                    ? milestoneJobID(archive.workflowID, archive.milestoneID, archive.attempt)
                    : undefined,
              },
            ).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
          : undefined
        const inferredControlApplied = inferredControlReplay?.result?.applied === true
        if (dispatchClaim) {
          yield* appendWorkflowMessageRuntimeJournal(archive.workflowID, {
            action: "reject",
            kind: "consultation",
            sourceSessionID: sessionID,
            sourceRole: archive.role,
            milestoneID: archive.milestoneID,
            attempt: archive.attempt,
            status: "rejected",
            request: compactMarkdown(output, 700),
            response:
              "Workflow session claimed milestone dispatch without confirmed workflow control. workflow-message is consultation/notification only; use workflow update_xml, resume, plan_complete, force_complete, or scheduler-created milestone sessions for dispatch.",
          }).pipe(Effect.ignore)
        }
        yield* applyWorkflowUpdateFromOutput({
          workflowID: archive.workflowID,
          role: archive.role,
          output,
        }).pipe(Effect.ignore)
        const controlCommand = parseWorkflowControlCommand(output)
        const hasWorkflowControl = controlCommand !== undefined || implicitWorkflowResume(output)
        if (options?.control !== false && hasWorkflowControl) {
          yield* applyWorkflowControl(archive.workflowID, output, "workflow managed prompt output", {
            exceptJobID:
              archive.milestoneID && archive.attempt !== undefined
                ? milestoneJobID(archive.workflowID, archive.milestoneID, archive.attempt)
                : undefined,
            sourceSessionID: sessionID,
            sourceAgent: agent,
          }).pipe(Effect.ignore)
        }
        const followup =
          options?.consult !== false && !dispatchMessageMisuse && (!dispatchClaim || inferredControlApplied)
            ? yield* resolveConsultRequests(
                archive.workflowID,
                sessionID,
                agent,
                input.model,
                archive.role,
                archive.milestoneID,
                archive.attempt,
                output,
              )
            : undefined
        return { dispatchClaim: dispatchClaim && !inferredControlApplied, followup }
      })
      let current = yield* runOnce(text)
      let archived = yield* archiveResult(current)
      if (archived?.followup) current = { ...current, result: archived.followup }
      if (archive && archived?.dispatchClaim) {
        current = yield* runOnce(
          workflowDispatchCorrectionPrompt({
            workflow: yield* get(archive.workflowID),
            role: archive.role,
            milestoneID: archive.milestoneID,
            previous: latestText(current.result),
          }),
        )
        archived = yield* archiveResult(current)
        if (archived?.followup) current = { ...current, result: archived.followup }
        if (archived?.dispatchClaim) {
          yield* blockPlanning(
            archive.workflowID,
            "Workflow session claimed milestone dispatch without confirmed workflow control. Workflow messages and natural-language assignments are consultation/notification only; use workflow update_xml, resume, plan_complete, force_complete, or scheduler-created milestone sessions.",
          )
          return current.result
        }
      }
      const expectation = options?.expect
      if (!expectation) return current.result
      const maxAttempts = Math.max(1, Math.trunc(expectation.maxAttempts ?? 6))
      for (const attempt of Array.from({ length: maxAttempts - 1 }, (_, index) => index + 2)) {
        if (expectation.matches(latestText(current.result))) return current.result
        if (archive && (yield* workflowIsBlocked(archive.workflowID))) return current.result
        current = yield* runOnce(workflowExpectedOutputPrompt(expectation, attempt, maxAttempts))
        archived = yield* archiveResult(current)
        if (archived?.followup) current = { ...current, result: archived.followup }
        if (archive && archived?.dispatchClaim) {
          current = yield* runOnce(
            workflowDispatchCorrectionPrompt({
              workflow: yield* get(archive.workflowID),
              role: archive.role,
              milestoneID: archive.milestoneID,
              previous: latestText(current.result),
            }),
          )
          archived = yield* archiveResult(current)
          if (archived?.followup) current = { ...current, result: archived.followup }
          if (archived?.dispatchClaim) {
            yield* blockPlanning(
              archive.workflowID,
              "Workflow session claimed milestone dispatch without confirmed workflow control. Workflow messages and natural-language assignments are consultation/notification only; use workflow update_xml, resume, plan_complete, force_complete, or scheduler-created milestone sessions.",
            )
            return current.result
          }
        }
      }
      return current.result
    })

    const workflowSessionContext = Effect.fn("Workflow.workflowSessionContext")(function* (sessionID: SessionID) {
      const member = Database.use((db) =>
        db
          .select()
          .from(WorkflowMemberTable)
          .where(eq(WorkflowMemberTable.session_id, sessionID))
          .orderBy(asc(WorkflowMemberTable.time_created))
          .all()
          .at(-1),
      )
      if (member) {
        const workflow = yield* get(member.workflow_id)
        if (["cancelled", "completed"].includes(workflow.status)) return undefined
        const items = yield* milestones(workflow.id)
        return {
          workflow,
          role: member.role,
          milestoneID: workflowSessionMilestoneID(items, sessionID),
          attempt: workflowSessionAttempt(items, sessionID),
        }
      }
      const direct = Database.use((db) =>
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
          .orderBy(asc(WorkflowTable.time_created))
          .all()
          .filter((row) => !["cancelled", "completed"].includes(row.status))
          .at(-1),
      )
      if (!direct) return undefined
      const workflow = toInfo(direct)
      const items = yield* milestones(workflow.id)
      return {
        workflow,
        role: workflowSessionRole(workflow, items, sessionID),
        milestoneID: workflowSessionMilestoneID(items, sessionID),
        attempt: workflowSessionAttempt(items, sessionID),
      }
    })

    const workflowMessageTextWithRetry = Effect.fn("Workflow.workflowMessageTextWithRetry")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      for (const delay of [0, 25, 75]) {
        if (delay > 0) yield* Effect.sleep(`${delay} millis`)
        const found = (yield* session.messages({ sessionID: input.sessionID, limit: 20 })).find(
          (item) => item.info.id === input.messageID,
        )
        const text = found ? messageText(found) : ""
        if (text) return text
      }
      return ""
    })

    const observeWorkflowMessage = Effect.fn("Workflow.observeWorkflowMessage")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
      agent: string
      model: NonNullable<WorkflowInfo["model"]>
    }) {
      const key = workflowMessageKey(input.sessionID, input.messageID)
      if (workflowManagedMessageKeys.has(key) || observedWorkflowMessageKeys.has(key)) return
      observedWorkflowMessageKeys.add(key)
      const message = (yield* session.messages({ sessionID: input.sessionID, limit: 20 })).find(
        (item) => item.info.id === input.messageID,
      )
      if (!message) {
        observedWorkflowMessageKeys.delete(key)
        return
      }
      if (
        message.info.role === "assistant" &&
        workflowManagedMessageKeys.has(workflowMessageKey(input.sessionID, message.info.parentID))
      ) {
        observedWorkflowMessageKeys.delete(key)
        return
      }
      const text = yield* workflowMessageTextWithRetry({
        sessionID: input.sessionID,
        messageID: input.messageID,
      })
      if (!text.trim()) {
        observedWorkflowMessageKeys.delete(key)
        return
      }
      const context = yield* workflowSessionContext(input.sessionID)
      if (!context) {
        observedWorkflowMessageKeys.delete(key)
        return
      }
      yield* archiveWorkflowSession({
        workflowID: context.workflow.id,
        sessionID: input.sessionID,
        role: context.role,
        milestoneID: context.milestoneID,
        attempt: context.attempt,
      }).pipe(Effect.ignore)
      yield* writeReferenceIndex(context.workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(context.workflow.id).pipe(Effect.ignore)
      yield* writeOrganization(context.workflow.id).pipe(Effect.ignore)
      const appliedWorkflowUpdate = yield* applyWorkflowUpdateFromOutput({
        workflowID: context.workflow.id,
        role: context.role,
        output: text,
      })
      if (appliedWorkflowUpdate && context.role === "main_pm") {
        yield* queueContinuePlanning(yield* get(context.workflow.id), "main PM workflow update").pipe(Effect.ignore)
      }
      const controlCommand = parseWorkflowControlCommand(text)
      if (controlCommand || implicitWorkflowResume(text)) {
        yield* applyWorkflowControl(context.workflow.id, text, "workflow session control output", {
          sourceSessionID: input.sessionID,
          sourceAgent: input.agent,
        }).pipe(Effect.ignore)
        if (controlCommand || implicitWorkflowResume(text)) return
      }
      const contextItems = yield* milestones(context.workflow.id)
      const active = context.milestoneID ? contextItems.find((item) => item.id === context.milestoneID) : undefined
      const expectation = workflowSessionExpectation({
        role: context.role,
        milestoneID: context.milestoneID,
        milestoneStatus: active?.status,
        workflowStatus: context.workflow.status,
      })
      const consults = parseConsultRequests(text)
      const dispatchClaimWithoutControl = workflowDispatchClaimWithoutControl(text, context.role)
      const followup =
        !dispatchClaimWithoutControl && expectation && !appliedWorkflowUpdate && consults.length === 0 && !expectation.matches(text)
          ? latestText(
              yield* runPrompt(
                input.sessionID,
                input.agent,
                input.model,
                workflowExpectedOutputPrompt(expectation, 1, Math.max(1, Math.trunc(expectation.maxAttempts ?? 6))),
                {
                  workflowID: context.workflow.id,
                  role: context.role,
                  milestoneID: context.milestoneID,
                  attempt: context.attempt,
                },
                { consult: false, expect: expectation },
              ),
            )
          : text
      const followupWorkflowUpdate = parseWorkflowUpdateXml(followup) !== undefined
      if (followupWorkflowUpdate && context.role === "main_pm") {
        yield* queueContinuePlanning(yield* get(context.workflow.id), "main PM hook workflow update").pipe(Effect.ignore)
      }
      const followupConsults = parseConsultRequests(followup)
      if (dispatchClaimWithoutControl || workflowDispatchClaimWithoutControl(followup, context.role)) {
        if (workflowMessageDispatchMisuse(dispatchClaimWithoutControl ? text : followup, context.role)) {
          yield* rejectWorkflowMessageDispatchMisuse({
            workflowID: context.workflow.id,
            sourceSessionID: input.sessionID,
            sourceRole: context.role,
            sourceMilestoneID: context.milestoneID,
            sourceAttempt: context.attempt,
            text: dispatchClaimWithoutControl ? text : followup,
          })
        }
        const correction = latestText(
          yield* runPrompt(
            input.sessionID,
            input.agent,
            input.model,
            workflowDispatchCorrectionPrompt({
              workflow: context.workflow,
              role: context.role,
              milestoneID: context.milestoneID,
              previous: dispatchClaimWithoutControl ? text : followup,
            }),
            {
              workflowID: context.workflow.id,
              role: context.role,
              milestoneID: context.milestoneID,
              attempt: context.attempt,
            },
            { consult: false, expect: workflowDispatchCorrectionExpectation(context.role) },
          ),
        )
        const correctionWorkflowUpdate = yield* applyWorkflowUpdateFromOutput({
          workflowID: context.workflow.id,
          role: context.role,
          output: correction,
        })
        if (correctionWorkflowUpdate && context.role === "main_pm") {
          yield* queueContinuePlanning(yield* get(context.workflow.id), "main PM dispatch correction workflow update").pipe(Effect.ignore)
        }
        const correctionControlCommand = parseWorkflowControlCommand(correction)
        if (correctionControlCommand || implicitWorkflowResume(correction)) {
          yield* applyWorkflowControl(context.workflow.id, correction, "workflow-message dispatch correction", {
            sourceSessionID: input.sessionID,
            sourceAgent: input.agent,
          }).pipe(Effect.ignore)
          if (correctionControlCommand || implicitWorkflowResume(correction)) return
        }
        const correctionConsults = parseConsultRequests(correction)
        if (workflowDispatchClaimWithoutControl(correction, context.role)) {
          yield* blockPlanning(
            context.workflow.id,
            "Workflow session claimed milestone dispatch without confirmed workflow control. Workflow messages and natural-language assignments are consultation/notification only; use workflow update_xml, resume, plan_complete, or the scheduler-created milestone sessions.",
          )
          return
        }
        if (correctionConsults.length > 0) {
          yield* resolveConsultRequests(
            context.workflow.id,
            input.sessionID,
            input.agent,
            input.model,
            context.role,
            context.milestoneID,
            context.attempt,
            correction,
          ).pipe(Effect.ignore)
          yield* advanceWorkflowAfterSession(context.workflow.id, "workflow dispatch correction consultation completed")
          return
        }
        if (correctionWorkflowUpdate) {
          yield* advanceWorkflowAfterSession(context.workflow.id, "workflow dispatch correction completed")
          return
        }
        yield* blockPlanning(
          context.workflow.id,
          "Workflow session stopped after a dispatch claim without producing a real workflow update/control/consultation output.",
        )
        return
      }
      if (followupConsults.length === 0) {
        if (appliedWorkflowUpdate || followupWorkflowUpdate || !expectation || expectation.matches(followup)) {
          yield* advanceWorkflowAfterSession(context.workflow.id, "workflow session completed")
        }
        return
      }
      yield* resolveConsultRequests(
        context.workflow.id,
        input.sessionID,
        input.agent,
        input.model,
        context.role,
        context.milestoneID,
        context.attempt,
        followup,
      ).pipe(Effect.ignore)
      yield* advanceWorkflowAfterSession(context.workflow.id, "workflow consultation completed")
    })

    const activeRequesterWorkflow = Effect.fn("Workflow.activeRequesterWorkflow")(function* (sessionID: SessionID) {
      const row = Database.use((db) =>
        db
          .select()
          .from(WorkflowTable)
          .where(eq(WorkflowTable.root_session_id, sessionID))
          .orderBy(asc(WorkflowTable.time_created))
          .all()
          .filter((item) => !["cancelled", "completed", "failed"].includes(item.status))
          .at(-1),
      )
      return row ? toInfo(row) : undefined
    })

    const requesterMessageText = Effect.fn("Workflow.requesterMessageText")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      const message = (yield* session.messages({ sessionID: input.sessionID, limit: 20 })).find(
        (item) => item.info.id === input.messageID,
      )
      if (!message) return ""
      return compactMarkdown(messageText(message), 2400)
    })

    const requesterMessageTextWithRetry = Effect.fn("Workflow.requesterMessageTextWithRetry")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      for (const delay of [0, 25, 75]) {
        if (delay > 0) yield* Effect.sleep(`${delay} millis`)
        const text = yield* requesterMessageText(input)
        if (text) return text
      }
      return ""
    })

    const observeRequesterMessage = Effect.fn("Workflow.observeRequesterMessage")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      const key = workflowMessageKey(input.sessionID, input.messageID)
      if (workflowManagedMessageKeys.has(key) || observedRequesterMessageKeys.has(key)) return
      observedRequesterMessageKeys.add(key)
      const workflow = yield* activeRequesterWorkflow(input.sessionID)
      if (!workflow) {
        observedRequesterMessageKeys.delete(key)
        return
      }
      const text = yield* requesterMessageTextWithRetry(input)
      if (!text) {
        observedRequesterMessageKeys.delete(key)
        return
      }
      yield* answerPendingRequesterConsultations(workflow, input.sessionID, text).pipe(Effect.ignore)
      if (yield* applyRequesterAcceptance(yield* get(workflow.id), text)) return
      if (workflowMessageDispatchMisuse(text, "requester")) {
        yield* archiveWorkflowSession({ workflowID: workflow.id, sessionID: input.sessionID, role: "requester" }).pipe(
          Effect.ignore,
        )
        yield* rejectWorkflowMessageDispatchMisuse({
          workflowID: workflow.id,
          sourceSessionID: input.sessionID,
          sourceRole: "requester",
          text,
          response:
            "requester workflow-message assignment is not direct dispatch; converting this requester direction into workflow control so milestone sessions are created by the scheduler.",
        })
        const override = yield* applyRequesterDirectExecutionOverride({
          workflow,
          sourceSessionID: input.sessionID,
          message: text,
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.succeed({
              applied: false,
              message: `Requester workflow-message dispatch override failed: ${errorFromCause(cause)}`,
            }),
          ),
        )
        yield* notifyMainPM(
          workflow.id,
          `Requester workflow-message dispatch override ${override.applied ? "applied" : "failed"}: ${override.message}`,
        ).pipe(Effect.ignore)
        yield* publishUpdated(workflow.id).pipe(Effect.ignore)
        return
      }
      if (parseConsultRequests(text).length > 0) {
        yield* archiveWorkflowSession({ workflowID: workflow.id, sessionID: input.sessionID, role: "requester" }).pipe(
          Effect.ignore,
        )
        yield* resolveConsultRequests(
          workflow.id,
          input.sessionID,
          "workflow-requester",
          workflow.model,
          "requester",
          undefined,
          undefined,
          text,
        ).pipe(Effect.ignore)
        yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
        yield* writeArchiveIndex(workflow.id).pipe(Effect.ignore)
        yield* writeOrganization(workflow.id).pipe(Effect.ignore)
        yield* publishUpdated(workflow.id).pipe(Effect.ignore)
        return
      }
      const target = workflow.pmSessionID
        ? { sessionID: workflow.pmSessionID }
        : yield* ensureCompanyMember({
            workflow,
            role: "main_pm",
            specialty: "strategy",
            title: workflowSessionTitle("Main PM", workflow.title),
            prompt: input.message,
          })
      if (!target?.sessionID) return
      const id = `intervention_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
      const now = Date.now()
      const interventionPath = workflowArtifactPath(workflow, workflowInterventionPath(id))
      Database.use((db) =>
        db
          .insert(WorkflowInterventionTable)
          .values({
            workflow_id: workflow.id,
            id,
            from_session_id: input.sessionID,
            target_session_id: target.sessionID,
            target_role: "main_pm" as const,
            timing: "temporary-interrupt" as const,
            message: text,
            path: interventionPath,
            status: "queued" as const,
            time_created: now,
            time_updated: now,
          })
          .run(),
      )
      yield* writeInterventionArtifacts(workflow.id).pipe(Effect.ignore)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* archiveWorkflowSession({ workflowID: workflow.id, sessionID: input.sessionID, role: "requester" }).pipe(Effect.ignore)
      const targetInfo = yield* session
        .get(target.sessionID)
        .pipe(Effect.mapError((error) => new Error({ message: error.message })))
      const result = yield* runPrompt(
        target.sessionID,
        targetInfo.agent ?? "workflow-main-pm",
        workflow.model,
        [
          "Requester sent a new strategic direction in the requester session.",
          "",
          workflowReferencePrompt(workflow),
          "",
          `Intervention document: ${interventionPath}`,
          "",
          "Requester message:",
          text,
          "",
          "Incorporate this direction into workflow supervision. If it changes scope or sequencing, update workflow docs, ask clarification, or prepare the team to resume with revised plans.",
          "End with exactly one workflow control XML block:",
          '<opencode-workflow-control action="resume">ready to continue with this direction</opencode-workflow-control>',
          "or",
          '<opencode-workflow-control action="block">needs requester clarification or plan rewrite before continuing</opencode-workflow-control>',
        ].join("\n"),
        {
          workflowID: workflow.id,
          role: "main_pm",
        },
        { consult: false, control: false },
      )
      const resultText = latestText(result)
      if (parseConsultRequests(resultText).length > 0) {
        yield* resolveConsultRequests(
          workflow.id,
          target.sessionID,
          targetInfo.agent ?? "workflow-main-pm",
          workflow.model,
          "main_pm",
          undefined,
          undefined,
          resultText,
        ).pipe(Effect.ignore)
      }
      Database.use((db) =>
        db
          .update(WorkflowInterventionTable)
          .set({
            response: resultText,
            status: "delivered" as const,
            time_updated: Date.now(),
          })
          .where(and(eq(WorkflowInterventionTable.workflow_id, workflow.id), eq(WorkflowInterventionTable.id, id)))
          .run(),
      )
      yield* writeInterventionArtifacts(workflow.id).pipe(Effect.ignore)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflow.id).pipe(Effect.ignore)
      yield* writeOrganization(workflow.id).pipe(Effect.ignore)
      yield* applyWorkflowControl(workflow.id, resultText, "requester strategic direction", {
        sourceSessionID: target.sessionID,
        sourceAgent: targetInfo.agent ?? "workflow-main-pm",
      }).pipe(Effect.ignore)
      yield* publishUpdated(workflow.id).pipe(Effect.ignore)
    })

    const runMainPMNotification = Effect.fn("Workflow.runMainPMNotification")(function* (
      workflowID: WorkflowID,
      message: string,
      source?: { milestoneID?: WorkflowMilestoneID; jobID?: string; notificationJobID?: string },
    ) {
      const workflow = yield* get(workflowID)
      if (!workflow.pmSessionID) return
      const result = yield* runPrompt(
        workflow.pmSessionID,
        "workflow-main-pm",
        workflow.model,
        [
          "Workflow progress update for main PM supervision.",
          "",
          workflowReferencePrompt(workflow),
          "",
          message,
          "",
          `Update ${workflowMainPlanPath()} or workflow.xml only if this changes scope, sequencing, risks, or acceptance. Do not rewrite progress.md, organization.md, or index.md directly; the runtime regenerates them. Do not do implementation work from the main PM session.`,
        ].join("\n"),
        {
          workflowID,
          role: "main_pm",
        },
        { consult: false, control: false },
      )
      const resultText = latestText(result)
      if (parseConsultRequests(resultText).length > 0) {
        yield* resolveConsultRequests(
          workflowID,
          workflow.pmSessionID,
          "workflow-main-pm",
          workflow.model,
          "main_pm",
          undefined,
          undefined,
          resultText,
        ).pipe(Effect.ignore)
      }
      yield* writeProgress(workflowID).pipe(Effect.ignore)
      yield* writeMainPMSupervisionNote({
        workflowID,
        message,
        output: resultText,
      }).pipe(Effect.ignore)
      const next = yield* applyWorkflowControl(workflowID, resultText, "main PM supervision", {
        exceptJobID: [source?.jobID, source?.notificationJobID].filter((item): item is string => !!item),
        sourceSessionID: workflow.pmSessionID,
        sourceAgent: "workflow-main-pm",
      })
      if (next.status === "blocked") {
        if (source?.milestoneID) {
          const item = (yield* milestones(workflowID)).find((milestone) => milestone.id === source.milestoneID)
          if (item && interruptedMilestone(item.status)) {
            yield* updateMilestone(workflowID, source.milestoneID, { status: "blocked" }).pipe(Effect.ignore)
          }
        }
        return
      }
      if (!["pending", "running", "planning", "dispatching", "executing", "reviewing"].includes(next.status)) return
      yield* refreshWorkflowXml(workflowID, next.status).pipe(
        Effect.catchCause((cause) =>
          blockWorkflow(
            next,
            `Main PM produced invalid workflow XML during supervision: ${errorFromCause(cause)}`,
          ).pipe(Effect.as(false)),
        ),
      )
    })

    const notifyMainPM = Effect.fn("Workflow.notifyMainPM")(function* (
      workflowID: WorkflowID,
      message: string,
      source?: { milestoneID?: WorkflowMilestoneID; jobID?: string },
    ) {
      const workflow = yield* get(workflowID)
      if (!workflow.pmSessionID) return
      const jobID = `${workflowID}:main-pm-note:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}`
      yield* background.start({
        id: jobID,
        type: "workflow.notification",
        title: `${workflow.title} main PM update`,
        metadata: { workflowID, reason: "main_pm_notification", milestoneID: source?.milestoneID },
        run: runMainPMNotification(workflowID, message, { ...source, notificationJobID: jobID }).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : appendWorkflowEventRuntimeJournal(workflowID, {
                  action: "main_pm_notification.failed",
                  message: errorFromCause(cause),
                  milestoneID: source?.milestoneID,
                }).pipe(Effect.ignore),
          ),
          Effect.as("main PM notification completed"),
        ),
      })
    })

    const observeWorkflowUserMessage = Effect.fn("Workflow.observeWorkflowUserMessage")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      const key = workflowMessageKey(input.sessionID, input.messageID)
      if (workflowManagedMessageKeys.has(key) || observedWorkflowMessageKeys.has(key)) return
      observedWorkflowMessageKeys.add(key)
      const context = yield* workflowSessionContext(input.sessionID)
      if (!context || context.role === "requester") {
        observedWorkflowMessageKeys.delete(key)
        return
      }
      const text = yield* requesterMessageTextWithRetry(input)
      if (!text) {
        observedWorkflowMessageKeys.delete(key)
        return
      }
      if (/^\/workflow-continue(?:\s|$)/i.test(text.trim())) {
        observedWorkflowMessageKeys.delete(key)
        return
      }
      const id = `intervention_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
      const interventionPath = workflowArtifactPath(context.workflow, workflowInterventionPath(id))
      Database.use((db) =>
        db
          .insert(WorkflowInterventionTable)
          .values({
            workflow_id: context.workflow.id,
            id,
            from_session_id: context.workflow.rootSessionID,
            target_session_id: input.sessionID,
            target_role: context.role,
            timing: "temporary-interrupt" as const,
            message: text,
            path: interventionPath,
            status: "delivered" as const,
            time_created: Date.now(),
            time_updated: Date.now(),
          })
          .run(),
      )
      yield* archiveWorkflowSession({
        workflowID: context.workflow.id,
        sessionID: input.sessionID,
        role: context.role,
        milestoneID: context.milestoneID,
        attempt: context.attempt,
      }).pipe(Effect.ignore)
      yield* writeInterventionArtifacts(context.workflow.id).pipe(Effect.ignore)
      yield* writeReferenceIndex(context.workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(context.workflow.id).pipe(Effect.ignore)
      yield* writeOrganization(context.workflow.id).pipe(Effect.ignore)
      yield* notifyMainPM(
        context.workflow.id,
        [
          `Manual direction was added directly to ${roleSessionTitle(context.role)} session ${input.sessionID}.`,
          ...(context.milestoneID ? [`Milestone: ${context.milestoneID}`] : []),
          `Intervention document: ${interventionPath}`,
          `Message: ${compactMarkdown(text, 320).replace(/\n/g, " ")}`,
        ].join("\n"),
        { milestoneID: context.milestoneID },
      ).pipe(Effect.ignore)
      yield* publishUpdated(context.workflow.id).pipe(Effect.ignore)
    })

    const workflowIsBlocked = Effect.fn("Workflow.workflowIsBlocked")(function* (workflowID: WorkflowID) {
      return (yield* get(workflowID)).status === "blocked"
    })

    const queueCompanyStandupRequests = Effect.fn("Workflow.queueCompanyStandupRequests")(function* (input: {
      workflow: WorkflowInfo
      standupPath: string
      reason: string
    }) {
      const mainPMSessionID =
        input.workflow.pmSessionID ?? (yield* members(input.workflow.id)).find((member) => member.role === "main_pm")?.sessionID
      if (!mainPMSessionID) return 0
      const activeMembers = (yield* members(input.workflow.id)).filter(
        (member) => member.status === "active" && member.role !== "main_pm",
      )
      if (activeMembers.length === 0) return 0
      const now = Date.now()
      const requests = activeMembers.map((member) => {
        const id = `standup_request_${now.toString(36)}_${staffSlug(member.id)}`
        const interventionPath = workflowArtifactPath(input.workflow, workflowInterventionPath(id))
        return {
          id,
          member,
          interventionPath,
          message: [
            "Workflow company standup request.",
            "",
            `Reason: ${input.reason}`,
            `Standup document: ${input.standupPath}`,
            "",
            "Update your workflow member status with workflow action=status_update, including availability, currentFocus, blockers, and progressNote.",
            "Then acknowledge this standup request with workflow_message action=ack for this message id.",
          ].join("\n"),
        }
      })
      yield* Effect.all(
        requests.map((request) =>
          Effect.sync(() =>
            Database.use((db) =>
              db
                .insert(WorkflowInterventionTable)
                .values({
                  workflow_id: input.workflow.id,
                  id: request.id,
                  from_session_id: mainPMSessionID,
                  target_session_id: request.member.sessionID,
                  target_role: request.member.role,
                  timing: "after-task" as const,
                  message: request.message,
                  path: request.interventionPath,
                  status: "queued" as const,
                  time_created: now,
                  time_updated: now,
                })
                .run(),
            ),
          ),
        ),
        { discard: true, concurrency: 1 },
      )
      yield* Effect.all(
        requests.map((request) =>
          upsertWorkflowMessage({
            workflowID: input.workflow.id,
            id: request.id,
            kind: "standup",
            fromSessionID: mainPMSessionID,
            fromRole: "main_pm",
            toSessionID: request.member.sessionID,
            toRole: request.member.role,
            timing: "after-task",
            body: request.message,
            status: "queued",
            timeCreated: now,
            timeUpdated: now,
          }),
        ),
        { discard: true },
      )
      yield* Effect.all(
        requests.map((request) =>
          appendWorkflowMessageRuntimeJournal(input.workflow.id, {
            action: "send",
            kind: "standup",
            messageID: request.id,
            sourceSessionID: mainPMSessionID,
            sourceRole: "main_pm",
            targetSessionID: request.member.sessionID,
            targetRole: request.member.role,
            status: "queued",
            request: request.message,
          }),
        ),
        { discard: true },
      )
      yield* writeInterventionArtifacts(input.workflow.id).pipe(Effect.ignore)
      yield* writeReferenceIndex(input.workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(input.workflow.id).pipe(Effect.ignore)
      return requests.length
    })

    const runCompanyStandup = Effect.fn("Workflow.runCompanyStandup")(function* (
      workflowID: WorkflowID,
      reason: string,
      jobID?: string,
    ) {
      const workflow = yield* get(workflowID)
      if (!workflow.pmSessionID) return
      const progress = progressMarkdown({
        workflow,
        milestones: yield* milestones(workflowID),
        members: yield* members(workflowID),
        interventions: yield* interventions(workflowID),
      })
      const id = `standup_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
      const standupPath = workflowArtifactPath(workflow, workflowStandupPath(id))
      const result = yield* runPrompt(
        workflow.pmSessionID,
        "workflow-main-pm",
        workflow.model,
        [
          "Run a short workflow company standup.",
          "",
          `Workflow title: ${workflow.title}`,
          `Workflow request: ${compactMarkdown(workflow.request, 1200)}`,
          "",
          workflowReferencePrompt(workflow),
          "",
          `Reason: ${reason}`,
          "",
          "Progress snapshot:",
          progress,
          "",
          "Summarize current risk, active owners, blocked or waiting work, and whether any employee should consult another employee.",
          "If another employee should be asked now, emit workflow communication XML with the right timing. Do not implement from the main PM session.",
        ].join("\n"),
        {
          workflowID,
          role: "main_pm",
        },
        { control: false },
      )
      const output = latestText(result)
      yield* writeNote(standupPath, standupMarkdown({ workflow, reason, progress, output }))
      yield* appendStandupIndex(workflow, standupPath, reason)
      yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
      yield* applyWorkflowControl(workflowID, output, "company standup", {
        exceptJobID: jobID,
        sourceSessionID: workflow.pmSessionID,
        sourceAgent: "workflow-main-pm",
      }).pipe(Effect.ignore)
      yield* queueCompanyStandupRequests({ workflow: yield* get(workflowID), standupPath, reason }).pipe(Effect.ignore)
      yield* publishUpdated(workflowID).pipe(Effect.ignore)
    })

    const triggerCompanyStandup = Effect.fn("Workflow.triggerCompanyStandup")(function* (workflowID: WorkflowID, reason: string) {
      if (
        (yield* background.list()).some(
          (job) => job.type === "workflow.standup" && job.metadata?.workflowID === workflowID && job.status === "running",
        )
      ) {
        return
      }
      const workflow = yield* get(workflowID)
      const jobID = `${workflowID}:standup:${Date.now().toString(36)}`
      yield* background.start({
        id: jobID,
        type: "workflow.standup",
        title: `${workflow.title} company standup`,
        metadata: { workflowID },
        run: runCompanyStandup(workflowID, reason, jobID).pipe(
          Effect.catchCause((cause) =>
            writeNote(
              workflowArtifactPath(workflow, workflowStandupPath(`failed_${Date.now().toString(36)}`)),
              `# Failed Company Standup\n\nReason: ${reason}\n\n${errorFromCause(cause)}\n`,
            ).pipe(Effect.asVoid),
          ),
          Effect.as("workflow company standup completed"),
        ),
      })
    })

    const blockWorkflow = Effect.fn("Workflow.blockWorkflow")(function* (
      workflow: WorkflowInfo,
      message: string,
      options?: { kind?: "product" | "runtime" },
    ) {
      const info = yield* setStatus(workflow.id, "blocked", { error: message })
      const pmSessionID =
        info.pmSessionID ?? workflow.pmSessionID ?? (yield* members(workflow.id)).find((member) => member.role === "main_pm")?.sessionID
      if (pmSessionID) {
        yield* recordMainPMSystemReport({ ...info, pmSessionID }, message).pipe(Effect.ignore)
        yield* runPrompt(
          pmSessionID,
          "workflow-main-pm",
          workflow.model,
          options?.kind === "runtime"
            ? [
                "The workflow runtime detected a scheduling problem.",
                "",
                `Workflow: ${workflow.id}`,
                `Reason: ${message}`,
                "",
                "This is a workflow runner or dispatch issue, not product clarification. Do not rewrite product scope or workflow XML unless the graph is actually wrong.",
                "Inspect the affected session and workflow artifacts, then use workflow resume/continue after the runner state is healthy.",
              ].join("\n")
            : [
                "The workflow is blocked and needs product clarification.",
                "",
                `Workflow: ${workflow.id}`,
                `Reason: ${message}`,
                "",
                "Review the original request and provide clarification or revised workflow XML if needed.",
              ].join("\n"),
          {
            workflowID: workflow.id,
            role: "main_pm",
          },
          { consult: false },
        ).pipe(Effect.ignore)
      }
      yield* archiveWorkflowSessions(workflow.id).pipe(Effect.ignore)
      return info
    })

    const blockPlanning = Effect.fn("Workflow.blockPlanning")(function* (workflowID: WorkflowID, message: string) {
      const workflow = yield* get(workflowID)
      return yield* blockWorkflow(workflow, message)
    })

    const writeNote = Effect.fn("Workflow.writeNote")(function* (file: string, text: string) {
      const ctx = yield* InstanceState.context
      yield* Effect.promise(() => writeFileEnsured(path.join(ctx.directory, file), text))
    })

    const archiveWorkflowSession = Effect.fn("Workflow.archiveWorkflowSession")(function* (input: {
      workflowID: WorkflowID
      sessionID: SessionID
      role?: WorkflowSessionRef["role"]
      prompt?: string
      milestoneID?: WorkflowMilestoneID
      attempt?: number
    }) {
      const workflow = yield* get(input.workflowID)
      const ctx = yield* InstanceState.context
      const info = yield* session.get(input.sessionID).pipe(Effect.mapError((error) => new Error({ message: error.message })))
      const role = input.role ?? workflowSessionRole(workflow, yield* milestones(workflow.id), input.sessionID)
      const messages = yield* session.messages({ sessionID: input.sessionID })
      yield* archiveWorkflowSessionMessages({
        workflow,
        session: info,
        messages,
        role,
        prompt: input.prompt,
        milestoneID: input.milestoneID,
        attempt: input.attempt,
        file: path.join(ctx.directory, workflowArtifactPath(workflow, workflowSessionArchivePath(input.sessionID))),
      })
      yield* writeNote(
        workflowArtifactPath(workflow, workflowSessionSummaryPath(input.sessionID)),
        archiveSessionSummaryMarkdown({
          workflow,
          session: info,
          role,
          milestoneID: input.milestoneID,
          attempt: input.attempt,
          messages,
        }),
      )
      yield* writeWorkflowState(input.workflowID, workflow).pipe(Effect.ignore)
    })

    const archiveWorkflowSessions = Effect.fn("Workflow.archiveWorkflowSessions")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      const items = yield* milestones(workflowID)
      const staff = yield* members(workflowID)
      const refs: WorkflowArchiveSessionRef[] = [
        ...(workflow.rootSessionID ? [{ role: "requester" as const, sessionID: workflow.rootSessionID }] : []),
        ...(workflow.pmSessionID ? [{ role: "main_pm" as const, sessionID: workflow.pmSessionID }] : []),
        ...(workflow.testerSessionID ? [{ role: "tester" as const, sessionID: workflow.testerSessionID }] : []),
        ...staff.map((member) => ({ role: member.role, sessionID: member.sessionID })),
        ...items.flatMap((milestone) =>
          milestone.session.map((ref) => ({
            ...ref,
            milestoneID: ref.milestoneID ?? milestone.id,
          })),
        ),
      ]
      const uniqueRefs = Array.from(
        refs
          .reduce((result, ref) => {
            const existing = result.get(ref.sessionID)
            result.set(ref.sessionID, existing?.milestoneID ? existing : ref)
            return result
          }, new Map<SessionID, WorkflowArchiveSessionRef>())
          .values(),
      )
      yield* Effect.all(
        uniqueRefs.map((ref) =>
          archiveWorkflowSession({
            workflowID,
            sessionID: ref.sessionID,
            role: ref.role,
            milestoneID: ref.milestoneID,
            attempt: ref.attempt,
          }).pipe(Effect.ignore),
        ),
        { concurrency: 2, discard: true },
      )
      yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
      yield* writeOrganization(workflowID).pipe(Effect.ignore)
    })

    const writePrecreatedPlans = Effect.fn("Workflow.writePrecreatedPlans")(function* (
      workflow: WorkflowInfo,
      items: WorkflowMilestoneInfo[],
    ) {
      const ctx = yield* InstanceState.context
      yield* Effect.promise(() =>
        Promise.all(
          items.map((milestone) =>
            writeFileEnsuredIfMissing(
              path.join(ctx.directory, workflowStoredPath(workflow, milestone.planPath, milestone.id, "plan.md")),
              defaultMilestonePlan(workflow, milestone),
            ),
          ),
        ),
      )
    })

    const applyWorkflowUpdateFromOutput = Effect.fn("Workflow.applyWorkflowUpdateFromOutput")(function* (input: {
      workflowID: WorkflowID
      role: WorkflowSessionRef["role"]
      output: string
    }) {
      const xml = parseWorkflowUpdateXml(input.output)
      if (!xml) return false
      const workflow = yield* get(input.workflowID)
      if (input.role !== "main_pm" && input.role !== "department_pm") return false
      const definition = yield* Effect.try({
        try: () => parseXmlDefinition(xml, workflow),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      }).pipe(
        Effect.catchCause((cause) =>
          setStatus(
            input.workflowID,
            "blocked",
            { error: `${roleSessionTitle(input.role)} produced invalid workflow update XML: ${errorFromCause(cause)}` },
          ).pipe(Effect.as(undefined)),
        ),
      )
      if (!definition) return false
      yield* writeNote(workflowArtifactPath(workflow, "workflow.xml"), xml)
      yield* saveDefinition(input.workflowID, xml, definition, workflow.status)
      yield* writePrecreatedPlans(yield* get(input.workflowID), yield* milestones(input.workflowID))
      yield* publishUpdated(input.workflowID)
      return true
    })

    const createAgentSession = Effect.fn("Workflow.createAgentSession")(function* (input: {
      title: string
      agent: string
      parentID?: SessionID
      permission?: Permission.Ruleset
      model?: WorkflowInfo["model"]
    }) {
      return yield* session.create({
        parentID: input.parentID,
        title: input.title,
        agent: input.agent,
        model: input.model
          ? { id: input.model.modelID, providerID: input.model.providerID, variant: input.model.variant }
          : undefined,
        permission: input.permission,
      })
    })

    const memberPermission = (role: WorkflowSessionRef["role"]) => {
      if (role === "main_pm" || role === "department_pm") return pmPermission()
      if (role === "reviewer" || role === "expert") return reviewerPermission()
      return undefined
    }

    const ensureCompanyMember = Effect.fn("Workflow.ensureCompanyMember")(function* (input: {
      workflow: WorkflowInfo
      role: WorkflowSessionRef["role"]
      specialty?: string
      title?: string
      milestoneID?: WorkflowMilestoneID
      strictSpecialty?: boolean
      prompt?: string
      modelWeight?: number
    }) {
      const specialty = roleSpecialty(input.role, input.specialty)
      const staff = yield* members(input.workflow.id)
      const milestoneItems = yield* milestones(input.workflow.id)
      const limit = staffLimitForRole(input.workflow.staffing, input.role)
      const all = staff.filter((member) => member.role === input.role)
      const selectorPrompt = input.prompt ?? specialty
      const selected = selectWorkflowMember({
        role: input.role,
        specialty,
        members: staff,
        milestones: milestoneItems,
        excludeMilestoneID: input.milestoneID,
        limit,
        workflow: input.workflow,
        prompt: selectorPrompt,
        modelWeight: input.modelWeight,
      })
      if (selected && (!input.strictSpecialty || selected.specialty === specialty)) {
        if (
          selected.specialty !== specialty &&
          ["department_pm", "executor", "expert", "reviewer"].includes(input.role) &&
          !milestoneItems.some((milestone) =>
            milestone.session.some((ref) => ref.role === input.role && ref.sessionID === selected.sessionID),
          )
        ) {
          const now = Date.now()
          const title = input.title ?? workflowMemberTitle(input.role, specialty, all.filter((member) => member.specialty === specialty).length + 1)
          Database.use((db) =>
            db
              .update(WorkflowMemberTable)
              .set({ specialty, title, time_updated: now })
              .where(and(eq(WorkflowMemberTable.workflow_id, input.workflow.id), eq(WorkflowMemberTable.id, selected.id)))
              .run(),
          )
          if (input.workflow.rootSessionID && !input.skipAssignmentSideEffects) {
            yield* session.setParent({ sessionID: selected.sessionID, parentID: input.workflow.rootSessionID }).pipe(Effect.ignore)
          }
          if (!input.skipAssignmentSideEffects) {
            yield* session.setTitle({ sessionID: selected.sessionID, title }).pipe(Effect.ignore)
            yield* writeOrganization(input.workflow.id).pipe(Effect.ignore)
          }
          return {
            ...selected,
            specialty,
            title,
            time: {
              ...selected.time,
              updated: now,
            },
          }
        }
        if (input.workflow.rootSessionID && !input.skipAssignmentSideEffects) {
          yield* session.setParent({ sessionID: selected.sessionID, parentID: input.workflow.rootSessionID }).pipe(Effect.ignore)
        }
        if (!input.skipAssignmentSideEffects) {
          yield* session.setTitle({ sessionID: selected.sessionID, title: selected.title }).pipe(Effect.ignore)
        }
        return selected
      }

      if (limit <= 0) return undefined
      if (all.length >= limit) return undefined

      const index = all.length + 1
      const nextSpecialty = all.some((member) => member.specialty === specialty) ? `${specialty}-${index}` : specialty
      const title = input.title ?? workflowMemberTitle(input.role, nextSpecialty, index)
      const initialModel = selectWorkflowModelFromWhitelist({
        workflow: input.workflow,
        role: input.role,
        prompt: selectorPrompt,
        modelWeight: input.modelWeight,
        fallback: input.workflow.model,
      })
      const created = yield* createAgentSession({
        title,
        agent: workflowAgentForRole(input.role),
        parentID: input.workflow.rootSessionID,
        permission: memberPermission(input.role),
        model: initialModel,
      })
      const ctx = yield* InstanceState.context
      Database.use((db) =>
        db
          .insert(SessionTable)
          .values(
            workflowStateSessionRow({
              ctx,
              workflow: input.workflow,
              session: workflowStateSessionSnapshot({
                workflow: input.workflow,
                ref: { sessionID: created.id, role: input.role, title },
                info: created,
              }),
            }),
          )
          .onConflictDoNothing()
          .run(),
      )
      const now = Date.now()
      const member = {
        workflow_id: input.workflow.id,
        id: workflowMemberID(input.role, nextSpecialty, index),
        role: input.role,
        specialty: nextSpecialty,
        title,
        session_id: created.id,
        capacity: 1,
        status: "active" as const,
        availability: "idle" as const,
        current_focus: null,
        blockers: [],
        progress_note: null,
        model: workflowModelRef(initialModel) ?? null,
        model_weight: initialModel ? Math.trunc(workflowModelWeight(initialModel.weight ?? input.modelWeight)) : null,
        model_cache_until: workflowModelCacheUntil(initialModel, now) ?? null,
        time_created: now,
        time_updated: now,
      }
      Database.use((db) => db.insert(WorkflowMemberTable).values(member).run())
      if (!input.skipAssignmentSideEffects) yield* writeOrganization(input.workflow.id).pipe(Effect.ignore)
      return toMember(member)
    })

    const reconcileCompanyStaffing = Effect.fn("Workflow.reconcileCompanyStaffing")(function* (workflow: WorkflowInfo) {
      const staff = yield* members(workflow.id)
      const now = Date.now()
      const updates = (["main_pm", "department_pm", "expert", "executor", "reviewer", "tester"] as const).flatMap(
        (role) =>
          staff
            .filter((member) => member.role === role)
            .toSorted((a, b) => a.time.created - b.time.created || a.id.localeCompare(b.id))
            .flatMap((member, index) => {
              const status = index < staffLimitForRole(workflow.staffing, role) ? ("active" as const) : ("paused" as const)
              if (member.status === status) return []
              return [{ member, status }]
            }),
      )
      updates.forEach((update) =>
        Database.use((db) =>
          db
            .update(WorkflowMemberTable)
            .set({ status: update.status, time_updated: now })
            .where(and(eq(WorkflowMemberTable.workflow_id, workflow.id), eq(WorkflowMemberTable.id, update.member.id)))
            .run(),
        ),
      )
      return updates.length
    })

    const ensureCompany = Effect.fn("Workflow.ensureCompany")(function* (workflow: WorkflowInfo) {
      yield* Effect.all(
        [
          { role: "main_pm" as const, specialty: "strategy", title: workflowSessionTitle("Main PM", workflow.title) },
          { role: "department_pm" as const, specialty: "product" },
          { role: "executor" as const, specialty: "engineering" },
          { role: "reviewer" as const, specialty: "functional-review" },
          { role: "tester" as const, specialty: "quality", title: workflowSessionTitle("Tester", workflow.title) },
          { role: "expert" as const, specialty: "technical-advisory" },
        ].flatMap((item) =>
          Array.from({ length: staffLimitForRole(workflow.staffing, item.role) }, (_, index) =>
            ensureCompanyMember({
              workflow,
              role: item.role,
              specialty: index === 0 ? item.specialty : `${item.specialty}-${index + 1}`,
              title: item.title,
              strictSpecialty: true,
            }).pipe(Effect.ignore),
          ),
        ),
        { concurrency: 1, discard: true },
      )
      yield* reconcileCompanyStaffing(workflow)
      yield* writeOrganization(workflow.id).pipe(Effect.ignore)
    })

    const appendSession = (milestone: WorkflowMilestoneInfo, ref: WorkflowSessionRef) =>
      milestone.session.some(
        (item) =>
          item.role === ref.role &&
          item.sessionID === ref.sessionID &&
          item.milestoneID === ref.milestoneID &&
          item.attempt === ref.attempt,
      )
        ? milestone
        : {
            ...milestone,
            session: [...milestone.session, ref],
          }

    const assignMilestoneMember = Effect.fn("Workflow.assignMilestoneMember")(function* (input: {
      workflow: WorkflowInfo
      milestone: WorkflowMilestoneInfo
      role: WorkflowSessionRef["role"]
      specialty: string
      title: string
      attempt: number
      prompt: string
      modelWeight?: number
    }) {
      const member = yield* withWorkflowMemberAssignmentQueue(
        input.workflow.id,
        Effect.gen(function* () {
          const current = (yield* milestones(input.workflow.id)).find((item) => item.id === input.milestone.id)
          if (!current || ["done", "completed", "skipped", "cancelled"].includes(current.status)) return undefined
          const member = yield* ensureCompanyMember({
            workflow: input.workflow,
            role: input.role,
            specialty: input.specialty,
            title: input.title,
            milestoneID: input.milestone.id,
            prompt: input.prompt,
            modelWeight: input.modelWeight,
            skipAssignmentSideEffects: true,
          })
          if (!member) return undefined
          const latest = (yield* milestones(input.workflow.id)).find((item) => item.id === input.milestone.id)
          if (!latest || ["done", "completed", "skipped", "cancelled"].includes(latest.status)) return undefined
          yield* updateMilestoneSession(
            input.workflow.id,
            input.milestone.id,
            appendSession(latest, {
              role: input.role,
              sessionID: member.sessionID,
              milestoneID: input.milestone.id,
              attempt: input.attempt,
            }).session,
          )
          return member
        }),
      )
      if (!member) return undefined
      if (input.workflow.rootSessionID) {
        yield* session.setParent({ sessionID: member.sessionID, parentID: input.workflow.rootSessionID }).pipe(Effect.ignore)
      }
      yield* session.setTitle({ sessionID: member.sessionID, title: member.title }).pipe(Effect.ignore)
      return member
    })

    const waitForMilestoneMember = Effect.fn("Workflow.waitForMilestoneMember")(function* (input: {
      workflow: WorkflowInfo
      milestone: WorkflowMilestoneInfo
      role: WorkflowSessionRef["role"]
      specialty: string
      title: string
      attempt: number
      prompt: string
      modelWeight?: number
    }) {
      if (staffLimitForRole(input.workflow.staffing, input.role) <= 0) return undefined
      while (true) {
        const workflow = yield* get(input.workflow.id)
        if (workflow.status === "blocked" || workflow.status === "cancelled" || workflow.status === "completed") return undefined
        const current = (yield* milestones(input.workflow.id)).find((item) => item.id === input.milestone.id)
        if (!current || ["done", "completed", "skipped", "cancelled"].includes(current.status)) return undefined
        const member = yield* assignMilestoneMember({
          workflow,
          milestone: current,
          role: input.role,
          specialty: input.specialty,
          title: input.title,
          attempt: input.attempt,
          prompt: input.prompt,
          modelWeight: input.modelWeight,
        })
        if (member) {
          if (current.waitingFor) yield* updateMilestone(input.workflow.id, input.milestone.id, { waitingFor: null }).pipe(Effect.ignore)
          return member
        }
        if (current.waitingFor !== "staffing") {
          yield* updateMilestone(input.workflow.id, input.milestone.id, { waitingFor: "staffing" }).pipe(Effect.ignore)
        }
        yield* Effect.sleep("2 seconds")
      }
    })

    const runExecutorPeerSync = Effect.fn("Workflow.runExecutorPeerSync")(function* (input: {
      workflow: WorkflowInfo
      milestone: WorkflowMilestoneInfo
      executorSessionID: SessionID
      attempt: number
    }) {
      const peer = (yield* milestones(input.workflow.id))
        .filter((item) => item.id !== input.milestone.id && ["approved", "done", "completed"].includes(item.status))
        .flatMap((item) =>
          item.session
            .filter((ref) => ref.role === "executor" && ref.sessionID !== input.executorSessionID)
            .map((ref) => ({ milestone: item, ref })),
        )
        .at(-1)
      if (!peer) return undefined
      const question = [
        `Workflow-triggered executor peer sync before milestone ${input.milestone.id}.`,
        "",
        `Starting milestone: ${input.milestone.title ?? input.milestone.id}`,
        `Starting scope: ${input.milestone.prompt}`,
        "",
        `Your completed milestone: ${peer.milestone.id}`,
        `Completed scope: ${peer.milestone.prompt}`,
        "",
        "Share concise technical handoff notes: file boundaries, integration contracts, hazards, tests, and anything the next executor should avoid repeating.",
      ].join("\n")
      const peerInfo = yield* session
        .get(peer.ref.sessionID)
        .pipe(Effect.mapError((error) => new Error({ message: error.message })))
      const answer = yield* runPrompt(
        peer.ref.sessionID,
        peerInfo.agent ?? "workflow-executor",
        input.workflow.model,
        [
          "The workflow manager is asking you for peer technical handoff to another executor.",
          "",
          workflowReferencePrompt(input.workflow),
          "",
          question,
        ].join("\n"),
        {
          workflowID: input.workflow.id,
          role: "executor",
          milestoneID: peer.milestone.id,
          attempt: peer.ref.attempt,
        },
        { consult: false },
      )
      yield* recordConsultation({
        workflow: input.workflow,
        fromSessionID: input.executorSessionID,
        toSessionID: peer.ref.sessionID,
        fromRole: "executor",
        toRole: "executor",
        milestoneID: input.milestone.id,
        reason: "workflow-triggered executor peer technical sync",
        timing: "after-task",
        question,
        answer: latestText(answer),
        status: "answered",
      })
      return latestText(answer)
    })

    const handleMissingMilestoneOutput = Effect.fn("Workflow.handleMissingMilestoneOutput")(function* (input: {
      workflow: WorkflowInfo
      milestone: WorkflowMilestoneInfo
      attempt: number
      jobID?: string
      actor: string
      requiredOutput: string
    }) {
      if (input.attempt >= workflowMilestoneAttemptLimit) {
        yield* updateMilestone(input.workflow.id, input.milestone.id, { status: "blocked" }).pipe(Effect.ignore)
        yield* blockWorkflow(
          input.workflow,
          `Milestone ${input.milestone.id} exceeded ${workflowMilestoneAttemptLimit} attempts without required ${input.requiredOutput} from ${input.actor}. Inspect the owning session output and resume or force-skip explicitly after the runtime state is healthy.`,
          { kind: "runtime" },
        )
        return
      }
      yield* notifyMainPM(
        input.workflow.id,
        `${input.actor} did not produce required ${input.requiredOutput} for milestone ${input.milestone.id} on attempt ${input.attempt}/${workflowMilestoneAttemptLimit}. The milestone will retry, but will block instead of retrying forever if the limit is reached.`,
        { milestoneID: input.milestone.id, jobID: input.jobID },
      ).pipe(Effect.ignore)
      const retry = yield* updateMilestone(input.workflow.id, input.milestone.id, { status: "pending" })
      if (!retry) return
      yield* Effect.sleep("10 millis")
      const workflow = yield* setStatus(input.workflow.id, "executing", { error: "" })
      yield* runMilestone(workflow, retry, input.jobID)
    })

    const runMilestone = Effect.fn("Workflow.runMilestone")(function* (
      workflow: WorkflowInfo,
      milestone: WorkflowMilestoneInfo,
      jobID?: string,
    ) {
      if (workflow.status === "cancelled") return
      const attempt = milestone.attempt + 1
      const milestoneSource = { milestoneID: milestone.id, jobID }
      const planned = yield* updateMilestone(workflow.id, milestone.id, {
        status: "planning",
        attempt,
      })
      if (!planned) return

      const pm = yield* assignMilestoneMember({
        workflow,
        role: "department_pm",
        specialty: milestone.department ?? "product",
        title: workflowSessionTitle("Department PM", milestone.title ?? String(milestone.id)),
        attempt,
        milestone,
        prompt: milestone.prompt,
      })
      if (!pm) return yield* blockWorkflow(workflow, `No department PM is available for milestone ${milestone.id}`)
      const pmPrompt = promptDepartmentPm({ workflow, milestone })
      const pmResult = yield* runPrompt(pm.sessionID, "workflow-department-pm", workflow.model, pmPrompt, {
        workflowID: workflow.id,
        role: "department_pm",
        milestoneID: milestone.id,
        attempt,
      }, { expect: handoffExpectation("department PM execution plan") })
      if (yield* workflowIsBlocked(workflow.id)) return
      if (!hasHandoffSummary(latestText(pmResult))) {
        yield* handleMissingMilestoneOutput({
          workflow,
          milestone,
          attempt,
          jobID,
          actor: "Department PM",
          requiredOutput: "plan Handoff Summary",
        })
        return
      }
      yield* writeNote(workflowArtifactPath(workflow, milestone.id, "plan.md"), latestText(pmResult))
      yield* notifyMainPM(
        workflow.id,
        `Department PM finished planning milestone ${milestone.id}. Plan file: ${workflowArtifactPath(workflow, milestone.id, "plan.md")}`,
        milestoneSource,
      )
      if (yield* workflowIsBlocked(workflow.id)) return
      if (
        !(yield* milestones(workflow.id)).some(
          (item) => item.id === milestone.id && item.status !== "skipped" && item.status !== "cancelled",
        )
      ) {
        yield* schedule(workflow.id)
        return
      }
      const refreshed = yield* refreshWorkflowXml(workflow.id, "dispatching")
      if (
        refreshed &&
        !(yield* milestones(workflow.id)).some(
          (item) => item.id === milestone.id && item.status !== "skipped" && item.status !== "cancelled",
        )
      ) {
        yield* schedule(workflow.id)
        return
      }

      const executing = (yield* milestones(workflow.id)).find(
        (item) => item.id === milestone.id && item.status !== "skipped" && item.status !== "cancelled",
      )
      if (!executing) return
      const expert = yield* assignMilestoneMember({
        workflow,
        role: "expert",
        specialty: milestone.department ?? "technical-advisory",
        title: workflowSessionTitle("Technical Advisor", milestone.title ?? String(milestone.id)),
        attempt,
        milestone,
        prompt: milestone.prompt,
      })
      const expertPath = expert ? workflowArtifactPath(workflow, workflowExpertNotePath(milestone.id, attempt)) : undefined
      if (expert) {
        const expertResult = yield* runPrompt(
          expert.sessionID,
          "workflow-expert",
          workflow.model,
          promptExpert({ workflow, milestone: { ...milestone, attempt } }),
          {
            workflowID: workflow.id,
            role: "expert",
            milestoneID: milestone.id,
            attempt,
          },
          { expect: handoffExpectation("technical advisor note") },
        )
        if (yield* workflowIsBlocked(workflow.id)) return
        if (!hasHandoffSummary(latestText(expertResult))) {
          yield* handleMissingMilestoneOutput({
            workflow,
            milestone,
            attempt,
            jobID,
            actor: "Technical advisor",
            requiredOutput: "technical Handoff Summary",
          })
          return
        }
        yield* writeNote(expertPath!, latestText(expertResult))
        yield* notifyMainPM(workflow.id, `Technical advisor finished notes for milestone ${milestone.id}. Notes: ${expertPath}`, milestoneSource)
        if (yield* workflowIsBlocked(workflow.id)) return
      }
      yield* updateMilestone(workflow.id, milestone.id, { status: "executing" })
      yield* schedule(workflow.id).pipe(Effect.ignore)
      if (yield* workflowIsBlocked(workflow.id)) return
      const executor = yield* waitForMilestoneMember({
        workflow,
        role: "executor",
        specialty: milestone.department ?? "engineering",
        title: workflowSessionTitle("Executor", milestone.title ?? String(milestone.id)),
        attempt,
        milestone,
        prompt: milestone.prompt,
      })
      if (!executor) return yield* blockWorkflow(workflow, `No executor is available for milestone ${milestone.id}`)
      const peerContext = yield* runExecutorPeerSync({
        workflow,
        milestone,
        executorSessionID: executor.sessionID,
        attempt,
      }).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
      const executorResult = yield* runPrompt(
        executor.sessionID,
        "workflow-executor",
        workflow.model,
        promptExecutor({ workflow, milestone, expertPath, peerContext }),
        {
          workflowID: workflow.id,
          role: "executor",
          milestoneID: milestone.id,
          attempt,
        },
        { expect: executorExpectation(milestone.id) },
      )
      if (yield* workflowIsBlocked(workflow.id)) return
      const executorOutput = latestText(executorResult)
      if (!workflowResultComplete(executorOutput)) {
        yield* handleMissingMilestoneOutput({
          workflow,
          milestone,
          attempt,
          jobID,
          actor: "Executor",
          requiredOutput: "completion XML",
        })
        return
      }
      yield* notifyMainPM(
        workflow.id,
        `Executor finished milestone ${milestone.id}. Department PM will perform functional review against the plan and workflow request.`,
        milestoneSource,
      )
      if (yield* workflowIsBlocked(workflow.id)) return

      const reviewing = (yield* milestones(workflow.id)).find((item) => item.id === milestone.id)
      if (!reviewing) return
      if (reviewing.review === "skip") {
        yield* updateMilestone(workflow.id, milestone.id, { status: "approved" })
        yield* notifyMainPM(
          workflow.id,
          `Milestone ${milestone.id} skipped department PM functional review because workflow.xml sets review="skip".`,
          milestoneSource,
        )
        if (yield* workflowIsBlocked(workflow.id)) return
        yield* schedule(workflow.id)
        return
      }
      yield* updateMilestone(workflow.id, milestone.id, {
        status: "reviewing",
        session: appendSession(reviewing, { role: "department_pm", sessionID: pm.sessionID, milestoneID: milestone.id, attempt })
          .session,
      })
      const reviewerResult = yield* runPrompt(
        pm.sessionID,
        "workflow-department-pm",
        workflow.model,
        promptReviewer({ workflow, milestone, executorOutput }),
        {
          workflowID: workflow.id,
          role: "department_pm",
          milestoneID: milestone.id,
          attempt,
        },
        { expect: reviewExpectation(milestone.id) },
      )
      if (yield* workflowIsBlocked(workflow.id)) return
      const reviewText = latestText(reviewerResult)
      const reviewPath = workflowArtifactPath(workflow, workflowMilestoneReviewPath(milestone.id, attempt))
      yield* writeNote(reviewPath, reviewText)
      if (approved(reviewText)) {
        yield* updateMilestone(workflow.id, milestone.id, {
          status: "approved",
          reviewPath,
        })
        yield* notifyMainPM(workflow.id, `Department PM approved milestone ${milestone.id}. Review file: ${reviewPath}`, milestoneSource)
        if (yield* workflowIsBlocked(workflow.id)) return
        yield* schedule(workflow.id)
        return
      }
      if (rejected(reviewText) && attempt >= 3) {
        yield* updateMilestone(workflow.id, milestone.id, { status: "rejected", reviewPath })
        yield* notifyMainPM(
          workflow.id,
          `Department PM rejected milestone ${milestone.id} for the third time. Workflow will block for direction.`,
          milestoneSource,
        )
        yield* blockWorkflow(workflow, `Milestone ${milestone.id} rejected ${attempt} times`)
        return
      }
      if (!rejected(reviewText)) {
        yield* updateMilestone(workflow.id, milestone.id, { status: "rejected", reviewPath })
        yield* notifyMainPM(
          workflow.id,
          `Department PM did not give a usable approve/reject decision for milestone ${milestone.id}. Workflow will block.`,
          milestoneSource,
        )
        yield* blockWorkflow(workflow, `Department PM did not return an approve/reject decision for milestone ${milestone.id}`)
        return
      }
      yield* updateMilestone(workflow.id, milestone.id, { status: "pending", reviewPath })
      yield* notifyMainPM(workflow.id, `Department PM rejected milestone ${milestone.id}. It is returned to planning for another attempt.`, {
        milestoneID: milestone.id,
        jobID,
      })
      if (yield* workflowIsBlocked(workflow.id)) return
      yield* schedule(workflow.id)
    })

    const reopenFeedbackMilestones = Effect.fn("Workflow.reopenFeedbackMilestones")(function* (input: {
      workflow: WorkflowInfo
      source: string
      output: string
      milestoneIDs: WorkflowMilestoneID[]
    }) {
      const current = yield* milestones(input.workflow.id)
      const targets = feedbackMilestoneIDs(current, input.milestoneIDs)
      if (targets.length === 0) return false
      const targetSet = new Set(targets.map(String))
      for (const item of current) {
        if (targetSet.has(String(item.id))) {
          yield* updateMilestone(input.workflow.id, item.id, { status: "pending" })
          continue
        }
        if (item.status === "testing") yield* updateMilestone(input.workflow.id, item.id, { status: "approved" })
      }
      yield* setStatus(input.workflow.id, "executing", { error: "" })
      yield* notifyMainPM(
        input.workflow.id,
        `${input.source} reopened ${targets.map(String).join(", ")} for another PM/executor/test loop. Feedback: ${compactMarkdown(input.output, 320).replace(/\n/g, " ")}`,
      )
      yield* writeReferenceIndex(input.workflow.id).pipe(Effect.ignore)
      yield* writeProgress(input.workflow.id).pipe(Effect.ignore)
      yield* schedule(input.workflow.id)
      return true
    })

    const runTester = Effect.fn("Workflow.runTester")(function* (workflow: WorkflowInfo) {
      const items = yield* milestones(workflow.id)
      yield* setStatus(workflow.id, "testing")
      for (const item of items) {
        if (item.status === "approved") yield* updateMilestone(workflow.id, item.id, { status: "testing" })
      }
      const tester = yield* ensureCompanyMember({
        workflow,
        role: "tester",
        specialty: "quality",
        title: workflowSessionTitle("Tester", workflow.title),
        prompt: workflow.request,
      })
      if (!tester) return yield* blockWorkflow(workflow, "No tester is available for workflow completion review")
      const testerPrompt = promptTester({ workflow, milestones: items })
      const testerResult = yield* runPrompt(tester.sessionID, "workflow-tester", workflow.model, testerPrompt, {
        workflowID: workflow.id,
        role: "tester",
      }, { expect: testExpectation() })
      if (yield* workflowIsBlocked(workflow.id)) return
      const testPath = workflowArtifactPath(workflow, workflowTestPlanPath())
      const testerOutput = latestText(testerResult)
      yield* writeNote(testPath, testerOutput)
      yield* setStatus(workflow.id, "testing", { testerSessionID: tester.sessionID, testPath })
      yield* notifyMainPM(workflow.id, `Tester finished completeness and regression review. Test notes: ${testPath}`)
      if (yield* workflowIsBlocked(workflow.id)) return
      const testDecision = parseTestDecision(testerOutput)
      if (testDecision !== "pass") {
        if (
          testDecision === "fail" &&
          (yield* reopenFeedbackMilestones({
            workflow,
            source: "Tester completeness review",
            output: testerOutput,
            milestoneIDs: parseGateMilestoneIDs(testerOutput, "test"),
          }))
        ) {
          return
        }
        const reason = testDecision === "fail" ? "Tester failed workflow completeness review" : "Tester did not provide a test gate decision"
        yield* blockWorkflow(workflow, `${reason}: ${compactMarkdown(testerOutput, 240).replace(/\n/g, " ")}`)
        return
      }
      const expert = yield* ensureCompanyMember({
        workflow,
        role: "expert",
        specialty: "performance-and-architecture",
        title: workflowSessionTitle("Technical Advisor", workflow.title),
        prompt: workflow.request,
      })
      const technicalPath = workflowArtifactPath(workflow, workflowTechnicalAssessmentPath())
      if (!expert) {
        yield* writeNote(technicalPath, "_No technical advisor configured._")
        yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
        yield* blockWorkflow(workflow, "No technical advisor is available for final architecture and performance assessment")
        return
      }
      const expertOutput = latestText(
        yield* runPrompt(
          expert.sessionID,
          "workflow-expert",
          workflow.model,
          [
            "The workflow is ready for final technical assessment.",
            "",
            workflowReferencePrompt(workflow),
            "",
            "Review performance, architecture, integration risk, and optimization opportunities before requester acceptance.",
            "This is a hard delivery gate. End with exactly one technical gate XML block:",
            '<opencode-workflow-technical decision="pass">architecture and performance are acceptable for requester acceptance</opencode-workflow-technical>',
            '<opencode-workflow-technical decision="fail" milestones="comma-separated affected milestone ids">specific architecture, performance, or integration risks that must be reopened</opencode-workflow-technical>',
            "If you need another role before deciding, emit workflow communication XML to main_pm with timing=\"interrupt\", then still return the best current technical gate decision.",
            "",
            "Tester output:",
            testerOutput,
          ].join("\n"),
          {
            workflowID: workflow.id,
            role: "expert",
          },
          { expect: technicalExpectation() },
        )
      )
      if (yield* workflowIsBlocked(workflow.id)) return
      yield* writeNote(technicalPath, expertOutput)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* notifyMainPM(workflow.id, `Technical advisor finished final architecture and performance assessment. Notes: ${technicalPath}`)
      if (yield* workflowIsBlocked(workflow.id)) return
      const technicalDecision = parseTechnicalDecision(expertOutput)
      if (technicalDecision !== "pass") {
        if (
          technicalDecision === "fail" &&
          (yield* reopenFeedbackMilestones({
            workflow,
            source: "Technical advisor architecture and performance assessment",
            output: expertOutput,
            milestoneIDs: parseGateMilestoneIDs(expertOutput, "technical"),
          }))
        ) {
          return
        }
        const reason =
          technicalDecision === "fail"
            ? "Technical advisor failed final architecture and performance assessment"
            : "Technical advisor did not provide a technical gate decision"
        yield* blockWorkflow(workflow, `${reason}: ${compactMarkdown(expertOutput, 240).replace(/\n/g, " ")}`)
        return
      }
      yield* setStatus(workflow.id, "accepting")
      if (!workflow.pmSessionID) return yield* blockWorkflow(workflow, "No main PM session is available for final acceptance")
      const mainPmResult = yield* runPrompt(
        workflow.pmSessionID,
        "workflow-main-pm",
        workflow.model,
        promptAcceptance({ workflow, role: "main_pm", testerOutput, expertOutput }),
        {
          workflowID: workflow.id,
          role: "main_pm",
        },
        { expect: acceptanceExpectation("main_pm") },
      )
      if (yield* workflowIsBlocked(workflow.id)) return
      const mainPmOutput = latestText(mainPmResult)
      yield* writeNote(workflowArtifactPath(workflow, workflowAcceptancePath("main_pm")), mainPmOutput)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      const mainPmDecision = parseAcceptanceDecision(mainPmOutput)
      if (mainPmDecision !== "approve") {
        if (
          mainPmDecision === "reject" &&
          (yield* reopenFeedbackMilestones({
            workflow,
            source: "Main PM final acceptance",
            output: mainPmOutput,
            milestoneIDs: parseGateMilestoneIDs(mainPmOutput, "acceptance"),
          }))
        ) {
          return
        }
        const reason = mainPmDecision === "reject" ? "Main PM rejected final acceptance" : "Main PM did not provide a final acceptance decision"
        yield* blockWorkflow(workflow, `${reason}: ${compactMarkdown(mainPmOutput, 240).replace(/\n/g, " ")}`)
        return
      }
      if (!workflow.rootSessionID) return yield* blockWorkflow(workflow, "No requester session is available for final acceptance")
      const requesterResult = yield* runPrompt(
        workflow.rootSessionID,
        workflow.agent ?? "build",
        workflow.model,
        promptAcceptance({
          workflow,
          role: "requester",
          testerOutput,
          expertOutput,
          mainPmOutput,
        }),
        {
          workflowID: workflow.id,
          role: "requester",
        },
        { expect: acceptanceExpectation("requester") },
      )
      if (yield* workflowIsBlocked(workflow.id)) return
      const requesterOutput = latestText(requesterResult)
      yield* writeNote(workflowArtifactPath(workflow, workflowAcceptancePath("requester")), requesterOutput)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      const requesterDecision = parseAcceptanceDecision(requesterOutput)
      if (requesterDecision !== "approve") {
        if (
          requesterDecision === "reject" &&
          (yield* reopenFeedbackMilestones({
            workflow,
            source: "Requester final acceptance",
            output: requesterOutput,
            milestoneIDs: parseGateMilestoneIDs(requesterOutput, "acceptance"),
          }))
        ) {
          return
        }
        const reason = requesterDecision === "reject" ? "Requester rejected final acceptance" : "Requester did not provide a final acceptance decision"
        yield* blockWorkflow(workflow, `${reason}: ${compactMarkdown(requesterOutput, 240).replace(/\n/g, " ")}`)
        return
      }
      if (!workflowComplete(requesterOutput)) {
        yield* blockWorkflow(
          workflow,
          `Requester approved without the workflow completion marker: ${compactMarkdown(requesterOutput, 240).replace(/\n/g, " ")}`,
        )
        return
      }
      for (const item of yield* milestones(workflow.id)) {
        if (item.status === "testing" || item.status === "approved") {
          yield* updateMilestone(workflow.id, item.id, { status: "done" })
        }
      }
      yield* setStatus(workflow.id, "completed", {
        testerSessionID: tester.sessionID,
        testPath,
      })
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* writeProgress(workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflow.id).pipe(Effect.ignore)
    })

    const recoverInterruptedProgress = Effect.fn("Workflow.recoverInterruptedProgress")(function* (
      workflowID: WorkflowID,
      items: WorkflowMilestoneInfo[],
    ) {
      for (const job of (yield* background.list()).filter(
        (job) => job.status === "running" && job.type === "workflow.milestone" && job.metadata?.workflowID === workflowID,
      )) {
        yield* background.cancel(job.id).pipe(Effect.ignore)
      }
      for (const item of items.filter((item) => retryableMilestone(item.status))) {
        for (const attempt of Array.from({ length: item.attempt + 2 }, (_, index) => index + 1)) {
          yield* background.cancel(milestoneJobID(workflowID, item.id, attempt)).pipe(Effect.ignore)
        }
        if (item.status !== "pending") yield* updateMilestone(workflowID, item.id, { status: "pending" })
      }
      for (const item of items.filter((item) => item.status === "testing")) {
        yield* updateMilestone(workflowID, item.id, { status: "approved" })
      }
    })

    const recoverStaleActiveMilestones = Effect.fn("Workflow.recoverStaleActiveMilestones")(function* (
      workflow: WorkflowInfo,
      items: WorkflowMilestoneInfo[],
    ) {
      const running = new Set(
        (yield* background.list())
          .filter(
            (job) =>
              job.status === "running" &&
              job.type === "workflow.milestone" &&
              job.metadata?.workflowID === workflow.id,
          )
          .map((job) => String(job.metadata?.milestoneID ?? "")),
      )
      const stale = items.filter((item) => interruptedMilestone(item.status) && !running.has(String(item.id)))
      if (stale.length === 0) return false
      const exhausted = stale.filter((item) => item.attempt >= workflowMilestoneAttemptLimit)
      if (exhausted.length === 0) {
        for (const item of stale) {
          yield* updateMilestone(workflow.id, item.id, { status: "pending" })
        }
        const staleSummary = stale.map((item) => `${item.id}:${item.status}:attempt${item.attempt}`).join(", ")
        const staleMessage = `Workflow watchdog found orphaned active milestone job(s) and queued retry: ${staleSummary}.`
        yield* recordMainPMSystemReport(workflow, staleMessage).pipe(Effect.ignore)
        yield* notifyMainPM(workflow.id, staleMessage).pipe(Effect.ignore)
        yield* setStatus(workflow.id, "executing", { error: "" }).pipe(Effect.ignore)
        return true
      }
      for (const item of stale) {
        yield* updateMilestone(workflow.id, item.id, { status: "blocked" })
      }
      const staleSummary = exhausted.map((item) => `${item.id}:${item.status}:attempt${item.attempt}`).join(", ")
      yield* blockWorkflow(
        workflow,
        `Workflow watchdog detected orphaned active milestone job(s): ${staleSummary}. These milestones were active, but no running workflow.milestone background job exists. This is a scheduler/runtime dispatch problem; inspect the owning session archive and retry explicitly with workflow resume/continue after the runner state is healthy.`,
        { kind: "runtime" },
      )
      return true
    })

    const escalateStaleWaitingMilestones = Effect.fn("Workflow.escalateStaleWaitingMilestones")(function* (
      workflow: WorkflowInfo,
    ) {
      const now = Date.now()
      const stale = Database.use((db) =>
        db
          .select()
          .from(WorkflowMilestoneTable)
          .where(and(eq(WorkflowMilestoneTable.workflow_id, workflow.id), eq(WorkflowMilestoneTable.status, "pending")))
          .all()
          .filter(
            (row) =>
              (row.waiting_for === "staffing" || row.waiting_for === "scheduling") &&
              now - row.time_updated >= workflowWaitingTimeoutMillis,
          )
          .map(toMilestone),
      )
      if (stale.length === 0) return 0
      const lines = stale.map((item) =>
        `- ${item.id}: waitingFor=${item.waitingFor}; title=${item.title ?? item.id}; department=${item.department ?? "unspecified"}`,
      )
      const result = yield* intervene({
        workflowID: workflow.id,
        sourceSessionID: workflow.rootSessionID,
        targetRole: "main_pm",
        timing: "temporary-interrupt",
        message: [
          "Workflow watchdog found ready milestones waiting too long without dispatch.",
          "",
          ...lines,
          "",
          "Decide whether to expand staffing, change scheduling mode, force-skip a gate, or block for requester direction. Do not rewrite product scope or workflow.xml unless the graph itself is wrong.",
        ].join("\n"),
      }).pipe(
        Effect.as({ status: "queued" as const }),
        Effect.catchCause((cause) =>
          Effect.succeed({
            status: "failed" as const,
            error: errorFromCause(cause),
          }),
        ),
      )
      for (const item of stale) {
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({ time_updated: now })
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, workflow.id),
                eq(WorkflowMilestoneTable.id, item.id),
                eq(WorkflowMilestoneTable.status, "pending"),
              ),
            )
            .run(),
        )
      }
      yield* appendWorkflowMessageRuntimeJournal(workflow.id, {
        action: "waiting_watchdog",
        kind: "intervention",
        targetRole: "main_pm",
        milestones: stale.map((item) => ({
          id: item.id,
          waitingFor: item.waitingFor,
          status: item.status,
        })),
        ...result,
      }).pipe(Effect.ignore)
      yield* writeProgress(workflow.id).pipe(Effect.ignore)
      yield* writeWorkflowState(workflow.id).pipe(Effect.ignore)
      yield* publishUpdated(workflow.id).pipe(Effect.ignore)
      return stale.length
    })

    const blockActiveMilestones = Effect.fn("Workflow.blockActiveMilestones")(function* (workflowID: WorkflowID) {
      for (const item of (yield* milestones(workflowID)).filter((milestone) =>
        interruptedMilestone(milestone.status),
      )) {
        yield* updateMilestone(workflowID, item.id, { status: "blocked" }).pipe(Effect.ignore)
      }
    })

    const cancelWorkflowRuns = Effect.fn("Workflow.cancelWorkflowRuns")(function* (
      workflowID: WorkflowID,
      exceptJobID?: string | string[],
    ) {
      const exceptJobIDs = new Set(Array.isArray(exceptJobID) ? exceptJobID : exceptJobID ? [exceptJobID] : [])
      for (const job of (yield* background.list()).filter(
        (job) => job.metadata?.workflowID === workflowID && job.status === "running" && !exceptJobIDs.has(job.id),
      )) {
        yield* background.cancel(job.id).pipe(Effect.ignore)
      }
    })

    const cancelMilestoneRuns = Effect.fn("Workflow.cancelMilestoneRuns")(function* (
      workflowID: WorkflowID,
      milestoneID: WorkflowMilestoneID,
    ) {
      const current = (yield* milestones(workflowID)).find((item) => item.id === milestoneID)
      yield* cancelMilestoneBackgroundRuns(workflowID, milestoneID, current?.attempt ?? 0)
    })

    const hasActiveMilestoneRun = Effect.fn("Workflow.hasActiveMilestoneRun")(function* (
      workflowID: WorkflowID,
      milestoneID: WorkflowMilestoneID,
    ) {
      return (yield* background.list()).some(
        (job) =>
          job.status === "running" &&
          job.type === "workflow.milestone" &&
          job.metadata?.workflowID === workflowID &&
          job.metadata?.milestoneID === milestoneID,
      )
    })

    const activeMilestoneRunIsCurrent = Effect.fn("Workflow.activeMilestoneRunIsCurrent")(function* (
      workflowID: WorkflowID,
      milestoneID: WorkflowMilestoneID,
      currentJobID?: string | string[],
    ) {
      const currentJobIDs = new Set(Array.isArray(currentJobID) ? currentJobID : currentJobID ? [currentJobID] : [])
      if (currentJobIDs.size === 0) return false
      return (yield* background.list()).some(
        (job) =>
          job.status === "running" &&
          job.type === "workflow.milestone" &&
          job.metadata?.workflowID === workflowID &&
          job.metadata?.milestoneID === milestoneID &&
          currentJobIDs.has(job.id),
      )
    })

    const startMilestoneJob = Effect.fn("Workflow.startMilestoneJob")(function* (
      workflow: WorkflowInfo,
      current: WorkflowMilestoneInfo,
      options?: { delay?: "50 millis" },
    ) {
      const jobID = milestoneJobID(workflow.id, current.id, current.attempt + 1)
      const run = runMilestone(workflow, current, jobID).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.interrupt
            : Effect.gen(function* () {
                yield* updateMilestone(workflow.id, current.id, { status: "failed" })
                yield* setStatus(workflow.id, "failed", {
                  error: `Milestone ${current.id} failed: ${errorFromCause(cause)}`,
                })
              }),
        ),
        Effect.as(`milestone ${current.id} completed`),
      )
      yield* background.start({
        id: jobID,
        type: "workflow.milestone",
        title: `${workflow.title} ${current.id}`,
        metadata: { workflowID: workflow.id, milestoneID: current.id },
        run: options?.delay ? run.pipe(Effect.delay(options.delay)) : run,
      })
      return jobID
    })

    const schedule: (
      workflowID: WorkflowID,
      options?: { bypassStagedGate?: boolean },
    ) => Effect.Effect<WorkflowInfo, unknown> = Effect.fn("Workflow.schedule")(
      function* (workflowID: WorkflowID, options?: { bypassStagedGate?: boolean }) {
      const workflow = yield* get(workflowID)
      if (workflow.status === "cancelled" || workflow.status === "completed" || workflow.status === "blocked") return workflow
      const items = yield* milestones(workflowID)
      if (yield* recoverStaleActiveMilestones(workflow, items)) return yield* schedule(workflowID, options)
      yield* deliverReadyInterventions(workflowID).pipe(Effect.ignore)
      const schedulableItems = items.filter((item) => item.status !== "cancelled")
      const runningMilestoneIDs = new Set(
        (yield* background.list())
          .filter(
            (job) =>
              job.status === "running" &&
              job.type === "workflow.milestone" &&
              job.metadata?.workflowID === workflowID,
          )
          .map((job) => String(job.metadata?.milestoneID ?? "")),
      )
      const definition: WorkflowDefinition = {
        steps: { type: "parallel", children: [] },
        milestones: schedulableItems.map((item) => ({
          type: "milestone",
          id: item.id,
          title: item.title,
          department: item.department,
          prompt: item.prompt,
          dependsOn: item.dependsOn,
        })),
      }
      const unblocked = dependencyUnblockedMilestones(definition, milestoneStates(schedulableItems))
        .map((candidate) => schedulableItems.find((item) => item.id === candidate.id))
        .filter((item) => item && item.session.length === 0)
      if (unblocked.length > 0) {
        for (const item of unblocked) {
          yield* updateMilestone(workflowID, item.id, { status: "pending" })
        }
        return yield* schedule(workflowID, options)
      }
      const blockedByDependency = dependencyBlockedMilestones(definition, milestoneStates(schedulableItems))
      if (blockedByDependency.length > 0) {
        for (const item of blockedByDependency) {
          yield* updateMilestone(workflowID, item.id, { status: "blocked" })
        }
        const blockedIDs = blockedByDependency.map((item) => item.id).join(", ")
        return yield* blockWorkflow(
          workflow,
          `Milestones blocked by failed dependencies: ${blockedIDs}. Use force_skip, reopen the failed dependency, update workflow XML, or block for requester direction.`,
        )
      }
      if (
        schedulableItems.length > 0 &&
        schedulableItems.every((item) => ["approved", "done", "completed", "skipped"].includes(item.status))
      ) {
        yield* runTester(workflow)
        return yield* get(workflowID)
      }
      const activeMilestoneCount = items.filter((item) =>
        ["planning", "executing", "reviewing", "testing"].includes(item.status),
      ).length
      const activeRoleCount = (role: WorkflowSessionRef["role"]) =>
        new Set(
          schedulableItems
            .filter((item) => roleBusyForMilestoneStatus(role, item.status))
            .flatMap((item) => item.session.filter((ref) => ref.role === role).map((ref) => ref.sessionID)),
        ).size
      const availableStarts = staffLimitForRole(workflow.staffing, "department_pm") - activeRoleCount("department_pm")
      const readyAll = readyMilestones(definition, milestoneStates(schedulableItems))
      if (
        workflowSchedulingMode(workflow) === "staged" &&
        !options?.bypassStagedGate &&
        activeMilestoneCount === 0 &&
        readyAll.length > 0 &&
        schedulableItems.some((item) => item.session.length > 0 || terminalMilestone(item.status))
      ) {
        return yield* blockWorkflow(
          workflow,
          `Staged scheduling gate reached before ${readyAll.map((item) => item.id).join(", ")}. Use workflow resume to dispatch the next stage.`,
        )
      }
      const activeLimit = workflowSchedulingActiveLimit(workflow)
      const schedulingCapacity = activeLimit === undefined ? readyAll.length : Math.max(0, activeLimit - activeMilestoneCount)
      const schedulableByID = new Map(schedulableItems.map((item) => [item.id, item]))
      const pipelineWaitingReady = readyAll.filter((item) => schedulableByID.get(item.id)?.waitingFor === "pipeline_items")
      const runnableReady = readyAll.filter((item) => schedulableByID.get(item.id)?.waitingFor !== "pipeline_items")
      const ready = runnableReady.slice(
        0,
        Math.max(0, Math.min(availableStarts, schedulingCapacity)),
      )
      const waiting = runnableReady.slice(ready.length)
      const readyIDs = new Set(ready.map((item) => item.id))
      const readyAllIDs = new Set(readyAll.map((item) => item.id))
      for (const item of schedulableItems) {
        if (item.waitingFor === "pipeline_items") continue
        if (readyIDs.has(item.id) || (item.waitingFor && !readyAllIDs.has(item.id))) {
          yield* updateMilestone(workflowID, item.id, { waitingFor: null }).pipe(Effect.ignore)
        }
      }
      if (waiting.length > 0) {
        const reason = availableStarts <= schedulingCapacity ? "staffing" : "scheduling"
        for (const item of waiting) {
          const current = schedulableByID.get(item.id)
          if (current?.waitingFor !== reason) {
            yield* updateMilestone(workflowID, item.id, { waitingFor: reason }).pipe(Effect.ignore)
          }
        }
      }
      yield* escalateStaleWaitingMilestones(workflow).pipe(Effect.ignore)
      if (ready.length === 0) {
        if (pipelineWaitingReady.length > 0 || waiting.length > 0) return workflow
        if (readyAll.length > 0 && (availableStarts <= 0 || schedulingCapacity <= 0)) return workflow
        if (schedulableItems.some((item) => item.status === "rejected")) {
          for (const item of schedulableItems.filter((item) => item.status === "rejected")) {
            yield* updateMilestone(workflowID, item.id, { status: "pending" })
          }
          yield* schedule(workflowID, options)
          return yield* get(workflowID)
        }
        if (activeMilestoneCount > 0) return workflow
        if (schedulableItems.some((item) => item.status === "blocked" && runningMilestoneIDs.has(String(item.id)))) return workflow
        return yield* blockWorkflow(workflow, "No runnable milestones are available")
      }
      yield* setStatus(workflowID, "executing")
      for (const item of ready) {
        const current = schedulableItems.find((milestone) => milestone.id === item.id)
        if (!current) continue
        yield* startMilestoneJob(workflow, current)
      }
      yield* triggerCompanyStandup(
        workflowID,
        `scheduled ${ready.length} milestone${ready.length === 1 ? "" : "s"}: ${ready.map((item) => item.id).join(", ")}`,
      ).pipe(Effect.ignore)
      return yield* get(workflowID)
    },
    )

    const applyWorkflowControl = Effect.fn("Workflow.applyWorkflowControl")(function* (
      workflowID: WorkflowID,
      output: string,
      reason: string,
      options?: { exceptJobID?: string | string[]; sourceSessionID?: SessionID; sourceAgent?: string; deferStart?: boolean },
    ) {
      const command = parseWorkflowControlCommand(output)
      const action = command?.action ?? (implicitWorkflowResume(output) ? "resume" : undefined)
      if (!action) return yield* get(workflowID)
      const workflow = yield* get(workflowID)
      if (command?.action === "resume" && command.message) {
        const sourceSessionID = options?.sourceSessionID ?? workflow.pmSessionID ?? workflow.rootSessionID
        const sourceRole = sourceSessionID
          ? yield* workflowToolCommandSourceRole(workflow, sourceSessionID).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
          : undefined
        if (workflowCommandMessageClaimsDispatch(command.message, sourceRole)) {
          return yield* blockWorkflow(
            workflow,
            "Workflow resume control claimed employee dispatch in the message body. Resume only schedules milestones already represented in workflow.xml; use update_xml, plan_complete, or force_complete for real dispatch/control.",
          )
        }
      }
      if (
        command &&
        !["resume", "block"].includes(command.action)
      ) {
        const sourceSessionID = options?.sourceSessionID ?? workflow.pmSessionID ?? workflow.rootSessionID
        if (!sourceSessionID) {
          return yield* blockWorkflow(
            workflow,
            `Workflow control command ${command.action} from ${reason} could not be applied because no source session is known.`,
          )
        }
        const replay = yield* handleWorkflowToolCommand(
          {
            ...command,
            id: Bus.createID(),
            workflowID,
            sourceSessionID,
            ...(options?.sourceAgent ? { sourceAgent: options.sourceAgent } : {}),
          },
          {
            currentJobID: options?.exceptJobID,
          },
        )
        if (!replay.result.applied) {
          return yield* blockWorkflow(
            workflow,
            `Workflow control command ${command.action} from ${reason} was rejected: ${replay.result.message}`,
          )
        }
        return yield* get(workflowID)
      }
      if (action === "block") {
        yield* cancelWorkflowRuns(workflowID, options?.exceptJobID)
        yield* blockActiveMilestones(workflowID)
        return yield* blockWorkflow(
          workflow,
          `Main PM kept workflow blocked after ${reason}: ${compactMarkdown(output, 240).replace(/\n/g, " ")}`,
        )
      }
      if (workflow.status === "cancelled" || workflow.status === "completed") return workflow
      const doctorBlocked = yield* blockWorkflowResumeForDoctorIssues(workflowID, `${reason} resume`)
      if (doctorBlocked) return doctorBlocked
      yield* background.cancel(workflowID).pipe(Effect.ignore)
      const items = yield* milestones(workflowID)
      yield* recoverInterruptedProgress(workflowID, items)
      const recoveredItems = yield* milestones(workflowID)
      const hasMilestoneSessions = recoveredItems.some((item) => item.session.length > 0)
      const shouldContinuePlanning = !!workflow.pmSessionID && !hasMilestoneSessions
      const next = yield* setStatus(workflowID, shouldContinuePlanning ? "planning" : "executing", { error: "" })
      const startControl = background.start({
        id: `${workflowID}:control:${Date.now().toString(36)}`,
        type: "workflow",
        title: next.title,
        metadata: { workflowID },
        run: Effect.gen(function* () {
          yield* Effect.sleep("10 millis")
          yield* (shouldContinuePlanning ? continuePlanning(workflowID) : schedule(workflowID, { bypassStagedGate: true }))
        }).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : blockPlanning(workflowID, `Workflow control resume failed: ${errorFromCause(cause)}`).pipe(Effect.asVoid),
          ),
          Effect.as("workflow control resumed"),
        ),
      })
      if (options?.deferStart) return { ...next, afterCommit: startControl }
      yield* startControl
      return next
    })

    const workflowToolResumeBlock = (message: string) =>
      `<opencode-workflow-control action="resume">${message}</opencode-workflow-control>`

    const workflowToolCommandWorkflowID = Effect.fn("Workflow.workflowToolCommandWorkflowID")(function* (
      input: WorkflowToolCommand,
    ) {
      if (input.workflowID) return input.workflowID
      const context = yield* workflowSessionContext(input.sourceSessionID)
      if (context) return context.workflow.id
      return Database.use((db) =>
        db
          .select()
          .from(WorkflowTable)
          .where(
            or(
              eq(WorkflowTable.root_session_id, input.sourceSessionID),
              eq(WorkflowTable.pm_session_id, input.sourceSessionID),
              eq(WorkflowTable.tester_session_id, input.sourceSessionID),
            ),
          )
          .orderBy(asc(WorkflowTable.time_updated))
          .all()
          .at(-1),
      )?.id
    })

    const workflowToolCommandSourceRole = Effect.fn("Workflow.workflowToolCommandSourceRole")(function* (
      workflow: WorkflowInfo,
      sessionID: SessionID,
    ) {
      if (workflow.rootSessionID === sessionID) return "requester"
      if (workflow.pmSessionID === sessionID) return "main_pm"
      if (workflow.testerSessionID === sessionID) return "tester"
      const member = Database.use((db) =>
        db
          .select()
          .from(WorkflowMemberTable)
          .where(and(eq(WorkflowMemberTable.workflow_id, workflow.id), eq(WorkflowMemberTable.session_id, sessionID)))
          .orderBy(asc(WorkflowMemberTable.time_created))
          .all()
          .at(-1),
      )
      if (member) return member.role
      const assignment = workflowSessionAssignment(yield* milestones(workflow.id), sessionID)
      return assignment?.ref.role
    })

    const workflowCommandAfterCommit = (result: Record<string, unknown>, afterCommit: Effect.Effect<unknown>) => ({
      ...result,
      afterCommit,
    })

    const queueWorkflowSchedule = Effect.fn("Workflow.queueWorkflowSchedule")(function* (
      workflowID: WorkflowID,
      reason: string,
      options?: { bypassStagedGate?: boolean; delay?: "10 millis" | "50 millis" },
    ) {
      const workflow = yield* get(workflowID)
      yield* background.start({
        id: `${workflowID}:schedule:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}`,
        type: "workflow",
        title: `${workflow.title} schedule`,
        metadata: { workflowID, reason },
        run: Effect.gen(function* () {
          yield* Effect.sleep(options?.delay ?? "10 millis")
          yield* schedule(
            workflowID,
            options?.bypassStagedGate === undefined ? undefined : { bypassStagedGate: options.bypassStagedGate },
          )
        }).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : blockPlanning(workflowID, `Workflow scheduled dispatch failed after ${reason}: ${errorFromCause(cause)}`).pipe(
                  Effect.asVoid,
                ),
          ),
          Effect.as("workflow schedule completed"),
        ),
      })
    })

    const applyMilestoneToolStatus = Effect.fn("Workflow.applyMilestoneToolStatus")(function* (
      workflowID: WorkflowID,
      milestoneID: WorkflowMilestoneID,
      status: WorkflowMilestoneInfo["status"],
    ) {
      const workflow = yield* get(workflowID)
      const updated = yield* updateMilestone(workflowID, milestoneID, { status })
      if (!updated) {
        return workflowCommandRejection("unknown_milestone", `Milestone ${milestoneID} was not found.`)
      }
      if (status === "blocked") {
        yield* blockWorkflow(workflow, `Workflow tool blocked milestone ${milestoneID}`).pipe(Effect.ignore)
        return { workflowID, applied: true, message: `Milestone ${milestoneID} was blocked.` }
      }
      if (status === "failed") {
        yield* setStatus(workflowID, "executing", { error: "" }).pipe(Effect.ignore)
        return workflowCommandAfterCommit(
          { workflowID, applied: true, message: `Milestone ${milestoneID} was marked failed and scheduling was requested.` },
          queueWorkflowSchedule(workflowID, "milestone_status failed"),
        )
      }
      if (status === "cancelled") {
        yield* setStatus(workflowID, "executing", { error: "" }).pipe(Effect.ignore)
        return workflowCommandAfterCommit(
          { workflowID, applied: true, message: `Milestone ${milestoneID} was cancelled and scheduling was requested.` },
          queueWorkflowSchedule(workflowID, "milestone_status cancelled"),
        )
      }
      yield* setStatus(workflowID, "executing", { error: "" }).pipe(Effect.ignore)
      return workflowCommandAfterCommit(
        { workflowID, applied: true, message: `Milestone ${milestoneID} was set to ${status} and scheduling was requested.` },
        queueWorkflowSchedule(workflowID, `milestone_status ${status}`),
      )
    })

    const applyWorkflowToolCommand = Effect.fn("Workflow.applyWorkflowToolCommand")(function* (
      input: WorkflowToolCommand,
      options?: { currentJobID?: string | string[] },
    ) {
      const workflowID = yield* workflowToolCommandWorkflowID(input)
      if (!workflowID) return workflowCommandRejection("workflow_not_active", "No workflow is associated with this command.")
      const workflow = yield* get(workflowID)
      const sourceRole = yield* workflowToolCommandSourceRole(workflow, input.sourceSessionID)
      const sourceRoleName = sourceRole ? roleSessionTitle(sourceRole) : "unknown"
      if (input.action === "status") {
        yield* expireWorkflowMessages(workflowID).pipe(Effect.ignore)
        const recovered = yield* recoverStaleActiveMilestones(workflow, yield* milestones(workflowID)).pipe(
          Effect.catchCause(() => Effect.succeed(false)),
        )
        if (!recovered) yield* escalateStaleWaitingMilestones(workflow).pipe(Effect.ignore)
        yield* deliverReadyInterventions(workflowID).pipe(Effect.ignore)
        yield* publishUpdated(workflowID).pipe(Effect.ignore)
        return recovered
          ? workflowCommandAfterCommit(
              { workflowID, applied: true, message: "Workflow status was refreshed and scheduling was requested." },
              queueWorkflowSchedule(workflowID, "status recovery"),
            )
          : { workflowID, applied: true, message: "Workflow status was refreshed." }
      }
      if (input.action === "status_update") {
        if (!input.availability) {
          return {
            workflowID,
            ...workflowCommandRejection("precondition_failed", "status_update requires availability."),
          }
        }
        const member = Database.use((db) =>
          db
            .select()
            .from(WorkflowMemberTable)
            .where(and(eq(WorkflowMemberTable.workflow_id, workflowID), eq(WorkflowMemberTable.session_id, input.sourceSessionID)))
            .get(),
        )
        if (!member) {
          return {
            workflowID,
            ...workflowCommandRejection(
              "not_authorized",
              "Only workflow employee sessions can update their workflow member status.",
            ),
          }
        }
        Database.use((db) =>
          db
            .update(WorkflowMemberTable)
            .set({
              availability: input.availability,
              current_focus: input.currentFocus ? compactMarkdown(input.currentFocus, 500) : null,
              blockers: (input.blockers ?? []).map((blocker) => compactMarkdown(blocker, 300)).filter(Boolean).slice(0, 20),
              progress_note: input.progressNote ? compactMarkdown(input.progressNote, 500) : null,
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMemberTable.workflow_id, workflowID), eq(WorkflowMemberTable.id, member.id)))
            .run(),
        )
        yield* writeOrganization(workflowID).pipe(Effect.ignore)
        yield* writeProgress(workflowID).pipe(Effect.ignore)
        yield* writeWorkflowState(workflowID).pipe(Effect.ignore)
        yield* publishUpdated(workflowID).pipe(Effect.ignore)
        return {
          workflowID,
          applied: true,
          message: `Workflow member ${member.title} reported availability=${input.availability}.`,
        }
      }
      if (input.action === "scheduling") {
        if (!input.schedulingMode) {
          return {
            workflowID,
            ...workflowCommandRejection("precondition_failed", "scheduling requires schedulingMode."),
          }
        }
        if (input.schedulingMode === "economical" && !input.schedulingMaxActive) {
          return {
            workflowID,
            ...workflowCommandRejection("precondition_failed", "economical scheduling requires schedulingMaxActive."),
          }
        }
        if (!sourceRole || !["requester", "main_pm"].includes(sourceRole)) {
          return {
            workflowID,
            ...workflowCommandRejection(
              "not_authorized",
              `Role ${sourceRoleName} cannot change workflow scheduling. Ask requester or main PM to use this control.`,
            ),
          }
        }
        if (workflow.status === "cancelled" || workflow.status === "completed") {
          return {
            workflowID,
            ...workflowCommandRejection(
              "workflow_not_active",
              `Workflow is ${workflow.status}; scheduling cannot be changed after it is terminal.`,
            ),
          }
        }
        const scheduling = normalizeScheduling({
          mode: input.schedulingMode,
          ...(input.schedulingMode === "economical" ? { maxActive: input.schedulingMaxActive } : {}),
        })
        Database.use((db) =>
          db
            .update(WorkflowTable)
            .set({
              scheduling,
              status: workflow.status === "blocked" ? "executing" : workflow.status,
              error: null,
              time_updated: Date.now(),
            })
            .where(eq(WorkflowTable.id, workflowID))
            .run(),
        )
        yield* writeProgress(workflowID).pipe(Effect.ignore)
        yield* writeWorkflowState(workflowID).pipe(Effect.ignore)
        yield* publishUpdated(workflowID).pipe(Effect.ignore)
        return workflowCommandAfterCommit(
          {
            workflowID,
            applied: true,
            message: `Workflow scheduling changed to ${scheduling.mode}${scheduling.mode === "economical" ? ` maxActive=${scheduling.maxActive}` : ""} and scheduling was requested.`,
          },
          queueWorkflowSchedule(workflowID, "scheduling command", { bypassStagedGate: input.schedulingMode !== "staged" }),
        )
      }
      if (input.action === "resume") {
        if (workflowCommandMessageClaimsDispatch(input.message, sourceRole)) {
          return {
            workflowID,
            ...workflowCommandRejection(
              "precondition_failed",
              "resume message cannot assign, dispatch, route, or start employee sessions. Resume only schedules milestones already represented in workflow.xml. To create work, update workflow.xml/update_xml; to close a planning gate, use plan_complete or force_complete and confirm applied=true.",
            ),
          }
        }
        const resumed = yield* applyWorkflowControl(
          workflowID,
          workflowToolResumeBlock(input.message ?? "workflow tool requested resume"),
          "workflow tool",
          {
            sourceSessionID: input.sourceSessionID,
            sourceAgent: input.sourceAgent,
            deferStart: true,
          },
        )
        return resumed.afterCommit
          ? workflowCommandAfterCommit(
              { workflowID, applied: true, message: "Workflow resume was applied and scheduling was requested." },
              resumed.afterCommit,
            )
          : { workflowID, applied: true, message: "Workflow resume was applied." }
      }
      if (input.action === "block") {
        yield* cancelWorkflowRuns(workflowID)
        yield* blockActiveMilestones(workflowID)
        yield* blockWorkflow(workflow, `Workflow tool block: ${input.message ?? "blocked"}`).pipe(Effect.ignore)
        return { workflowID, applied: true, message: "Workflow was blocked and active work was cancelled." }
      }
      if (input.action === "update_xml") {
        if (!input.xml?.trim()) {
          return {
            workflowID,
            ...workflowCommandRejection("precondition_failed", "update_xml requires XML."),
          }
        }
        const xml = input.xml
        const parsed = yield* Effect.try({
          try: () => parseXmlDefinition(xml, workflow),
          catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
        }).pipe(
          Effect.map((definition) => ({ definition })),
          Effect.catch((error: Error) => Effect.succeed({ error })),
        )
        if (parsed.error) {
          yield* blockWorkflow(workflow, `Workflow tool XML update failed: ${parsed.error.message}`).pipe(Effect.ignore)
          return {
            workflowID,
            ...workflowCommandRejection("invalid_xml", `XML update failed validation: ${parsed.error.message}`),
          }
        }
        const definition = parsed.definition
        yield* writeNote(workflowArtifactPath(workflow, "workflow.xml"), xml)
        yield* saveDefinition(workflowID, xml, definition, "dispatching")
        yield* writePrecreatedPlans(yield* get(workflowID), yield* milestones(workflowID))
        return workflowCommandAfterCommit(
          { workflowID, applied: true, message: "Workflow XML was saved, validated, and scheduling was requested." },
          schedule(workflowID, { bypassStagedGate: true }),
        )
      }
      if (input.action === "milestone_status") {
        if (!input.milestoneID || !input.milestoneStatus) {
          return {
            workflowID,
            ...workflowCommandRejection("precondition_failed", "milestone_status requires milestoneID and milestoneStatus."),
          }
        }
        const current = (yield* milestones(workflowID)).find((item) => item.id === input.milestoneID)
        if (!current) {
          return {
            workflowID,
            ...workflowCommandRejection("unknown_milestone", `Milestone ${input.milestoneID} was not found.`),
          }
        }
        const targetStatus = canonicalMilestoneStatus(input.milestoneStatus)
        const currentStatus = canonicalMilestoneStatus(current.status)
        if (targetStatus === currentStatus) {
          return {
            workflowID,
            applied: true,
            message: `Milestone ${input.milestoneID} is already ${targetStatus}.`,
          }
        }
        if (
          currentStatus === "planning" &&
          sourceRole === "department_pm" &&
          (targetStatus === "approved" || targetStatus === "done") &&
          current.session.some(
            (ref) =>
              ref.role === "department_pm" &&
              ref.sessionID === input.sourceSessionID &&
              ref.milestoneID === current.id,
          )
        ) {
          if (yield* activeMilestoneRunIsCurrent(workflowID, input.milestoneID, options?.currentJobID)) {
            return {
              workflowID,
              applied: true,
              message: `Milestone ${input.milestoneID} accepted milestone_status=${input.milestoneStatus} from its owning department PM as plan_complete; the active milestone run will continue to executor dispatch.`,
            }
          }
          if (yield* hasActiveMilestoneRun(workflowID, input.milestoneID)) {
            yield* cancelMilestoneRuns(workflowID, input.milestoneID)
          }
          yield* updateMilestone(workflowID, input.milestoneID, { status: "pending" })
          yield* setStatus(workflowID, "executing", { error: "" }).pipe(Effect.ignore)
          return workflowCommandAfterCommit(
            {
              workflowID,
              applied: true,
              message: `Milestone ${input.milestoneID} accepted milestone_status=${input.milestoneStatus} from its owning department PM as plan_complete and scheduling was requested for executor dispatch.`,
            },
            queueWorkflowSchedule(workflowID, "owning department PM milestone_status alias"),
          )
        }
        if (
          currentStatus === "planning" &&
          (sourceRole === "requester" || sourceRole === "main_pm") &&
          (targetStatus === "approved" || targetStatus === "done")
        ) {
          yield* cancelMilestoneRuns(workflowID, input.milestoneID)
          const result = yield* applyMilestoneToolStatus(workflowID, input.milestoneID, "done")
          return {
            ...result,
            message: `Milestone ${input.milestoneID} accepted milestone_status=${input.milestoneStatus} from ${sourceRoleName} as force_complete for a planning gate, cancelled stale active runs, and requested scheduling.`,
          }
        }
        const allowed = legalMilestoneTransitions(current.status).map(canonicalMilestoneStatus)
        if (!allowed.includes(targetStatus)) {
          const guidance = milestoneTransitionGuidance({ currentStatus: current.status, targetStatus })
          return {
            workflowID,
            ...workflowCommandRejection(
              "illegal_transition",
              `Milestone ${input.milestoneID} cannot transition from ${current.status} to ${input.milestoneStatus}.${guidance}`,
              [...new Set(allowed)],
            ),
          }
        }
        yield* cancelMilestoneRuns(workflowID, input.milestoneID)
        return yield* applyMilestoneToolStatus(workflowID, input.milestoneID, targetStatus)
      }
      if (input.action === "plan_complete") {
        if (!input.milestoneID) {
          return {
            workflowID,
            ...workflowCommandRejection("precondition_failed", "plan_complete requires milestoneID."),
          }
        }
        if (!sourceRole || !["requester", "main_pm", "department_pm"].includes(sourceRole)) {
          return {
            workflowID,
            ...workflowCommandRejection(
              "not_authorized",
              `Role ${sourceRoleName} cannot complete milestone planning. Ask the department PM or main PM to close the plan gate.`,
            ),
          }
        }
        const current = (yield* milestones(workflowID)).find((item) => item.id === input.milestoneID)
        if (!current) {
          return {
            workflowID,
            ...workflowCommandRejection("unknown_milestone", `Milestone ${input.milestoneID} was not found.`),
          }
        }
        if (!["pending", "planning", "blocked"].includes(current.status)) {
          return {
            workflowID,
            ...workflowCommandRejection(
              "illegal_transition",
              `Milestone ${input.milestoneID} cannot apply plan_complete from ${current.status}.`,
              ["pending", "planning", "blocked"],
            ),
          }
        }
        if (
          sourceRole === "department_pm" &&
          (yield* activeMilestoneRunIsCurrent(workflowID, input.milestoneID, options?.currentJobID))
        ) {
          return {
            workflowID,
            applied: true,
            message: `Milestone ${input.milestoneID} planning gate was acknowledged; the active milestone run will continue to executor dispatch.`,
          }
        }
        const staleActiveRun = yield* hasActiveMilestoneRun(workflowID, input.milestoneID)
        if (staleActiveRun) {
          yield* cancelMilestoneRuns(workflowID, input.milestoneID)
        }
        if (current.status !== "pending") yield* updateMilestone(workflowID, input.milestoneID, { status: "pending" })
        yield* setStatus(workflowID, "executing", { error: "" }).pipe(Effect.ignore)
        return workflowCommandAfterCommit(
          {
            workflowID,
            applied: true,
            message:
              sourceRole === "department_pm"
                ? staleActiveRun
                  ? `Milestone ${input.milestoneID} planning gate was acknowledged, stale active planning runs were cancelled, and scheduling was requested for executor dispatch.`
                  : `Milestone ${input.milestoneID} planning gate was acknowledged and scheduling was requested for executor dispatch.`
                : `Milestone ${input.milestoneID} planning gate was acknowledged by ${sourceRoleName}, stale active planning runs were cancelled, and scheduling was requested for executor dispatch.`,
          },
          queueWorkflowSchedule(workflowID, "plan_complete"),
        )
      }
      if (input.action === "force_complete" || input.action === "force_skip") {
        if (!input.milestoneID) {
          return {
            workflowID,
            ...workflowCommandRejection("precondition_failed", `${input.action} requires milestoneID.`),
          }
        }
        if (!sourceRole || !["requester", "main_pm"].includes(sourceRole)) {
          return {
            workflowID,
            ...workflowCommandRejection(
              "not_authorized",
              `Role ${sourceRoleName} cannot ${input.action}. Ask requester or main PM to use this override.`,
            ),
          }
        }
        const current = (yield* milestones(workflowID)).find((item) => item.id === input.milestoneID)
        if (!current) {
          return {
            workflowID,
            ...workflowCommandRejection("unknown_milestone", `Milestone ${input.milestoneID} was not found.`),
          }
        }
        if (terminalMilestone(current.status) && current.status !== "approved") {
          return {
            workflowID,
            applied: true,
            message: `Milestone ${input.milestoneID} is already terminal (${current.status}).`,
          }
        }
        yield* cancelMilestoneRuns(workflowID, input.milestoneID)
        const status = input.action === "force_complete" ? "done" : "skipped"
        const result = yield* applyMilestoneToolStatus(workflowID, input.milestoneID, status)
        return {
          ...result,
          message: `Milestone ${input.milestoneID} was ${status === "done" ? "force-completed" : "force-skipped"} by ${sourceRoleName} and scheduling was requested.`,
        }
      }
      if (input.action === "workflow_status") {
        if (!input.workflowStatus) {
          return {
            workflowID,
            ...workflowCommandRejection("precondition_failed", "workflow_status requires workflowStatus."),
          }
        }
        if (input.workflowStatus === "blocked") {
          yield* blockWorkflow(workflow, `Workflow tool status block: ${input.message ?? "blocked"}`).pipe(Effect.ignore)
          return { workflowID, applied: true, message: "Workflow was blocked." }
        }
        yield* setStatus(workflowID, input.workflowStatus, { error: "" }).pipe(Effect.ignore)
        if (["pending", "running", "planning", "dispatching", "executing", "reviewing"].includes(input.workflowStatus)) {
          const resumed = yield* applyWorkflowControl(
            workflowID,
            workflowToolResumeBlock(input.message ?? `workflow tool set status ${input.workflowStatus}`),
            "workflow tool status",
            {
              sourceSessionID: input.sourceSessionID,
              sourceAgent: input.sourceAgent,
              deferStart: true,
            },
          )
          if (resumed.afterCommit) {
            return workflowCommandAfterCommit(
              { workflowID, applied: true, message: `Workflow status was set to ${input.workflowStatus} and scheduling was requested.` },
              resumed.afterCommit,
            )
          }
        }
        return { workflowID, applied: true, message: `Workflow status was set to ${input.workflowStatus}.` }
      }
      if (input.action === "complete") {
        const items = yield* milestones(workflowID)
        if (!items.every((item) => ["approved", "done", "completed", "skipped", "testing"].includes(item.status))) {
          yield* setStatus(workflowID, "executing", { error: "" }).pipe(Effect.ignore)
          return workflowCommandAfterCommit(
            { workflowID, applied: true, message: "Workflow still has unfinished milestones; scheduling was requested." },
            queueWorkflowSchedule(workflowID, "complete unfinished"),
          )
        }
        for (const item of items.filter((item) => item.status === "approved" || item.status === "testing")) {
          yield* updateMilestone(workflowID, item.id, { status: "done" })
        }
        yield* setStatus(workflowID, "completed", { error: "" }).pipe(Effect.ignore)
        return { workflowID, applied: true, message: "Workflow was completed." }
      }
      return {
        workflowID,
        ...workflowCommandRejection("precondition_failed", `Unsupported workflow action: ${input.action}.`),
      }
    })

    const publishWorkflowToolCommandResult = Effect.fn("Workflow.publishWorkflowToolCommandResult")(function* (
      input: WorkflowToolCommand,
      result: { workflowID?: WorkflowID; applied: boolean; message: string; rejection?: WorkflowToolCommandRejection },
    ) {
      if (!input.id) return
      yield* bus.publish(WorkflowToolCommandResultEvent, workflowToolCommandResultPayload(input, result))
    })

    const workflowToolCommandResultPayload = (
      input: WorkflowToolCommand,
      result: { workflowID?: WorkflowID; applied: boolean; message: string; rejection?: WorkflowToolCommandRejection },
    ) => ({
      id: input.id ?? Bus.createID(),
      action: input.action,
      applied: result.applied,
      ...(result.workflowID ? { workflowID: result.workflowID } : {}),
      ...(result.rejection ? { rejection: result.rejection } : {}),
      message: result.message,
    })

    const workflowToolCommandSnapshot = Effect.fn("Workflow.workflowToolCommandSnapshot")(function* (
      workflowID: WorkflowID,
      input: WorkflowToolCommand,
    ) {
      const workflow = yield* get(workflowID)
      const milestone = input.milestoneID
        ? (yield* milestones(workflowID)).find((item) => item.id === input.milestoneID)
        : undefined
      return {
        workflowStatus: workflow.status,
        ...(milestone ? { milestoneStatus: milestone.status } : {}),
      }
    })

    const appendWorkflowToolCommandJournal = Effect.fn("Workflow.appendWorkflowToolCommandJournal")(function* (
      input: WorkflowToolCommand,
      result: { workflowID?: WorkflowID; applied: boolean; message: string; rejection?: WorkflowToolCommandRejection },
      before?: { workflowStatus?: string; milestoneStatus?: string },
      after?: { workflowStatus?: string; milestoneStatus?: string },
    ) {
      const workflowID = result.workflowID ?? input.workflowID
      if (!workflowID) return
      const workflow = yield* get(workflowID)
      const ctx = yield* InstanceState.context
      const file = projectWorkflowPath(ctx.directory, workflow, workflowCommandJournalPath())
      const existing = yield* Effect.promise(() => readFile(file, "utf8")).pipe(
        Effect.catchCause(() => Effect.succeed("")),
      )
      const sourceRole = yield* workflowToolCommandSourceRole(workflow, input.sourceSessionID).pipe(
        Effect.catchCause(() => Effect.succeed(undefined)),
      )
      yield* Effect.promise(() =>
        appendFileEnsured(
          file,
          `${JSON.stringify({
            seq: existing.trim() ? existing.trim().split(/\r?\n/).length + 1 : 1,
            id: input.id ?? `anonymous-${Date.now().toString(36)}`,
            ts: new Date().toISOString(),
            source: {
              sessionID: input.sourceSessionID,
              role: sourceRole ?? "unknown",
              ...(input.sourceAgent ? { agent: input.sourceAgent } : {}),
            },
            action: input.action,
            ...(input.milestoneID ? { milestoneID: input.milestoneID } : {}),
            from: before ?? {},
            to: after ?? {},
            outcome: result.applied ? "applied" : "rejected",
            ...(result.rejection ? { rejection: result.rejection } : {}),
            message: result.message,
          })}\n`,
        ),
      )
    })

    const replayWorkflowToolCommandJournalResult = Effect.fn("Workflow.replayWorkflowToolCommandJournalResult")(function* (
      workflowID: WorkflowID,
      input: WorkflowToolCommand,
    ) {
      if (!input.id) return
      const workflow = yield* get(workflowID)
      const ctx = yield* InstanceState.context
      const text = yield* Effect.promise(() =>
        readFile(projectWorkflowPath(ctx.directory, workflow, workflowCommandJournalPath()), "utf8"),
      ).pipe(Effect.catchCause(() => Effect.succeed("")))
      const rows = yield* Effect.all(
        text
          .trim()
          .split(/\r?\n/)
          .filter(Boolean)
          .map((line) =>
            Effect.try({
              try: () => JSON.parse(line),
              catch: () => undefined,
            }).pipe(Effect.catchCause(() => Effect.succeed(undefined))),
          ),
      )
      const row = rows.find((item) => item?.id === input.id)
      if (!row) return
      const action = typeof row.action === "string" ? row.action : input.action
      const result = {
        workflowID,
        applied: row.outcome === "applied",
        message:
          typeof row.message === "string"
            ? row.message
            : `Workflow command ${action} was previously ${row.outcome === "applied" ? "applied" : "rejected"}.`,
        ...(row.rejection ? { rejection: row.rejection } : {}),
      }
      return {
        input: { ...input, action, workflowID },
        result,
      }
    })

    const applyAndPublishWorkflowToolCommand = Effect.fn("Workflow.applyAndPublishWorkflowToolCommand")(function* (
      input: WorkflowToolCommand,
      workflowID: WorkflowID | undefined,
      pending?: ReturnType<typeof workflowToolCommandDeferred>,
      options?: { currentJobID?: string | string[] },
    ) {
      const journalReplay =
        input.id && workflowID
          ? yield* replayWorkflowToolCommandJournalResult(workflowID, input).pipe(
              Effect.catchCause(() => Effect.succeed(undefined)),
            )
          : undefined
      if (input.id && pending && journalReplay) {
        rememberWorkflowToolCommandResult(input.id, journalReplay)
        workflowToolCommandInflight.delete(input.id)
        pending.resolve(journalReplay)
        yield* publishWorkflowToolCommandResult(journalReplay.input, journalReplay.result).pipe(Effect.ignore)
        return journalReplay
      }
      const before = workflowID
        ? yield* workflowToolCommandSnapshot(workflowID, input).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
        : undefined
      const result = yield* applyWorkflowToolCommand(input, options).pipe(
        Effect.catchCause((cause) => {
          if (workflowCommandDurabilityFailure(cause)) return Effect.failCause(cause)
          const message = `Workflow command failed: ${errorFromCause(cause)}`
          return Effect.succeed({
            ...((workflowID ?? input.workflowID) ? { workflowID: workflowID ?? input.workflowID } : {}),
            ...workflowCommandRejection("precondition_failed", message),
          })
        }),
      )
      const afterWorkflowID = result.workflowID ?? workflowID
      const after = afterWorkflowID
        ? yield* workflowToolCommandSnapshot(afterWorkflowID, input).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
        : undefined
      const afterCommit = result.afterCommit
      const replayResult = { ...result }
      delete replayResult.afterCommit
      const replay = { input, result: replayResult }
      yield* appendWorkflowToolCommandJournal(input, replayResult, before, after)
      yield* publishWorkflowToolCommandResult(input, replayResult).pipe(Effect.ignore)
      if (afterCommit) yield* afterCommit.pipe(Effect.ignore)
      if (input.id && pending) {
        rememberWorkflowToolCommandResult(input.id, replay)
        workflowToolCommandInflight.delete(input.id)
        pending.resolve(replay)
      }
      return replay
    })

    const handleWorkflowToolCommand = Effect.fn("Workflow.handleWorkflowToolCommand")(function* (
      input: WorkflowToolCommand,
      options?: { currentJobID?: string | string[] },
    ) {
      const cached = input.id ? workflowToolCommandResults.get(input.id) : undefined
      if (cached) {
        yield* publishWorkflowToolCommandResult(cached.input, cached.result).pipe(Effect.ignore)
        return cached
      }
      const inflight = input.id ? workflowToolCommandInflight.get(input.id) : undefined
      if (inflight) {
        const replay = yield* Effect.promise(() => inflight.promise)
        yield* publishWorkflowToolCommandResult(replay.input, replay.result).pipe(Effect.ignore)
        return replay
      }
      const pending = input.id ? workflowToolCommandDeferred() : undefined
      if (input.id && pending) workflowToolCommandInflight.set(input.id, pending)
      const workflowID = yield* workflowToolCommandWorkflowID(input).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
      const run = applyAndPublishWorkflowToolCommand(input, workflowID, pending, options).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            if (input.id && pending) {
              yield* Effect.sync(() => {
                workflowToolCommandInflight.delete(input.id)
                pending.reject(new globalThis.Error(errorFromCause(cause)))
              })
            }
            return yield* Effect.failCause(cause)
          }),
        ),
      )
      if (!workflowID) return yield* run
      return yield* withWorkflowToolCommandQueue(workflowID, run)
    })

    const dispatchCommand = Effect.fn("Workflow.dispatchCommand")(function* (input: WorkflowToolCommand) {
      yield* InstanceState.get(initState)
      const command = input.id ? input : { ...input, id: Bus.createID() }
      const replay = yield* handleWorkflowToolCommand(command)
      return workflowToolCommandResultPayload(replay.input, replay.result)
    })

    const updateWorkflowFileError = Effect.fn("Workflow.updateWorkflowFileError")(function* (
      workflowID: WorkflowID,
      message: string,
    ) {
      Database.use((db) =>
        db
          .update(WorkflowTable)
          .set({ error: message, time_updated: Date.now() })
          .where(eq(WorkflowTable.id, workflowID))
          .run(),
      )
      yield* publishUpdated(workflowID).pipe(Effect.ignore)
    })

    const workflowForFile = Effect.fn("Workflow.workflowForFile")(function* (file: string) {
      const ctx = yield* InstanceState.context
      return Database.use((db) =>
        db
          .select()
          .from(WorkflowTable)
          .where(eq(WorkflowTable.project_id, ctx.project.id))
          .all()
          .map(toInfo)
          .filter((workflow) => containedPath(path.resolve(workflow.directory, workflow.path), file))
          .toSorted((a, b) => b.path.length - a.path.length)
          .at(0),
      )
    })

    const refreshActiveWorkflowsForContextFile = Effect.fn("Workflow.refreshActiveWorkflowsForContextFile")(function* () {
      const ctx = yield* InstanceState.context
      const workflows = Database.use((db) =>
        db
          .select()
          .from(WorkflowTable)
          .where(eq(WorkflowTable.project_id, ctx.project.id))
          .all()
          .map(toInfo)
          .filter((workflow) => !["cancelled", "completed"].includes(workflow.status)),
      )
      for (const workflow of workflows) {
        yield* publishUpdated(workflow.id).pipe(Effect.ignore)
      }
    })

    const handleWorkflowPlanFileUpdate = Effect.fn("Workflow.handleWorkflowPlanFileUpdate")(function* (
      workflow: WorkflowInfo,
      workflowRelativePath: string,
      file: string,
    ) {
      const milestoneID = workflowPlanFileMilestoneID(workflowRelativePath)
      if (!milestoneID) return false
      const items = yield* milestones(workflow.id)
      const milestone = items.find((item) => item.id === milestoneID)
      if (!milestone || milestone.status !== "planning") return false
      const text = yield* Effect.promise(() => readFile(file, "utf8")).pipe(Effect.catchCause(() => Effect.succeed("")))
      if (!text.trim()) return false
      const owner = milestone.session
        .filter((ref) => ref.role === "department_pm" && ref.milestoneID === milestone.id)
        .toSorted((a, b) => (b.attempt ?? 0) - (a.attempt ?? 0))
        .at(0)
      const hasDownstream = items.some((item) => item.dependsOn.some((dependency) => dependency === milestone.id))
      const closeGate = hasDownstream && workflowPlanClaimsClosedGate(text)
      const sourceSessionID = closeGate
        ? workflow.pmSessionID ?? workflow.rootSessionID ?? owner?.sessionID
        : owner?.sessionID ?? workflow.pmSessionID ?? workflow.rootSessionID
      if (!sourceSessionID) return false
      if (parseWorkflowControlCommand(text) || implicitWorkflowResume(text)) {
        yield* applyWorkflowControl(workflow.id, text, `workflow plan file ${workflowRelativePath}`, {
          sourceSessionID,
          sourceAgent: "workflow-file-watcher",
        }).pipe(Effect.ignore)
        return true
      }
      if (!hasHandoffSummary(text)) return false
      const replay = yield* handleWorkflowToolCommand({
        id: Bus.createID(),
        action: closeGate ? "force_complete" : "plan_complete",
        workflowID: workflow.id,
        sourceSessionID,
        sourceAgent: "workflow-file-watcher",
        milestoneID,
        message: closeGate
          ? `Workflow file watcher observed completed gate plan ${workflowRelativePath} and closed it to unblock downstream work.`
          : `Workflow file watcher observed completed plan ${workflowRelativePath} and requested milestone continuation.`,
      })
      return replay.result.applied
    })

    const handleWorkflowFileUpdate = Effect.fn("Workflow.handleWorkflowFileUpdate")(function* (input: {
      file: string
      event: "add" | "change" | "unlink"
    }) {
      const ctx = yield* InstanceState.context
      const relative = normalizedRelativePath(ctx.directory, input.file)
      if (!relative) return
      if (!path.normalize(relative).startsWith(workflowDir + path.sep) && !codexContextFile(relative)) return
      const file = path.resolve(ctx.directory, relative)
      yield* Effect.sleep("350 millis")
      const info =
        input.event === "unlink"
          ? undefined
          : yield* Effect.promise(() => stat(file)).pipe(Effect.catch(() => Effect.succeed(undefined)))
      if (info && !info.isFile()) return
      const signature = workflowFileSignature(input.event, file, info?.size, info?.mtimeMs)
      if (observedWorkflowFiles.get(file) === signature) return
      observedWorkflowFiles.set(file, signature)
      if (codexContextFile(relative)) {
        yield* refreshActiveWorkflowsForContextFile()
        return
      }
      if (isWorkflowStatePath(relative)) {
        yield* syncWorkflowStateFile(file).pipe(Effect.ignore)
        return
      }

      const workflow = yield* workflowForFile(file)
      if (!workflow) return
      const workflowRelative = workflowRelativeFile(workflow, file)
      if (!workflowRelative) return
      const workflowRelativePath = path.normalize(workflowRelative)
      if (workflowRelativePath !== "workflow.xml" && !workflowPipelineItemPaths(workflow.xml).some((item) => path.normalize(item) === workflowRelativePath)) {
        if (
          input.event !== "unlink" &&
          (yield* handleWorkflowPlanFileUpdate(workflow, workflowRelativePath, file).pipe(Effect.catchCause(() => Effect.succeed(false))))
        ) {
          return
        }
        yield* publishUpdated(workflow.id).pipe(Effect.ignore)
        return
      }
      if (input.event === "unlink") {
        yield* updateWorkflowFileError(
          workflow.id,
          workflowRelativePath === "workflow.xml"
            ? "workflow.xml was deleted; keeping the last valid workflow graph"
            : `pipeline items file ${workflowRelative} was deleted; keeping the last valid workflow graph`,
        )
        return
      }
      const xmlFile = workflowRelativePath === "workflow.xml" ? file : projectWorkflowPath(ctx.directory, workflow, "workflow.xml")
      const xml = yield* Effect.promise(() => readFile(xmlFile, "utf8")).pipe(
        Effect.catch((error: unknown) =>
          updateWorkflowFileError(
            workflow.id,
            `workflow.xml could not be read after ${workflowRelative} changed: ${
              error instanceof globalThis.Error ? error.message : String(error)
            }`,
          ).pipe(Effect.as(undefined as string | undefined)),
        ),
      )
      if (!xml) return
      const definition = yield* Effect.try({
        try: () => parseXmlDefinition(xml, workflow),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      }).pipe(
        Effect.catch((error: Error) =>
          updateWorkflowFileError(workflow.id, `workflow.xml validation failed: ${error.message}`).pipe(
            Effect.as(undefined as WorkflowDefinition | undefined),
          ),
        ),
      )
      if (!definition) return
      yield* saveDefinition(
        workflow.id,
        xml,
        definition,
        ["cancelled", "completed"].includes(workflow.status) ? workflow.status : "dispatching",
      )
      yield* writePrecreatedPlans(yield* get(workflow.id), yield* milestones(workflow.id))
      yield* publishUpdated(workflow.id).pipe(Effect.ignore)
    })

    const runPlanning = Effect.fn("Workflow.runPlanning")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      yield* ensureCompany(workflow)
      const main = yield* ensureCompanyMember({
        workflow,
        role: "main_pm",
        specialty: "strategy",
        title: workflowSessionTitle("Main PM", workflow.title),
        prompt: workflow.request,
      })
      if (!main) return yield* blockWorkflow(workflow, "Workflow has no available main product manager")
      yield* setMainProductManagerSession(workflowID, main.sessionID)
      const mainPrompt = promptMainPm({ workflow: { ...workflow, pmSessionID: main.sessionID } })
      yield* runPrompt(main.sessionID, "workflow-main-pm", workflow.model, mainPrompt, {
        workflowID,
        role: "main_pm",
      }, { expect: mainPlanningExpectation() })
      yield* continuePlanning(workflowID)
    })

    const resumeActiveWorkflowsOnStartup = Effect.fn("Workflow.resumeActiveWorkflowsOnStartup")(function* () {
      if (!workflowAutorunEnabled()) return
      const ctx = yield* InstanceState.context
      const activeStatuses: WorkflowInfo["status"][] = [
        "pending",
        "running",
        "planning",
        "dispatching",
        "executing",
        "reviewing",
        "testing",
        "accepting",
      ]
      const activeJobs = new Set(
        (yield* background.list())
          .filter((job) => job.status === "running" && job.metadata?.workflowID)
          .map((job) => String(job.metadata?.workflowID)),
      )
      const workflows = Database.use((db) =>
        db
          .select()
          .from(WorkflowTable)
          .where(and(eq(WorkflowTable.project_id, ctx.project.id), inArray(WorkflowTable.status, activeStatuses)))
          .all()
          .map(toInfo),
      )
      const startupResumeStarted = Date.now()
      // Recovery can overlap a fresh workflow.start before every artifact is visible on disk.
      for (const workflow of workflows.filter(
        (item) => !activeJobs.has(item.id) && startupResumeStarted - item.time.created > 5_000,
      )) {
        yield* applyWorkflowControl(
          workflow.id,
          workflowToolResumeBlock("opencode startup resume"),
          "opencode startup",
        ).pipe(Effect.catchCause(() => Effect.void))
      }
    })

    const handleMessageUpdated = Effect.fn("Workflow.handleMessageUpdated")(function* (properties: {
      sessionID: SessionID
      info: MessageV2.Info
    }) {
      const info = properties.info
      if (info.role === "user") {
        yield* Effect.all(
          [
            observeRequesterMessage({
              sessionID: properties.sessionID,
              messageID: info.id,
            }),
            observeWorkflowUserMessage({
              sessionID: properties.sessionID,
              messageID: info.id,
            }),
          ],
          { discard: true },
        )
        return
      }
      if (!info.time.completed || info.error) return
      yield* observeWorkflowMessage({
        sessionID: properties.sessionID,
        messageID: info.id,
        agent: info.agent,
        model: {
          providerID: info.providerID,
          modelID: info.modelID,
          ...(info.variant ? { variant: info.variant } : {}),
        },
      })
    })

    const initState = yield* InstanceState.make(
      Effect.fn("Workflow.initState")(function* () {
        const instance = yield* InstanceState.context
        const bridge = yield* EffectBridge.make()
        const unregisterWorkflowToolCommandDispatcher = registerWorkflowToolCommandDispatcher(instance.directory, (input) =>
          bridge.promise(dispatchCommand(input)),
        )
        const unregisterWorkflowMessageSendDispatcher = registerWorkflowMessageSendDispatcher(instance.directory, (input) =>
          bridge.promise(dispatchMessageSend(input)),
        )
        const globalWorkflowCommandHandler = (event: { directory?: string; payload?: { type?: string; properties?: WorkflowToolCommand } }) => {
          if (event.directory && event.directory !== instance.directory) return
          if (event.payload?.type !== WorkflowToolCommandEvent.type) return
          if (!event.payload.properties?.id) return
          bridge.fork(handleWorkflowToolCommand(event.payload.properties))
        }
        const globalMessageUpdatedHandler = (event: { directory?: string; payload?: { type?: string; properties?: { sessionID: SessionID; info: MessageV2.Info } } }) => {
          if (event.directory && event.directory !== instance.directory) return
          if (event.payload?.type !== MessageV2.Event.Updated.type) return
          bridge.fork(handleMessageUpdated(event.payload.properties))
        }
        GlobalBus.on("event", globalWorkflowCommandHandler)
        GlobalBus.on("event", globalMessageUpdatedHandler)
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            unregisterWorkflowToolCommandDispatcher()
            unregisterWorkflowMessageSendDispatcher()
            GlobalBus.off("event", globalWorkflowCommandHandler)
            GlobalBus.off("event", globalMessageUpdatedHandler)
          }),
        )
        yield* syncWorkflowStatesFromDisk().pipe(Effect.ignore)
        yield* (yield* bus.subscribe(WorkflowToolCommandEvent)).pipe(
          Stream.runForEach((payload) => handleWorkflowToolCommand(payload.properties).pipe(Effect.ignore)),
          Effect.forkScoped,
        )
        yield* (yield* bus.subscribe(FileWatcher.Event.Updated)).pipe(
          Stream.runForEach((payload) =>
            handleWorkflowFileUpdate(payload.properties).pipe(Effect.catchCause(() => Effect.void)),
          ),
          Effect.forkScoped,
        )
        yield* (yield* bus.subscribe(SessionStatus.Event.Idle)).pipe(
          Stream.runForEach((payload) =>
            Effect.gen(function* () {
              const workflow = Database.use((db) =>
                db
                  .select()
                  .from(WorkflowTable)
                  .where(eq(WorkflowTable.pm_session_id, payload.properties.sessionID))
                  .get() ??
                (() => {
                  const member = db
                    .select()
                    .from(WorkflowMemberTable)
                    .where(
                      and(
                        eq(WorkflowMemberTable.session_id, payload.properties.sessionID),
                        eq(WorkflowMemberTable.role, "main_pm"),
                      ),
                    )
                    .orderBy(asc(WorkflowMemberTable.time_updated))
                    .all()
                    .at(-1)
                  return member
                    ? db
                        .select()
                        .from(WorkflowTable)
                        .where(eq(WorkflowTable.id, member.workflow_id))
                        .get()
                    : undefined
                })(),
              )
              if (workflow && ["planning", "dispatching", "blocked", "failed"].includes(workflow.status)) {
                yield* archiveWorkflowSession({
                  workflowID: workflow.id,
                  sessionID: payload.properties.sessionID,
                  role: "main_pm",
                }).pipe(Effect.ignore)
                const latest = (yield* session.messages({ sessionID: payload.properties.sessionID, limit: 8 })).find(
                  (item) => item.info.role === "assistant" && item.info.time.completed && !item.info.error,
                )
                const text = latest ? messageText(latest) : ""
                if (workflowDispatchClaimWithoutControl(text, "main_pm")) {
                  const workflowInfo = toInfo(workflow)
                  const inferredControl = workflowInferredDispatchControl({
                    text,
                    role: "main_pm",
                    milestones: yield* milestones(workflow.id),
                  })
                  if (inferredControl) {
                    const replay = yield* handleWorkflowToolCommand({
                      ...inferredControl,
                      id: Bus.createID(),
                      workflowID: workflow.id,
                      sourceSessionID: payload.properties.sessionID,
                      sourceAgent: "workflow-main-pm",
                    }).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
                    if (replay?.result?.applied) return
                  }
                  const correction = latestText(
                    yield* runPrompt(
                      payload.properties.sessionID,
                      "workflow-main-pm",
                      workflowInfo.model,
                      workflowDispatchCorrectionPrompt({
                        workflow: workflowInfo,
                        role: "main_pm",
                        previous: text,
                      }),
                      {
                        workflowID: workflow.id,
                        role: "main_pm",
                      },
                      { consult: false, expect: workflowDispatchCorrectionExpectation("main_pm") },
                    ),
                  )
                  if (
                    yield* applyWorkflowUpdateFromOutput({
                      workflowID: workflow.id,
                      role: "main_pm",
                      output: correction,
                    })
                  ) {
                    yield* queueContinuePlanning(yield* get(workflow.id), "main PM idle dispatch correction workflow update").pipe(
                      Effect.ignore,
                    )
                    return
                  }
                  if (parseWorkflowControlCommand(correction) || implicitWorkflowResume(correction)) {
                    yield* applyWorkflowControl(workflow.id, correction, "main PM idle dispatch correction", {
                      sourceSessionID: payload.properties.sessionID,
                      sourceAgent: "workflow-main-pm",
                    }).pipe(Effect.ignore)
                    return
                  }
                  const correctionConsults = parseConsultRequests(correction)
                  if (correctionConsults.length > 0 && !workflowDispatchClaimWithoutControl(correction, "main_pm")) {
                    yield* resolveConsultRequests(
                      workflow.id,
                      payload.properties.sessionID,
                      "workflow-main-pm",
                      workflowInfo.model,
                      "main_pm",
                      undefined,
                      undefined,
                      correction,
                    ).pipe(Effect.ignore)
                    yield* advanceWorkflowAfterSession(workflow.id, "main PM idle dispatch correction consultation completed")
                    return
                  }
                  yield* blockPlanning(
                    workflow.id,
                    "Main PM session became idle after claiming dispatch without a confirmed workflow control result.",
                  )
                  return
                }
                if (text && implicitWorkflowResume(text)) {
                  yield* applyWorkflowControl(workflow.id, text, "main PM session idle", {
                    sourceSessionID: payload.properties.sessionID,
                    sourceAgent: "workflow-main-pm",
                  }).pipe(Effect.ignore)
                  return
                }
                if (yield* queueContinuePlanning(toInfo(workflow), "main PM session idle")) return
              }
              const context = yield* workflowSessionContext(payload.properties.sessionID)
              if (!context?.milestoneID) return
              if (["blocked", "cancelled", "completed", "failed"].includes(context.workflow.status)) return
              const contextItems = yield* milestones(context.workflow.id)
              const active = contextItems.find((item) => item.id === context.milestoneID)
              if (!active || ["approved", "completed", "done", "skipped", "cancelled"].includes(active.status)) return
              const running = (yield* background.list()).some(
                (job) =>
                  job.status === "running" &&
                  job.type === "workflow.milestone" &&
                  job.metadata?.workflowID === context.workflow.id &&
                  job.metadata?.milestoneID === active.id,
              )
              if (!running) {
                const expectation = workflowSessionExpectation({
                  role: context.role,
                  milestoneID: context.milestoneID,
                  milestoneStatus: active.status,
                  workflowStatus: context.workflow.status,
                })
                const latest = (yield* session.messages({ sessionID: payload.properties.sessionID, limit: 8 })).find(
                  (item) => item.info.role === "assistant" && item.info.time.completed && !item.info.error,
                )
                const text = latest ? messageText(latest) : ""
                if (workflowDispatchClaimWithoutControl(text, context.role)) {
                  const inferredControl = workflowInferredDispatchControl({
                    text,
                    role: context.role,
                    milestoneID: context.milestoneID,
                    milestoneStatus: active.status,
                    milestones: contextItems,
                  })
                  if (inferredControl) {
                    const replay = yield* handleWorkflowToolCommand({
                      ...inferredControl,
                      id: Bus.createID(),
                      workflowID: context.workflow.id,
                      sourceSessionID: payload.properties.sessionID,
                      sourceAgent: workflowAgentForRole(context.role),
                    }).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
                    if (replay?.result?.applied) return
                  }
                  const info = yield* session
                    .get(payload.properties.sessionID)
                    .pipe(Effect.mapError((error) => new Error({ message: error.message })))
                  const correction = latestText(
                    yield* runPrompt(
                      payload.properties.sessionID,
                      info.agent ?? workflowAgentForRole(context.role),
                      context.workflow.model,
                      workflowDispatchCorrectionPrompt({
                        workflow: context.workflow,
                        role: context.role,
                        milestoneID: context.milestoneID,
                        previous: text,
                      }),
                      {
                        workflowID: context.workflow.id,
                        role: context.role,
                        milestoneID: context.milestoneID,
                        attempt: context.attempt,
                      },
                      { consult: false, expect: workflowDispatchCorrectionExpectation(context.role) },
                    ),
                  )
                  if (
                    yield* applyWorkflowUpdateFromOutput({
                      workflowID: context.workflow.id,
                      role: context.role,
                      output: correction,
                    })
                  ) {
                    yield* advanceWorkflowAfterSession(context.workflow.id, "idle dispatch correction workflow update")
                    return
                  }
                  if (parseWorkflowControlCommand(correction) || implicitWorkflowResume(correction)) {
                    yield* applyWorkflowControl(context.workflow.id, correction, "idle dispatch correction", {
                      sourceSessionID: payload.properties.sessionID,
                      sourceAgent: info.agent ?? workflowAgentForRole(context.role),
                    }).pipe(Effect.ignore)
                    return
                  }
                  const correctionConsults = parseConsultRequests(correction)
                  if (correctionConsults.length > 0 && !workflowDispatchClaimWithoutControl(correction, context.role)) {
                    yield* resolveConsultRequests(
                      context.workflow.id,
                      payload.properties.sessionID,
                      info.agent ?? workflowAgentForRole(context.role),
                      context.workflow.model,
                      context.role,
                      context.milestoneID,
                      context.attempt,
                      correction,
                    ).pipe(Effect.ignore)
                    yield* advanceWorkflowAfterSession(context.workflow.id, "idle dispatch correction consultation completed")
                    return
                  }
                  yield* blockPlanning(
                    context.workflow.id,
                    "Workflow session became idle after claiming dispatch without a confirmed workflow control result.",
                  )
                  return
                }
                if (
                  expectation &&
                  text.trim() &&
                  !expectation.matches(text) &&
                  parseConsultRequests(text).length === 0 &&
                  !parseWorkflowUpdateXml(text)
                ) {
                  const info = yield* session
                    .get(payload.properties.sessionID)
                    .pipe(Effect.mapError((error) => new Error({ message: error.message })))
                  yield* runPrompt(
                    payload.properties.sessionID,
                    info.agent ?? workflowAgentForRole(context.role),
                    context.workflow.model,
                    workflowExpectedOutputPrompt(expectation, 1, Math.max(1, Math.trunc(expectation.maxAttempts ?? 6))),
                    {
                      workflowID: context.workflow.id,
                      role: context.role,
                      milestoneID: context.milestoneID,
                      attempt: context.attempt,
                    },
                    { consult: false, expect: expectation },
                  ).pipe(Effect.ignore)
                }
              }
              if (!(yield* recoverStaleActiveMilestones(context.workflow, contextItems))) return
              yield* schedule(context.workflow.id).pipe(Effect.ignore)
            }),
          ),
          Effect.forkScoped,
        )
        yield* (yield* bus.subscribe(MessageV2.Event.Updated)).pipe(
          Stream.runForEach((payload) => handleMessageUpdated(payload.properties).pipe(Effect.catchCause(() => Effect.void))),
          Effect.forkScoped,
        )
        yield* resumeActiveWorkflowsOnStartup().pipe(Effect.delay("500 millis"), Effect.catchCause(() => Effect.void), Effect.forkScoped)
      }),
    )

    const start = Effect.fn("Workflow.start")(function* (input: StartInput) {
      yield* InstanceState.get(initState)
      const ctx = yield* InstanceState.context
      const existing = input.sessionID
        ? Database.use((db) =>
            db
              .select()
              .from(WorkflowTable)
              .where(
                and(
                  eq(WorkflowTable.project_id, ctx.project.id),
                  eq(WorkflowTable.root_session_id, input.sessionID as SessionID),
                ),
              )
              .orderBy(asc(WorkflowTable.time_created))
              .all()
              .filter((item) => !["completed", "cancelled", "failed"].includes(item.status))
              .at(-1),
          )
        : undefined
      if (existing) {
        const workflow = yield* ensureRequesterSession(toInfo(existing))
        yield* normalizeWorkflowSessions(workflow).pipe(Effect.ignore)
        return yield* get(workflow.id)
      }
      const requester =
        input.sessionID !== undefined
          ? yield* session.get(input.sessionID).pipe(Effect.mapError((error) => new Error({ message: error.message })))
          : undefined
      const request = (input.prompt ?? requester?.title ?? "").trim()
      if (request.length === 0) {
        return yield* new Error({ message: "Workflow prompt is required" })
      }
      const title = input.title ?? titleFromRequest(request) ?? "Workflow request"
      const time = Date.now()
      const initialXml = workflowXmlFromText(request)
      const initialStatus = initialXml ? ("dispatching" as const) : ("planning" as const)
      const model = yield* Effect.try({
        try: () => modelFromInput(input.model, input.variant),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      })
      const staffing = normalizeStaffing(input.staffing)
      const scheduling = normalizeScheduling(input.scheduling)
      const modelWhitelist = normalizeWorkflowModelWhitelist(input.modelWhitelist)
      const root = requester ?? (yield* session.create({
        title: workflowRequesterTitle(request),
        agent: input.agent,
        model: model ? { id: model.modelID, providerID: model.providerID, variant: model.variant } : undefined,
      }))
      if (requester) yield* session.setTitle({ sessionID: requester.id, title: workflowRequesterTitle(request) }).pipe(Effect.ignore)
      const id = WorkflowID.ascending()
      const info: WorkflowInfo = {
        id,
        projectID: ProjectID.make(ctx.project.id),
        rootSessionID: root.id,
        request,
        title,
        directory: ctx.directory,
        path: workflowFolderPath(id),
        xml: initialXml ?? defaultXml,
        status: initialStatus,
        staffing,
        scheduling,
        model,
        modelWhitelist,
        agent: input.agent,
        time: {
          created: time,
          updated: time,
        },
      }
      Database.use((db) =>
        db
          .insert(ProjectTable)
          .values({
            id: ctx.project.id,
            worktree: ctx.project.worktree,
            vcs: ctx.project.vcs ?? null,
            name: ctx.project.name,
            icon_url: ctx.project.icon?.url,
            icon_url_override: ctx.project.icon?.override,
            icon_color: ctx.project.icon?.color,
            time_created: ctx.project.time.created,
            time_updated: ctx.project.time.updated,
            time_initialized: ctx.project.time.initialized,
            sandboxes: ctx.project.sandboxes,
            commands: ctx.project.commands,
          })
          .onConflictDoNothing()
          .run(),
      )
      Database.use((db) =>
        db
          .insert(SessionTable)
          .values(
            workflowStateSessionRow({
              ctx,
              workflow: info,
              session: workflowStateSessionSnapshot({
                workflow: info,
                ref: { sessionID: root.id, role: "requester", title: workflowRequesterTitle(request) },
                info: root,
              }),
            }),
          )
          .onConflictDoNothing()
          .run(),
      )
      yield* Effect.promise(async () =>
        Promise.all([
          writeFileEnsured(projectWorkflowPath(ctx.directory, info, "workflow.xml"), info.xml),
          writeWorkflowStateFile(
            projectWorkflowPath(ctx.directory, info, workflowManifestFileName),
            workflowManifest(info),
          ),
          writeFileEnsured(
            projectWorkflowPath(ctx.directory, info, workflowMainPlanPath()),
            initialXml
              ? [
                  `# ${info.title}`,
                  "",
                  "The requester supplied canonical workflow XML. It was imported directly into workflow.xml and will be dispatched without waiting for a main PM rewrite.",
                  "",
                  "## Request",
                  "",
                  info.request,
                  "",
                ].join("\n")
              : `# ${info.title}\n\n${info.request}\n`,
          ),
        ]),
      )
      Database.use((db) =>
        db.insert(WorkflowTable)
          .values({
            id: info.id,
            project_id: info.projectID,
            root_session_id: info.rootSessionID,
            request: info.request,
            title: info.title,
            directory: info.directory,
            path: info.path,
            xml: info.xml,
            status: info.status,
            staffing: info.staffing,
            scheduling: info.scheduling,
            model: info.model,
            model_whitelist: info.modelWhitelist,
            agent: info.agent,
            time_created: info.time.created,
            time_updated: info.time.updated,
          })
          .run(),
      )
      yield* saveDefinition(id, info.xml, parseXmlDefinition(info.xml, info), initialStatus)
      yield* writePrecreatedPlans(info, yield* milestones(id))
      yield* ensureCompany(info)
      const mainPM = (yield* members(id)).find((item) => item.role === "main_pm")
      if (mainPM) yield* setMainProductManagerSession(id, mainPM.sessionID)
      yield* archiveWorkflowSession({ workflowID: id, sessionID: root.id, role: "requester" }).pipe(Effect.ignore)
      yield* writeArchiveIndex(id).pipe(Effect.ignore)
      yield* writeOrganization(id).pipe(Effect.ignore)
      yield* writeProgress(id).pipe(Effect.ignore)
      yield* writeInterventionArtifacts(id).pipe(Effect.ignore)
      yield* writeReferenceIndex(id).pipe(Effect.ignore)
      yield* ensureStandupIndex(id).pipe(Effect.ignore)
      const initialized = yield* get(id)
      yield* writeWorkflowState(id, initialized).pipe(Effect.ignore)
      yield* events.publish(Event.Created, { workflowID: id, info })
      if (!workflowAutorunEnabled()) return info
      yield* background.start({
        id,
        type: "workflow",
        title: info.title,
        metadata: { workflowID: id },
        run: (initialXml ? schedule(id) : runPlanning(id)).pipe(
          Effect.delay("10 millis"),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : blockPlanning(id, `Workflow ${initialXml ? "dispatch" : "planning"} failed: ${errorFromCause(cause)}`).pipe(
                  Effect.asVoid,
                ),
          ),
          Effect.as(initialXml ? "workflow dispatched" : "workflow planning completed"),
        ),
      })
      return info
    })

    const updateXml = Effect.fn("Workflow.updateXml")(function* (input: UpdateXmlInput) {
      yield* InstanceState.get(initState)
      const workflow = yield* get(input.workflowID)
      const definition = yield* Effect.try({
        try: () => parseXmlDefinition(input.xml, workflow),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      })
      yield* writeNote(workflowArtifactPath(workflow, "workflow.xml"), input.xml)
      const result = yield* saveDefinition(input.workflowID, input.xml, definition)
      yield* writePrecreatedPlans(yield* get(input.workflowID), yield* milestones(input.workflowID))
      yield* publishUpdated(input.workflowID)
      return result
    })

    const updateStaffing = Effect.fn("Workflow.updateStaffing")(function* (input: UpdateStaffingInput) {
      yield* InstanceState.get(initState)
      const workflow = yield* get(input.workflowID)
      const staffing = normalizeStaffing(input.staffing)
      const modelWhitelist =
        input.modelWhitelist === undefined ? workflow.modelWhitelist : normalizeWorkflowModelWhitelist(input.modelWhitelist)
      Database.use((db) =>
        db
          .update(WorkflowTable)
          .set({ staffing, model_whitelist: modelWhitelist ?? null, time_updated: Date.now() })
          .where(eq(WorkflowTable.id, input.workflowID))
          .run(),
      )
      const next = yield* get(input.workflowID)
      yield* ensureCompany(next)
      yield* writeOrganization(input.workflowID).pipe(Effect.ignore)
      yield* writeArchiveIndex(input.workflowID).pipe(Effect.ignore)
      yield* writeProgress(input.workflowID).pipe(Effect.ignore)
      yield* publishUpdated(workflow.id)
      return yield* schedule(input.workflowID)
    })

    const updateIntervention = Effect.fn("Workflow.updateIntervention")(function* (
      workflowID: WorkflowID,
      interventionID: string,
      patch: Partial<Pick<WorkflowInterventionInfo, "response" | "status" | "targetSessionID">>,
    ) {
      Database.use((db) =>
        db
          .update(WorkflowInterventionTable)
          .set({
            ...(patch.response !== undefined ? { response: patch.response } : {}),
            ...(patch.status !== undefined ? { status: patch.status } : {}),
            ...(patch.targetSessionID !== undefined ? { target_session_id: patch.targetSessionID } : {}),
            time_updated: Date.now(),
          })
          .where(and(eq(WorkflowInterventionTable.workflow_id, workflowID), eq(WorkflowInterventionTable.id, interventionID)))
          .run(),
      )
      const updated = (yield* interventions(workflowID)).find((item) => item.id === interventionID)
      if (updated && (patch.status !== undefined || patch.response !== undefined)) {
        yield* appendWorkflowMessageRuntimeJournal(workflowID, {
          action: patch.status === "delivered" ? "deliver" : patch.status === "acked" ? "ack" : "update",
          kind: "intervention",
          messageID: interventionID,
          sessionID: updated.targetSessionID,
          targetSessionID: updated.targetSessionID,
          targetRole: updated.targetRole,
          status: updated.status,
          response: updated.response,
        }).pipe(Effect.ignore)
      }
      yield* writeInterventionArtifacts(workflowID).pipe(Effect.ignore)
      yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
      yield* events.publish(Event.GraphUpdated, { workflowID })
      return updated
    })

    const deliverIntervention = Effect.fn("Workflow.deliverIntervention")(function* (
      workflowID: WorkflowID,
      interventionID: string,
      jobID?: string,
    ) {
      const workflow = yield* get(workflowID)
      const intervention = (yield* interventions(workflowID)).find((item) => item.id === interventionID)
      if (!intervention?.targetSessionID) return
      const target = yield* session
        .get(intervention.targetSessionID)
        .pipe(Effect.mapError((error) => new Error({ message: error.message })))
      const workflowMessage = Database.use((db) =>
        db
          .select()
          .from(WorkflowMessageTable)
          .where(and(eq(WorkflowMessageTable.workflow_id, workflowID), eq(WorkflowMessageTable.id, interventionID)))
          .get(),
      )
      if (workflowMessage?.kind === "standup") {
        const result = yield* runPrompt(
          intervention.targetSessionID,
          target.agent ?? workflowAgentForRole(intervention.targetRole),
          workflow.model,
          [
            "Workflow company standup request received.",
            "",
            workflowReferencePrompt(workflow),
            "",
            `Message id: ${interventionID}`,
            `Timing: ${intervention.timing}`,
            `Standup request document: ${intervention.path}`,
            "",
            "Standup request:",
            intervention.message,
            "",
            "Do not edit workflow.xml, dispatch milestones, or output resume/block only because of a standup.",
            "Report your current state with workflow action=status_update, including availability, currentFocus, blockers, and progressNote.",
            "Then acknowledge this standup request with workflow_message action=ack and this message id.",
          ].join("\n"),
          {
            workflowID,
            role: intervention.targetRole,
          },
          { consult: false, control: false },
        )
        yield* updateIntervention(workflowID, interventionID, {
          response: latestText(result),
          status: "delivered",
        }).pipe(Effect.ignore)
        return
      }
      const result = yield* runPrompt(
        intervention.targetSessionID,
        target.agent ?? workflowAgentForRole(intervention.targetRole),
        workflow.model,
        [
          "Requester intervention received for this workflow company.",
          "",
          workflowReferencePrompt(workflow),
          "",
          `Timing: ${intervention.timing}`,
          `Intervention document: ${intervention.path}`,
          "",
          "Requester message:",
          intervention.message,
          "",
          intervention.timing === "interrupt"
            ? "The workflow has been paused. Decide whether to revise workflow.xml, update plans, ask clarification, or prepare the team to resume."
            : "Incorporate this direction into your role. If it changes scope or sequencing, update workflow docs or notify the main PM.",
          "End with exactly one workflow control XML block:",
          '<opencode-workflow-control action="resume">ready to continue with this direction</opencode-workflow-control>',
          "or",
          '<opencode-workflow-control action="block">needs requester clarification or plan rewrite before continuing</opencode-workflow-control>',
        ].join("\n"),
        {
          workflowID,
          role: intervention.targetRole,
        },
        { consult: false, control: false, expect: workflowControlExpectation() },
      )
      const output = latestText(result)
      if (parseConsultRequests(output).length > 0) {
        const items = yield* milestones(workflowID)
        yield* resolveConsultRequests(
          workflowID,
          intervention.targetSessionID,
          target.agent ?? workflowAgentForRole(intervention.targetRole),
          workflow.model,
          intervention.targetRole,
          workflowSessionMilestoneID(items, intervention.targetSessionID),
          workflowSessionAttempt(items, intervention.targetSessionID),
          output,
        ).pipe(Effect.ignore)
      }
      const control = parseWorkflowControlCommand(output)
      yield* updateIntervention(workflowID, interventionID, {
        response: output,
        status: intervention.timing === "interrupt" && (!control || control.action === "block") ? "blocked" : "delivered",
      }).pipe(Effect.ignore)
      yield* applyWorkflowControl(workflowID, output, `intervention ${interventionID}`, {
        exceptJobID: jobID,
        sourceSessionID: intervention.targetSessionID,
      }).pipe(Effect.ignore)
      if (intervention.targetRole !== "main_pm") {
        yield* notifyMainPM(workflowID, `Requester intervention ${interventionID} was delivered to ${roleSessionTitle(intervention.targetRole)}.`).pipe(
          Effect.ignore,
        )
      }
    })

    const afterTaskInterventionShouldWait = Effect.fn("Workflow.afterTaskInterventionShouldWait")(function* (
      workflowID: WorkflowID,
      intervention: WorkflowInterventionInfo,
    ) {
      if (intervention.timing !== "after-task" || !intervention.targetSessionID) return false
      const assignment = workflowSessionAssignment(yield* milestones(workflowID), intervention.targetSessionID)
      if (!assignment) return false
      return interruptedMilestone(assignment.milestone.status) || assignment.milestone.status === "testing"
    })

    const startInterventionDelivery = Effect.fn("Workflow.startInterventionDelivery")(function* (
      workflow: WorkflowInfo,
      interventionID: string,
    ) {
      if (!workflowAutorunEnabled()) return false
      const intervention = (yield* interventions(workflow.id)).find((item) => item.id === interventionID)
      if (!intervention || intervention.status !== "queued") return false
      if (yield* afterTaskInterventionShouldWait(workflow.id, intervention)) return false
      const jobID = `${workflow.id}:${intervention.id}`
      if ((yield* background.list()).some((job) => job.id === jobID && job.status === "running")) return true
      yield* background.start({
        id: jobID,
        type: "workflow.intervention",
        title: `${workflow.title} requester intervention`,
        metadata: { workflowID: workflow.id },
        run: deliverIntervention(workflow.id, intervention.id, jobID).pipe(
          Effect.catchCause((cause) =>
            updateIntervention(workflow.id, intervention.id, {
              response: errorFromCause(cause),
              status: "failed",
            }).pipe(Effect.asVoid),
          ),
          Effect.as("workflow intervention delivered"),
        ),
      })
      return true
    })

    const deliverReadyInterventions = Effect.fn("Workflow.deliverReadyInterventions")(function* (
      workflowID: WorkflowID,
    ) {
      const workflow = yield* get(workflowID)
      if (workflow.status === "cancelled" || workflow.status === "completed") return 0
      let started = 0
      for (const intervention of (yield* interventions(workflowID)).filter((item) => item.status === "queued")) {
        if (
          workflow.rootSessionID &&
          intervention.fromSessionID === workflow.rootSessionID &&
          intervention.targetRole === "main_pm" &&
          requesterDirectExecutionOverride(intervention.message)
        ) {
          const override = yield* applyRequesterDirectExecutionOverride({
            workflow,
            sourceSessionID: workflow.rootSessionID,
            message: intervention.message,
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.succeed({
                applied: false,
                message: `Requester direct-execution override failed: ${errorFromCause(cause)}`,
              }),
            ),
          )
          yield* updateIntervention(workflowID, intervention.id, {
            response: override.message,
            status: override.applied ? "acked" : "failed",
          }).pipe(Effect.ignore)
          yield* notifyMainPM(
            workflowID,
            `Requester direct-execution override ${override.applied ? "applied" : "failed"}: ${override.message}`,
          ).pipe(Effect.ignore)
          started++
          continue
        }
        if (yield* startInterventionDelivery(workflow, intervention.id)) started++
      }
      return started
    })

    const applyRequesterDirectExecutionOverride = Effect.fn("Workflow.applyRequesterDirectExecutionOverride")(function* (input: {
      workflow: WorkflowInfo
      sourceSessionID: SessionID
      message: string
    }) {
      const planning = (yield* milestones(input.workflow.id)).filter((item) => item.status === "planning")
      if (planning.length === 0) {
        yield* applyWorkflowControl(
          input.workflow.id,
          workflowToolResumeBlock(input.message),
          "requester direct execution override",
          {
            sourceSessionID: input.sourceSessionID,
          },
        ).pipe(Effect.ignore)
        return {
          applied: true,
          message: "Requester direct-execution override requested scheduling; no active planning gate needed force completion.",
        }
      }
      const results = yield* Effect.all(
        planning.map((item) =>
          Effect.gen(function* () {
            const command: WorkflowToolCommand = {
              id: Bus.createID(),
              action: "force_complete",
              workflowID: input.workflow.id,
              sourceSessionID: input.sourceSessionID,
              milestoneID: item.id,
              message: `Requester direct-execution override closed planning gate ${item.id}: ${compactMarkdown(input.message, 240)}`,
            }
            return yield* applyAndPublishWorkflowToolCommand(command, input.workflow.id)
          }),
        ),
        { concurrency: 1 },
      )
      const applied = results.filter((result) => result.result.applied)
      if (applied.length > 0) {
        return {
          applied: true,
          message: `Requester direct-execution override force-completed planning gate(s): ${applied.map((item) => item.input.milestoneID).filter(Boolean).join(", ")}.`,
        }
      }
      return {
        applied: false,
        message: `Requester direct-execution override could not close planning gates: ${results.map((item) => item.result.message).join(" | ")}`,
      }
    })

    const intervene = Effect.fn("Workflow.intervene")(function* (input: InterveneInput) {
      yield* InstanceState.get(initState)
      const message = input.message.trim()
      if (!message) return yield* new Error({ message: "Workflow intervention message is required" })
      const workflow = yield* get(input.workflowID)
      const sourceSessionID = input.sourceSessionID ?? workflow.rootSessionID
      const targetRole = input.targetRole ?? (input.targetSessionID ? workflowSessionRole(workflow, yield* milestones(workflow.id), input.targetSessionID) : "main_pm")
      const target =
        input.targetSessionID ??
        (targetRole === "requester"
          ? workflow.rootSessionID
          : (yield* ensureCompanyMember({
            workflow,
            role: targetRole,
            specialty: input.targetSpecialty ?? roleSpecialty(targetRole),
            prompt: input.message,
          }))?.sessionID)
      if (!target) return yield* new Error({ message: `No ${roleSessionTitle(targetRole)} session is available` })
      if (!sourceSessionID) return yield* new Error({ message: "Workflow requester session is not available" })

      const id = `intervention_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
      const now = Date.now()
      const timing = input.timing ?? "temporary-interrupt"
      Database.use((db) =>
        db.insert(WorkflowInterventionTable)
          .values({
            workflow_id: workflow.id,
            id,
            from_session_id: sourceSessionID,
            target_session_id: target,
            target_role: targetRole,
            timing,
            message,
            path: workflowArtifactPath(workflow, workflowInterventionPath(id)),
            status: "queued" as const,
            time_created: now,
            time_updated: now,
          })
          .run(),
      )
      const sourceRole = yield* workflowToolCommandSourceRole(workflow, sourceSessionID).pipe(
        Effect.catchCause(() => Effect.succeed(undefined)),
      )
      yield* upsertWorkflowMessage({
        workflowID: workflow.id,
        id,
        kind: "intervention",
        fromSessionID: sourceSessionID,
        fromRole: sourceRole,
        toSessionID: target,
        toRole: targetRole,
        timing,
        body: message,
        status: "queued",
        timeCreated: now,
        timeUpdated: now,
      }).pipe(Effect.ignore)
      yield* writeInterventionArtifacts(workflow.id).pipe(Effect.ignore)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflow.id).pipe(Effect.ignore)
      yield* archiveWorkflowSession({ workflowID: workflow.id, sessionID: target, role: targetRole }).pipe(Effect.ignore)
      if (
        workflow.rootSessionID === sourceSessionID &&
        targetRole === "main_pm" &&
        requesterDirectExecutionOverride(message)
      ) {
        const override = yield* applyRequesterDirectExecutionOverride({
          workflow,
          sourceSessionID,
          message,
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.succeed({
              applied: false,
              message: `Requester direct-execution override failed: ${errorFromCause(cause)}`,
            }),
          ),
        )
        yield* updateIntervention(workflow.id, id, {
          response: override.message,
          status: override.applied ? "acked" : "failed",
        }).pipe(Effect.ignore)
        yield* notifyMainPM(
          workflow.id,
          `Requester direct-execution override ${override.applied ? "applied" : "failed"}: ${override.message}`,
        ).pipe(Effect.ignore)
        return yield* publishUpdated(workflow.id)
      }
      if (timing === "interrupt") {
        yield* cancelWorkflowRuns(workflow.id).pipe(Effect.ignore)
        for (const item of (yield* milestones(workflow.id)).filter((milestone) => interruptedMilestone(milestone.status))) {
          yield* updateMilestone(workflow.id, item.id, { status: "blocked" }).pipe(Effect.ignore)
        }
        yield* setStatus(workflow.id, "blocked", {
          error: `Requester intervention: ${compactMarkdown(message, 160).replace(/\n/g, " ")}`,
        }).pipe(Effect.ignore)
      }
      if (!workflowAutorunEnabled()) {
        return yield* publishUpdated(workflow.id)
      }
      yield* startInterventionDelivery(workflow, id).pipe(Effect.ignore)
      return yield* publishUpdated(workflow.id)
    })

    const dispatchMessageSend = Effect.fn("Workflow.dispatchMessageSend")(function* (
      input: WorkflowMessageSendCommand,
    ): Effect.Effect<WorkflowMessageSendResult> {
      yield* InstanceState.get(initState)
      const workflow = yield* get(input.workflowID)
      const sourceRole = yield* workflowToolCommandSourceRole(workflow, input.sourceSessionID)
      if (!sourceRole) {
        return {
          workflowID: workflow.id,
          applied: false,
          kind: input.kind,
          message: `Session ${input.sourceSessionID} is not a member of workflow ${workflow.id}.`,
          rejection: {
            code: "not_authorized",
            reason: "Only workflow member sessions can send runtime workflow messages.",
          },
        }
      }
      if (input.kind !== "intervention" && input.kind !== "handoff") {
        return {
          workflowID: workflow.id,
          applied: false,
          kind: input.kind,
          message: "Runtime delivery is currently available for intervention and handoff messages only.",
          rejection: {
            code: "precondition_failed",
            reason: "Use workflow_message inbox/answer for consultation messages until the unified lifecycle lands.",
          },
        }
      }
      const attachments = (input.attachments ?? []).map((item) => item.trim()).filter(Boolean)
      const invalidAttachment = attachments.find(workflowMessageAttachmentInvalid)
      if (input.kind === "handoff" && (attachments.length === 0 || invalidAttachment)) {
        return {
          workflowID: workflow.id,
          applied: false,
          kind: input.kind,
          message: attachments.length === 0
            ? "Workflow handoff delivery requires attachments."
            : `Workflow handoff attachment is invalid: ${invalidAttachment}.`,
          rejection: {
            code: "precondition_failed",
            reason: attachments.length === 0
              ? "handoff requires at least one workflow-relative artifact path."
              : "handoff attachments must be relative workflow artifact paths and cannot use absolute paths or '..'.",
          },
        }
      }
      const message = workflowMessageWithAttachments(input.message.trim(), attachments)
      const interventionResult = yield* intervene({
        workflowID: workflow.id,
        sourceSessionID: input.sourceSessionID,
        targetRole: input.targetRole,
        targetSpecialty: input.targetSpecialty,
        targetSessionID: input.targetSessionID,
        timing: input.timing,
        message,
      }).pipe(
        Effect.map(() => ({ ok: true as const })),
        Effect.catch((error) =>
          Effect.succeed({
            ok: false as const,
            message: error instanceof globalThis.Error ? error.message : String(error),
          }),
        ),
      )
      if (!interventionResult.ok) {
        return {
          workflowID: workflow.id,
          applied: false,
          kind: input.kind,
          message: `Workflow intervention delivery was not queued: ${interventionResult.message}`,
          rejection: {
            code: "precondition_failed",
            reason: interventionResult.message,
          },
        }
      }
      const row = Database.use((db) =>
        db
          .select()
          .from(WorkflowInterventionTable)
          .where(
            and(
              eq(WorkflowInterventionTable.workflow_id, workflow.id),
              eq(WorkflowInterventionTable.from_session_id, input.sourceSessionID),
              eq(WorkflowInterventionTable.message, message),
            ),
          )
          .orderBy(asc(WorkflowInterventionTable.time_created))
          .all()
          .at(-1),
      )
      if (!row) {
        return {
          workflowID: workflow.id,
          applied: false,
          kind: input.kind,
          message: "Workflow intervention delivery did not create a tracked message.",
          rejection: {
            code: "precondition_failed",
            reason: "The workflow runtime accepted the request but no intervention row was found.",
          },
        }
      }
      return {
        workflowID: workflow.id,
        applied: true,
        kind: input.kind,
        message: `${input.kind === "handoff" ? "Handoff" : "Intervention"} ${row.id} was queued for runtime delivery to ${roleSessionTitle(row.target_role)} ${row.target_session_id}.`,
        messageID: row.id,
        status: "queued",
      }
    })

    const resume = Effect.fn("Workflow.resume")(function* (workflowID: WorkflowID) {
      yield* InstanceState.get(initState)
      yield* syncWorkflowStateFromDisk(workflowID).pipe(Effect.ignore)
      const current = yield* get(workflowID)
      if (current.status === "cancelled" || current.status === "completed") return current
      const doctorBlocked = yield* blockWorkflowResumeForDoctorIssues(workflowID, "workflow resume")
      if (doctorBlocked) return doctorBlocked
      yield* background.cancel(workflowID).pipe(Effect.ignore)
      yield* expireWorkflowMessages(workflowID).pipe(Effect.ignore)
      const items = yield* milestones(workflowID)
      yield* recoverInterruptedProgress(workflowID, items)
      const recoveredItems = yield* milestones(workflowID)
      const hasMilestoneSessions = recoveredItems.some((item) => item.session.length > 0)
      const shouldRunPlanning = !current.pmSessionID && !hasMilestoneSessions
      const shouldContinuePlanning = !!current.pmSessionID && !hasMilestoneSessions
      const workflow = yield* setStatus(
        workflowID,
        shouldRunPlanning || shouldContinuePlanning ? "planning" : "executing",
        { error: "" },
      )
      yield* background.start({
        id: workflowID,
        type: "workflow",
        title: workflow.title,
        metadata: { workflowID },
        run: (
          shouldRunPlanning
            ? runPlanning(workflowID)
            : shouldContinuePlanning
              ? continuePlanning(workflowID)
              : schedule(workflowID, { bypassStagedGate: true })
        ).pipe(
          Effect.delay("10 millis"),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : blockPlanning(workflowID, `Workflow resume failed: ${errorFromCause(cause)}`).pipe(Effect.asVoid),
          ),
          Effect.as("workflow resumed"),
        ),
      })
      return workflow
    })

    const resumeMilestoneFromSession = Effect.fn("Workflow.resumeMilestoneFromSession")(function* (input: {
      sessionID: SessionID
      message?: string
    }) {
      const context = yield* workflowSessionContext(input.sessionID)
      if (!context?.milestoneID) return false
      const workflow = yield* get(context.workflow.id)
      if (workflow.status === "cancelled" || workflow.status === "completed") return true
      const current = (yield* milestones(workflow.id)).find((item) => item.id === context.milestoneID)
      if (!current) return false
      yield* archiveWorkflowSession({
        workflowID: workflow.id,
        sessionID: input.sessionID,
        role: context.role,
        milestoneID: context.milestoneID,
        attempt: context.attempt,
      }).pipe(Effect.ignore)
      if (terminalMilestone(current.status) || current.status === "testing") {
        yield* setStatus(workflow.id, "executing", { error: "" }).pipe(Effect.ignore)
        yield* schedule(workflow.id, { bypassStagedGate: true }).pipe(Effect.ignore)
        return true
      }
      const workflowJobs = (yield* background.list()).filter(
        (job) => job.status === "running" && job.type === "workflow" && job.metadata?.workflowID === workflow.id,
      )
      yield* Effect.all(workflowJobs.map((job) => background.cancel(job.id).pipe(Effect.ignore)), { discard: true })
      const selectedJobs = (yield* background.list()).filter(
        (job) =>
          job.status === "running" &&
          job.type === "workflow.milestone" &&
          job.metadata?.workflowID === workflow.id &&
          job.metadata?.milestoneID === context.milestoneID,
      )
      yield* Effect.all(selectedJobs.map((job) => background.cancel(job.id).pipe(Effect.ignore)), { discard: true })
      yield* Effect.all(
        Array.from({ length: current.attempt + 2 }, (_, index) =>
          background.cancel(milestoneJobID(workflow.id, current.id, index + 1)).pipe(Effect.ignore),
        ),
        { discard: true },
      )
      const reset = yield* updateMilestone(workflow.id, current.id, { status: "pending" })
      const next = yield* setStatus(workflow.id, "executing", { error: "" })
      yield* startMilestoneJob(next, reset ?? { ...current, status: "pending" }, { delay: "50 millis" })
      return true
    })

    const continueFromSession = Effect.fn("Workflow.continueFromSession")(function* (input: {
      sessionID: SessionID
      message?: string
    }) {
      yield* InstanceState.get(initState)
      if (yield* resumeMilestoneFromSession(input)) {
        const context = yield* workflowSessionContext(input.sessionID)
        if (context) return yield* get(context.workflow.id)
      }
      const workflowID = yield* workflowToolCommandWorkflowID({
        action: "resume",
        sourceSessionID: input.sessionID,
        message: input.message,
      })
      if (!workflowID) return yield* new Error({ message: `No workflow is associated with session ${input.sessionID}` })
      yield* applyWorkflowControl(
        workflowID,
        workflowToolResumeBlock(input.message ?? "/workflow-continue requested"),
        "/workflow-continue",
        {
          sourceSessionID: input.sessionID,
        },
      ).pipe(Effect.ignore)
      return yield* get(workflowID)
    })

    const cancel = Effect.fn("Workflow.cancel")(function* (workflowID: WorkflowID) {
      const items = yield* milestones(workflowID)
      yield* background.cancel(workflowID).pipe(Effect.ignore)
      for (const item of items) {
        for (const attempt of Array.from({ length: item.attempt }, (_, index) => index + 1)) {
          yield* background.cancel(milestoneJobID(workflowID, item.id, attempt)).pipe(Effect.ignore)
        }
        if (["pending", "planning", "executing", "reviewing", "rejected"].includes(item.status)) {
          yield* updateMilestone(workflowID, item.id, { status: "cancelled" })
        }
      }
      return yield* setStatus(workflowID, "cancelled")
    })

    return Service.of({
      start,
      get,
      list,
      doctor,
      graph,
      updateXml,
      updateStaffing,
      dispatchCommand,
      intervene,
      continueFromSession,
      resume,
      cancel,
    })
  }),
)

export const defaultLayer = Layer.suspend(() =>
  layer.pipe(
    Layer.provide(BackgroundJob.defaultLayer),
    Layer.provide(Bus.layer),
    Layer.provide(EventV2Bridge.defaultLayer),
    Layer.provide(Session.defaultLayer),
    Layer.provide(SessionPrompt.defaultLayer),
  ),
)

export * as Workflow from "./workflow"
