// @ts-nocheck
import path from "path"
import { appendFileSync, mkdirSync } from "fs"
import { appendFile, cp, mkdir, readFile, stat, writeFile } from "fs/promises"

import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { Bus } from "@/bus"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { FileWatcher } from "@/file/watcher"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { ProjectID } from "@/project/schema"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { MessageV2 } from "@/session/message-v2"
import { SessionPrompt } from "@/session/prompt"
import { MessageID, SessionID } from "@/session/schema"
import { Database, and, asc, eq, inArray, or } from "@/storage/db"
import { BackgroundJob } from "@/background/job"
import { Cause, Effect, Context, Layer, Schema, Stream } from "effect"

import { parseWorkflowXml } from "./parse"
import { readyMilestones } from "./scheduler"
import { WorkflowToolCommandEvent, type WorkflowToolCommand } from "./command"
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
  WorkflowMemberTable,
  WorkflowMilestoneTable,
  WorkflowTable,
} from "./workflow.sql"

const workflowDir = path.join(".opencode", "workflows")
const defaultXml = `<workflow>
  <ordered>
    <milestone id="requirements" title="Clarify requirements" department="product">Clarify the user request, constraints, and acceptance criteria.</milestone>
    <milestone id="implementation" title="Implement solution" department="engineering">Implement the requested change end to end.</milestone>
    <milestone id="verification" title="Verify solution" department="quality">Verify behavior with focused tests and checks.</milestone>
  </ordered>
</workflow>`
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
  timing: Schema.optional(WorkflowCommunicationTiming),
  targetRole: Schema.optional(WorkflowRole),
  targetSessionID: Schema.optional(SessionID),
}).annotate({ identifier: "WorkflowInterveneInput" })
export type InterveneInput = typeof InterveneInput.Type

export const ListInput = Schema.Struct({
  sessionID: Schema.optional(SessionID),
}).annotate({ identifier: "WorkflowListInput" })
export type ListInput = typeof ListInput.Type

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
  readonly graph: (workflowID: WorkflowID) => Effect.Effect<WorkflowGraph, Error>
  readonly updateXml: (input: UpdateXmlInput) => Effect.Effect<WorkflowGraph, Error>
  readonly updateStaffing: (input: UpdateStaffingInput) => Effect.Effect<WorkflowInfo, Error>
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

function staffLimit(value: number | undefined, fallback: number, minimum: number) {
  if (value === undefined || !Number.isFinite(value)) return fallback
  return Math.max(minimum, Math.min(12, Math.trunc(value)))
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
}) {
  const whitelist = workflowModelWhitelistForRole(input.workflow, input.role)
  if (whitelist.length === 0) return input.fallback
  const selectionWeight = input.modelWeight === undefined ? workflowModelComplexity(input.prompt) : workflowModelWeight(input.modelWeight)
  return whitelist
    .toSorted(
      (a, b) =>
        Math.abs(workflowModelWeight(a.weight) - selectionWeight) - Math.abs(workflowModelWeight(b.weight) - selectionWeight) ||
        workflowModelWeight(b.weight) - workflowModelWeight(a.weight),
    )
    .at(0)
}

function workflowModelSelectionPrompt(input: {
  workflow: Pick<WorkflowInfo, "model" | "modelWhitelist">
  role: WorkflowSessionRef["role"]
  selected?: WorkflowInfo["model"] | WorkflowModelWhitelistItem
  prompt: string
  modelWeight?: number
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
    "Role model whitelist; higher weight means stronger reasoning and usually higher token cost:",
    ...whitelist
      .toSorted((a, b) => workflowModelWeight(a.weight) - workflowModelWeight(b.weight))
      .map((item) => `- ${workflowModelRefText(item)} weight=${workflowModelWeight(item.weight)}`),
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

export function selectWorkflowMember(input: {
  role: WorkflowSessionRef["role"]
  specialty: string
  members: WorkflowMemberInfo[]
  milestones: WorkflowMilestoneInfo[]
  excludeMilestoneID?: WorkflowMilestoneID
  limit?: number
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
    const load = (assignments.get(a.sessionID) ?? 0) - (assignments.get(b.sessionID) ?? 0)
    if (load !== 0) return load
    const specialty = Number(b.specialty === input.specialty) - Number(a.specialty === input.specialty)
    if (specialty !== 0) return specialty
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

function workflowFolderPath(time: number, title: string) {
  return path.join(workflowDir, `${workflowFolderTime(time)}_${workflowFolderTitle(title)}`)
}

function workflowFolderTime(time: number) {
  const date = new Date(time)
  return [
    date.getFullYear(),
    pad2(date.getMonth() + 1),
    pad2(date.getDate()),
    "_",
    pad2(date.getHours()),
    pad2(date.getMinutes()),
    pad2(date.getSeconds()),
    "_",
    String(date.getMilliseconds()).padStart(3, "0"),
  ].join("")
}

function pad2(value: number) {
  return String(value).padStart(2, "0")
}

function workflowFolderTitle(title: string) {
  return (
    title
      .replace(/[<>:"/\\|?*\x00-\x1F]+/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .replace(/[. ]+$/g, "")
      .slice(0, 80) || "workflow"
  )
}

function isLegacyWorkflowPath(workflow: Pick<WorkflowInfo, "id" | "path">) {
  return path.normalize(workflow.path) === path.normalize(workflowPath(workflow.id))
}

function rewriteWorkflowStoredPath(workflow: Pick<WorkflowInfo, "id" | "path">, stored: string) {
  const legacy = path.normalize(workflowPath(workflow.id))
  const current = path.normalize(stored)
  if (current === legacy) return workflow.path
  if (current.startsWith(legacy + path.sep)) return path.join(workflow.path, path.relative(legacy, current))
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

async function writeFileEnsured(file: string, content: string) {
  await mkdir(path.dirname(file), { recursive: true })
  await writeFile(file, content)
}

async function appendFileEnsured(file: string, content: string) {
  await mkdir(path.dirname(file), { recursive: true })
  await appendFile(file, content)
}

async function writeFileEnsuredIfMissing(file: string, content: string) {
  if (await exists(file)) return
  await writeFileEnsured(file, content)
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
  ensureColumn("workflow_member", "time_created", "ALTER TABLE workflow_member ADD COLUMN time_created integer NOT NULL DEFAULT 0")
  ensureColumn("workflow_member", "time_updated", "ALTER TABLE workflow_member ADD COLUMN time_updated integer NOT NULL DEFAULT 0")
  ensureColumn("workflow_milestone", "attempt", "ALTER TABLE workflow_milestone ADD COLUMN attempt integer NOT NULL DEFAULT 0")
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
  return path.join(String(milestoneID), `expert-${attempt}.md`)
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
  const pageSize = 20
  let before: string | undefined
  let wrote = false
  while (true) {
    const page = yield* MessageV2.page({
      sessionID: input.session.id,
      limit: pageSize,
      before,
    }).pipe(Effect.mapError((error) => new Error({ message: error.message })))
    if (page.items.length === 0) break
    wrote = true
    yield* Effect.promise(() =>
      appendFileEnsured(
        input.file,
        `${page.items
          .toReversed()
          .map(archiveMessageMarkdown)
          .join("\n")}\n`,
      ),
    )
    if (!page.more || !page.cursor) break
    before = page.cursor
  }
  if (!wrote) yield* Effect.promise(() => appendFileEnsured(input.file, "_No messages recorded yet._\n"))
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
    `- Main PM plan: main-plan.md`,
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
    `Tester completeness review: test-plan.md`,
    `Technical advisor assessment: technical-assessment.md`,
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
    `- Tester completeness review: test-plan.md`,
    `- Technical advisor assessment: technical-assessment.md`,
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
      : input.members.map((member) => `- ${member.title} [${member.status}] session: ${member.sessionID}`)),
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
          : items.map((item) => `  - ${workflowModelRefText(item)} weight=${workflowModelWeight(item.weight)}`)),
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
    "- main-plan.md",
    `- ${workflowReferenceIndexPath()}`,
    `- ${workflowRequesterMemoryPath()}`,
    `- ${workflowInterventionIndexPath()}`,
    ...(workflow.testPath ? [`- ${path.relative(workflow.path, workflow.testPath)}`] : []),
    "- technical-assessment.md",
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
    "Use timing=\"after-task\" for normal handoff, timing=\"temporary-interrupt\" when you need a quick answer before continuing, and timing=\"interrupt\" when the current task should pause until direction changes.",
    "Set model-weight=\"0-100\" when delegating; use lower weights for routine/focused tasks and higher weights for complex, risky, architectural, or ambiguous tasks.",
    "If the only valid blocker is a requester/user decision, send it to to-role=\"requester\" with 2-3 explicit options, mark one option as Recommended, and include the tradeoff for each option. Do not stop silently after asking.",
    "Main PM and department PM sessions may revise the workflow graph directly by emitting:",
    '<opencode-workflow-update reason="why the graph changed"><workflow>...</workflow></opencode-workflow-update>',
    "Use workflow updates when a milestone is too broad, requester strategy changes, or the company needs new ordered/parallel work. Preserve completed milestone ids when they remain valid.",
    "The workflow manager will prompt the target employee session, record the exchange in the workflow graph, update the reference library, and inject the answer back here.",
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
            return `- ${member.sessionID === input.currentSessionID ? "(you) " : ""}${member.title} [${member.role}/${member.specialty}] session=${member.sessionID} status=${member.status} capacity=${member.capacity} assignments=${assignments.join("; ") || "none"}`
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

export function temporaryInterruptPauseStatus(status: WorkflowMilestoneInfo["status"] | undefined) {
  if (status === "planning" || status === "executing" || status === "reviewing" || status === "running") return status
  return undefined
}

function errorFromCause(cause: Cause.Cause<unknown>) {
  const error = Cause.squash(cause)
  return error instanceof globalThis.Error ? error.message : String(error)
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
          path: workflowArtifactPath(info, "main-plan.md"),
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
            path: info.testPath ?? workflowArtifactPath(info, "test-plan.md"),
            summary: "Tester-created targeted tests, regression checks, and completeness review.",
          },
          {
            id: `${info.id}:technical-assessment`,
            title: "Technical advisor assessment",
            path: workflowArtifactPath(info, "technical-assessment.md"),
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
      summary: `${roleSessionTitle(member.role)} / ${member.specialty} / capacity ${member.capacity}`,
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
    "Do not hide a complex feature behind a single broad milestone. Every milestone must be small enough for one executor session to finish, one department PM session to functionally review, and the tester to verify for completeness.",
    "PM roles must not edit implementation code or perform code changes. PMs may write workflow, plan, decomposition, reference, organization, and review documents under the workflow directory; implementation belongs to executor sessions.",
    `Write the canonical XML to ${workflowArtifactPath(input.workflow, "workflow.xml")}.`,
    `Write the high-level plan to ${workflowArtifactPath(input.workflow, "main-plan.md")}.`,
    `Maintain the company organization chart at ${workflowArtifactPath(input.workflow, "organization.md")}.`,
    `Maintain the reference library at ${workflowArtifactPath(input.workflow, workflowReferenceIndexPath())}; link important child documents from main-plan.md.`,
    "Do not claim that dispatch has started unless you have written workflow.xml or emitted an <opencode-workflow-update> block. The workflow runtime creates department PM, executor, reviewer, and tester sessions after it validates the XML.",
    "Use the built-in workflow tool to inspect and control runtime state: call action=status before supervising, action=update_xml after creating or replacing the canonical XML, and action=resume when dispatch should continue. Do not rely only on natural-language claims of dispatch.",
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "Use this XML structure only:",
    "<workflow><ordered|parallel><milestone id=\"slug\" title=\"title\" department=\"team\">milestone scope</milestone></ordered|parallel></workflow>",
    "Use <ordered> when milestones must run sequentially and <parallel> when they can run concurrently. Nest groups when useful.",
    "Milestone content must include concrete deliverables, likely files or modules, acceptance checks, and handoff constraints.",
    "For complex plugin or engine work, split by real subsystems instead of naming the whole system. Examples: contracts and architecture, manager lifecycle, CPU simulation, GPU rendering path, materials and shaders, textures and atlases, asset importers, emitter shapes, particle types or modules, editor authoring UI, serialization/runtime API, docs, tests, and build integration.",
    "Write main-plan.md with a linked file index for workflow.xml, each milestone plan.md path, and any future decomposition files.",
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
    `Write the split rationale and child mapping to ${workflowArtifactPath(input.workflow, input.milestone.id, "decomposition.md")}.`,
    "Each child milestone must name concrete files or modules, deliverables, acceptance checks, and whether it can run in parallel.",
    "For plugin or graphics work, split independent concerns such as manager lifecycle, CPU/GPU paths, materials, textures, importers, emitter shapes, particle types, editor UI, serialization/runtime API, docs, tests, and build integration.",
    "Only keep this milestone executable when it is already small enough for one executor session to complete without guessing.",
    "Keep every plan file linked to the parent workflow and to any related child plan files.",
    "Include a `## Handoff Summary` section describing the executor-ready scope, assumptions, risks, and expected evidence.",
    "If you need strategy clarification, emit an opencode workflow message to main_pm or requester. If you need technical guidance, emit one to expert.",
    "Use the built-in workflow tool with action=status at the start. If you split this milestone, call action=update_xml with the full revised workflow XML after writing it. If a real blocker prevents dispatch, call action=block with the blocker reason.",
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
    `Write targeted test notes to ${workflowArtifactPath(input.workflow, "test-plan.md")}.`,
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
      "Write workflow.xml and main-plan.md, or emit:",
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
    description: "workflow control XML with resume/block action",
    reminder:
      'Decide whether the workflow should resume or remain blocked and end with `<opencode-workflow-control action="resume">...</opencode-workflow-control>` or `<opencode-workflow-control action="block">...</opencode-workflow-control>`.',
    matches: (text) => parseWorkflowControlAction(text) !== undefined,
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
    "If a requester/user decision is truly required, emit `<opencode-workflow-message to-role=\"requester\" timing=\"interrupt\" reason=\"decision required\">...Options: 1. ... (Recommended) ... 2. ...</opencode-workflow-message>` with concrete options and impacts.",
    `This is automatic continuation attempt ${attempt} of ${maxAttempts}.`,
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

export function parseWorkflowControlAction(text: string) {
  const action = /<opencode-workflow-control\b[^>]*\baction=["'](resume|block)["'][^>]*>/i.exec(text)?.[1]
  if (action === "resume" || action === "block") return action
  return undefined
}

function implicitWorkflowResume(text: string) {
  const toolingBlocker = /Bun is not defined/i.test(text) && /(stale|runner|tooling|运行器|工具|不是产品|不需要产品澄清|不需要修订\s*XML)/i.test(text)
  const dispatching = /Status\s*:\s*dispatching/i.test(text) || /状态\s*[:：]?\s*dispatching/i.test(text)
  const handoff = /(继续派发|开始派发|接手\s*M\d+|M\d+[-_\w]*\s*[:：]\s*(in_progress|pending)|下一步\s*[:：]?.*M\d+)/i.test(text)
  return (dispatching && (handoff || toolingBlocker)) || (toolingBlocker && handoff)
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

function parseXmlDefinition(xml: string) {
  try {
    return parseWorkflowXml(xml)
  } catch (error) {
    throw new globalThis.Error(error instanceof globalThis.Error ? error.message : String(error))
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
    const observedWorkflowFiles = new Map<string, string>()
    const workflowMessageKey = (sessionID: SessionID, messageID: MessageID) => `${sessionID}:${messageID}`

    const publishUpdated = Effect.fn("Workflow.publishUpdated")(function* (workflowID: WorkflowID) {
      const info = yield* get(workflowID)
      yield* events.publish(Event.Updated, { workflowID, info })
      yield* events.publish(Event.GraphUpdated, { workflowID })
      return info
    })

    const ensureAuditablePath = Effect.fn("Workflow.ensureAuditablePath")(function* (workflow: WorkflowInfo) {
      if (!isLegacyWorkflowPath(workflow)) return workflow
      const ctx = yield* InstanceState.context
      const nextPath = workflowFolderPath(workflow.time.created, workflow.title)
      yield* Effect.promise(() => ensureWorkflowDirectory(ctx.directory, workflow.path, nextPath))
      Database.use((db) =>
        db
          .update(WorkflowTable)
          .set({ path: nextPath, time_updated: Date.now() })
          .where(eq(WorkflowTable.id, workflow.id))
          .run(),
      )
      return { ...workflow, path: nextPath, time: { ...workflow.time, updated: Date.now() } }
    })

    const get = Effect.fn("Workflow.get")(function* (workflowID: WorkflowID) {
      const row = Database.use((db) => db.select().from(WorkflowTable).where(eq(WorkflowTable.id, workflowID)).get())
      if (!row) return yield* new Error({ message: `Workflow not found: ${workflowID}` })
      return yield* ensureAuditablePath(toInfo(row))
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
      return milestone
    })

    const saveDefinition = Effect.fn("Workflow.saveDefinition")(function* (
      workflowID: WorkflowID,
      xml: string,
      definition: WorkflowDefinition,
      status: WorkflowInfo["status"] = "dispatching",
    ) {
      const workflow = yield* get(workflowID)
      const edges = edgesFrom(definition)
      const now = Date.now()
      const existing = new Map((yield* milestones(workflowID)).map((milestone) => [milestone.id, milestone]))
      const definitionIDs = new Set(definition.milestones.map((milestone) => milestone.id))
      const retained = [...existing.values()].filter(
        (milestone) => milestone.session.length > 0 && !definitionIDs.has(milestone.id),
      )
      const rows = [
        ...definition.milestones.map((milestone) => ({
          workflow_id: workflowID,
          id: milestone.id,
          title: milestone.title,
          department: milestone.department,
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
          prompt: milestone.prompt,
          depends_on: milestone.dependsOn.filter((id) => definitionIDs.has(id)),
          status: "skipped" as const,
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
        try: () => parseXmlDefinition(xml),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      })
      yield* saveDefinition(workflowID, xml, definition, status)
      yield* writePrecreatedPlans(yield* get(workflowID), yield* milestones(workflowID))
      yield* publishUpdated(workflowID)
      return true
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
      Database.use((db) =>
        db
          .insert(WorkflowConsultationTable)
          .values({
            workflow_id: input.workflow.id,
            id: `consult_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
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
            time_created: Date.now(),
            time_updated: Date.now(),
          })
          .run(),
      )
      yield* writeReferenceIndex(input.workflow.id).pipe(Effect.ignore)
      yield* writeOrganization(input.workflow.id).pipe(Effect.ignore)
      yield* writeProgress(input.workflow.id).pipe(Effect.ignore)
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
    ) => Effect.Effect<void, unknown> = Effect.fn("Workflow.resolveConsultRequests")(function* (
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
      const workflow = yield* get(workflowID)
      const items = yield* milestones(workflowID)
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
          yield* notifyMainPM(
            workflowID,
            `Milestone ${sourceMilestoneID} is temporarily waiting for ${roleSessionTitle(targetRole)} consultation before continuing.`,
          ).pipe(Effect.ignore)
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
          { consult: false, modelWeight: request.modelWeight },
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
          }).pipe(Effect.ignore)
        }
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
          ].join("\n"),
          {
            workflowID,
            role: sourceRole,
            milestoneID: sourceMilestoneID,
            attempt: sourceAttempt,
          },
          {
            consult: false,
            expect: workflowSessionExpectation({
              role: sourceRole,
              milestoneID: sourceMilestoneID,
              milestoneStatus: sourceMilestoneID
                ? (yield* milestones(workflowID)).find((item) => item.id === sourceMilestoneID)?.status
                : undefined,
              workflowStatus: (yield* get(workflowID)).status,
            }),
          },
        )
        if (pauseStatus && sourceMilestoneID && !(yield* workflowIsBlocked(workflowID))) {
          yield* updateMilestone(workflowID, sourceMilestoneID, { status: pauseStatus }).pipe(Effect.ignore)
          yield* notifyMainPM(
            workflowID,
            `Temporary consultation for milestone ${sourceMilestoneID} was answered. The milestone returned to ${pauseStatus}.`,
          ).pipe(Effect.ignore)
        }
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
        try: () => parseXmlDefinition(xml),
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
      options?: { consult?: boolean; expect?: WorkflowPromptExpectation; modelWeight?: number },
    ) {
      const runOnce = Effect.fn("Workflow.runPromptOnce")(function* (nextText: string) {
        const basePromptText = archive ? yield* workflowPromptText(sessionID, nextText, archive) : nextText
        const selectionWorkflow = archive ? yield* get(archive.workflowID) : undefined
        const selectedModel = selectionWorkflow
          ? selectWorkflowModelFromWhitelist({
              workflow: selectionWorkflow,
              role: archive!.role,
              prompt: basePromptText,
              modelWeight: options?.modelWeight,
              fallback: model,
            })
          : model
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
        yield* writeReferenceIndex(archive.workflowID).pipe(Effect.ignore)
        yield* writeArchiveIndex(archive.workflowID).pipe(Effect.ignore)
        yield* writeOrganization(archive.workflowID).pipe(Effect.ignore)
        yield* applyWorkflowUpdateFromOutput({
          workflowID: archive.workflowID,
          role: archive.role,
          output: latestText(input.result),
        }).pipe(Effect.ignore)
        if (options?.consult !== false) {
          yield* resolveConsultRequests(
            archive.workflowID,
            sessionID,
            agent,
            input.model,
            archive.role,
            archive.milestoneID,
            archive.attempt,
            latestText(input.result),
          ).pipe(Effect.ignore)
        }
      })
      let current = yield* runOnce(text)
      yield* archiveResult(current)
      const expectation = options?.expect
      if (!expectation) return current.result
      const maxAttempts = Math.max(1, Math.trunc(expectation.maxAttempts ?? 6))
      for (const attempt of Array.from({ length: maxAttempts - 1 }, (_, index) => index + 2)) {
        if (expectation.matches(latestText(current.result))) return current.result
        if (archive && (yield* workflowIsBlocked(archive.workflowID))) return current.result
        current = yield* runOnce(workflowExpectedOutputPrompt(expectation, attempt, maxAttempts))
        yield* archiveResult(current)
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
      const message = (yield* session.messages({ sessionID: input.sessionID, limit: 20 })).find(
        (item) => item.info.id === input.messageID,
      )
      if (!message) return
      if (
        message.info.role === "assistant" &&
        workflowManagedMessageKeys.has(workflowMessageKey(input.sessionID, message.info.parentID))
      ) {
        return
      }
      const text = yield* workflowMessageTextWithRetry({
        sessionID: input.sessionID,
        messageID: input.messageID,
      })
      if (!text.trim()) return
      const context = yield* workflowSessionContext(input.sessionID)
      if (!context) return
      observedWorkflowMessageKeys.add(key)
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
      const controlAction = parseWorkflowControlAction(text)
      if (context.role === "main_pm" && (controlAction || implicitWorkflowResume(text))) {
        yield* applyWorkflowControl(context.workflow.id, text, "main PM workflow handoff").pipe(Effect.ignore)
        if (controlAction === "resume" || implicitWorkflowResume(text)) return
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
      const followup =
        expectation && !appliedWorkflowUpdate && consults.length === 0 && !expectation.matches(text)
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
      const workflow = yield* activeRequesterWorkflow(input.sessionID)
      if (!workflow) return
      const text = yield* requesterMessageTextWithRetry(input)
      if (!text) return
      observedRequesterMessageKeys.add(key)
      yield* answerPendingRequesterConsultations(workflow, input.sessionID, text).pipe(Effect.ignore)
      if (yield* applyRequesterAcceptance(yield* get(workflow.id), text)) return
      const target = workflow.pmSessionID
        ? { sessionID: workflow.pmSessionID }
        : yield* ensureCompanyMember({
            workflow,
            role: "main_pm",
            specialty: "strategy",
            title: workflowSessionTitle("Main PM", workflow.title),
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
      )
      Database.use((db) =>
        db
          .update(WorkflowInterventionTable)
          .set({
            response: latestText(result),
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
      yield* applyWorkflowControl(workflow.id, latestText(result), "requester strategic direction").pipe(Effect.ignore)
      yield* publishUpdated(workflow.id).pipe(Effect.ignore)
    })

    const notifyMainPM = Effect.fn("Workflow.notifyMainPM")(function* (
      workflowID: WorkflowID,
      message: string,
      source?: { milestoneID?: WorkflowMilestoneID; jobID?: string },
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
          "Update main-plan.md, progress.md, or workflow.xml only if this changes scope, sequencing, risks, or acceptance. Do not do implementation work from the main PM session.",
        ].join("\n"),
        {
          workflowID,
          role: "main_pm",
        },
      )
      yield* writeProgress(workflowID).pipe(Effect.ignore)
      yield* writeMainPMSupervisionNote({
        workflowID,
        message,
        output: latestText(result),
      }).pipe(Effect.ignore)
      const next = yield* applyWorkflowControl(workflowID, latestText(result), "main PM supervision", {
        exceptJobID: source?.jobID,
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

    const observeWorkflowUserMessage = Effect.fn("Workflow.observeWorkflowUserMessage")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      const key = workflowMessageKey(input.sessionID, input.messageID)
      if (workflowManagedMessageKeys.has(key) || observedWorkflowMessageKeys.has(key)) return
      const context = yield* workflowSessionContext(input.sessionID)
      if (!context || context.role === "requester") return
      const text = yield* requesterMessageTextWithRetry(input)
      if (!text) return
      if (/^\/workflow-continue(?:\s|$)/i.test(text.trim())) return
      observedWorkflowMessageKeys.add(key)
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
      )
      const output = latestText(result)
      yield* writeNote(standupPath, standupMarkdown({ workflow, reason, progress, output }))
      yield* appendStandupIndex(workflow, standupPath, reason)
      yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
      yield* applyWorkflowControl(workflowID, output, "company standup", { exceptJobID: jobID }).pipe(Effect.ignore)
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

    const blockWorkflow = Effect.fn("Workflow.blockWorkflow")(function* (workflow: WorkflowInfo, message: string) {
      const info = yield* setStatus(workflow.id, "blocked", { error: message })
      const pmSessionID = info.pmSessionID ?? workflow.pmSessionID
      if (pmSessionID) {
        yield* runPrompt(
          pmSessionID,
          "workflow-main-pm",
          workflow.model,
          [
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
      yield* archiveWorkflowSessionMessages({
        workflow,
        session: info,
        role,
        prompt: input.prompt,
        milestoneID: input.milestoneID,
        attempt: input.attempt,
        file: path.join(ctx.directory, workflowArtifactPath(workflow, workflowSessionArchivePath(input.sessionID))),
      })
      const messages = (
        yield* MessageV2.page({
          sessionID: input.sessionID,
          limit: 60,
        }).pipe(Effect.mapError((error) => new Error({ message: error.message })))
      ).items
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
        try: () => parseXmlDefinition(xml),
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
    }) {
      const specialty = roleSpecialty(input.role, input.specialty)
      const staff = yield* members(input.workflow.id)
      const limit = staffLimitForRole(input.workflow.staffing, input.role)
      const all = staff.filter((member) => member.role === input.role)
      const selected = selectWorkflowMember({
        role: input.role,
        specialty,
        members: staff,
        milestones: yield* milestones(input.workflow.id),
        excludeMilestoneID: input.milestoneID,
        limit,
      })
      if (selected && (!input.strictSpecialty || selected.specialty === specialty)) {
        if (input.workflow.rootSessionID) {
          yield* session.setParent({ sessionID: selected.sessionID, parentID: input.workflow.rootSessionID }).pipe(Effect.ignore)
        }
        yield* session.setTitle({ sessionID: selected.sessionID, title: selected.title }).pipe(Effect.ignore)
        return selected
      }

      if (limit <= 0) return undefined
      if (all.length >= limit) return undefined

      const index = all.length + 1
      const nextSpecialty = all.some((member) => member.specialty === specialty) ? `${specialty}-${index}` : specialty
      const title = input.title ?? workflowMemberTitle(input.role, nextSpecialty, index)
      const created = yield* createAgentSession({
        title,
        agent: workflowAgentForRole(input.role),
        parentID: input.workflow.rootSessionID,
        permission: memberPermission(input.role),
        model: input.workflow.model,
      })
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
        time_created: now,
        time_updated: now,
      }
      Database.use((db) => db.insert(WorkflowMemberTable).values(member).run())
      yield* writeOrganization(input.workflow.id).pipe(Effect.ignore)
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

      const pm = yield* ensureCompanyMember({
        workflow,
        role: "department_pm",
        specialty: milestone.department ?? "product",
        title: workflowSessionTitle("Department PM", milestone.title ?? String(milestone.id)),
        milestoneID: milestone.id,
      })
      if (!pm) return yield* blockWorkflow(workflow, `No department PM is available for milestone ${milestone.id}`)
      yield* updateMilestone(workflow.id, milestone.id, {
        session: appendSession(planned, { role: "department_pm", sessionID: pm.sessionID, milestoneID: milestone.id, attempt })
          .session,
      })
      const pmPrompt = promptDepartmentPm({ workflow, milestone })
      const pmResult = yield* runPrompt(pm.sessionID, "workflow-department-pm", workflow.model, pmPrompt, {
        workflowID: workflow.id,
        role: "department_pm",
        milestoneID: milestone.id,
        attempt,
      }, { expect: handoffExpectation("department PM execution plan") })
      if (yield* workflowIsBlocked(workflow.id)) return
      if (!hasHandoffSummary(latestText(pmResult))) {
        yield* notifyMainPM(workflow.id, `Department PM did not produce a dispatchable plan marker for milestone ${milestone.id}. It will be rescheduled until the plan marker is present.`, milestoneSource).pipe(Effect.ignore)
        yield* updateMilestone(workflow.id, milestone.id, { status: "pending" }).pipe(Effect.ignore)
        yield* schedule(workflow.id)
        return
      }
      yield* writeNote(workflowArtifactPath(workflow, milestone.id, "plan.md"), latestText(pmResult))
      yield* notifyMainPM(
        workflow.id,
        `Department PM finished planning milestone ${milestone.id}. Plan file: ${workflowArtifactPath(workflow, milestone.id, "plan.md")}`,
        milestoneSource,
      )
      if (yield* workflowIsBlocked(workflow.id)) return
      if (!(yield* milestones(workflow.id)).some((item) => item.id === milestone.id && item.status !== "skipped")) {
        yield* schedule(workflow.id)
        return
      }
      const refreshed = yield* refreshWorkflowXml(workflow.id, "dispatching")
      if (refreshed && !(yield* milestones(workflow.id)).some((item) => item.id === milestone.id && item.status !== "skipped")) {
        yield* schedule(workflow.id)
        return
      }

      const executing = (yield* milestones(workflow.id)).find((item) => item.id === milestone.id && item.status !== "skipped")
      if (!executing) return
      const expert = yield* ensureCompanyMember({
        workflow,
        role: "expert",
        specialty: milestone.department ?? "technical-advisory",
        title: workflowSessionTitle("Technical Advisor", milestone.title ?? String(milestone.id)),
        milestoneID: milestone.id,
      })
      const expertPath = expert ? workflowArtifactPath(workflow, workflowExpertNotePath(milestone.id, attempt)) : undefined
      if (expert) {
        yield* updateMilestone(workflow.id, milestone.id, {
          session: appendSession(executing, { role: "expert", sessionID: expert.sessionID, milestoneID: milestone.id, attempt })
            .session,
        })
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
          yield* notifyMainPM(workflow.id, `Technical advisor did not produce a handoff marker for milestone ${milestone.id}. The milestone will be rescheduled instead of advancing without evidence.`, milestoneSource).pipe(Effect.ignore)
          yield* updateMilestone(workflow.id, milestone.id, { status: "pending" }).pipe(Effect.ignore)
          yield* schedule(workflow.id)
          return
        }
        yield* writeNote(expertPath!, latestText(expertResult))
        yield* notifyMainPM(workflow.id, `Technical advisor finished notes for milestone ${milestone.id}. Notes: ${expertPath}`, milestoneSource)
        if (yield* workflowIsBlocked(workflow.id)) return
      }
      yield* updateMilestone(workflow.id, milestone.id, { status: "executing" })
      const executor = yield* ensureCompanyMember({
        workflow,
        role: "executor",
        specialty: milestone.department ?? "engineering",
        title: workflowSessionTitle("Executor", milestone.title ?? String(milestone.id)),
        milestoneID: milestone.id,
      })
      if (!executor) return yield* blockWorkflow(workflow, `No executor is available for milestone ${milestone.id}`)
      const afterExpert = (yield* milestones(workflow.id)).find((item) => item.id === milestone.id) ?? executing
      yield* updateMilestone(workflow.id, milestone.id, {
        session: appendSession(afterExpert, { role: "executor", sessionID: executor.sessionID, milestoneID: milestone.id, attempt })
          .session,
      })
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
        yield* notifyMainPM(workflow.id, `Executor did not produce a completion marker for milestone ${milestone.id}. The milestone will be rescheduled instead of moving to review.`, milestoneSource).pipe(Effect.ignore)
        yield* updateMilestone(workflow.id, milestone.id, { status: "pending" }).pipe(Effect.ignore)
        yield* schedule(workflow.id)
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
      const reviewPath = workflowArtifactPath(workflow, milestone.id, `review-${attempt}.md`)
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
      })
      if (!tester) return yield* blockWorkflow(workflow, "No tester is available for workflow completion review")
      const testerPrompt = promptTester({ workflow, milestones: items })
      const testerResult = yield* runPrompt(tester.sessionID, "workflow-tester", workflow.model, testerPrompt, {
        workflowID: workflow.id,
        role: "tester",
      }, { expect: testExpectation() })
      if (yield* workflowIsBlocked(workflow.id)) return
      const testPath = workflowArtifactPath(workflow, "test-plan.md")
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
      })
      const technicalPath = workflowArtifactPath(workflow, "technical-assessment.md")
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
      workflowID: WorkflowID,
      items: WorkflowMilestoneInfo[],
    ) {
      const running = new Set(
        (yield* background.list())
          .filter(
            (job) =>
              job.status === "running" &&
              job.type === "workflow.milestone" &&
              job.metadata?.workflowID === workflowID,
          )
          .map((job) => String(job.metadata?.milestoneID ?? "")),
      )
      const stale = items.filter((item) => interruptedMilestone(item.status) && !running.has(String(item.id)))
      if (stale.length === 0) return false
      for (const item of stale) {
        yield* updateMilestone(workflowID, item.id, { status: "pending" })
      }
      return true
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
      exceptJobID?: string,
    ) {
      for (const job of (yield* background.list()).filter(
        (job) => job.metadata?.workflowID === workflowID && job.status === "running" && job.id !== exceptJobID,
      )) {
        yield* background.cancel(job.id).pipe(Effect.ignore)
      }
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

    const schedule: (workflowID: WorkflowID) => Effect.Effect<WorkflowInfo, unknown> = Effect.fn("Workflow.schedule")(
      function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      if (workflow.status === "cancelled" || workflow.status === "completed" || workflow.status === "blocked") return workflow
      const items = yield* milestones(workflowID)
      if (yield* recoverStaleActiveMilestones(workflowID, items)) return yield* schedule(workflowID)
      const definition: WorkflowDefinition = {
        steps: { type: "parallel", children: [] },
        milestones: items.map((item) => ({
          type: "milestone",
          id: item.id,
          title: item.title,
          department: item.department,
          prompt: item.prompt,
          dependsOn: item.dependsOn,
        })),
      }
      if (items.length > 0 && items.every((item) => ["approved", "done", "completed", "skipped"].includes(item.status))) {
        yield* runTester(workflow)
        return yield* get(workflowID)
      }
      const activeMilestoneCount = items.filter((item) =>
        ["planning", "executing", "reviewing", "testing"].includes(item.status),
      ).length
      const activeRoleCount = (role: WorkflowSessionRef["role"]) =>
        new Set(
          items
            .filter((item) => roleBusyForMilestoneStatus(role, item.status))
            .flatMap((item) => item.session.filter((ref) => ref.role === role).map((ref) => ref.sessionID)),
        ).size
      const availableStarts = Math.min(
        staffLimitForRole(workflow.staffing, "department_pm") - activeRoleCount("department_pm"),
        staffLimitForRole(workflow.staffing, "executor") - activeRoleCount("executor"),
        staffLimitForRole(workflow.staffing, "expert") - activeRoleCount("expert"),
      )
      const ready = readyMilestones(definition, milestoneStates(items)).slice(
        0,
        Math.max(0, availableStarts),
      )
      if (ready.length === 0) {
        if (items.some((item) => item.status === "rejected")) {
          for (const item of items.filter((item) => item.status === "rejected")) {
            yield* updateMilestone(workflowID, item.id, { status: "pending" })
          }
          yield* schedule(workflowID)
          return yield* get(workflowID)
        }
        if (activeMilestoneCount > 0) return workflow
        return yield* blockWorkflow(workflow, "No runnable milestones are available")
      }
      yield* setStatus(workflowID, "executing")
      for (const item of ready) {
        const current = items.find((milestone) => milestone.id === item.id)
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
      options?: { exceptJobID?: string },
    ) {
      const action = parseWorkflowControlAction(output) ?? (implicitWorkflowResume(output) ? "resume" : undefined)
      if (!action) return yield* get(workflowID)
      const workflow = yield* get(workflowID)
      if (action === "block") {
        yield* cancelWorkflowRuns(workflowID, options?.exceptJobID)
        yield* blockActiveMilestones(workflowID)
        return yield* blockWorkflow(
          workflow,
          `Main PM kept workflow blocked after ${reason}: ${compactMarkdown(output, 240).replace(/\n/g, " ")}`,
        )
      }
      if (workflow.status === "cancelled" || workflow.status === "completed") return workflow
      yield* background.cancel(workflowID).pipe(Effect.ignore)
      const items = yield* milestones(workflowID)
      yield* recoverInterruptedProgress(workflowID, items)
      const recoveredItems = yield* milestones(workflowID)
      const hasMilestoneSessions = recoveredItems.some((item) => item.session.length > 0)
      const shouldContinuePlanning = !!workflow.pmSessionID && !hasMilestoneSessions
      const next = yield* setStatus(workflowID, shouldContinuePlanning ? "planning" : "executing", { error: "" })
      yield* background.start({
        id: `${workflowID}:control:${Date.now().toString(36)}`,
        type: "workflow",
        title: next.title,
        metadata: { workflowID },
        run: (shouldContinuePlanning ? continuePlanning(workflowID) : schedule(workflowID)).pipe(
          Effect.delay("10 millis"),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : blockPlanning(workflowID, `Workflow control resume failed: ${errorFromCause(cause)}`).pipe(Effect.asVoid),
          ),
          Effect.as("workflow control resumed"),
        ),
      })
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

    const applyWorkflowToolCommand = Effect.fn("Workflow.applyWorkflowToolCommand")(function* (
      input: WorkflowToolCommand,
    ) {
      const workflowID = yield* workflowToolCommandWorkflowID(input)
      if (!workflowID) return
      const workflow = yield* get(workflowID)
      if (input.action === "status") {
        yield* publishUpdated(workflowID).pipe(Effect.ignore)
        return
      }
      if (input.action === "resume") {
        yield* applyWorkflowControl(
          workflowID,
          workflowToolResumeBlock(input.message ?? "workflow tool requested resume"),
          "workflow tool",
        ).pipe(Effect.ignore)
        return
      }
      if (input.action === "block") {
        yield* cancelWorkflowRuns(workflowID)
        yield* blockActiveMilestones(workflowID)
        yield* blockWorkflow(workflow, `Workflow tool block: ${input.message ?? "blocked"}`).pipe(Effect.ignore)
        return
      }
      if (input.action === "update_xml") {
        if (!input.xml?.trim()) return
        const xml = input.xml
        const definition = yield* Effect.try({
          try: () => parseXmlDefinition(xml),
          catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
        }).pipe(
          Effect.catch((error: Error) =>
            blockWorkflow(workflow, `Workflow tool XML update failed: ${error.message}`).pipe(
              Effect.as(undefined as WorkflowDefinition | undefined),
            ),
          ),
        )
        if (!definition) return
        yield* writeNote(workflowArtifactPath(workflow, "workflow.xml"), xml)
        yield* saveDefinition(workflowID, xml, definition, "dispatching")
        yield* writePrecreatedPlans(yield* get(workflowID), yield* milestones(workflowID))
        yield* applyWorkflowControl(
          workflowID,
          workflowToolResumeBlock(input.message ?? "workflow XML updated by workflow tool"),
          "workflow tool XML update",
        ).pipe(Effect.ignore)
        return
      }
      if (input.action === "milestone_status") {
        if (!input.milestoneID || !input.milestoneStatus) return
        yield* updateMilestone(workflowID, input.milestoneID, { status: input.milestoneStatus })
        if (input.milestoneStatus === "blocked") {
          yield* blockWorkflow(
            yield* get(workflowID),
            `Workflow tool blocked milestone ${input.milestoneID}: ${input.message ?? "blocked"}`,
          ).pipe(Effect.ignore)
          return
        }
        if (input.milestoneStatus === "failed") {
          yield* setStatus(workflowID, "failed", {
            error: `Workflow tool marked milestone ${input.milestoneID} failed${input.message ? `: ${input.message}` : ""}`,
          }).pipe(Effect.ignore)
          return
        }
        yield* setStatus(workflowID, "executing", { error: "" }).pipe(Effect.ignore)
        yield* schedule(workflowID).pipe(Effect.ignore)
        return
      }
      if (input.action === "workflow_status") {
        if (!input.workflowStatus) return
        if (input.workflowStatus === "blocked") {
          yield* blockWorkflow(workflow, `Workflow tool status block: ${input.message ?? "blocked"}`).pipe(Effect.ignore)
          return
        }
        yield* setStatus(workflowID, input.workflowStatus, { error: "" }).pipe(Effect.ignore)
        if (["pending", "running", "planning", "dispatching", "executing", "reviewing"].includes(input.workflowStatus)) {
          yield* applyWorkflowControl(
            workflowID,
            workflowToolResumeBlock(input.message ?? `workflow tool set status ${input.workflowStatus}`),
            "workflow tool status",
          ).pipe(Effect.ignore)
        }
        return
      }
      if (input.action === "complete") {
        const items = yield* milestones(workflowID)
        if (!items.every((item) => ["approved", "done", "completed", "skipped", "testing"].includes(item.status))) {
          yield* setStatus(workflowID, "executing", { error: "" }).pipe(Effect.ignore)
          yield* schedule(workflowID).pipe(Effect.ignore)
          return
        }
        for (const item of items.filter((item) => item.status === "approved" || item.status === "testing")) {
          yield* updateMilestone(workflowID, item.id, { status: "done" })
        }
        yield* setStatus(workflowID, "completed", { error: "" }).pipe(Effect.ignore)
      }
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

      const workflow = yield* workflowForFile(file)
      if (!workflow) return
      const workflowRelative = workflowRelativeFile(workflow, file)
      if (!workflowRelative) return
      if (path.normalize(workflowRelative) !== "workflow.xml") {
        yield* publishUpdated(workflow.id).pipe(Effect.ignore)
        return
      }
      if (input.event === "unlink") {
        yield* updateWorkflowFileError(workflow.id, "workflow.xml was deleted; keeping the last valid workflow graph")
        return
      }
      const xml = yield* Effect.promise(() => readFile(file, "utf8")).pipe(
        Effect.catch((error: unknown) =>
          updateWorkflowFileError(
            workflow.id,
            `workflow.xml could not be read: ${error instanceof globalThis.Error ? error.message : String(error)}`,
          ).pipe(Effect.as(undefined as string | undefined)),
        ),
      )
      if (!xml) return
      const definition = yield* Effect.try({
        try: () => parseXmlDefinition(xml),
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
      for (const workflow of workflows.filter((item) => !activeJobs.has(item.id))) {
        yield* applyWorkflowControl(
          workflow.id,
          workflowToolResumeBlock("opencode startup resume"),
          "opencode startup",
        ).pipe(Effect.catchCause(() => Effect.void))
      }
    })

    const initState = yield* InstanceState.make(
      Effect.fn("Workflow.initState")(function* () {
        yield* (yield* bus.subscribe(WorkflowToolCommandEvent)).pipe(
          Stream.runForEach((payload) =>
            applyWorkflowToolCommand(payload.properties).pipe(Effect.catchCause(() => Effect.void)),
          ),
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
                  .get(),
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
                if (text && implicitWorkflowResume(text)) {
                  yield* applyWorkflowControl(workflow.id, text, "main PM session idle").pipe(Effect.ignore)
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
              if (!(yield* recoverStaleActiveMilestones(context.workflow.id, contextItems))) return
              yield* setStatus(context.workflow.id, "executing", { error: "" }).pipe(Effect.ignore)
              yield* schedule(context.workflow.id).pipe(Effect.ignore)
            }),
          ),
          Effect.forkScoped,
        )
        yield* (yield* bus.subscribe(MessageV2.Event.Updated)).pipe(
          Stream.runForEach((payload) => {
            const info = payload.properties.info
            if (info.role === "user") {
              return Effect.all(
                [
                  observeRequesterMessage({
                    sessionID: payload.properties.sessionID,
                    messageID: info.id,
                  }),
                  observeWorkflowUserMessage({
                    sessionID: payload.properties.sessionID,
                    messageID: info.id,
                  }),
                ],
                { discard: true },
              ).pipe(Effect.catchCause(() => Effect.void))
            }
            if (!info.time.completed || info.error) return Effect.void
            return observeWorkflowMessage({
              sessionID: payload.properties.sessionID,
              messageID: info.id,
              agent: info.agent,
              model: {
                providerID: info.providerID,
                modelID: info.modelID,
                ...(info.variant ? { variant: info.variant } : {}),
              },
            }).pipe(Effect.catchCause(() => Effect.void))
          }),
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
      const model = yield* Effect.try({
        try: () => modelFromInput(input.model, input.variant),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      })
      const staffing = normalizeStaffing(input.staffing)
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
        path: workflowFolderPath(time, title),
        xml: defaultXml,
        status: "planning",
        staffing,
        model,
        modelWhitelist,
        agent: input.agent,
        time: {
          created: time,
          updated: time,
        },
      }
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
            model: info.model,
            model_whitelist: info.modelWhitelist,
            agent: info.agent,
            time_created: info.time.created,
            time_updated: info.time.updated,
          })
          .run(),
      )
      yield* Effect.promise(async () =>
        Promise.all([
          writeFileEnsured(projectWorkflowPath(ctx.directory, info, "workflow.xml"), info.xml),
          writeFileEnsured(projectWorkflowPath(ctx.directory, info, "main-plan.md"), `# ${info.title}\n\n${info.request}\n`),
        ]),
      )
      yield* saveDefinition(id, info.xml, parseXmlDefinition(info.xml), "planning")
      yield* writePrecreatedPlans(info, yield* milestones(id))
      yield* ensureCompany(info)
      yield* archiveWorkflowSession({ workflowID: id, sessionID: root.id, role: "requester" }).pipe(Effect.ignore)
      yield* writeArchiveIndex(id).pipe(Effect.ignore)
      yield* writeOrganization(id).pipe(Effect.ignore)
      yield* writeProgress(id).pipe(Effect.ignore)
      yield* writeInterventionArtifacts(id).pipe(Effect.ignore)
      yield* ensureStandupIndex(id).pipe(Effect.ignore)
      yield* events.publish(Event.Created, { workflowID: id, info })
      if (!workflowAutorunEnabled()) return info
      yield* background.start({
        id,
        type: "workflow",
        title: info.title,
        metadata: { workflowID: id },
        run: runPlanning(id).pipe(
          Effect.delay("10 millis"),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : blockPlanning(id, `Workflow planning failed: ${errorFromCause(cause)}`).pipe(Effect.asVoid),
          ),
          Effect.as("workflow planning completed"),
        ),
      })
      return info
    })

    const updateXml = Effect.fn("Workflow.updateXml")(function* (input: UpdateXmlInput) {
      yield* InstanceState.get(initState)
      yield* get(input.workflowID)
      const definition = yield* Effect.try({
        try: () => parseXmlDefinition(input.xml),
        catch: (error) => new Error({ message: error instanceof globalThis.Error ? error.message : String(error) }),
      })
      yield* writeNote(workflowArtifactPath(yield* get(input.workflowID), "workflow.xml"), input.xml)
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
      return yield* publishUpdated(workflow.id)
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
      yield* writeInterventionArtifacts(workflowID).pipe(Effect.ignore)
      yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
      yield* events.publish(Event.GraphUpdated, { workflowID })
      return (yield* interventions(workflowID)).find((item) => item.id === interventionID)
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
        { consult: false, expect: workflowControlExpectation() },
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
      const control = parseWorkflowControlAction(output)
      yield* updateIntervention(workflowID, interventionID, {
        response: output,
        status: intervention.timing === "interrupt" && control !== "resume" ? "blocked" : "delivered",
      }).pipe(Effect.ignore)
      yield* applyWorkflowControl(workflowID, output, `intervention ${interventionID}`, {
        exceptJobID: jobID,
      }).pipe(Effect.ignore)
      if (intervention.targetRole !== "main_pm") {
        yield* notifyMainPM(workflowID, `Requester intervention ${interventionID} was delivered to ${roleSessionTitle(intervention.targetRole)}.`).pipe(
          Effect.ignore,
        )
      }
    })

    const intervene = Effect.fn("Workflow.intervene")(function* (input: InterveneInput) {
      yield* InstanceState.get(initState)
      const message = input.message.trim()
      if (!message) return yield* new Error({ message: "Workflow intervention message is required" })
      const workflow = yield* get(input.workflowID)
      const targetRole = input.targetRole ?? (input.targetSessionID ? workflowSessionRole(workflow, yield* milestones(workflow.id), input.targetSessionID) : "main_pm")
      const target =
        input.targetSessionID ??
        (targetRole === "requester"
          ? workflow.rootSessionID
          : (yield* ensureCompanyMember({
              workflow,
              role: targetRole,
              specialty: roleSpecialty(targetRole),
            }))?.sessionID)
      if (!target) return yield* new Error({ message: `No ${roleSessionTitle(targetRole)} session is available` })

      const id = `intervention_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
      const now = Date.now()
      const timing = input.timing ?? "temporary-interrupt"
      Database.use((db) =>
        db.insert(WorkflowInterventionTable)
          .values({
            workflow_id: workflow.id,
            id,
            from_session_id: workflow.rootSessionID,
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
      yield* writeInterventionArtifacts(workflow.id).pipe(Effect.ignore)
      yield* writeReferenceIndex(workflow.id).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflow.id).pipe(Effect.ignore)
      yield* archiveWorkflowSession({ workflowID: workflow.id, sessionID: target, role: targetRole }).pipe(Effect.ignore)
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
      const jobID = `${workflow.id}:${id}`
      yield* background.start({
        id: jobID,
        type: "workflow.intervention",
        title: `${workflow.title} requester intervention`,
        metadata: { workflowID: workflow.id },
        run: deliverIntervention(workflow.id, id, jobID).pipe(
          Effect.catchCause((cause) =>
            updateIntervention(workflow.id, id, {
              response: errorFromCause(cause),
              status: "failed",
            }).pipe(Effect.asVoid),
          ),
          Effect.as("workflow intervention delivered"),
        ),
      })
      return yield* publishUpdated(workflow.id)
    })

    const resume = Effect.fn("Workflow.resume")(function* (workflowID: WorkflowID) {
      yield* InstanceState.get(initState)
      const current = yield* get(workflowID)
      if (current.status === "cancelled" || current.status === "completed") return current
      yield* background.cancel(workflowID).pipe(Effect.ignore)
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
        run: (shouldRunPlanning ? runPlanning(workflowID) : shouldContinuePlanning ? continuePlanning(workflowID) : schedule(workflowID)).pipe(
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
        yield* schedule(workflow.id).pipe(Effect.ignore)
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

    return Service.of({ start, get, list, graph, updateXml, updateStaffing, intervene, continueFromSession, resume, cancel })
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
