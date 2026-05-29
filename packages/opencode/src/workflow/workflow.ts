import path from "path"
import { cp, mkdir, readFile, stat, writeFile } from "fs/promises"

import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { Bus } from "@/bus"
import { BusEvent } from "@/bus/bus-event"
import { InstanceState } from "@/effect/instance-state"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { ProjectID } from "@/project/schema"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { MessageV2 } from "@/session/message-v2"
import { SessionPrompt } from "@/session/prompt"
import { MessageID, SessionID } from "@/session/schema"
import { Database, and, asc, eq, inArray } from "@/storage/db"
import { BackgroundJob } from "@/background/job"
import { Cause, Effect, Context, Layer, Schema, Stream } from "effect"

import { parseWorkflowXml } from "./parse"
import { readyMilestones } from "./scheduler"
import {
  WorkflowGraph,
  WorkflowGraphEdge,
  WorkflowGraphNode,
  WorkflowID,
  WorkflowInfo,
  WorkflowMilestone,
  WorkflowMilestoneID,
  type WorkflowConsultationInfo,
  type WorkflowDefinition,
  type WorkflowMilestoneInfo,
  type WorkflowSessionRef,
} from "./schema"
import { WorkflowConsultationTable, WorkflowEdgeTable, WorkflowMilestoneTable, WorkflowTable } from "./workflow.sql"

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
  model text,
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
  model text,
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
SELECT id, project_id, root_session_id, pm_session_id, tester_session_id, request, title, directory, path, xml, status, model, agent, test_path, error, time_created, time_updated, time_completed
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
let schemaEnsured = false

type WorkflowArchiveSessionRef = {
  role: WorkflowSessionRef["role"]
  sessionID: SessionID
  milestoneID?: WorkflowMilestoneID
  attempt?: number
}

type WorkflowConsultRequest = {
  targetSessionID: SessionID
  reason?: string
  question: string
}

export const StartInput = Schema.Struct({
  sessionID: Schema.optional(SessionID),
  prompt: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  variant: Schema.optional(Schema.String),
  agent: Schema.optional(Schema.String),
  title: Schema.optional(Schema.String),
}).annotate({ identifier: "WorkflowStartInput" })
export type StartInput = typeof StartInput.Type

export const UpdateXmlInput = Schema.Struct({
  workflowID: WorkflowID,
  xml: Schema.String,
}).annotate({ identifier: "WorkflowUpdateXmlInput" })
export type UpdateXmlInput = typeof UpdateXmlInput.Type

export const ListInput = Schema.Struct({
  sessionID: Schema.optional(SessionID),
}).annotate({ identifier: "WorkflowListInput" })
export type ListInput = typeof ListInput.Type

const CreatedPayload = Schema.Struct({
  workflowID: WorkflowID,
  info: WorkflowInfo,
}).annotate({ identifier: "WorkflowCreatedEvent" })

const UpdatedPayload = Schema.Struct({
  workflowID: WorkflowID,
  info: WorkflowInfo,
}).annotate({ identifier: "WorkflowUpdatedEvent" })

const NodeUpdatedPayload = Schema.Struct({
  workflowID: WorkflowID,
  milestone: WorkflowMilestone,
}).annotate({ identifier: "WorkflowNodeUpdatedEvent" })

const GraphUpdatedPayload = Schema.Struct({
  workflowID: WorkflowID,
  graph: WorkflowGraph,
}).annotate({ identifier: "WorkflowGraphUpdatedEvent" })

export const Event = {
  Created: BusEvent.define("workflow.created", CreatedPayload),
  Updated: BusEvent.define("workflow.updated", UpdatedPayload),
  NodeUpdated: BusEvent.define("workflow.node.updated", NodeUpdatedPayload),
  GraphUpdated: BusEvent.define("workflow.graph.updated", GraphUpdatedPayload),
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

async function writeFileEnsuredIfMissing(file: string, content: string) {
  if (await Bun.file(file).exists()) return
  await writeFileEnsured(file, content)
}

function ensureSchema() {
  if (schemaEnsured) return
  const client = Database.Client().$client
  client.exec(ensureSchemaSql)
  const rootSession = client
    .prepare("SELECT [notnull] FROM pragma_table_info('workflow') WHERE name = 'root_session_id'")
    .get() as { notnull: number } | undefined
  if (rootSession?.notnull) client.exec(ensureWorkflowOwnershipSql)
  if (client.prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '__drizzle_migrations'").get()) {
    client.exec(ensureWorkflowMigrationSql)
  }
  schemaEnsured = true
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
    model: row.model ?? undefined,
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
    ...(input.messages.length === 0
      ? ["_No messages recorded yet._"]
      : input.messages.flatMap((message) => [
          `### ${message.info.role} ${message.info.id}`,
          "",
          `Created: ${new Date(message.info.time.created).toISOString()}`,
          ...(message.info.role === "assistant"
            ? [
                ...(message.info.time.completed
                  ? [`Completed: ${new Date(message.info.time.completed).toISOString()}`]
                  : []),
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
        ])),
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
    compactMarkdown(assistant ? messageText(assistant) : "_No completed assistant summary is available yet._"),
    "",
    ...(user ? ["## Initial Request To This Session", "", compactMarkdown(messageText(user), 1200), ""] : []),
    "## Consultation",
    "",
    `Other workflow sessions can consult this session with: <opencode-workflow-consult target-session="${input.session.id}" reason="short reason">question</opencode-workflow-consult>`,
    "",
  ].join("\n")
}

function referenceIndexMarkdown(input: {
  workflow: WorkflowInfo
  milestones: WorkflowMilestoneInfo[]
  consultations: WorkflowConsultationInfo[]
}) {
  const sessionRows: WorkflowArchiveSessionRef[] = [
    ...(input.workflow.rootSessionID ? [{ role: "requester" as const, sessionID: input.workflow.rootSessionID }] : []),
    ...(input.workflow.pmSessionID ? [{ role: "main_pm" as const, sessionID: input.workflow.pmSessionID }] : []),
    ...(input.workflow.testerSessionID ? [{ role: "tester" as const, sessionID: input.workflow.testerSessionID }] : []),
    ...input.milestones.flatMap((milestone) =>
      milestone.session.map((ref) => ({
        ...ref,
        milestoneID: ref.milestoneID ?? milestone.id,
      })),
    ),
  ]
  return [
    `# ${input.workflow.title} Reference Library`,
    "",
    `Workflow: ${input.workflow.id}`,
    `Status: ${input.workflow.status}`,
    "",
    "## Session Summaries",
    "",
    ...(sessionRows.length === 0
      ? ["_No session summaries have been archived yet._"]
      : sessionRows.map(
          (ref) =>
            `- ${roleSessionTitle(ref.role)}${ref.milestoneID ? ` / ${ref.milestoneID}` : ""}${ref.attempt !== undefined ? ` / attempt ${ref.attempt}` : ""}: ${workflowSessionSummaryPath(ref.sessionID)} (full: ${workflowSessionArchivePath(ref.sessionID)})`,
        )),
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
    "## Consultation History",
    "",
    ...(input.consultations.length === 0
      ? ["_No cross-session consultations have been recorded yet._"]
      : input.consultations.map(
          (consultation) =>
            `- ${consultation.fromRole} ${consultation.fromSessionID} -> ${consultation.toRole} ${consultation.toSessionID}: ${compactMarkdown(consultation.question, 160).replace(/\n/g, " ")}`,
        )),
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
    "- main-plan.md",
    `- ${workflowReferenceIndexPath()}`,
    ...(workflow.testPath ? [`- ${path.relative(workflow.path, workflow.testPath)}`] : []),
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
  return milestones.flatMap((milestone) => milestone.session).find((ref) => ref.sessionID === sessionID)?.role ?? "requester"
}

function workflowSessionMilestoneID(milestones: WorkflowMilestoneInfo[], sessionID: SessionID) {
  return milestones.find((milestone) => milestone.session.some((ref) => ref.sessionID === sessionID))?.id
}

function workflowAgentForRole(role: WorkflowSessionRef["role"]) {
  if (role === "main_pm") return "workflow-main-pm"
  if (role === "department_pm") return "workflow-department-pm"
  if (role === "executor") return "workflow-executor"
  if (role === "reviewer") return "workflow-reviewer"
  if (role === "tester") return "workflow-tester"
  return "build"
}

function workflowReferencePrompt(workflow: WorkflowInfo) {
  return [
    `Workflow root: ${workflow.path}`,
    `Workflow index: ${workflowArtifactPath(workflow, "index.md")}`,
    `Reference library: ${workflowArtifactPath(workflow, workflowReferenceIndexPath())}`,
    `Workflow XML: ${workflowArtifactPath(workflow, "workflow.xml")}`,
    "",
    "Read the workflow index and reference library when present so you know the whole workflow, current completion state, session ids, related plans, and prior decisions.",
    "When a completed workflow session owns information you need, ask it instead of guessing by emitting exactly one or more consultation markers:",
    '<opencode-workflow-consult target-session="ses_xxx" reason="why this session has the answer">question for that session</opencode-workflow-consult>',
    "The workflow manager will ask that completed session, record the question and answer in the workflow, and inject the answer back here.",
  ].join("\n")
}

function consultationAttribute(attributes: string, name: string) {
  return new RegExp(`${name}=["']([^"']+)["']`, "i").exec(attributes)?.[1]
}

function parseConsultRequests(text: string): WorkflowConsultRequest[] {
  return Array.from(text.matchAll(/<opencode-workflow-consult\b([^>]*)>([\s\S]*?)<\/opencode-workflow-consult>/gi))
    .map((match) => ({
      target: consultationAttribute(match[1] ?? "", "target-session") ?? consultationAttribute(match[1] ?? "", "targetSessionID"),
      reason: consultationAttribute(match[1] ?? "", "reason"),
      question: (match[2] ?? "").trim(),
    }))
    .flatMap((item) => {
      if (!item.target || !item.question) return []
      try {
        return [
          {
            targetSessionID: SessionID.make(item.target),
            ...(item.reason ? { reason: item.reason } : {}),
            question: item.question,
          },
        ]
      } catch {
        return []
      }
    })
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

function errorFromCause(cause: Cause.Cause<unknown>) {
  const error = Cause.squash(cause)
  return error instanceof globalThis.Error ? error.message : String(error)
}

function graphFrom(
  info: WorkflowInfo,
  milestones: WorkflowMilestoneInfo[],
  edges: WorkflowGraphEdge[],
  consultations: WorkflowConsultationInfo[],
): WorkflowGraph {
  const mainPMID = `${info.id}:main_pm`
  const testerID = `${info.id}:tester`
  const libraryID = `${info.id}:reference`
  const sessionNodeID = (milestone: WorkflowMilestoneInfo, session: WorkflowSessionRef) =>
    `${milestone.id}:${session.role}:${session.attempt ?? 0}`
  const sessionNodeIDs = new Map<SessionID, string>([
    ...(info.rootSessionID ? [[info.rootSessionID, info.id] as const] : []),
    ...(info.pmSessionID ? [[info.pmSessionID, mainPMID] as const] : []),
    ...(info.testerSessionID ? [[info.testerSessionID, testerID] as const] : []),
    ...milestones.flatMap((milestone) =>
      milestone.session.map((session) => [session.sessionID, sessionNodeID(milestone, session)] as const),
    ),
  ])
  const sessionRefs: WorkflowArchiveSessionRef[] = [
    ...(info.rootSessionID ? [{ role: "requester" as const, sessionID: info.rootSessionID }] : []),
    ...(info.pmSessionID ? [{ role: "main_pm" as const, sessionID: info.pmSessionID }] : []),
    ...(info.testerSessionID ? [{ role: "tester" as const, sessionID: info.testerSessionID }] : []),
    ...milestones.flatMap((milestone) =>
      milestone.session.map((ref) => ({
        ...ref,
        milestoneID: ref.milestoneID ?? milestone.id,
      })),
    ),
  ]
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
              ? session.role === "reviewer"
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
          ...(executor && reviewer
            ? [
                {
                  id: `${sessionNodeID(milestone, executor)}->${sessionNodeID(milestone, reviewer)}`,
                  from: sessionNodeID(milestone, executor),
                  to: sessionNodeID(milestone, reviewer),
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
    ...sessionRefs.flatMap((ref) => {
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
    }),
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
          label: "consult",
          question: consultation.question,
          answer: consultation.answer,
          summary: `${roleSessionTitle(consultation.fromRole)} consulted ${roleSessionTitle(consultation.toRole)}`,
        },
      ]
    }),
  ].filter(
    (edge, index, all) =>
      all.findIndex(
        (item) => item.id === edge.id || (item.from === edge.from && item.to === edge.to && item.kind === edge.kind),
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
    {
      id: libraryID,
      type: "document" as const,
      title: "Reference library",
      path: workflowArtifactPath(info, workflowReferenceIndexPath()),
      summary: "Workflow reference library with session summaries, plans, and consultation history.",
    },
    ...(info.pmSessionID
      ? [
          {
            id: mainPMID,
            type: "session" as const,
            title: "Main product manager",
            role: "main_pm" as const,
            sessionID: info.pmSessionID,
          },
        ]
      : []),
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
      ...milestone.session.map((session) => ({
        id: sessionNodeID(milestone, session),
        type: "session" as const,
        title: `${session.role} ${milestone.title ?? milestone.id}`,
        role: session.role,
        sessionID: session.sessionID,
        milestoneID: milestone.id,
      })),
    ]),
    ...(info.testerSessionID
      ? [
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
  ].filter((node, index, all) => all.findIndex((item) => item.id === node.id) === index)
  return {
    workflow: info,
    milestones,
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
  })
}

function promptMainPm(input: { workflow: WorkflowInfo }) {
  return [
    "You are the main product manager for an automated opencode workflow.",
    "",
    "Create an implementation-scale workflow plan for this request.",
    "Do not hide a complex feature behind a single broad milestone. Every milestone must be small enough for one executor session to finish and one reviewer session to verify.",
    `Write the canonical XML to ${workflowArtifactPath(input.workflow, "workflow.xml")}.`,
    `Write the high-level plan to ${workflowArtifactPath(input.workflow, "main-plan.md")}.`,
    `Maintain the reference library at ${workflowArtifactPath(input.workflow, workflowReferenceIndexPath())}; link important child documents from main-plan.md.`,
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
    `You are the department product manager for milestone ${input.milestone.id}.`,
    "",
    `Main request: ${input.workflow.request}`,
    `Milestone title: ${input.milestone.title ?? input.milestone.id}`,
    `Department: ${input.milestone.department ?? "unspecified"}`,
    `Milestone scope: ${input.milestone.prompt}`,
    "",
    workflowReferencePrompt(input.workflow),
    "",
    `Save a detailed execution plan to ${workflowArtifactPath(input.workflow, input.milestone.id, "plan.md")}.`,
    `If this milestone is still too broad, update ${workflowArtifactPath(input.workflow, "workflow.xml")} before writing a broad plan.`,
    `When splitting, replace this milestone with ordered/parallel child milestones whose ids are prefixed with "${input.milestone.id}-". Preserve the dependency intent and do not leave dependencies pointing at a removed milestone id.`,
    `Write the split rationale and child mapping to ${workflowArtifactPath(input.workflow, input.milestone.id, "decomposition.md")}.`,
    "Each child milestone must name concrete files or modules, deliverables, acceptance checks, and whether it can run in parallel.",
    "For plugin or graphics work, split independent concerns such as manager lifecycle, CPU/GPU paths, materials, textures, importers, emitter shapes, particle types, editor UI, serialization/runtime API, docs, tests, and build integration.",
    "Only keep this milestone executable when it is already small enough for one executor session to complete without guessing.",
    "Keep every plan file linked to the parent workflow and to any related child plan files.",
  ].join("\n")
}

function promptExecutor(input: { workflow: WorkflowInfo; milestone: WorkflowMilestoneInfo }) {
  return [
    `You are the executor for workflow milestone ${input.milestone.id}.`,
    "",
    `Main request: ${input.workflow.request}`,
    `Detailed plan file: ${workflowArtifactPath(input.workflow, input.milestone.id, "plan.md")}`,
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "Before editing, read the linked workflow and plan files for this milestone.",
    "If the plan is still a broad umbrella for multiple independent subsystems, do not mark it complete by doing a shallow slice. Explain the required split in the milestone plan or output so the reviewer can reject it back to planning.",
    "Execute the plan in continuous plan execution mode until this milestone is complete.",
    "At the end, include this exact XML marker:",
    `<opencode-workflow-result milestone="${input.milestone.id}" status="complete">`,
    "summary of completed work",
    "</opencode-workflow-result>",
  ].join("\n")
}

function promptReviewer(input: { workflow: WorkflowInfo; milestone: WorkflowMilestoneInfo; executorOutput: string }) {
  return [
    `You are the reviewer for workflow milestone ${input.milestone.id}.`,
    "",
    `Main request: ${input.workflow.request}`,
    `Detailed plan file: ${workflowArtifactPath(input.workflow, input.milestone.id, "plan.md")}`,
    "",
    workflowReferencePrompt(input.workflow),
    "",
    "Review whether the executor completed this milestone. Check the repository state, linked workflow files, plan files, and relevant diffs.",
    "Reject work that treats a broad subsystem as complete without handling the concrete child concerns called out by the plan.",
    "Reject work that should have been decomposed into smaller workflow milestones but was implemented as a vague single pass.",
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
    "You are the workflow tester.",
    "",
    `Main request: ${input.workflow.request}`,
    `Write targeted test notes to ${workflowArtifactPath(input.workflow, "test-plan.md")}.`,
    workflowReferencePrompt(input.workflow),
    "",
    "Create or update focused tests for the approved milestones, then run the most relevant checks.",
    "Test by concrete subsystem and linked plan file, not only by top-level feature wording.",
    "If you need product clarification, ask the main product manager session explicitly.",
    "",
    "Approved milestones:",
    ...input.milestones.map((milestone) => `- ${milestone.id}: ${milestone.title ?? milestone.prompt}`),
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
  BackgroundJob.Service | Bus.Service | Session.Service | SessionPrompt.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    ensureSchema()
    const bus = yield* Bus.Service
    const session = yield* Session.Service
    const prompt = yield* SessionPrompt.Service
    const background = yield* BackgroundJob.Service

    const publishUpdated = Effect.fn("Workflow.publishUpdated")(function* (workflowID: WorkflowID) {
      const info = yield* get(workflowID)
      yield* bus.publish(Event.Updated, { workflowID, info })
      yield* bus.publish(Event.GraphUpdated, { workflowID, graph: yield* graph(workflowID) })
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
      const sessionIDs = [
        workflow.pmSessionID,
        workflow.testerSessionID,
        ...items.flatMap((milestone) => milestone.session.map((ref) => ref.sessionID)),
      ].filter((id): id is SessionID => !!id && id !== workflow.rootSessionID)
      const titles = workflowSessionTitles(workflow, items)
      if (sessionIDs.length === 0) return
      yield* Effect.all(
        Array.from(new Set(sessionIDs)).map((sessionID) =>
          Effect.all([
            session.setParent({ sessionID, parentID: workflow.rootSessionID! }),
            session.setTitle({ sessionID, title: titles.get(sessionID) ?? "Workflow agent" }),
          ]).pipe(Effect.ignore),
        ),
      )
    })

    const writeArchiveIndex = Effect.fn("Workflow.writeArchiveIndex")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      yield* writeNote(workflowArtifactPath(workflow, "index.md"), archiveIndexMarkdown(workflow, yield* milestones(workflowID)))
    })

    const writeReferenceIndex = Effect.fn("Workflow.writeReferenceIndex")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      yield* writeNote(
        workflowArtifactPath(workflow, workflowReferenceIndexPath()),
        referenceIndexMarkdown({
          workflow,
          milestones: yield* milestones(workflowID),
          consultations: yield* consultations(workflowID),
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
      if (!input?.sessionID) {
        const result = yield* Effect.all(rows.map((row) => ensureRequesterSession(toInfo(row))))
        yield* Effect.all(result.map((workflow) => normalizeWorkflowSessions(workflow).pipe(Effect.ignore)))
        yield* Effect.all(result.map((workflow) => archiveWorkflowSessions(workflow.id).pipe(Effect.ignore)))
        return result
      }
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
                .filter((row) => row.session.some((ref) => ref.sessionID === input.sessionID))
                .map((row) => row.workflow_id),
            )
      const result = (yield* Effect.all(rows.map((row) => ensureRequesterSession(toInfo(row))))).filter(
        (workflow) =>
          workflow.rootSessionID === input.sessionID ||
          workflow.pmSessionID === input.sessionID ||
          workflow.testerSessionID === input.sessionID ||
          milestoneWorkflowIDs.has(workflow.id),
      )
      yield* Effect.all(result.map((workflow) => normalizeWorkflowSessions(workflow).pipe(Effect.ignore)))
      yield* Effect.all(result.map((workflow) => archiveWorkflowSessions(workflow.id).pipe(Effect.ignore)))
      return result
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

    const graph = Effect.fn("Workflow.graph")(function* (workflowID: WorkflowID) {
      const info = yield* get(workflowID)
      yield* normalizeWorkflowSessions(info)
      const next = yield* get(workflowID)
      const items = yield* milestones(workflowID)
      const edges = Database.use((db) =>
        db
          .select()
          .from(WorkflowEdgeTable)
          .where(eq(WorkflowEdgeTable.workflow_id, workflowID))
          .all()
          .map((row) => row.data ?? { id: `${row.from_id}->${row.to_id}`, from: String(row.from_id), to: String(row.to_id) }),
      )
      return graphFrom(next, items, edges, yield* consultations(workflowID))
    })

    const setStatus = Effect.fn("Workflow.setStatus")(function* (
      workflowID: WorkflowID,
      status: WorkflowInfo["status"],
      extra?: { error?: string; testerSessionID?: SessionID; testPath?: string },
    ) {
      const patch = {
        status,
        ...(extra?.error !== undefined ? { error: extra.error } : {}),
        ...(extra?.testerSessionID !== undefined ? { tester_session_id: extra.testerSessionID } : {}),
        ...(extra?.testPath !== undefined ? { test_path: extra.testPath } : {}),
        time_updated: Date.now(),
        ...(status === "completed" || status === "failed" || status === "cancelled"
          ? { time_completed: Date.now() }
          : { time_completed: null }),
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
      if (milestone) yield* bus.publish(Event.NodeUpdated, { workflowID, milestone })
      yield* publishUpdated(workflowID)
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
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
      yield* bus.publish(Event.GraphUpdated, { workflowID, graph: yield* graph(workflowID) })
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
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
            question: input.question,
            answer: input.answer,
            status: input.status,
            time_created: Date.now(),
            time_updated: Date.now(),
          })
          .run(),
      )
      yield* writeReferenceIndex(input.workflow.id).pipe(Effect.ignore)
      yield* bus.publish(Event.GraphUpdated, { workflowID: input.workflow.id, graph: yield* graph(input.workflow.id) })
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
    ) => Effect.Effect<void, unknown> = Effect.fn("Workflow.resolveConsultRequests")(function* (
      workflowID,
      sourceSessionID,
      sourceAgent,
      model,
      sourceRole,
      sourceMilestoneID,
      sourceAttempt,
      text,
    ) {
      const requests = parseConsultRequests(text)
      if (requests.length === 0) return
      const workflow = yield* get(workflowID)
      const items = yield* milestones(workflowID)
      for (const request of requests) {
        if (request.targetSessionID === sourceSessionID) continue
        const targetRole = workflowSessionRole(workflow, items, request.targetSessionID)
        const targetMilestoneID = workflowSessionMilestoneID(items, request.targetSessionID)
        const targetInfo = yield* session
          .get(request.targetSessionID)
          .pipe(Effect.mapError((error) => new Error({ message: error.message })))
        const answer = yield* runPrompt(
          request.targetSessionID,
          targetInfo.agent ?? workflowAgentForRole(targetRole),
          model,
          [
            "A peer session in the same workflow is asking for consultation.",
            "",
            workflowReferencePrompt(workflow),
            "",
            `Asking session: ${sourceSessionID}`,
            `Asking role: ${roleSessionTitle(sourceRole)}`,
            ...(request.reason ? [`Reason: ${request.reason}`] : []),
            "",
            "Question:",
            request.question,
            "",
            "Answer concisely with the facts, decisions, files, risks, and acceptance notes this peer needs. Do not modify workflow XML unless the answer requires a product-manager correction.",
          ].join("\n"),
          {
            workflowID,
            role: targetRole,
            milestoneID: targetMilestoneID,
          },
          { consult: false },
        )
        const answerText = latestText(answer)
        yield* recordConsultation({
          workflow,
          fromSessionID: sourceSessionID,
          toSessionID: request.targetSessionID,
          fromRole: sourceRole,
          toRole: targetRole,
          milestoneID: sourceMilestoneID,
          question: request.question,
          answer: answerText,
          status: "answered",
        })
        yield* runPrompt(
          sourceSessionID,
          sourceAgent,
          model,
          [
            "Workflow consultation response received.",
            "",
            `Consulted session: ${request.targetSessionID}`,
            `Consulted role: ${roleSessionTitle(targetRole)}`,
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
          { consult: false },
        )
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
      options?: { consult?: boolean },
    ) {
      const result = yield* prompt.prompt({
        sessionID,
        agent,
        model: modelRef(model),
        variant: model?.variant,
        messageID: MessageID.ascending(),
        parts: [textPart(text)],
      })
      if (archive) {
        yield* archiveWorkflowSession({
          workflowID: archive.workflowID,
          sessionID,
          role: archive.role,
          prompt: text,
          milestoneID: archive.milestoneID,
          attempt: archive.attempt,
        }).pipe(Effect.ignore)
        yield* writeReferenceIndex(archive.workflowID).pipe(Effect.ignore)
        yield* writeArchiveIndex(archive.workflowID).pipe(Effect.ignore)
        if (options?.consult !== false) {
          yield* resolveConsultRequests(
            archive.workflowID,
            sessionID,
            agent,
            model,
            archive.role,
            archive.milestoneID,
            archive.attempt,
            latestText(result),
          ).pipe(Effect.ignore)
        }
      }
      return result
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
      const info = yield* session.get(input.sessionID).pipe(Effect.mapError((error) => new Error({ message: error.message })))
      const messages = yield* session
        .messages({ sessionID: input.sessionID })
        .pipe(Effect.mapError((error) => new Error({ message: error.message })))
      const role = input.role ?? workflowSessionRole(workflow, yield* milestones(workflow.id), input.sessionID)
      yield* writeNote(
        workflowArtifactPath(workflow, workflowSessionArchivePath(input.sessionID)),
        archiveSessionMarkdown({
          workflow,
          session: info,
          role,
          prompt: input.prompt,
          milestoneID: input.milestoneID,
          attempt: input.attempt,
          messages,
        }),
      )
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
      const refs: WorkflowArchiveSessionRef[] = [
        ...(workflow.rootSessionID ? [{ role: "requester" as const, sessionID: workflow.rootSessionID }] : []),
        ...(workflow.pmSessionID ? [{ role: "main_pm" as const, sessionID: workflow.pmSessionID }] : []),
        ...(workflow.testerSessionID ? [{ role: "tester" as const, sessionID: workflow.testerSessionID }] : []),
        ...items.flatMap((milestone) =>
          milestone.session.map((ref) => ({
            ...ref,
            milestoneID: ref.milestoneID ?? milestone.id,
          })),
        ),
      ]
      yield* Effect.all(
        refs.map((ref) =>
          archiveWorkflowSession({
            workflowID,
            sessionID: ref.sessionID,
            role: ref.role,
            milestoneID: ref.milestoneID,
            attempt: ref.attempt,
          }).pipe(Effect.ignore),
        ),
        { concurrency: "unbounded", discard: true },
      )
      yield* writeReferenceIndex(workflowID).pipe(Effect.ignore)
      yield* writeArchiveIndex(workflowID).pipe(Effect.ignore)
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

    const appendSession = (milestone: WorkflowMilestoneInfo, ref: WorkflowSessionRef) => ({
      ...milestone,
      session: [...milestone.session, ref],
    })

    const runMilestone = Effect.fn("Workflow.runMilestone")(function* (
      workflow: WorkflowInfo,
      milestone: WorkflowMilestoneInfo,
    ) {
      if (workflow.status === "cancelled") return
      const attempt = milestone.attempt + 1
      const planned = yield* updateMilestone(workflow.id, milestone.id, {
        status: "planning",
        attempt,
      })
      if (!planned) return

      const pm = yield* createAgentSession({
        title: workflowSessionTitle("Department PM", milestone.title ?? String(milestone.id)),
        agent: "workflow-department-pm",
        parentID: workflow.rootSessionID,
        permission: pmPermission(),
        model: workflow.model,
      })
      yield* updateMilestone(workflow.id, milestone.id, {
        session: appendSession(planned, { role: "department_pm", sessionID: pm.id, milestoneID: milestone.id, attempt }).session,
      })
      const pmPrompt = promptDepartmentPm({ workflow, milestone })
      const pmResult = yield* runPrompt(pm.id, "workflow-department-pm", workflow.model, pmPrompt, {
        workflowID: workflow.id,
        role: "department_pm",
        milestoneID: milestone.id,
        attempt,
      })
      yield* writeNote(workflowArtifactPath(workflow, milestone.id, "plan.md"), latestText(pmResult))
      const refreshed = yield* refreshWorkflowXml(workflow.id, "dispatching")
      if (refreshed && !(yield* milestones(workflow.id)).some((item) => item.id === milestone.id && item.status !== "skipped")) {
        yield* schedule(workflow.id)
        return
      }

      const executing = (yield* milestones(workflow.id)).find((item) => item.id === milestone.id)
      if (!executing) return
      yield* updateMilestone(workflow.id, milestone.id, { status: "executing" })
      const executor = yield* createAgentSession({
        title: workflowSessionTitle("Executor", milestone.title ?? String(milestone.id)),
        agent: "workflow-executor",
        parentID: workflow.rootSessionID,
        model: workflow.model,
      })
      yield* updateMilestone(workflow.id, milestone.id, {
        session: appendSession(executing, { role: "executor", sessionID: executor.id, milestoneID: milestone.id, attempt })
          .session,
      })
      const executorResult = yield* runPrompt(
        executor.id,
        "workflow-executor",
        workflow.model,
        promptExecutor({ workflow, milestone }),
        {
          workflowID: workflow.id,
          role: "executor",
          milestoneID: milestone.id,
          attempt,
        },
      )

      const reviewing = (yield* milestones(workflow.id)).find((item) => item.id === milestone.id)
      if (!reviewing) return
      const reviewer = yield* createAgentSession({
        title: workflowSessionTitle("Reviewer", milestone.title ?? String(milestone.id)),
        agent: "workflow-reviewer",
        parentID: workflow.rootSessionID,
        permission: reviewerPermission(),
        model: workflow.model,
      })
      yield* updateMilestone(workflow.id, milestone.id, {
        status: "reviewing",
        session: appendSession(reviewing, { role: "reviewer", sessionID: reviewer.id, milestoneID: milestone.id, attempt })
          .session,
      })
      const reviewerResult = yield* runPrompt(
        reviewer.id,
        "workflow-reviewer",
        workflow.model,
        promptReviewer({ workflow, milestone, executorOutput: latestText(executorResult) }),
        {
          workflowID: workflow.id,
          role: "reviewer",
          milestoneID: milestone.id,
          attempt,
        },
      )
      const reviewText = latestText(reviewerResult)
      const reviewPath = workflowArtifactPath(workflow, milestone.id, `review-${attempt}.md`)
      yield* writeNote(reviewPath, reviewText)
      if (approved(reviewText)) {
        yield* updateMilestone(workflow.id, milestone.id, {
          status: "approved",
          reviewPath,
        })
        yield* schedule(workflow.id)
        return
      }
      if (rejected(reviewText) && attempt >= 3) {
        yield* updateMilestone(workflow.id, milestone.id, { status: "rejected", reviewPath })
        yield* blockWorkflow(workflow, `Milestone ${milestone.id} rejected ${attempt} times`)
        return
      }
      if (!rejected(reviewText)) {
        yield* updateMilestone(workflow.id, milestone.id, { status: "rejected", reviewPath })
        yield* blockWorkflow(workflow, `Reviewer did not return an approve/reject decision for milestone ${milestone.id}`)
        return
      }
      yield* updateMilestone(workflow.id, milestone.id, { status: "pending", reviewPath })
      yield* schedule(workflow.id)
    })

    const runTester = Effect.fn("Workflow.runTester")(function* (workflow: WorkflowInfo) {
      const items = yield* milestones(workflow.id)
      yield* setStatus(workflow.id, "testing")
      for (const item of items) {
        if (item.status === "approved") yield* updateMilestone(workflow.id, item.id, { status: "testing" })
      }
      const tester = yield* createAgentSession({
        title: workflowSessionTitle("Tester", workflow.title),
        agent: "workflow-tester",
        parentID: workflow.rootSessionID,
        model: workflow.model,
      })
      const testerPrompt = promptTester({ workflow, milestones: items })
      const testerResult = yield* runPrompt(tester.id, "workflow-tester", workflow.model, testerPrompt, {
        workflowID: workflow.id,
        role: "tester",
      })
      yield* writeNote(workflowArtifactPath(workflow, "test-plan.md"), latestText(testerResult))
      for (const item of yield* milestones(workflow.id)) {
        if (item.status === "testing" || item.status === "approved") {
          yield* updateMilestone(workflow.id, item.id, { status: "done" })
        }
      }
      yield* setStatus(workflow.id, "completed", {
        testerSessionID: tester.id,
        testPath: workflowArtifactPath(workflow, "test-plan.md"),
      })
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

    const schedule: (workflowID: WorkflowID) => Effect.Effect<WorkflowInfo, unknown> = Effect.fn("Workflow.schedule")(
      function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      if (workflow.status === "cancelled" || workflow.status === "completed" || workflow.status === "blocked") return workflow
      const items = yield* milestones(workflowID)
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
      const active = items.some((item) =>
        ["planning", "executing", "reviewing", "testing"].includes(item.status),
      )
      if (active) return workflow
      const ready = readyMilestones(definition, milestoneStates(items)).slice(0, 4)
      if (ready.length === 0) {
        if (items.some((item) => item.status === "rejected")) {
          for (const item of items.filter((item) => item.status === "rejected")) {
            yield* updateMilestone(workflowID, item.id, { status: "pending" })
          }
          yield* schedule(workflowID)
          return yield* get(workflowID)
        }
        return yield* blockWorkflow(workflow, "No runnable milestones are available")
      }
      yield* setStatus(workflowID, "executing")
      for (const item of ready) {
        const current = items.find((milestone) => milestone.id === item.id)
        if (!current) continue
        yield* background.start({
          id: milestoneJobID(workflowID, current.id, current.attempt + 1),
          type: "workflow.milestone",
          title: `${workflow.title} ${current.id}`,
          metadata: { workflowID, milestoneID: current.id },
          run: runMilestone(workflow, current).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.interrupt
                : Effect.gen(function* () {
                    yield* updateMilestone(workflowID, current.id, { status: "failed" })
                    yield* setStatus(workflowID, "failed", {
                      error: `Milestone ${current.id} failed: ${errorFromCause(cause)}`,
                    })
                  }),
            ),
            Effect.as(`milestone ${current.id} completed`),
          ),
        })
      }
      return yield* get(workflowID)
    },
    )

    const runPlanning = Effect.fn("Workflow.runPlanning")(function* (workflowID: WorkflowID) {
      const workflow = yield* get(workflowID)
      const main = yield* createAgentSession({
        title: workflowSessionTitle("Main PM", workflow.title),
        agent: "workflow-main-pm",
        parentID: workflow.rootSessionID,
        permission: pmPermission(),
        model: workflow.model,
      })
      yield* setMainProductManagerSession(workflowID, main.id)
      const mainPrompt = promptMainPm({ workflow: { ...workflow, pmSessionID: main.id } })
      yield* runPrompt(main.id, "workflow-main-pm", workflow.model, mainPrompt, {
        workflowID,
        role: "main_pm",
      })
      yield* continuePlanning(workflowID)
    })

    const initState = yield* InstanceState.make(
      Effect.fn("Workflow.initState")(function* () {
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
              if (!workflow || workflow.status !== "planning") return
              yield* archiveWorkflowSession({
                workflowID: workflow.id,
                sessionID: payload.properties.sessionID,
                role: "main_pm",
              }).pipe(Effect.ignore)
              const items = yield* milestones(workflow.id)
              if (!items.every((item) => item.session.length === 0 && item.status === "pending")) return
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
                      : blockPlanning(workflow.id, `Workflow planning failed: ${errorFromCause(cause)}`).pipe(
                          Effect.asVoid,
                        ),
                  ),
                  Effect.as("workflow planning continued"),
                ),
              })
            }),
          ),
          Effect.forkScoped,
        )
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
        model,
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
            model: info.model,
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
      yield* archiveWorkflowSession({ workflowID: id, sessionID: root.id, role: "requester" }).pipe(Effect.ignore)
      yield* writeArchiveIndex(id).pipe(Effect.ignore)
      yield* bus.publish(Event.Created, { workflowID: id, info })
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
      yield* bus.publish(Event.GraphUpdated, { workflowID: input.workflowID, graph: result })
      yield* publishUpdated(input.workflowID)
      return result
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

    return Service.of({ start, get, list, graph, updateXml, resume, cancel })
  }),
)

export const defaultLayer = Layer.suspend(() =>
  layer.pipe(
    Layer.provide(BackgroundJob.defaultLayer),
    Layer.provide(Bus.layer),
    Layer.provide(Session.defaultLayer),
    Layer.provide(SessionPrompt.defaultLayer),
  ),
)

export * as Workflow from "./workflow"
