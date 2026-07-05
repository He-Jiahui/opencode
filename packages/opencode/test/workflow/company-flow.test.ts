// @ts-nocheck
import { describe, expect } from "bun:test"
import { createHash } from "crypto"
import { Effect, Layer, Schema } from "effect"
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "fs"
import path from "path"

import { BackgroundJob } from "@/background/job"
import { Bus } from "@/bus"
import { BusEvent } from "@/bus/bus-event"
import { GlobalBus, type GlobalEvent } from "@/bus/global"
import { EventV2Bridge } from "@/event-v2-bridge"
import { FileWatcher } from "@/file/watcher"
import { Agent } from "@/agent/agent"
import { ModelID, ProviderID } from "@/provider/schema"
import { Database, and, eq } from "@/storage/db"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { WorkflowTool, workflowCommandJournalSummary, workflowDispatchSummary } from "@/tool/workflow"
import { WorkflowMessageTool } from "@/tool/workflow-message"
import type { Context } from "@/tool/tool"
import { Truncate } from "@/tool/truncate"
import { Workflow, workflowDispatchClaimWithoutControl, workflowMessageDispatchMisuse } from "@/workflow/workflow"
import {
  WorkflowToolCommandEvent,
  WorkflowToolCommandResultEvent,
  type WorkflowToolCommandResult,
} from "@/workflow/command"
import { WorkflowMilestoneID } from "@/workflow/schema"
import {
  WorkflowConsultationTable,
  WorkflowEdgeTable,
  WorkflowInterventionTable,
  WorkflowMessageTable,
  WorkflowMemberTable,
  WorkflowMilestoneTable,
  WorkflowTable,
} from "@/workflow/workflow.sql"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { TestInstance } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"

const testProviderID = ProviderID.make("test")
const testModelID = ModelID.make("workflow-test-model")
const failTesterOnce = new Set<string>()
const failTechnicalOnce = new Set<string>()
const failMainPmAcceptanceOnce = new Set<string>()
const failRequesterAcceptanceOnce = new Set<string>()
const pmSupervisionBlockOnce = new Set<string>()
const standupBlockOnce = new Set<string>()
const pmRefreshWorkflowXmlOnce = new Set<string>()
const departmentPmWorkflowUpdateOnce = new Set<string>()
const workflowXmlRewriteDirectories = new Map<string, string>()
const testSessionIdleEvent = BusEvent.define("session.idle", Schema.Struct({ sessionID: SessionID }))

function workflowStateTestContentHash(value: unknown) {
  return createHash("sha256").update(workflowStateTestStableJson(value)).digest("hex")
}

function workflowStateTestStableJson(value: unknown): string {
  if (value === undefined) return "null"
  if (value === null || typeof value !== "object") return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(workflowStateTestStableJson).join(",")}]`
  return `{${Object.entries(value as Record<string, unknown>)
    .filter((entry) => entry[1] !== undefined)
    .toSorted((a, b) => a[0].localeCompare(b[0]))
    .map((entry) => `${JSON.stringify(entry[0])}:${workflowStateTestStableJson(entry[1])}`)
    .join(",")}}`
}

const commandBusWorkflowXml = `<workflow>
  <ordered>
    <milestone id="requirements" title="Clarify requirements" department="product">Clarify the user request, constraints, and acceptance criteria.</milestone>
    <milestone id="implementation" title="Implement solution" department="engineering">Implement the requested change end to end.</milestone>
    <milestone id="verification" title="Verify solution" department="quality">Verify behavior with focused tests and checks.</milestone>
  </ordered>
</workflow>`
const failedDependencyWorkflowXml = `<workflow>
  <ordered>
    <milestone id="requirements" title="Clarify requirements" department="product">Clarify the user request, constraints, and acceptance criteria.</milestone>
    <parallel>
      <milestone id="implementation" title="Implement solution" department="engineering">Implement the requested change end to end.</milestone>
      <milestone id="verification" title="Verify solution" department="quality">Verify behavior with focused tests and checks.</milestone>
    </parallel>
  </ordered>
</workflow>`
const thirteenAuditIDs = Array.from({ length: 13 }, (_, index) => `audit-${String(index + 1).padStart(2, "0")}`)
const thirteenAuditWorkflowXml = `<workflow>
  <ordered>
    <milestone id="requirements" title="Clarify requirements" department="product">Clarify audit charter, rubric, and acceptance gates.</milestone>
    <parallel>
${thirteenAuditIDs
  .map(
    (id) =>
      `      <milestone id="${id}" title="Audit ${id}" department="engineering">Audit ${id} and write ${id}/report.md.</milestone>`,
  )
  .join("\n")}
    </parallel>
  </ordered>
</workflow>`
const parallelDepartmentWorkflowXml = `<workflow>
  <parallel>
    <milestone id="track-a" title="Track A" department="engineering">Plan and execute track A.</milestone>
    <milestone id="track-b" title="Track B" department="engineering">Plan and execute track B.</milestone>
  </parallel>
</workflow>`
const mixedDepartmentWorkflowXml = `<workflow>
  <parallel>
    <milestone id="graphics-track" title="Graphics Track" department="graphics">Plan graphics work.</milestone>
    <milestone id="audio-track" title="Audio Track" department="audio">Plan audio work.</milestone>
  </parallel>
</workflow>`
const economicalWorkflowXml = `<workflow>
  <parallel>
    <milestone id="eco-a" title="Economical A" department="engineering">Plan and execute economical track A.</milestone>
    <milestone id="eco-b" title="Economical B" department="engineering">Plan and execute economical track B.</milestone>
    <milestone id="eco-c" title="Economical C" department="engineering">Plan and execute economical track C.</milestone>
  </parallel>
</workflow>`
const reviewSkipWorkflowXml = `<workflow>
  <ordered>
    <milestone id="tiny" title="Tiny" department="engineering" review="skip">Plan and execute a small focused task.</milestone>
    <milestone id="after" title="After" department="engineering">Continue after the small task.</milestone>
  </ordered>
</workflow>`
const deleteActiveWorkflowXml = `<workflow>
  <ordered>
    <milestone id="implementation" title="Implement solution" department="engineering" review="skip">Implement the updated scope after requirements is removed from the graph.</milestone>
    <milestone id="verification" title="Verify solution" department="quality" review="skip">Verify the updated scope after implementation completes.</milestone>
  </ordered>
</workflow>`

const fakePromptLayer = Layer.effect(
  SessionPrompt.Service,
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const prompt = (input: FakePromptInput) =>
      Effect.gen(function* () {
        const now = Date.now()
        const model = input.model ?? { providerID: testProviderID, modelID: testModelID }
        const userID = input.messageID ?? MessageID.ascending()
        const user: MessageV2.User = {
          id: userID,
          sessionID: input.sessionID,
          role: "user",
          time: { created: now },
          agent: input.agent ?? "build",
          model: {
            providerID: model.providerID,
            modelID: model.modelID,
            ...(input.variant ? { variant: input.variant } : {}),
          },
        }
        yield* sessions.updateMessage(user)
        yield* Effect.all(
          textPromptParts(input).map((part) =>
            sessions.updatePart({
              id: part.id ?? PartID.ascending(),
              sessionID: input.sessionID,
              messageID: userID,
              type: "text" as const,
              text: part.text,
              ...(part.synthetic !== undefined ? { synthetic: part.synthetic } : {}),
              ...(part.ignored !== undefined ? { ignored: part.ignored } : {}),
              ...(part.time !== undefined ? { time: part.time } : {}),
              ...(part.metadata !== undefined ? { metadata: part.metadata } : {}),
            }),
          ),
          { discard: true },
        )
        const text = promptText(input.parts ?? [])
        if (text.includes("Standup control workflow") && text.includes("department product manager")) {
          yield* Effect.sleep("2 seconds")
        }
        if (text.includes("Update XML delete active workflow") && text.includes("department product manager")) {
          yield* Effect.sleep("2 seconds")
        }
        if (text.includes("Economical scheduling workflow") && text.includes("department product manager")) {
          yield* Effect.sleep("2 seconds")
        }
        if (
          text.includes("Executor peer sync workflow") &&
          text.includes("long-lived executor employee assigned to workflow milestone alpha")
        ) {
          yield* Effect.sleep("1 second")
        }
        if (
          text.includes("Parallel drain workflow") &&
          text.includes("long-lived executor employee assigned to workflow milestone parallel-a")
        ) {
          yield* Effect.sleep("15 seconds")
        }
        const result = fakeMessage({ ...input, messageID: userID, model })
        yield* sessions.updateMessage(result.info)
        yield* Effect.all(result.parts.map((part) => sessions.updatePart(part)), { discard: true })
        return result
      })
    return SessionPrompt.Service.of({
      cancel: () => Effect.void,
      prompt,
      loop: (input) => prompt({ sessionID: input.sessionID }),
      shell: (input) => prompt({ sessionID: input.sessionID, agent: input.agent, model: input.model }),
      command: (input) => prompt({ sessionID: input.sessionID, agent: input.agent, messageID: input.messageID }),
      resolvePromptParts: (template) => Effect.succeed([{ type: "text", text: template }]),
    })
  }),
)

const it = testEffect(
  Workflow.layer.pipe(
    Layer.provideMerge(fakePromptLayer),
    Layer.provideMerge(
      Layer.mergeAll(
        BackgroundJob.defaultLayer,
        Bus.layer,
        EventV2Bridge.defaultLayer,
        Session.defaultLayer,
        Truncate.defaultLayer,
        Agent.defaultLayer,
      ),
    ),
  ),
)

function workflowMessageContext(sessionID: SessionID): Context {
  return {
    sessionID,
    messageID: MessageID.ascending(),
    agent: "build",
    abort: AbortSignal.any([]),
    messages: [],
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}

describe("company workflow execution", () => {
  it.instance(
    "detects role messages that try to act as milestone dispatch",
    () =>
      Effect.sync(() => {
        expect(
          workflowMessageDispatchMisuse(
            '<opencode-workflow-message to-role="executor" specialty="graphics" timing="after-task" reason="first wave dispatch">Your assignment: audit-graphics-scene-renderer. Output report.md.</opencode-workflow-message>',
            "main_pm",
          ),
        ).toBe(true)
        expect(
          workflowMessageDispatchMisuse(
            '<opencode-workflow-message to-role="expert" timing="temporary-interrupt" reason="architecture question">Can you confirm the render architecture risk?</opencode-workflow-message>',
            "main_pm",
          ),
        ).toBe(false)
        expect(
          workflowMessageDispatchMisuse(
            '<opencode-workflow-message to-role="executor" timing="temporary-interrupt" reason="context question">Can you explain which files already contain the current evidence?</opencode-workflow-message>',
            "department_pm",
          ),
        ).toBe(false)
        expect(
          workflowMessageDispatchMisuse(
            '<opencode-workflow-message to-role="executor" specialty="graphics" timing="temporary-interrupt" reason="direct requester dispatch">Your assignment: audit-graphics-scene-renderer. Output report.md.</opencode-workflow-message>',
            "requester",
          ),
        ).toBe(true)
        expect(
          workflowDispatchClaimWithoutControl(
            [
              "Still no pickup. I will use the documented communication channel now.",
              "Your assignment: audit-graphics-scene-renderer.",
              "These messages route through the workflow manager and should prompt executor sessions directly.",
            ].join("\n"),
            "main_pm",
          ),
        ).toBe(true)
        expect(
          workflowDispatchClaimWithoutControl(
            [
              "I realize I have not used the documented dispatch channel yet.",
              "The proper mechanism for routing to employees is the raw <opencode-workflow-message> XML blocks.",
              "Per the requester direction, I am assigning the first wave directly.",
              "Your assignment: audit-graphics-scene-renderer.",
              "These four messages route through the workflow manager's communication channel and should prompt executor sessions directly.",
            ].join("\n"),
            "main_pm",
          ),
        ).toBe(true)
        expect(
          workflowDispatchClaimWithoutControl(
            [
              "You're right that hammering the control plane hasn't drained the queue.",
              "But I realize I haven't used the documented dispatch channel yet.",
              "I'll now use the documented communication channel - raw <opencode-workflow-message> blocks directed at the executor roles by specialty.",
              "Per the requester's direction, I'm assigning the first wave directly.",
              '<opencode-workflow-message to-role="executor" specialty="graphics" timing="after-task" reason="first wave dispatch">Your assignment: audit-graphics-scene-renderer.</opencode-workflow-message>',
              '<opencode-workflow-message to-role="executor" specialty="core" timing="after-task" reason="first wave dispatch">Your assignment: audit-core-spine.</opencode-workflow-message>',
              "These four messages route through the workflow manager's communication channel and should prompt the executor sessions directly.",
            ].join("\n"),
            "main_pm",
          ),
        ).toBe(true)
        expect(
          workflowDispatchClaimWithoutControl(
            '<opencode-workflow-control action="resume">continue scheduling</opencode-workflow-control>',
            "main_pm",
          ),
        ).toBe(false)
        expect(
          workflowDispatchClaimWithoutControl(
            "I queued milestone_status=approved for requirements and two resume nudges, but requirements still has not drained from planning.",
            "main_pm",
          ),
        ).toBe(true)
        expect(
          workflowDispatchClaimWithoutControl(
            "The control-plane queue appears stalled: milestone_status=approval has not transitioned and downstream tracks are still gated.",
            "main_pm",
          ),
        ).toBe(true)
      }),
  )

  it.instance(
    "restores workflow state from the project-local workflow folder",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: "Portable workflow restore",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
          modelWhitelist: {
            departmentPM: [
              {
                providerID: testProviderID,
                modelID: testModelID,
                weight: 80,
                cacheMinutes: 30,
              },
            ],
          },
        })
        const rootSessionID = started.rootSessionID
        if (!rootSessionID) throw new Error("workflow did not create a requester session")

        expect(started.path.split(/[\\/]/).at(-1)).not.toContain("Portable workflow restore")
        expect(started.path).toBe(path.join(".opencode", "workflows", started.id))
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = (yield* Effect.promise(() => Bun.file(statePath).json())) as WorkflowStateSnapshotJson
        expect(state.schema).toBe(2)
        expect(state.version).toBe(2)
        expect(state.journal?.commands?.highWater).toBe(0)
        expect(state.workflow.directory).toBe(".")
        expect(state.workflow.path).toBe(started.path)
        const rootSessionState = state.sessions.find((item) => item.id === rootSessionID)
        if (!rootSessionState?.path) throw new Error("Expected requester session sidecar in workflow state")
        expect(rootSessionState.path).toBe(path.join("state", "sessions", `${rootSessionID}.json`))
        expect(rootSessionState.messages).toBeUndefined()
        const rootSessionSidecar = JSON.parse(readFileSync(path.join(instance.directory, started.path, rootSessionState.path), "utf8"))
        expect(rootSessionSidecar.id).toBe(rootSessionID)
        expect(Array.isArray(rootSessionSidecar.messages)).toBe(true)
        const departmentPM = state.members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected department PM member in workflow state")
        expect(departmentPM.model?.providerID).toBe(testProviderID)
        expect(departmentPM.model?.modelID).toBe(testModelID)
        expect(departmentPM.modelWeight).toBe(80)
        expect(typeof departmentPM.modelCacheUntil).toBe("number")
        const legacyState = { ...state, version: 1 }
        delete legacyState.schema
        delete legacyState.manifest
        delete legacyState.journal
        writeFileSync(statePath, `${JSON.stringify(legacyState, null, 2)}\n`)

        Database.use((db) => {
          db.delete(WorkflowTable).where(eq(WorkflowTable.id, started.id)).run()
          db.delete(WorkflowMemberTable).where(eq(WorkflowMemberTable.workflow_id, started.id)).run()
          db.delete(SessionTable).where(eq(SessionTable.id, rootSessionID)).run()
        })

        const restored = yield* workflow.get(started.id)
        const restoredRoot = yield* sessions.get(rootSessionID)

        expect(restored.id).toBe(started.id)
        expect(restored.directory).toBe(instance.directory)
        expect(restored.path).toBe(started.path)
        expect(restoredRoot.id).toBe(rootSessionID)
        expect(restoredRoot.title).toContain("Portable workflow restore")
        expect(
          (yield* sessions.messages({ sessionID: rootSessionID, limit: 5 })).some((message) =>
            message.parts.some((part) => part.type === "text" && part.text.includes("project-local workflow snapshot")),
          ),
        ).toBe(true)
        const migratedState = (yield* Effect.promise(() => Bun.file(statePath).json())) as WorkflowStateSnapshotJson
        expect(migratedState.schema).toBe(2)
        expect(migratedState.version).toBe(2)
        expect(migratedState.workflow.directory).toBe(".")
        expect(migratedState.sessions.find((item) => item.id === rootSessionID)?.messages).toBeUndefined()
        const restoredDepartmentPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "department_pm")
        expect(restoredDepartmentPM?.model?.providerID).toBe(testProviderID)
        expect(restoredDepartmentPM?.model?.modelID).toBe(testModelID)
        expect(restoredDepartmentPM?.modelWeight).toBe(80)
        expect(restoredDepartmentPM?.modelCacheUntil).toBe(departmentPM.modelCacheUntil)
        expect((yield* workflow.list()).map((item) => item.id)).toContain(started.id)
      }),
  )

  it.instance(
    "updates existing session rows while restoring workflow state from disk",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Restore updates stale session rows",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const rootSessionID = started.rootSessionID
        if (!rootSessionID) throw new Error("workflow did not create a requester session")
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        const restoredTitle = "Requester: restored title from workflow-state"
        writeFileSync(
          statePath,
          `${JSON.stringify(
            {
              ...state,
              sessions: state.sessions.map((item) => (item.id === rootSessionID ? { ...item, title: restoredTitle } : item)),
            },
            null,
            2,
          )}\n`,
        )
        Database.use((db) => {
          db.update(SessionTable).set({ title: "Requester: stale database title" }).where(eq(SessionTable.id, rootSessionID)).run()
          db.delete(WorkflowTable).where(eq(WorkflowTable.id, started.id)).run()
        })

        yield* workflow.get(started.id)
        expect(
          Database.use((db) => db.select({ title: SessionTable.title }).from(SessionTable).where(eq(SessionTable.id, rootSessionID)).get())
            ?.title,
        ).toBe(restoredTitle)
      }),
  )

  it.instance(
    "updates stale session message rows while restoring workflow state from disk",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Restore updates stale session messages",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const rootSessionID = started.rootSessionID
        if (!rootSessionID) throw new Error("workflow did not create a requester session")

        const messageID = MessageID.ascending("msg_restore_content")
        const partID = PartID.ascending("prt_restore_content")
        const now = Date.now()
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        const sessionIndex = state.sessions.find((item) => item.id === rootSessionID)
        if (!sessionIndex?.path) throw new Error("Expected requester session sidecar in workflow state")
        const sessionStatePath = path.join(instance.directory, started.path, sessionIndex.path)
        const sessionState = JSON.parse(readFileSync(sessionStatePath, "utf8"))
        const messageData = {
          role: "user" as const,
          time: { created: now },
          agent: "build",
          model: { providerID: testProviderID, modelID: testModelID },
        }
        const storedPartData = {
          type: "text" as const,
          text: "authoritative snapshot text",
          time: { start: now, end: now },
        }
        const nextSessionState = {
          ...sessionState,
          messages: [
            ...(Array.isArray(sessionState.messages) ? sessionState.messages : []),
            {
              info: {
                id: messageID,
                sessionID: rootSessionID,
                ...messageData,
              },
              parts: [
                {
                  id: partID,
                  sessionID: rootSessionID,
                  messageID,
                  ...storedPartData,
                },
              ],
            },
          ],
        }
        writeFileSync(sessionStatePath, `${JSON.stringify(nextSessionState, null, 2)}\n`)
        writeFileSync(
          statePath,
          `${JSON.stringify(
            {
              ...state,
              sessions: state.sessions.map((item) =>
                item.id === rootSessionID ? { ...item, contentHash: workflowStateTestContentHash(nextSessionState) } : item,
              ),
            },
            null,
            2,
          )}\n`,
        )
        Database.use((db) => {
          db.insert(MessageTable)
            .values([{
              id: messageID,
              session_id: rootSessionID,
              time_created: now,
              time_updated: now,
              data: messageData as typeof MessageTable.$inferInsert.data,
            }])
            .run()
          db.insert(PartTable)
            .values([{
              id: partID,
              session_id: rootSessionID,
              message_id: messageID,
              time_created: now,
              time_updated: now,
              data: { ...storedPartData, text: "stale database text with newer timestamp" } as typeof PartTable.$inferInsert.data,
            }])
            .run()
        })

        yield* workflow.list()

        expect(Database.use((db) => db.select({ data: PartTable.data }).from(PartTable).where(eq(PartTable.id, partID)).get())?.data).toEqual(
          storedPartData,
        )
      }),
  )

  it.instance(
    "migrates legacy v1 workflow state with canonical statuses",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Legacy v1 migration workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const rootSessionID = started.rootSessionID
        if (!rootSessionID) throw new Error("workflow did not create a requester session")
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        const legacyMessageID = MessageID.ascending()
        const now = Date.now()
        const legacyMessage = {
          info: {
            id: legacyMessageID,
            sessionID: rootSessionID,
            role: "assistant",
            time: { created: now, completed: now },
            parentID: MessageID.ascending(),
            modelID: testModelID,
            providerID: testProviderID,
            mode: "build",
            agent: "build",
            path: { cwd: "", root: "" },
            cost: 0,
            tokens: {
              input: 0,
              output: 0,
              reasoning: 0,
              cache: {
                read: 0,
                write: 0,
              },
            },
            finish: "stop",
          },
          parts: [
            {
              id: PartID.ascending(),
              sessionID: rootSessionID,
              messageID: legacyMessageID,
              type: "text",
              text: "legacy v1 inline message",
              time: { start: now, end: now },
            },
          ],
        }
        const legacyState = {
          ...state,
          version: 1,
          workflow: { ...state.workflow, status: "running" },
          milestones: state.milestones.map((milestone) => ({
            ...milestone,
            status:
              milestone.id === "requirements"
                ? "completed"
                : milestone.id === "implementation"
                  ? "running"
                  : milestone.status,
          })),
          sessions: state.sessions.map((item) =>
            item.id === rootSessionID ? { ...item, path: undefined, messages: [legacyMessage] } : item,
          ),
        }
        delete legacyState.schema
        delete legacyState.manifest
        delete legacyState.journal
        writeFileSync(statePath, `${JSON.stringify(legacyState, null, 2)}\n`)

        const originalMilestoneCount = state.milestones.length
        const originalMemberCount = state.members.length
        const originalSessionCount = state.sessions.length
        const sessionIDs = [rootSessionID, ...state.members.flatMap((member) => (member.sessionID ? [member.sessionID] : []))]
        Database.use((db) => {
          db.delete(WorkflowEdgeTable).where(eq(WorkflowEdgeTable.workflow_id, started.id)).run()
          db.delete(WorkflowConsultationTable).where(eq(WorkflowConsultationTable.workflow_id, started.id)).run()
          db.delete(WorkflowInterventionTable).where(eq(WorkflowInterventionTable.workflow_id, started.id)).run()
          db.delete(WorkflowMilestoneTable).where(eq(WorkflowMilestoneTable.workflow_id, started.id)).run()
          db.delete(WorkflowMemberTable).where(eq(WorkflowMemberTable.workflow_id, started.id)).run()
          db.delete(WorkflowTable).where(eq(WorkflowTable.id, started.id)).run()
          sessionIDs.forEach((sessionID) => db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run())
        })

        const restored = yield* workflow.get(started.id)
        const graph = yield* workflow.graph(started.id)
        const migrated = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson

        expect(restored.status).toBe("executing")
        expect(graph.members.length).toBe(originalMemberCount)
        expect(graph.milestones.length).toBe(originalMilestoneCount)
        expect(migrated.sessions.length).toBe(originalSessionCount)
        expect(graph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
        expect(graph.milestones.find((milestone) => milestone.id === "implementation")?.status).toBe("executing")
        const legacyStoredMessage = Database.use((db) =>
          db.select().from(MessageTable).where(eq(MessageTable.id, legacyMessageID)).get(),
        )
        const legacyStoredPart = Database.use((db) =>
          db.select().from(PartTable).where(eq(PartTable.message_id, legacyMessageID)).get(),
        )
        const storedRootMessages = Database.use((db) =>
          db.select().from(MessageTable).where(eq(MessageTable.session_id, rootSessionID)).all(),
        )
        expect(legacyStoredMessage?.session_id).toBe(rootSessionID)
        expect(legacyStoredPart?.session_id).toBe(rootSessionID)
        expect(storedRootMessages.map((message) => message.id)).toContain(legacyMessageID)
        const restoredTexts = (yield* sessions.messages({ sessionID: rootSessionID })).flatMap((message) =>
          message.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])),
        )
        expect(restoredTexts.join("\n")).toContain("legacy v1 inline message")
        expect(migrated.schema).toBe(2)
        expect(migrated.version).toBe(2)
        expect(migrated.workflow.status).toBe("executing")
        expect(migrated.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
        expect(migrated.milestones.find((milestone) => milestone.id === "implementation")?.status).toBe("executing")
        const migratedSecondPass = JSON.stringify(migrated)
        yield* workflow.get(started.id)
        const remigrated = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        expect(remigrated.sessions.length).toBe(originalSessionCount)
        expect(JSON.stringify(remigrated)).toBe(migratedSecondPass)
      }),
    30_000,
  )

  it.instance(
    "recovers legacy queued workflow tool commands from v1 session history",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Legacy queued command workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const rootSessionID = started.rootSessionID
        if (!rootSessionID) throw new Error("workflow did not create a requester session")
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        const departmentPM = state.members.find((member) => member.role === "department_pm")
        if (!departmentPM?.sessionID) throw new Error("Expected department PM member session")
        const legacyMessageID = MessageID.ascending()
        const legacyMainMessageID = MessageID.ascending()
        const now = Date.now()
        const legacyToolPartID = PartID.ascending()
        const legacyMainToolPartID = PartID.ascending()
        const legacyToolMessage = {
          info: {
            id: legacyMessageID,
            sessionID: departmentPM.sessionID,
            role: "assistant",
            time: { created: now, completed: now },
            agent: "build",
            model: { providerID: testProviderID, modelID: testModelID },
          },
          parts: [
            {
              id: legacyToolPartID,
              sessionID: departmentPM.sessionID,
              messageID: legacyMessageID,
              type: "tool",
              tool: "workflow",
              state: {
                status: "completed",
                input: {
                  action: "milestone_status",
                  milestoneID: "requirements",
                  milestoneStatus: "done",
                  message: "legacy v1 queued department PM completion",
                },
                output: "Queued workflow command: milestone_status",
                metadata: {
                  workflowID: started.id,
                  queued: true,
                  action: "milestone_status",
                },
              },
              time: { start: now, end: now },
            },
          ],
        }
        const legacyMainToolMessage = {
          info: {
            id: legacyMainMessageID,
            sessionID: rootSessionID,
            role: "assistant",
            time: { created: now + 1, completed: now + 1 },
            agent: "build",
            model: { providerID: testProviderID, modelID: testModelID },
          },
          parts: [
            {
              id: legacyMainToolPartID,
              sessionID: rootSessionID,
              messageID: legacyMainMessageID,
              type: "tool",
              tool: "workflow",
              state: {
                status: "completed",
                input: {
                  action: "milestone_status",
                  milestoneID: "requirements",
                  milestoneStatus: "approved",
                  message: "legacy v1 queued main PM approval",
                },
                output: "Queued workflow command: milestone_status",
                metadata: {
                  workflowID: started.id,
                  queued: true,
                  action: "milestone_status",
                },
              },
              time: { start: now + 1, end: now + 1 },
            },
          ],
        }
        const requirementsSession = {
          role: "department_pm" as const,
          sessionID: departmentPM.sessionID,
          milestoneID: WorkflowMilestoneID.make("requirements"),
          attempt: 1,
        }
        const legacyState = {
          ...state,
          version: 1,
          workflow: { ...state.workflow, status: "dispatching" },
          milestones: state.milestones.map((milestone) => ({
            ...milestone,
            status: milestone.id === "requirements" ? "planning" : "pending",
            session: milestone.id === "requirements" ? [requirementsSession] : [],
          })),
          sessions: state.sessions.map((item) => {
            if (item.id === departmentPM.sessionID) return { ...item, path: undefined, messages: [legacyToolMessage] }
            if (item.id === rootSessionID) return { ...item, path: undefined, messages: [legacyMainToolMessage] }
            return item
          }),
        }
        delete legacyState.schema
        delete legacyState.manifest
        delete legacyState.journal
        const journalDir = path.join(instance.directory, started.path, "journal")
        rmSync(journalDir, { recursive: true, force: true })
        mkdirSync(journalDir, { recursive: true })
        writeFileSync(
          path.join(journalDir, "commands.jsonl"),
          `${JSON.stringify({
            seq: 1,
            id: "legacy-unrelated-rejected",
            action: "update_xml",
            outcome: "rejected",
            message: "legacy unrelated rejected command should not prevent queued gate recovery",
          })}\n`,
        )
        writeFileSync(statePath, `${JSON.stringify(legacyState, null, 2)}\n`)

        const sessionIDs = [rootSessionID, ...state.members.flatMap((member) => (member.sessionID ? [member.sessionID] : []))]
        Database.use((db) => {
          db.delete(WorkflowEdgeTable).where(eq(WorkflowEdgeTable.workflow_id, started.id)).run()
          db.delete(WorkflowConsultationTable).where(eq(WorkflowConsultationTable.workflow_id, started.id)).run()
          db.delete(WorkflowInterventionTable).where(eq(WorkflowInterventionTable.workflow_id, started.id)).run()
          db.delete(WorkflowMilestoneTable).where(eq(WorkflowMilestoneTable.workflow_id, started.id)).run()
          db.delete(WorkflowMemberTable).where(eq(WorkflowMemberTable.workflow_id, started.id)).run()
          db.delete(WorkflowTable).where(eq(WorkflowTable.id, started.id)).run()
          sessionIDs.forEach((sessionID) => db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run())
        })

        yield* workflow.get(started.id)
        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((graph) =>
              graph.milestones.find((milestone) => milestone.id === "requirements")?.status === "done" &&
              graph.milestones
                .find((milestone) => milestone.id === "implementation")
                ?.session.some((ref) => ref.role === "department_pm" || ref.role === "executor")
                ? graph
                : undefined,
            ),
          ),
          "legacy queued workflow command recovery did not dispatch implementation",
          "20 seconds",
        )
        const requirements = graph.milestones.find((milestone) => milestone.id === "requirements")
        const implementation = graph.milestones.find((milestone) => milestone.id === "implementation")
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))

        expect(requirements?.status).toBe("done")
        expect(implementation?.session.some((ref) => ref.role === "department_pm" || ref.role === "executor")).toBe(true)
        expect(journal.some((row) => row.id === "legacy-unrelated-rejected" && row.outcome === "rejected")).toBe(true)
        expect(journal.some((row) => row.id === `legacy-queued:${legacyMainToolPartID}` && row.outcome === "applied")).toBe(true)
        const migrated = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        expect(migrated.schema).toBe(2)
        expect(migrated.journal?.commands?.highWater).toBeGreaterThan(0)
      }),
    30_000,
  )

  it.instance(
    "resume recovers legacy queued workflow tool commands from an existing v1 state file",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: ["Legacy queued resume workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        const mainPM = state.members.find((member) => member.role === "main_pm")
        const mainPMSessionID = started.pmSessionID ?? mainPM?.sessionID
        if (!mainPMSessionID) throw new Error("workflow did not create a main PM session")
        const departmentPM = state.members.find((member) => member.role === "department_pm")
        if (!departmentPM?.sessionID) throw new Error("Expected department PM member session")
        yield* Effect.all(
          (yield* background.list())
            .filter((job) => job.metadata?.workflowID === started.id)
            .map((job) => background.cancel(job.id).pipe(Effect.ignore)),
          { discard: true },
        )

        const legacyMessageID = MessageID.ascending()
        const legacyToolPartID = PartID.ascending()
        const now = Date.now()
        const legacyToolMessage = {
          info: {
            id: legacyMessageID,
            sessionID: mainPMSessionID,
            role: "assistant",
            time: { created: now, completed: now },
            agent: "build",
            model: { providerID: testProviderID, modelID: testModelID },
          },
          parts: [
            {
              id: legacyToolPartID,
              sessionID: mainPMSessionID,
              messageID: legacyMessageID,
              type: "tool",
              tool: "workflow",
              state: {
                status: "completed",
                input: {
                  action: "milestone_status",
                  milestoneID: "requirements",
                  milestoneStatus: "approved",
                  message: "legacy queued main PM approval should drain during resume",
                },
                output: "Queued workflow command: milestone_status",
                metadata: {
                  workflowID: started.id,
                  queued: true,
                  action: "milestone_status",
                },
              },
              time: { start: now, end: now },
            },
          ],
        }
        const requirementsSession = {
          role: "department_pm" as const,
          sessionID: departmentPM.sessionID,
          milestoneID: WorkflowMilestoneID.make("requirements"),
          attempt: 1,
        }
        const legacyState = {
          ...state,
          version: 1,
          workflow: { ...state.workflow, status: "dispatching" },
          milestones: state.milestones.map((milestone) => ({
            ...milestone,
            status: milestone.id === "requirements" ? "planning" : "pending",
            session: milestone.id === "requirements" ? [requirementsSession] : [],
          })),
          sessions: state.sessions.map((item) =>
            item.id === mainPMSessionID
              ? { ...item, path: undefined, contentHash: undefined, messages: [legacyToolMessage] }
              : item,
          ),
        }
        delete legacyState.schema
        delete legacyState.manifest
        delete legacyState.journal
        rmSync(path.join(instance.directory, started.path, "journal"), { recursive: true, force: true })
        writeFileSync(statePath, `${JSON.stringify(legacyState, null, 2)}\n`)
        Database.use((db) => {
          db.update(WorkflowTable)
            .set({
              status: "dispatching",
              error: null,
              time_updated: Date.now(),
            })
            .where(eq(WorkflowTable.id, started.id))
            .run()
          db.update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              session: [requirementsSession],
              attempt: 1,
              time_updated: Date.now(),
            })
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, started.id),
                eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("requirements")),
              ),
            )
            .run()
          for (const milestoneID of ["implementation", "verification"]) {
            db.update(WorkflowMilestoneTable)
              .set({
                status: "pending",
                session: [],
                attempt: 0,
                time_updated: Date.now(),
              })
              .where(
                and(
                  eq(WorkflowMilestoneTable.workflow_id, started.id),
                  eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make(milestoneID)),
                ),
              )
              .run()
          }
        })

        const stale = yield* workflow.graph(started.id)
        expect(stale.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("planning")
        expect(stale.milestones.find((milestone) => milestone.id === "implementation")?.session.length).toBe(0)

        yield* workflow.resume(started.id)
        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((graph) =>
              graph.milestones.find((milestone) => milestone.id === "requirements")?.status === "done" &&
              graph.milestones
                .find((milestone) => milestone.id === "implementation")
                ?.session.some((ref) => ref.role === "department_pm" || ref.role === "executor")
                ? graph
                : undefined,
            ),
          ),
          "workflow resume did not recover legacy queued command and dispatch implementation",
          "20 seconds",
        )
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))

        expect(graph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
        expect(journal.some((row) => row.id === `legacy-queued:${legacyToolPartID}` && row.outcome === "applied")).toBe(true)
      }),
    30_000,
  )

  it.instance(
    "keeps workflow state light and fresh after status transitions",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Light workflow state snapshot", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const rootSessionID = started.rootSessionID
        if (!rootSessionID) throw new Error("workflow did not create a requester session")

        const now = Date.now()
        const message: MessageV2.Assistant = {
          id: MessageID.ascending(),
          sessionID: rootSessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "build",
          agent: "build",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          finish: "stop",
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: rootSessionID,
          messageID: message.id,
          type: "text",
          text: `large snapshot payload\n${"x".repeat(100_000)}`,
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)

        const result = yield* workflow.dispatchCommand({
          action: "force_complete",
          workflowID: started.id,
          sourceSessionID: rootSessionID,
          milestoneID: WorkflowMilestoneID.make("requirements"),
          message: "close requirements to refresh workflow-state.json",
        })
        expect(result.applied).toBe(true)

        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const fresh = yield* pollWithTimeout(
          Effect.promise(() => Bun.file(statePath).json()).pipe(
            Effect.map((state) => {
              const snapshot = state as WorkflowStateSnapshotJson
              const requirements = snapshot.milestones.find((item) => item.id === "requirements")
              const rootSessionState = snapshot.sessions.find((item) => item.id === rootSessionID)
              return requirements?.status === "done" && rootSessionState?.path ? snapshot : undefined
            }),
          ),
          "workflow-state.json did not refresh after status transition",
          "5 seconds",
        )
        const rootSessionState = fresh.sessions.find((item) => item.id === rootSessionID)
        if (!rootSessionState?.path) throw new Error("Expected requester session sidecar in fresh workflow state")
        expect(rootSessionState.messages).toBeUndefined()
        expect(readFileSync(statePath, "utf8").length).toBeLessThan(50 * 1024)
        expect(readFileSync(path.join(instance.directory, started.path, rootSessionState.path), "utf8")).toContain(
          "large snapshot payload",
        )
      }),
    35_000,
  )

  it.instance(
    "restores deleted workflow database rows and continues dispatch",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Deleted workflow database restore", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
          modelWhitelist: {
            departmentPM: [
              {
                providerID: testProviderID,
                modelID: testModelID,
                weight: 75,
                cacheMinutes: 45,
              },
            ],
          },
        })
        const rootSessionID = started.rootSessionID
        if (!rootSessionID) throw new Error("workflow did not create a requester session")
        const originalGraph = yield* workflow.graph(started.id)
        const originalDepartmentPM = originalGraph.members.find((member) => member.role === "department_pm")
        if (!originalDepartmentPM) throw new Error("Expected department PM member before DB deletion")
        const sessionIDs = [rootSessionID, ...originalGraph.members.map((member) => member.sessionID)]

        Database.use((db) => {
          db.delete(WorkflowEdgeTable).where(eq(WorkflowEdgeTable.workflow_id, started.id)).run()
          db.delete(WorkflowConsultationTable).where(eq(WorkflowConsultationTable.workflow_id, started.id)).run()
          db.delete(WorkflowInterventionTable).where(eq(WorkflowInterventionTable.workflow_id, started.id)).run()
          db.delete(WorkflowMilestoneTable).where(eq(WorkflowMilestoneTable.workflow_id, started.id)).run()
          db.delete(WorkflowMemberTable).where(eq(WorkflowMemberTable.workflow_id, started.id)).run()
          db.delete(WorkflowTable).where(eq(WorkflowTable.id, started.id)).run()
          sessionIDs.forEach((sessionID) => db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run())
        })

        const restored = yield* workflow.get(started.id)
        const restoredRoot = yield* sessions.get(rootSessionID)
        const restoredGraph = yield* workflow.graph(started.id)
        const restoredDepartmentPM = restoredGraph.members.find((member) => member.role === "department_pm")
        expect(restored.id).toBe(started.id)
        expect(restoredRoot.id).toBe(rootSessionID)
        expect(restoredDepartmentPM?.model?.providerID).toBe(testProviderID)
        expect(restoredDepartmentPM?.model?.modelID).toBe(testModelID)
        expect(restoredDepartmentPM?.modelWeight).toBe(75)
        expect(restoredDepartmentPM?.modelCacheUntil).toBe(originalDepartmentPM.modelCacheUntil)

        const result = yield* workflow.dispatchCommand({
          action: "force_complete",
          workflowID: started.id,
          sourceSessionID: rootSessionID,
          milestoneID: WorkflowMilestoneID.make("requirements"),
          message: "close restored requirements gate and continue dispatch",
        })
        expect(result.applied).toBe(true)

        const dispatched = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              return requirements?.status === "done" &&
                implementation?.session.some((ref) => ref.role === "department_pm")
                ? info
                : undefined
            }),
          ),
          "restored workflow did not continue dispatching after DB deletion",
          "10 seconds",
        )
        expect(dispatched.workflow.status).not.toBe("blocked")
      }),
    30_000,
  )

  it.instance(
    "replays command journal status newer than the workflow-state snapshot",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Command journal WAL restore", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        expect(JSON.parse(readFileSync(statePath, "utf8")).journal?.commands?.highWater).toBe(0)
        mkdirSync(path.join(instance.directory, started.path, "journal"), { recursive: true })
        writeFileSync(
          path.join(instance.directory, started.path, "journal", "commands.jsonl"),
          `${JSON.stringify({
            seq: 1,
            id: Bus.createID(),
            ts: new Date().toISOString(),
            source: { sessionID: started.rootSessionID, role: "requester" },
            action: "force_complete",
            milestoneID: "requirements",
            from: { workflowStatus: "dispatching", milestoneStatus: "pending" },
            to: { workflowStatus: "executing", milestoneStatus: "done" },
            outcome: "applied",
            message: "Recovered command journal status.",
          })}\n`,
        )
        Database.use((db) => {
          db.delete(WorkflowTable).where(eq(WorkflowTable.id, started.id)).run()
        })

        const restored = yield* workflow.get(started.id)
        const graph = yield* workflow.graph(started.id)
        const state = JSON.parse(readFileSync(statePath, "utf8"))

        expect(restored.status).toBe("executing")
        expect(graph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
        expect(state.journal?.commands?.highWater).toBe(1)
        expect(state.milestones.find((milestone: { id?: string }) => milestone.id === "requirements")?.status).toBe("done")
      }),
  )

  it.instance(
    "replays message journal status newer than the workflow-state snapshot",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Message journal WAL restore",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        if (!started.rootSessionID) throw new Error("Expected requester session")

        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        const now = Date.now()
        state.consultations = [
          {
            id: "consult_restore",
            workflowID: started.id,
            fromSessionID: started.rootSessionID,
            toSessionID: started.rootSessionID,
            fromRole: "requester",
            toRole: "requester",
            reason: "restore consultation",
            timing: "temporary-interrupt",
            question: "Can the workflow continue?",
            answer: "_Pending answer._",
            status: "pending",
            time: { created: now, updated: now },
          },
        ]
        state.interventions = [
          {
            id: "intervention_restore",
            workflowID: started.id,
            fromSessionID: started.rootSessionID,
            targetSessionID: started.rootSessionID,
            targetRole: "requester",
            timing: "temporary-interrupt",
            message: "Please acknowledge.",
            path: path.join(started.path, "interventions", "intervention_restore.md"),
            status: "queued",
            time: { created: now, updated: now },
          },
          {
            id: "intervention_delivered_restore",
            workflowID: started.id,
            fromSessionID: started.rootSessionID,
            targetSessionID: started.rootSessionID,
            targetRole: "requester",
            timing: "temporary-interrupt",
            message: "Please handle this delivered intervention.",
            path: path.join(started.path, "interventions", "intervention_delivered_restore.md"),
            status: "queued",
            time: { created: now, updated: now },
          },
        ]
        writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`)
        mkdirSync(path.join(instance.directory, started.path, "journal"), { recursive: true })
        writeFileSync(
          path.join(instance.directory, started.path, "journal", "messages.jsonl"),
          [
            JSON.stringify({
              ts: new Date(now + 1).toISOString(),
              workflowID: started.id,
              action: "answer",
              kind: "consultation",
              messageID: "consult_restore",
              sessionID: started.rootSessionID,
              status: "answered",
              response: "The workflow can continue.",
            }),
            JSON.stringify({
              ts: new Date(now + 2).toISOString(),
              workflowID: started.id,
              action: "ack",
              kind: "intervention",
              messageID: "intervention_restore",
              sessionID: started.rootSessionID,
              status: "acked",
              response: "Acknowledged.",
            }),
            JSON.stringify({
              ts: new Date(now + 3).toISOString(),
              workflowID: started.id,
              action: "deliver",
              kind: "intervention",
              messageID: "intervention_delivered_restore",
              sessionID: started.rootSessionID,
              status: "delivered",
              response: "Delivered by runtime.",
            }),
          ].join("\n") + "\n",
        )
        Database.use((db) => {
          db.delete(WorkflowTable).where(eq(WorkflowTable.id, started.id)).run()
        })

        yield* workflow.get(started.id)
        const graph = yield* workflow.graph(started.id)
        const restoredState = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson

        expect(graph.consultations.find((item) => item.id === "consult_restore")?.status).toBe("answered")
        expect(graph.consultations.find((item) => item.id === "consult_restore")?.answer).toContain("can continue")
        expect(graph.interventions.find((item) => item.id === "intervention_restore")?.status).toBe("acked")
        expect(graph.interventions.find((item) => item.id === "intervention_restore")?.response).toBe("Acknowledged.")
        expect(graph.interventions.find((item) => item.id === "intervention_delivered_restore")?.status).toBe("delivered")
        expect(graph.interventions.find((item) => item.id === "intervention_delivered_restore")?.response).toBe("Delivered by runtime.")
        expect(restoredState.journal?.messages?.highWater).toBe(3)
      }),
  )

  it.instance(
    "replays event journal high-water newer than the workflow-state snapshot",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Event journal WAL restore",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        const highWater = state.journal?.events?.highWater ?? 0
        const journalPath = path.join(instance.directory, started.path, "journal", "events.jsonl")
        const graphPath = path.join(instance.directory, started.path, "graph")
        const revisionPath = path.join(graphPath, "rev-999.xml")
        mkdirSync(path.dirname(journalPath), { recursive: true })
        mkdirSync(graphPath, { recursive: true })
        writeFileSync(revisionPath, "<workflow><ordered></ordered></workflow>\n")
        writeFileSync(
          journalPath,
          `${existsSync(journalPath) ? readFileSync(journalPath, "utf8") : ""}${JSON.stringify({
            seq: highWater + 1,
            ts: new Date().toISOString(),
            action: "graph.revised",
            path: path.join(started.path, "graph", "rev-999.xml"),
            milestones: 0,
            edges: 0,
          })}\n`,
        )
        Database.use((db) => {
          db.delete(WorkflowTable).where(eq(WorkflowTable.id, started.id)).run()
        })

        yield* workflow.get(started.id)
        const restoredState = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson

        expect(restoredState.journal?.events?.highWater).toBe(highWater + 1)
        expect(existsSync(revisionPath)).toBe(true)
      }),
  )

  it.instance(
    "doctor reports broken workflow state snapshots",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Doctor broken state workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const clean = yield* workflow.doctor({ workflowID: started.id })
        expect(clean.checked).toBe(1)
        expect(clean.issues.some((issue) => issue.code === "invalid_state_json")).toBe(false)
        expect(clean.issues.some((issue) => issue.code === "unsupported_state_version")).toBe(false)

        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8")) as WorkflowStateSnapshotJson
        expect(state.schema).toBe(2)
        expect(state.version).toBe(2)

        const legacyState = { ...state, version: 1 }
        delete legacyState.schema
        writeFileSync(statePath, `${JSON.stringify(legacyState, null, 2)}\n`)

        const legacy = yield* workflow.doctor({ workflowID: started.id })
        expect(legacy.ok).toBe(true)
        expect(legacy.issues.some((issue) => issue.code === "unsupported_state_version")).toBe(false)

        const sessionStatePath = state.sessions.find((item) => item.path)?.path
        if (!sessionStatePath) throw new Error("Expected workflow state to reference a session sidecar")
        expect(state.sessions.find((item) => item.path)?.contentHash).toBeString()
        const sessionStateFile = path.join(instance.directory, started.path, sessionStatePath)
        const originalSessionState = readFileSync(sessionStateFile, "utf8")
        writeFileSync(sessionStateFile, "{not-json")

        const brokenSessionState = yield* workflow.doctor({ workflowID: started.id })
        expect(brokenSessionState.ok).toBe(false)
        expect(brokenSessionState.issues.some((issue) => issue.code === "invalid_session_state_json")).toBe(true)
        writeFileSync(sessionStateFile, originalSessionState)

        writeFileSync(
          sessionStateFile,
          `${JSON.stringify({ ...JSON.parse(originalSessionState), title: "tampered sidecar title" }, null, 2)}\n`,
        )
        const mismatchedSessionState = yield* workflow.doctor({ workflowID: started.id })
        expect(mismatchedSessionState.ok).toBe(false)
        expect(mismatchedSessionState.issues.some((issue) => issue.code === "session_state_hash_mismatch")).toBe(true)

        const fixedSessionState = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(fixedSessionState.issues.some((issue) => issue.code === "session_state_hash_mismatch")).toBe(false)

        writeFileSync(statePath, `${JSON.stringify({ ...state, schema: 99, version: 99 }, null, 2)}\n`)

        const future = yield* workflow.doctor({ workflowID: started.id })
        expect(future.ok).toBe(true)
        expect(future.issues.some((issue) => issue.code === "unsupported_state_version")).toBe(true)

        writeFileSync(statePath, "{not-json")

        const broken = yield* workflow.doctor({ workflowID: started.id })
        expect(broken.ok).toBe(false)
        expect(
          broken.issues.some(
            (issue) =>
              issue.severity === "error" &&
              issue.code === "invalid_state_json" &&
              issue.path?.endsWith("workflow-state.json"),
          ),
        ).toBe(true)
      }),
  )

  it.instance(
    "blocks resume when workflow session sidecar hash does not match the state index",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Sidecar hash mismatch resume workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })

        const state = JSON.parse(
          readFileSync(path.join(instance.directory, started.path, "workflow-state.json"), "utf8"),
        ) as WorkflowStateSnapshotJson
        const sessionStatePath = state.sessions.find((item) => item.path)?.path
        if (!sessionStatePath) throw new Error("Expected workflow state to reference a session sidecar")
        const sessionStateFile = path.join(instance.directory, started.path, sessionStatePath)
        writeFileSync(
          sessionStateFile,
          `${JSON.stringify({ ...JSON.parse(readFileSync(sessionStateFile, "utf8")), title: "tampered before resume" }, null, 2)}\n`,
        )

        const resumed = yield* workflow.resume(started.id)
        expect(resumed.status).toBe("blocked")
        expect(resumed.error).toContain("session_state_hash_mismatch")
      }),
  )

  it.instance(
    "doctor fix rebuilds missing workflow projection files",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Doctor projection rebuild workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const staffFiles = readdirSync(path.join(instance.directory, started.path, "reference", "staff"))
          .filter((file) => file.endsWith(".md"))
          .map((file) => path.join("reference", "staff", file))
        const projectionPaths = [
          "index.md",
          "organization.md",
          "progress.md",
          path.join("reference", "index.md"),
          path.join("reference", "requester.md"),
          path.join("reference", "consultations", "index.md"),
          path.join("interventions", "index.md"),
          path.join("standups", "index.md"),
          "delivery-summary.md",
          ...staffFiles.slice(0, 1),
        ]
        expect((yield* workflow.doctor({ workflowID: started.id })).issues.some((issue) => issue.code === "missing_projection")).toBe(false)

        writeFileSync(path.join(instance.directory, started.path, "progress.md"), "")
        projectionPaths
          .filter((projectionPath) => projectionPath !== "progress.md")
          .forEach((projectionPath) => rmSync(path.join(instance.directory, started.path, projectionPath), { force: true }))

        const reported = yield* workflow.doctor({ workflowID: started.id })
        expect(reported.ok).toBe(true)
        expect(reported.issues.some((issue) => issue.code === "missing_projection")).toBe(true)
        expect(reported.issues.some((issue) => issue.code === "empty_projection" && issue.path?.endsWith("progress.md"))).toBe(true)

        const fixed = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(fixed.ok).toBe(true)
        expect(fixed.issues.some((issue) => issue.code === "missing_projection")).toBe(false)
        expect(fixed.issues.some((issue) => issue.code === "empty_projection")).toBe(false)
        projectionPaths.forEach((projectionPath) => {
          const rebuilt = path.join(instance.directory, started.path, projectionPath)
          expect(existsSync(rebuilt)).toBe(true)
          expect(readFileSync(rebuilt, "utf8").trim().length).toBeGreaterThan(0)
        })
      }),
  )

  it.instance(
    "doctor reports broken workflow manifests",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Doctor manifest workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const manifestPath = path.join(instance.directory, started.path, "manifest.json")
        const manifest = JSON.parse(readFileSync(manifestPath, "utf8"))
        expect(manifest.schema).toBe(2)
        expect(manifest.workflowID).toBe(started.id)
        expect(manifest.projectID).toBe(started.projectID)
        expect(manifest.title).toBe(started.title)
        expect(manifest.ownership["workflow.xml"]).toBe("engine")
        expect(manifest.ownership["journal/**"]).toBe("engine-append")
        expect(manifest.ownership["work/**"]).toBe("agent")
        expect((yield* workflow.doctor({ workflowID: started.id })).issues.some((issue) => issue.code === "missing_manifest")).toBe(
          false,
        )

        writeFileSync(manifestPath, "{not-json")
        const invalid = yield* workflow.doctor({ workflowID: started.id })
        expect(invalid.ok).toBe(false)
        expect(invalid.issues.some((issue) => issue.code === "invalid_manifest_json")).toBe(true)

        writeFileSync(manifestPath, `${JSON.stringify({ ...manifest, workflowID: "wfl_wrong" }, null, 2)}\n`)
        const mismatch = yield* workflow.doctor({ workflowID: started.id })
        expect(mismatch.ok).toBe(false)
        expect(mismatch.issues.some((issue) => issue.code === "manifest_workflow_mismatch")).toBe(true)
      }),
  )

  it.instance(
    "doctor fix removes temporary workflow write files",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Doctor temporary file workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const tempPath = path.join(instance.directory, started.path, ".tmp-workflow-state.json-test")
        writeFileSync(tempPath, "{partial")

        const reported = yield* workflow.doctor({ workflowID: started.id })
        expect(reported.ok).toBe(true)
        expect(reported.issues.some((issue) => issue.code === "temporary_file" && issue.path?.endsWith(".tmp-workflow-state.json-test"))).toBe(true)

        const fixed = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(fixed.ok).toBe(true)
        expect(fixed.issues.some((issue) => issue.code === "temporary_file")).toBe(false)
        expect(existsSync(tempPath)).toBe(false)
      }),
  )

  it.instance(
    "recovers workflow command dispatch after injected persistence interruptions",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        const previousFaultAt = process.env.OPENCODE_WORKFLOW_WRITE_FAULT_AT
        const previousFaultRun = process.env.OPENCODE_WORKFLOW_WRITE_FAULT_RUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) delete process.env.OPENCODE_WORKFLOW_AUTORUN
            else process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
            if (previousFaultAt === undefined) delete process.env.OPENCODE_WORKFLOW_WRITE_FAULT_AT
            else process.env.OPENCODE_WORKFLOW_WRITE_FAULT_AT = previousFaultAt
            if (previousFaultRun === undefined) delete process.env.OPENCODE_WORKFLOW_WRITE_FAULT_RUN
            else process.env.OPENCODE_WORKFLOW_WRITE_FAULT_RUN = previousFaultRun
          }),
        )

        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const recoverableCodes = new Set([
          "temporary_file",
          "invalid_journal_json",
          "journal_seq_gap",
          "missing_journal_seq",
          "missing_command_journal",
          "state_status_mismatch",
          "milestone_status_mismatch",
          "session_state_hash_mismatch",
          "missing_session_state",
          "invalid_session_state_json",
        ])
        const started = yield* workflow.start({
          prompt: ["Injected persistence fault workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        if (!started.rootSessionID) throw new Error("Expected workflow requester session")
        const requesterSessionID = started.rootSessionID

        for (const point of Array.from({ length: 20 }, (_, index) => index + 1)) {
          const faultXml = `<workflow>
  <ordered>
${Array.from(
  { length: 4 },
  (_, index) =>
    `    <milestone id="fault-${String(point).padStart(2, "0")}-${String(index + 1).padStart(2, "0")}" title="Fault ${point}.${index + 1}" department="engineering">Exercise persistence recovery for fault point ${point}.${index + 1}.</milestone>`,
).join("\n")}
  </ordered>
</workflow>`
          const command = {
            id: Bus.createID(),
            action: "update_xml" as const,
            workflowID: started.id,
            sourceSessionID: requesterSessionID,
            xml: faultXml,
            message: `inject persistence fault at write point ${point}`,
          }

          process.env.OPENCODE_WORKFLOW_WRITE_FAULT_AT = String(point)
          process.env.OPENCODE_WORKFLOW_WRITE_FAULT_RUN = `${started.id}:${point}`
          const interrupted = yield* workflow.dispatchCommand(command).pipe(
            Effect.map(() => false),
            Effect.catchCause(() => Effect.succeed(true)),
          )
          if (!interrupted) throw new Error(`Fault point ${point} did not interrupt workflow command dispatch`)

          delete process.env.OPENCODE_WORKFLOW_WRITE_FAULT_AT
          delete process.env.OPENCODE_WORKFLOW_WRITE_FAULT_RUN
          yield* workflow.doctor({ workflowID: started.id, fix: true })
          const retry = yield* workflow.dispatchCommand(command)
          expect(retry.applied).toBe(true)
          for (const job of (yield* background.list()).filter((item) => item.metadata?.workflowID === started.id)) {
            yield* background.cancel(job.id).pipe(Effect.ignore)
          }

          const graph = yield* workflow.graph(started.id)
          expect(graph.milestones.some((milestone) => milestone.id === `fault-${String(point).padStart(2, "0")}-04`)).toBe(true)
          const report = yield* workflow.doctor({ workflowID: started.id, fix: true })
          const remainingRecoverable = report.issues.filter((issue) => recoverableCodes.has(issue.code))
          if (!report.ok || remainingRecoverable.length > 0) {
            throw new Error(JSON.stringify({ point, issues: report.issues, remainingRecoverable }))
          }
        }
      }),
    120_000,
  )

  it.instance(
    "recovers workflow command dispatch after external process kill during a write",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const packageRoot = path.join(import.meta.dir, "..", "..")
        const helper = path.join(import.meta.dir, "kill-canary-child.ts")
        const childEnv = {
          ...process.env,
          OPENCODE_DB: path.join(instance.directory, "kill-canary.db"),
          OPENCODE_DB_BACKUPS: "0",
          OPENCODE_WORKFLOW_AUTORUN: "0",
        }
        const parseJsonLine = (text: string) => {
          const line = text
            .trim()
            .split(/\r?\n/)
            .toReversed()
            .find((item) => item.trim().startsWith("{"))
          if (!line) throw new Error(`Expected child JSON output, got: ${text}`)
          return JSON.parse(line)
        }
        const runChild = (args: string[], env = childEnv) =>
          Effect.promise(async () => {
            const proc = Bun.spawn(["bun", "run", "--conditions=browser", helper, ...args], {
              cwd: packageRoot,
              env,
              stdout: "pipe",
              stderr: "pipe",
            })
            const stdout = new Response(proc.stdout).text()
            const stderr = new Response(proc.stderr).text()
            const exitCode = await proc.exited
            const output = await stdout
            const error = await stderr
            if (exitCode !== 0) throw new Error(`child ${args.join(" ")} exited ${exitCode}: ${error}\n${output}`)
            return parseJsonLine(output)
          })
        const recoverableCodes = new Set([
          "temporary_file",
          "invalid_journal_json",
          "journal_seq_gap",
          "missing_journal_seq",
          "state_status_mismatch",
          "milestone_status_mismatch",
          "session_state_hash_mismatch",
          "missing_projection",
          "empty_projection",
        ])

        for (const faultPoint of Array.from({ length: 20 }, (_, index) => String(index + 1))) {
          const started = yield* runChild(["start", instance.directory])
          const marker = path.join(instance.directory, `kill-canary-marker-${faultPoint}.json`)
          const crash = Bun.spawn(["bun", "run", "--conditions=browser", helper, "crash", instance.directory, started.workflowID], {
            cwd: packageRoot,
            env: {
              ...childEnv,
              OPENCODE_WORKFLOW_WRITE_FAULT_AT: faultPoint,
              OPENCODE_WORKFLOW_WRITE_FAULT_RUN: `${started.workflowID}:external-kill:${faultPoint}`,
              OPENCODE_WORKFLOW_WRITE_FAULT_SIGNAL_FILE: marker,
              OPENCODE_WORKFLOW_WRITE_FAULT_WAIT_MS: "60000",
            },
            stdout: "pipe",
            stderr: "pipe",
          })
          const crashStdout = new Response(crash.stdout).text()
          const crashStderr = new Response(crash.stderr).text()
          yield* Effect.addFinalizer(() =>
            Effect.promise(async () => {
              crash.kill()
              await crash.exited.catch(() => undefined)
            }).pipe(Effect.ignore),
          )

          const markerInfo = yield* pollWithTimeout(
            Effect.sync(() => (existsSync(marker) ? JSON.parse(readFileSync(marker, "utf8")) : undefined)),
            `external kill canary did not reach write fault point ${faultPoint}`,
            "10 seconds",
          )
          expect(markerInfo.point).toBe(Number(faultPoint))
          expect(markerInfo.pid).toBeNumber()
          crash.kill("SIGKILL")
          const exitCode = yield* Effect.promise(() => crash.exited)
          yield* Effect.promise(() => Promise.all([crashStdout, crashStderr]))
          expect(exitCode).not.toBe(0)

          const retry = yield* runChild(["retry", instance.directory, started.workflowID])
          expect(retry.doctor.ok).toBe(true)
          expect(retry.doctor.issues.filter((issue: { code: string }) => recoverableCodes.has(issue.code))).toEqual([])
          expect(retry.dispatched.applied).toBe(true)
          expect(retry.milestoneIDs).toContain("kill-canary-c")
          expect(retry.report.ok).toBe(true)
          expect(retry.report.issues.filter((issue: { code: string }) => recoverableCodes.has(issue.code))).toEqual([])
        }
      }),
    600_000,
  )

  it.instance(
    "doctor fix truncates trailing invalid workflow journal rows",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Doctor journal tail workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const journalPath = path.join(instance.directory, started.path, "journal", "events.jsonl")
        const originalJournal = readFileSync(journalPath, "utf8")
        writeFileSync(journalPath, `${originalJournal}{partial`)

        const reported = yield* workflow.doctor({ workflowID: started.id })
        expect(reported.ok).toBe(false)
        expect(reported.issues.some((issue) => issue.code === "invalid_journal_json")).toBe(true)

        const fixed = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(fixed.ok).toBe(true)
        expect(fixed.issues.some((issue) => issue.code === "invalid_journal_json")).toBe(false)
        expect(readFileSync(journalPath, "utf8")).toBe(originalJournal)

        writeFileSync(
          journalPath,
          `${originalJournal}{partial\n${JSON.stringify({
            seq: 2,
            action: "graph.revised",
            path: "graph/rev-002.xml",
            time: Date.now(),
          })}\n`,
        )
        const stillInvalid = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(stillInvalid.ok).toBe(false)
        expect(stillInvalid.issues.some((issue) => issue.code === "invalid_journal_json")).toBe(true)
      }),
  )

  it.instance(
    "records workflow graph revisions for XML changes",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Graph revision workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const revisedXml = `<workflow>
  <ordered>
    <milestone id="requirements" title="Only requirements" department="product">Clarify the revised request.</milestone>
  </ordered>
</workflow>`

        yield* workflow.updateXml({ workflowID: started.id, xml: revisedXml })
        yield* workflow.updateXml({ workflowID: started.id, xml: revisedXml })

        const graphPath = path.join(instance.directory, started.path, "graph")
        const revisions = readdirSync(graphPath).filter((entry) => /^rev-\d+\.xml$/.test(entry)).sort()
        expect(revisions).toEqual(["rev-001.xml", "rev-002.xml"])
        expect(readFileSync(path.join(graphPath, "rev-001.xml"), "utf8")).toContain("Implement solution")
        expect(readFileSync(path.join(graphPath, "rev-002.xml"), "utf8")).toBe(revisedXml)
        const events = readFileSync(path.join(instance.directory, started.path, "journal", "events.jsonl"), "utf8")
          .trim()
          .split(/\r?\n/)
          .map((line) => JSON.parse(line))
        expect(events.map((event) => event.seq)).toEqual([1, 2])
        expect(events.map((event) => event.action)).toEqual(["graph.revised", "graph.revised"])
        expect(events.map((event) => path.basename(event.path))).toEqual(["rev-001.xml", "rev-002.xml"])
        expect((yield* workflow.doctor({ workflowID: started.id })).issues.some((issue) => issue.code === "invalid_journal_json")).toBe(
          false,
        )
      }),
  )

  it.instance(
    "preserves workflow work artifacts across repeated XML graph rebuilds",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Work artifact preservation workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const workPath = path.join(instance.directory, started.path, "work")
        mkdirSync(path.join(workPath, "staff", "main-pm"), { recursive: true })
        mkdirSync(path.join(workPath, "milestones", "removed-later", "artifacts", "nested"), { recursive: true })
        writeFileSync(path.join(workPath, "staff", "main-pm", "memory.md"), "# Main PM memory\n\nDurable note.\n")
        writeFileSync(
          path.join(workPath, "milestones", "removed-later", "artifacts", "nested", "evidence.bin"),
          Buffer.from([0, 1, 2, 3, 255]),
        )
        const before = snapshotDirectory(workPath)

        for (const index of Array.from({ length: 10 }, (_, item) => item + 1)) {
          yield* workflow.updateXml({
            workflowID: started.id,
            xml: `<workflow>
  <ordered>
    <milestone id="requirements" title="Requirements ${index}" department="product">Clarify the request revision ${index}.</milestone>
    <parallel>
      <milestone id="implementation-${index}" title="Implementation ${index}" department="engineering">Implement revision ${index}.</milestone>
      <milestone id="verification" title="Verification ${index}" department="quality">Verify revision ${index}.</milestone>
    </parallel>
  </ordered>
</workflow>`,
          })
        }

        expect(snapshotDirectory(workPath)).toEqual(before)
      }),
  )

  it.instance(
    "rejects engine writes into workflow agent-owned work artifacts",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Engine write ownership workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const outcome = yield* workflow
          .updateXml({
            workflowID: started.id,
            xml: `<workflow>
  <ordered>
    <milestone id="work" title="Reserved work area" department="product">This id would place the precreated plan in work/plan.md.</milestone>
  </ordered>
</workflow>`,
          })
          .pipe(
            Effect.as("applied"),
            Effect.catchCause((cause) => Effect.succeed(String(cause))),
          )

        expect(outcome).toContain("Workflow engine cannot write work/plan.md")
        expect(existsSync(path.join(instance.directory, started.path, "work", "plan.md"))).toBe(false)
      }),
  )

  it.instance(
    "doctor reports duplicate local workflow directories",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Doctor duplicate workflow directory",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })

        cpSync(
          path.join(instance.directory, started.path),
          path.join(instance.directory, ".opencode", "workflows", `${started.id}-fork`),
          { recursive: true },
        )
        rmSync(path.join(instance.directory, ".opencode", "workflows", `${started.id}-fork`, "workflow-state.json"))

        const report = yield* workflow.doctor({ workflowID: started.id })
        expect(report.ok).toBe(false)
        expect(
          report.issues.some(
            (issue) =>
              issue.severity === "error" &&
              issue.code === "duplicate_directory" &&
            issue.message.includes(`${started.id}-fork`),
          ),
        ).toBe(true)

        const blocked = yield* workflow.resume(started.id)
        expect(blocked.status).toBe("blocked")
        expect(blocked.error).toContain("duplicate_directory")
        expect(blocked.error).toContain("workflow doctor")
        const background = yield* BackgroundJob.Service
        expect(
          (yield* background.list()).some(
            (job) => job.status === "running" && job.type === "workflow" && job.metadata?.workflowID === started.id,
          ),
        ).toBe(false)

        const fixed = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(fixed.ok).toBe(true)
        expect(fixed.issues.some((issue) => issue.code === "duplicate_directory")).toBe(false)
        expect(existsSync(path.join(instance.directory, ".opencode", "workflows", `${started.id}-fork`))).toBe(false)
        expect(
          readdirSync(path.join(instance.directory, ".opencode", "workflows")).some((entry) =>
            entry.startsWith(`${started.id}-fork.orphaned-`),
          ),
        ).toBe(true)
      }),
  )

  it.instance(
    "doctor fix keeps the highest journal high-water workflow directory",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Doctor high-water duplicate workflow directory",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const forkPath = path.join(".opencode", "workflows", `${started.id}-fork`)
        const canonicalJournal = path.join(instance.directory, started.path, "journal", "commands.jsonl")
        const forkJournal = path.join(instance.directory, forkPath, "journal", "commands.jsonl")
        cpSync(path.join(instance.directory, started.path), path.join(instance.directory, forkPath), { recursive: true })
        mkdirSync(path.dirname(canonicalJournal), { recursive: true })
        mkdirSync(path.dirname(forkJournal), { recursive: true })
        writeFileSync(canonicalJournal, `${JSON.stringify({ seq: 1, action: "status", outcome: "applied" })}\n`)
        writeFileSync(
          forkJournal,
          [
            JSON.stringify({ seq: 1, action: "status", outcome: "applied" }),
            JSON.stringify({ seq: 2, action: "force_complete", outcome: "applied", message: "winner-high-water" }),
          ].join("\n") + "\n",
        )

        const report = yield* workflow.doctor({ workflowID: started.id })
        expect(report.ok).toBe(false)
        expect(
          report.issues.some(
            (issue) =>
              issue.code === "duplicate_directory" &&
              issue.message.includes(`${started.id}-fork`) &&
              issue.message.includes("highWater=2"),
          ),
        ).toBe(true)

        const fixed = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(fixed.ok).toBe(true)
        expect(fixed.issues.some((issue) => issue.code === "duplicate_directory")).toBe(false)
        expect(existsSync(path.join(instance.directory, forkPath))).toBe(false)
        expect(readFileSync(canonicalJournal, "utf8")).toContain("winner-high-water")
        expect(
          readdirSync(path.join(instance.directory, ".opencode", "workflows")).some((entry) =>
            entry.startsWith(`${started.id}.orphaned-`) && entry.includes("replaced"),
          ),
        ).toBe(true)
      }),
  )

  it.instance(
    "doctor migrates legacy workflow directories",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Doctor migrate legacy workflow directory",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const legacyPath = path.join(".opencode", "workflows", "20260704_legacy_prompt_path")
        const legacyPlanPath = path.join(legacyPath, "requirements", "plan.md")
        cpSync(path.join(instance.directory, started.path), path.join(instance.directory, legacyPath), { recursive: true })
        const legacyStateFile = path.join(instance.directory, legacyPath, "workflow-state.json")
        const legacyState = JSON.parse(readFileSync(legacyStateFile, "utf8"))
        writeFileSync(
          legacyStateFile,
          JSON.stringify({ ...legacyState, workflow: { ...legacyState.workflow, path: legacyPath } }, null, 2),
        )
        rmSync(path.join(instance.directory, started.path), { recursive: true, force: true })
        Database.use((db) => {
          db.update(WorkflowTable)
            .set({ path: legacyPath, time_updated: Date.now() })
            .where(eq(WorkflowTable.id, started.id))
            .run()
          db.update(WorkflowMilestoneTable)
            .set({ plan_path: legacyPlanPath, time_updated: Date.now() })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("requirements"))))
            .run()
        })

        const report = yield* workflow.doctor({ workflowID: started.id })
        expect(report.ok).toBe(false)
        expect(report.issues.some((issue) => issue.code === "noncanonical_path" && issue.path === legacyPath)).toBe(true)

        const migrated = yield* workflow.doctor({ workflowID: started.id, migrate: true })
        expect(migrated.ok).toBe(true)
        expect(migrated.issues.some((issue) => issue.code === "noncanonical_path")).toBe(false)
        expect(migrated.issues.some((issue) => issue.code === "duplicate_directory")).toBe(false)
        expect(existsSync(path.join(instance.directory, started.path))).toBe(true)
        expect(existsSync(path.join(instance.directory, legacyPath))).toBe(false)
        expect(
          readdirSync(path.join(instance.directory, ".opencode", "workflows")).some((entry) =>
            entry.startsWith("20260704_legacy_prompt_path.orphaned-"),
          ),
        ).toBe(true)
        const graph = yield* workflow.graph(started.id)
        expect(graph.workflow.path).toBe(started.path)
        expect(graph.milestones.find((milestone) => milestone.id === "requirements")?.planPath).toBe(
          path.join(started.path, "requirements", "plan.md"),
        )
        expect(JSON.parse(readFileSync(path.join(instance.directory, started.path, "workflow-state.json"), "utf8")).workflow.path).toBe(
          started.path,
        )
      }),
  )

  it.instance(
    "doctor reports unjournaled active workflow state and stale interventions",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Doctor unjournaled active workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const departmentPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "department_pm")
        if (!departmentPM || !started.rootSessionID) throw new Error("Expected requester and department PM sessions")
        const staleTime = Date.now() - 31 * 60 * 1000
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: 1,
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: WorkflowMilestoneID.make("requirements"),
                  attempt: 1,
                },
              ],
              time_updated: staleTime,
            })
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, started.id),
                eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("requirements")),
              ),
            )
            .run(),
        )
        Database.use((db) =>
          db
            .insert(WorkflowInterventionTable)
            .values({
              workflow_id: started.id,
              id: "intervention_stale_doctor",
              from_session_id: started.rootSessionID,
              target_session_id: departmentPM.sessionID,
              target_role: "department_pm",
              timing: "temporary-interrupt",
              message: "Please close requirements so downstream audits can dispatch.",
              path: path.join(started.path, "interventions", "intervention_stale_doctor.md"),
              status: "queued",
              time_created: staleTime,
              time_updated: staleTime,
            })
            .run(),
        )
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8"))
        state.milestones = state.milestones.map((milestone: { id?: string }) =>
          milestone.id === "requirements"
            ? {
                ...milestone,
                status: "planning",
                attempt: 1,
                session: [
                  {
                    role: "department_pm",
                    sessionID: departmentPM.sessionID,
                    milestoneID: "requirements",
                    attempt: 1,
                  },
                ],
              }
            : milestone,
        )
        state.interventions = [
          ...(Array.isArray(state.interventions) ? state.interventions : []),
          {
            id: "intervention_stale_doctor",
            workflowID: started.id,
            fromSessionID: started.rootSessionID,
            targetSessionID: departmentPM.sessionID,
            targetRole: "department_pm",
            timing: "temporary-interrupt",
            message: "Please close requirements so downstream audits can dispatch.",
            path: path.join(started.path, "interventions", "intervention_stale_doctor.md"),
            status: "queued",
            time: { created: staleTime, updated: staleTime },
          },
        ]
        writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`)

        const report = yield* workflow.doctor({ workflowID: started.id })
        expect(report.issues.some((issue) => issue.code === "missing_command_journal")).toBe(true)
        expect(report.issues.some((issue) => issue.code === "stale_intervention")).toBe(true)
        expect(report.issues.some((issue) => issue.code === "orphan_active_milestone")).toBe(true)

        mkdirSync(path.join(instance.directory, started.path, "journal"), { recursive: true })
        writeFileSync(
          path.join(instance.directory, started.path, "journal", "commands.jsonl"),
          '{"seq":1,"action":"status"}\n{"seq":3,"action":"resume"}\n',
        )
        writeFileSync(
          path.join(instance.directory, started.path, "journal", "messages.jsonl"),
          '{"seq":1,"action":"send"}\n{"action":"send"}\n',
        )
        const seqReport = yield* workflow.doctor({ workflowID: started.id })
        expect(seqReport.issues.some((issue) => issue.code === "journal_seq_gap")).toBe(true)
        expect(seqReport.issues.some((issue) => issue.code === "missing_journal_seq")).toBe(true)

        const fixed = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(fixed.issues.some((issue) => issue.code === "journal_seq_gap")).toBe(false)
        expect(fixed.issues.some((issue) => issue.code === "missing_journal_seq")).toBe(false)
        expect(fixed.issues.some((issue) => issue.code === "orphan_active_milestone")).toBe(false)
        const fixedGraph = yield* workflow.graph(started.id)
        expect(fixedGraph.workflow.status).toBe("blocked")
        expect(fixedGraph.workflow.error).toContain("workflow doctor --fix blocked orphan active milestone")
        expect(fixedGraph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("blocked")
        const fixedCommandJournal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const fixedMessageJournal = yield* Effect.promise(() => readWorkflowMessageJournal(instance.directory, started.path))
        expect(fixedCommandJournal).toEqual([
          expect.objectContaining({ seq: 1, action: "status" }),
          expect.objectContaining({ seq: 2, action: "resume" }),
        ])
        expect(fixedMessageJournal.slice(0, 2)).toEqual([
          expect.objectContaining({ seq: 1, action: "send" }),
          expect.objectContaining({ seq: 2, action: "send" }),
        ])
        expect(fixedMessageJournal.map((row) => row.seq)).toEqual(
          fixedMessageJournal.map((_, index) => index + 1),
        )
      }),
  )

  it.instance(
    "doctor fix clears combined recoverable workflow issues",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Doctor combined fix workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 0,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const departmentPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected department PM session")
        const now = Date.now()
        const orphanAttempt = 99
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: orphanAttempt,
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: WorkflowMilestoneID.make("requirements"),
                  attempt: orphanAttempt,
                },
              ],
              time_updated: now,
            })
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, started.id),
                eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("requirements")),
              ),
            )
            .run(),
        )

        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8"))
        state.milestones = state.milestones.map((milestone: { id?: string }) =>
          milestone.id === "requirements"
            ? {
                ...milestone,
                status: "planning",
                attempt: orphanAttempt,
                session: [
                  {
                    role: "department_pm",
                    sessionID: departmentPM.sessionID,
                    milestoneID: "requirements",
                    attempt: orphanAttempt,
                  },
                ],
              }
            : milestone,
        )
        writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`)

        const tempPath = path.join(instance.directory, started.path, ".tmp-workflow-state.json-combined")
        writeFileSync(tempPath, "{partial")
        mkdirSync(path.join(instance.directory, started.path, "journal"), { recursive: true })
        writeFileSync(
          path.join(instance.directory, started.path, "journal", "commands.jsonl"),
          '{"seq":1,"action":"status"}\n{"seq":3,"action":"resume"}\n{partial',
        )
        writeFileSync(
          path.join(instance.directory, started.path, "journal", "messages.jsonl"),
          '{"seq":1,"action":"send"}\n{"action":"ack"}\n',
        )
        cpSync(
          path.join(instance.directory, started.path),
          path.join(instance.directory, ".opencode", "workflows", `${started.id}-fork`),
          { recursive: true },
        )

        const report = yield* workflow.doctor({ workflowID: started.id })
        expect(report.issues.some((issue) => issue.code === "duplicate_directory")).toBe(true)
        expect(report.issues.some((issue) => issue.code === "temporary_file")).toBe(true)
        expect(report.issues.some((issue) => issue.code === "invalid_journal_json")).toBe(true)
        expect(report.issues.some((issue) => issue.code === "journal_seq_gap")).toBe(true)
        expect(report.issues.some((issue) => issue.code === "missing_journal_seq")).toBe(true)
        expect(report.issues.some((issue) => issue.code === "orphan_active_milestone")).toBe(true)

        const fixed = yield* workflow.doctor({ workflowID: started.id, fix: true })
        expect(fixed.ok).toBe(true)
        const remainingRecoverable = fixed.issues.filter((issue) =>
          [
            "duplicate_directory",
            "temporary_file",
            "invalid_journal_json",
            "journal_seq_gap",
            "missing_journal_seq",
            "orphan_active_milestone",
            "state_status_mismatch",
            "milestone_status_mismatch",
          ].includes(issue.code),
        )
        if (remainingRecoverable.length > 0) {
          const info = yield* workflow.graph(started.id)
          throw new Error(
            JSON.stringify({
              remainingRecoverable,
              workflow: info.workflow.status,
              milestone: info.milestones.find((milestone) => milestone.id === "requirements")?.status,
            }),
          )
        }
        expect(existsSync(tempPath)).toBe(false)
        expect(existsSync(path.join(instance.directory, ".opencode", "workflows", `${started.id}-fork`))).toBe(false)
        const fixedGraph = yield* workflow.graph(started.id)
        expect(fixedGraph.workflow.status).toBe("blocked")
        expect(fixedGraph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("blocked")
        const fixedCommandJournal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const fixedMessageJournal = yield* Effect.promise(() => readWorkflowMessageJournal(instance.directory, started.path))
        expect(fixedCommandJournal.map((row) => row.seq)).toEqual([1, 2])
        expect(fixedMessageJournal.map((row) => row.seq)).toEqual([1, 2])
      }),
    35_000,
  )

  it.instance(
    "records workflow member status updates from employee sessions",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Member status workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const member = (yield* workflow.graph(started.id)).members.find((item) => item.role === "department_pm")
        if (!member) throw new Error("Expected a department PM employee")

        const commandID = Bus.createID()
        const resultPromise = waitForWorkflowCommandResult(commandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: commandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: commandID,
              action: "status_update",
              workflowID: started.id,
              sourceSessionID: member.sessionID,
              availability: "blocked_waiting",
              currentFocus: "Waiting for a source document before closing requirements",
              blockers: ["requirements charter is missing"],
              progressNote: "asked main PM for the missing charter",
            },
          },
        })

        const result = yield* Effect.promise(() => resultPromise)
        expect(result?.applied).toBe(true)
        const updated = (yield* workflow.graph(started.id)).members.find((item) => item.id === member.id)
        expect(updated?.availability).toBe("blocked_waiting")
        expect(updated?.currentFocus).toContain("Waiting for a source document")
        expect(updated?.blockers).toContain("requirements charter is missing")
        expect(updated?.progressNote).toContain("asked main PM")
        const organization = yield* pollWithTimeout(
          Effect.promise(() => Bun.file(path.join(instance.directory, started.path, "organization.md")).text()).pipe(
            Effect.map((text) =>
              text.includes("availability: blocked_waiting") &&
              text.includes("Waiting for a source document") &&
              text.includes("requirements charter is missing") &&
              text.includes("asked main PM")
                ? text
                : undefined,
            ),
            Effect.catchCause(() => Effect.succeed(undefined)),
          ),
          "workflow organization did not include member status update",
          "5 seconds",
        )
        const progress = yield* pollWithTimeout(
          Effect.promise(() => Bun.file(path.join(instance.directory, started.path, "progress.md")).text()).pipe(
            Effect.map((text) => (text.includes("blocked_waiting") ? text : undefined)),
            Effect.catchCause(() => Effect.succeed(undefined)),
          ),
          "workflow progress did not include member status update",
        )
        expect(organization).toContain("availability: blocked_waiting")
        expect(organization).toContain("requirements charter is missing")
        expect(progress).toContain("blocked_waiting")
        expect(progress).toContain("asked main PM")
      }),
    30_000,
  )

  it.instance(
    "lets workflow employees close assigned collaboration messages with the tool",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Workflow message tool workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const graph = yield* workflow.graph(started.id)
        const mainPM = graph.members.find((item) => item.role === "main_pm")
        const departmentPM = graph.members.find((item) => item.role === "department_pm")
        if (!mainPM || !departmentPM) throw new Error("Expected main PM and department PM employees")

        const now = Date.now()
        Database.use((db) => {
          db.insert(WorkflowConsultationTable)
            .values({
              workflow_id: started.id,
              id: "consult_tool_test",
              from_session_id: mainPM.sessionID,
              to_session_id: departmentPM.sessionID,
              from_role: "main_pm",
              to_role: "department_pm",
              question: "Confirm the requirements gate can close.",
              answer: "_Pending answer._",
              status: "pending",
              time_created: now,
              time_updated: now,
            })
            .run()
          db.insert(WorkflowInterventionTable)
            .values({
              workflow_id: started.id,
              id: "intervention_tool_test",
              from_session_id: started.rootSessionID,
              target_session_id: departmentPM.sessionID,
              target_role: "department_pm",
              timing: "temporary-interrupt",
              message: "Requester confirms the current product scope.",
              path: path.join(started.path, "interventions", "intervention_tool_test.md"),
              status: "queued",
              time_created: now,
              time_updated: now,
            })
            .run()
        })

        const info = yield* WorkflowMessageTool
        const tool = yield* info.init()
        const ctx = workflowMessageContext(departmentPM.sessionID)
        const inbox = yield* tool.execute({ action: "inbox", workflowID: started.id }, ctx)
        expect(inbox.output).toContain("consult_tool_test")
        expect(inbox.output).toContain("intervention_tool_test")

        const answered = yield* tool.execute(
          {
            action: "answer",
            workflowID: started.id,
            messageID: "consult_tool_test",
            answer: "Requirements gate can close; the charter is sufficient.",
          },
          ctx,
        )
        expect(answered.metadata.updated).toBe(true)
        const acknowledged = yield* tool.execute(
          { action: "ack", workflowID: started.id, messageID: "intervention_tool_test" },
          ctx,
        )
        expect(acknowledged.metadata.updated).toBe(true)

        const consultation = Database.use((db) =>
          db
            .select()
            .from(WorkflowConsultationTable)
            .where(
              and(
                eq(WorkflowConsultationTable.workflow_id, started.id),
                eq(WorkflowConsultationTable.id, "consult_tool_test"),
              ),
            )
            .get(),
        )
        const intervention = Database.use((db) =>
          db
            .select()
            .from(WorkflowInterventionTable)
            .where(
              and(
                eq(WorkflowInterventionTable.workflow_id, started.id),
                eq(WorkflowInterventionTable.id, "intervention_tool_test"),
              ),
            )
            .get(),
        )
        expect(consultation?.status).toBe("answered")
        expect(consultation?.answer).toContain("charter is sufficient")
        expect(intervention?.status).toBe("acked")
        expect(intervention?.response).toBe("Acknowledged.")
        const journal = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, started.path, "journal", "messages.jsonl")).text(),
        )
        expect(journal).toContain("consult_tool_test")
        expect(journal).toContain("intervention_tool_test")
      }),
    30_000,
  )

  it.instance(
    "keeps delivered workflow interventions in the target inbox until acked",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Workflow delivered intervention inbox workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const graph = yield* workflow.graph(started.id)
        const departmentPM = graph.members.find((item) => item.role === "department_pm")
        if (!departmentPM) throw new Error("Expected department PM employee")

        const now = Date.now()
        Database.use((db) =>
          db
            .insert(WorkflowInterventionTable)
            .values({
              workflow_id: started.id,
              id: "intervention_delivered_tool_test",
              from_session_id: started.rootSessionID,
              target_session_id: departmentPM.sessionID,
              target_role: "department_pm",
              timing: "temporary-interrupt",
              message: "Runtime delivered this direction and still needs an explicit ack.",
              response: "Delivered by runtime.",
              path: path.join(started.path, "interventions", "intervention_delivered_tool_test.md"),
              status: "delivered",
              time_created: now,
              time_updated: now,
            })
            .run(),
        )

        const info = yield* WorkflowMessageTool
        const tool = yield* info.init()
        const ctx = workflowMessageContext(departmentPM.sessionID)
        const inbox = yield* tool.execute({ action: "inbox", workflowID: started.id }, ctx)
        expect(inbox.output).toContain("intervention_delivered_tool_test")
        expect(inbox.output).toContain("[delivered]")

        const acknowledged = yield* tool.execute(
          { action: "ack", workflowID: started.id, messageID: "intervention_delivered_tool_test" },
          ctx,
        )
        expect(acknowledged.metadata.status).toBe("acked")
        const closedInbox = yield* tool.execute({ action: "inbox", workflowID: started.id }, ctx)
        expect(closedInbox.output).not.toContain("intervention_delivered_tool_test")
      }),
    30_000,
  )

  it.instance(
    "expires stale workflow consultations and escalates back to the source session",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Workflow stale consultation escalation workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const graph = yield* workflow.graph(started.id)
        const mainPM = graph.members.find((item) => item.role === "main_pm")
        if (!mainPM) throw new Error("Expected main PM employee")

        const stale = Date.now() - 31 * 60 * 1000
        Database.use((db) =>
          db
            .insert(WorkflowConsultationTable)
            .values({
              workflow_id: started.id,
              id: "consult_expired_tool_test",
              from_session_id: started.rootSessionID!,
              to_session_id: mainPM.sessionID,
              from_role: "requester",
              to_role: "main_pm",
              reason: "requester needs confirmation",
              timing: "temporary-interrupt",
              question: "Can the requirements gate close without more clarification?",
              answer: "_Pending answer._",
              status: "pending",
              time_created: stale,
              time_updated: stale,
            })
            .run(),
        )

        const escalated = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const consultation = info.consultations.find((item) => item.id === "consult_expired_tool_test")
              const intervention = info.interventions.find((item) =>
                item.message.includes("Consultation consult_expired_tool_test expired"),
              )
              return consultation?.status === "expired" && intervention?.targetSessionID === started.rootSessionID
                ? { consultation, intervention }
                : undefined
            }),
          ),
          "stale workflow consultation did not expire and escalate",
          "10 seconds",
        )
        expect(escalated.consultation.answer).toContain("expired without an answer")
        const escalation = escalated.intervention
        if (!escalation) throw new Error("Expected expired consultation escalation intervention")
        expect(escalation.targetRole).toBe("requester")
        expect(["queued", "delivered"]).toContain(escalation.status)

        const info = yield* WorkflowMessageTool
        const tool = yield* info.init()
        const inbox = yield* tool.execute({ action: "inbox", workflowID: started.id }, workflowMessageContext(started.rootSessionID))
        expect(inbox.output).toContain(escalation.id)
        expect(inbox.output).toContain("Consultation consult_expired_tool_test expired")

        const staleEscalationTime = Date.now() - 31 * 60 * 1000
        Database.use((db) =>
          db
            .update(WorkflowInterventionTable)
            .set({ status: "delivered", time_created: staleEscalationTime, time_updated: staleEscalationTime })
            .where(and(eq(WorkflowInterventionTable.workflow_id, started.id), eq(WorkflowInterventionTable.id, escalation.id)))
            .run(),
        )
        const blocked = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const expired = info.interventions.find((item) => item.id === escalation.id)
              return info.workflow.status === "blocked" &&
                expired?.status === "expired" &&
                (info.workflow.error ?? "").includes("Workflow message escalation reached requester and expired")
                ? { info, expired }
                : undefined
            }),
          ),
          "requester escalation expiry did not block the workflow",
          "10 seconds",
        )
        expect(blocked.expired.response).toContain("expired without acknowledgement")
        expect(blocked.info.workflow.error).toContain("Human direction is required")

        const journal = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, started.path, "journal", "messages.jsonl")).text(),
        )
        expect(journal).toContain('"action":"expire"')
        expect(journal).toContain('"action":"escalate"')
        expect(journal).toContain("consult_expired_tool_test")
        expect(journal).toContain(escalation.id)
      }),
    35_000,
  )

  it.instance(
    "workflow message tool sends tracked messages into target inbox",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Workflow message send workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const graph = yield* workflow.graph(started.id)
        const mainPM = graph.members.find((item) => item.role === "main_pm")
        const departmentPM = graph.members.find((item) => item.role === "department_pm")
        const executor = graph.members.find((item) => item.role === "executor")
        if (!mainPM || !departmentPM || !executor) throw new Error("Expected main PM, department PM, and executor employees")

        const info = yield* WorkflowMessageTool
        const tool = yield* info.init()
        const rejectedAssignment = yield* tool.execute(
          {
            action: "send",
            workflowID: started.id,
            kind: "consultation",
            targetRole: "executor",
            message: "Your assignment: audit-graphics-scene-renderer. Output audit-graphics-scene-renderer/report.md.",
            reason: "first wave dispatch",
          },
          workflowMessageContext(mainPM.sessionID),
        )
        expect(rejectedAssignment.metadata.updated).toBe(false)
        expect(rejectedAssignment.output).toContain("workflow_message send is only for consultation or notification")
        expect(rejectedAssignment.output).toContain("action=update_xml, resume, or plan_complete")

        const sentConsultation = yield* tool.execute(
          {
            action: "send",
            workflowID: started.id,
            kind: "consultation",
            targetSessionID: departmentPM.sessionID,
            message: "Can requirements close after the charter update?",
            reason: "planning gate check",
          },
          workflowMessageContext(mainPM.sessionID),
        )
        expect(sentConsultation.metadata.updated).toBe(true)
        expect(sentConsultation.metadata.kind).toBe("consultation")

        const sentIntervention = yield* tool.execute(
          {
            action: "send",
            workflowID: started.id,
            kind: "intervention",
            targetRole: "department_pm",
            message: "Please acknowledge the requester scope before planning.",
          },
          workflowMessageContext(started.rootSessionID!),
        )
        expect(sentIntervention.metadata.updated).toBe(true)
        expect(sentIntervention.metadata.kind).toBe("intervention")
        const rejectedHandoff = yield* tool.execute(
          {
            action: "send",
            workflowID: started.id,
            kind: "handoff",
            targetRole: "department_pm",
            message: "The requirements handoff is ready for the next role.",
          },
          workflowMessageContext(started.rootSessionID!),
        )
        expect(rejectedHandoff.metadata.updated).toBe(false)
        expect(rejectedHandoff.metadata.status).toBe("precondition_failed")
        expect(rejectedHandoff.output).toContain("precondition_failed")
        expect(rejectedHandoff.output).toContain("handoff requires at least one workflow-relative artifact path")

        const sentHandoff = yield* tool.execute(
          {
            action: "send",
            workflowID: started.id,
            kind: "handoff",
            targetRole: "department_pm",
            message: "The requirements handoff is ready for the next role.",
            attachments: ["requirements/plan.md", "reference/index.md"],
          },
          workflowMessageContext(started.rootSessionID!),
        )
        expect(sentHandoff.metadata.updated).toBe(true)
        expect(sentHandoff.metadata.kind).toBe("handoff")
        const consultationID = sentConsultation.metadata.messageID
        const interventionID = sentIntervention.metadata.messageID
        const handoffID = sentHandoff.metadata.messageID
        if (!consultationID || !interventionID || !handoffID) throw new Error("Expected sent workflow message ids")

        const inbox = yield* tool.execute({ action: "inbox", workflowID: started.id }, workflowMessageContext(departmentPM.sessionID))
        expect(inbox.output).toContain(consultationID)
        expect(inbox.output).toContain(interventionID)
        expect(inbox.output).toContain(handoffID)
        expect(inbox.output).toContain("Can requirements close")
        expect(inbox.output).toContain("Please acknowledge")
        expect(inbox.output).toContain("requirements/plan.md")

        const messages = Database.use((db) =>
          db
            .select()
            .from(WorkflowMessageTable)
            .where(eq(WorkflowMessageTable.workflow_id, started.id))
            .all(),
        )
        const consultationMessage = messages.find((item) => item.id === consultationID)
        const interventionMessage = messages.find((item) => item.id === interventionID)
        const handoffMessage = messages.find((item) => item.id === handoffID)
        expect(consultationMessage?.kind).toBe("consultation")
        expect(consultationMessage?.status).toBe("pending")
        expect(consultationMessage?.to_session_id).toBe(departmentPM.sessionID)
        expect(interventionMessage?.kind).toBe("intervention")
        expect(interventionMessage?.status).toBe("queued")
        expect(handoffMessage?.kind).toBe("handoff")
        expect(handoffMessage?.attachments).toEqual(["requirements/plan.md", "reference/index.md"])

        Database.use((db) => {
          db
            .delete(WorkflowConsultationTable)
            .where(and(eq(WorkflowConsultationTable.workflow_id, started.id), eq(WorkflowConsultationTable.id, consultationID)))
            .run()
          db
            .delete(WorkflowInterventionTable)
            .where(and(eq(WorkflowInterventionTable.workflow_id, started.id), eq(WorkflowInterventionTable.id, handoffID)))
            .run()
        })
        const unifiedInbox = yield* tool.execute(
          { action: "inbox", workflowID: started.id },
          workflowMessageContext(departmentPM.sessionID),
        )
        expect(unifiedInbox.output).toContain(consultationID)
        expect(unifiedInbox.output).toContain(handoffID)

        const consultationAnswer = yield* tool.execute(
          {
            action: "answer",
            workflowID: started.id,
            messageID: consultationID,
            answer: "Yes, the charter can close from the unified message index.",
          },
          workflowMessageContext(departmentPM.sessionID),
        )
        expect(consultationAnswer.metadata.updated).toBe(true)
        expect(consultationAnswer.metadata.kind).toBe("consultation")
        expect(consultationAnswer.metadata.status).toBe("answered")

        const handoffAck = yield* tool.execute(
          { action: "ack", workflowID: started.id, messageID: handoffID },
          workflowMessageContext(departmentPM.sessionID),
        )
        expect(handoffAck.metadata.updated).toBe(true)
        expect(handoffAck.metadata.kind).toBe("handoff")
        const ackedHandoff = Database.use((db) =>
          db
            .select()
            .from(WorkflowMessageTable)
            .where(and(eq(WorkflowMessageTable.workflow_id, started.id), eq(WorkflowMessageTable.id, handoffID)))
            .get(),
        )
        expect(ackedHandoff?.kind).toBe("handoff")
        expect(ackedHandoff?.status).toBe("acked")
        expect(ackedHandoff?.attachments).toEqual(["requirements/plan.md", "reference/index.md"])
        const answeredConsultation = Database.use((db) =>
          db
            .select()
            .from(WorkflowMessageTable)
            .where(and(eq(WorkflowMessageTable.workflow_id, started.id), eq(WorkflowMessageTable.id, consultationID)))
            .get(),
        )
        expect(answeredConsultation?.status).toBe("answered")
        expect(answeredConsultation?.response).toContain("unified message index")

        const journal = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, started.path, "journal", "messages.jsonl")).text(),
        )
        expect(journal).toContain(consultationID)
        expect(journal).toContain(interventionID)
        expect(journal).toContain(handoffID)
        expect(journal).toContain('"action":"send"')
        expect(journal).toContain('"kind":"handoff"')
        expect(journal).toContain("requirements/plan.md")
      }),
    20_000,
  )

  it.instance(
    "workflow message intervention send uses runtime delivery",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Workflow message runtime delivery workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const info = yield* WorkflowMessageTool
        const tool = yield* info.init()
        const sent = yield* tool.execute(
          {
            action: "send",
            workflowID: started.id,
            kind: "intervention",
            targetRole: "main_pm",
            message: "Please incorporate the requester scope through runtime delivery.",
          },
          workflowMessageContext(started.rootSessionID!),
        )
        expect(sent.metadata.updated).toBe(true)
        expect(sent.metadata.kind).toBe("intervention")
        const interventionID = sent.metadata.messageID
        if (!interventionID) throw new Error("Expected runtime intervention id")

        const delivered = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((graph) => {
              const item = graph.interventions.find((intervention) => intervention.id === interventionID)
              return item?.status === "delivered" ? item : undefined
            }),
          ),
          "workflow_message intervention send did not deliver through the runtime",
          "5 seconds",
        )
        expect(delivered.targetRole).toBe("main_pm")
        expect(delivered.response).toContain('<opencode-workflow-control action="resume">')
        const message = Database.use((db) =>
          db
            .select()
            .from(WorkflowMessageTable)
            .where(and(eq(WorkflowMessageTable.workflow_id, started.id), eq(WorkflowMessageTable.id, interventionID)))
            .get(),
        )
        expect(message?.kind).toBe("intervention")
        expect(message?.status).toBe("delivered")
        expect(message?.response).toContain('<opencode-workflow-control action="resume">')
        const journal = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, started.path, "journal", "messages.jsonl")).text(),
        )
        expect(journal).toContain(interventionID)
        expect(journal).toContain('"action":"send"')
        expect(
          journal
            .trim()
            .split(/\r?\n/)
            .filter(Boolean)
            .map((line) => JSON.parse(line))
            .some(
              (row) =>
                row.messageID === interventionID && row.action === "deliver" && row.status === "delivered",
            ),
        ).toBe(true)
      }),
    20_000,
  )

  it.instance(
    "records requester direct intervention delivery and acknowledgement timeline",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Requester direct intervention lifecycle workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const mainPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "main_pm")
        if (!started.rootSessionID || !mainPM) throw new Error("Expected requester and main PM sessions")

        const info = yield* WorkflowMessageTool
        const tool = yield* info.init()
        const sent = yield* tool.execute(
          {
            action: "send",
            workflowID: started.id,
            kind: "intervention",
            targetSessionID: mainPM.sessionID,
            timing: "interrupt",
            message: "忽视 planning 直接推进是不允许静默丢失的指令;请确认收到后继续按 workflow control 检查。",
          },
          workflowMessageContext(started.rootSessionID),
        )
        expect(sent.metadata.updated).toBe(true)
        const interventionID = sent.metadata.messageID
        if (!interventionID) throw new Error("Expected requester intervention id")

        const delivered = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((graph) => {
              const item = graph.interventions.find((intervention) => intervention.id === interventionID)
              return item?.status === "delivered" ? item : undefined
            }),
          ),
          "requester direct intervention was not delivered to the main PM",
          "5 seconds",
        )
        expect(delivered.targetSessionID).toBe(mainPM.sessionID)
        expect(delivered.response).toContain('<opencode-workflow-control action="resume">')

        const inbox = yield* tool.execute({ action: "inbox", workflowID: started.id }, workflowMessageContext(mainPM.sessionID))
        expect(inbox.output).toContain(interventionID)
        expect(inbox.output).toContain("[delivered]")

        const acked = yield* tool.execute(
          { action: "ack", workflowID: started.id, messageID: interventionID },
          workflowMessageContext(mainPM.sessionID),
        )
        expect(acked.metadata.updated).toBe(true)
        expect(acked.metadata.status).toBe("acked")
        const closed = yield* workflow.graph(started.id)
        expect(closed.interventions.find((intervention) => intervention.id === interventionID)?.status).toBe("acked")

        const journal = yield* Effect.promise(() => readWorkflowMessageJournal(instance.directory, started.path))
        const rows = journal.filter((row) => row.messageID === interventionID)
        expect(rows.map((row) => row.action)).toContain("send")
        expect(rows.map((row) => row.action)).toContain("deliver")
        expect(rows.map((row) => row.action)).toContain("ack")
        expect(rows.find((row) => row.action === "send")?.status).toBe("queued")
        expect(rows.find((row) => row.action === "deliver")?.status).toBe("delivered")
        expect(rows.find((row) => row.action === "ack")?.status).toBe("acked")
        expect(rows.every((row) => typeof row.ts === "string")).toBe(true)
        expect(rows.every((row) => Number.isInteger(row.seq))).toBe(true)
      }),
    20_000,
  )

  it.instance(
    "respects temporary-interrupt and after-task intervention timing for active target sessions",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Intervention timing workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const executor = (yield* workflow.graph(started.id)).members.find((member) => member.role === "executor")
        if (!started.rootSessionID || !executor) throw new Error("Expected requester and executor sessions")

        const requirementsID = WorkflowMilestoneID.make("requirements")
        Database.use((db) => {
          db.update(WorkflowTable)
            .set({ status: "executing", time_updated: Date.now() })
            .where(eq(WorkflowTable.id, started.id))
            .run()
          db.update(WorkflowMilestoneTable)
            .set({
              status: "executing",
              attempt: 1,
              session: [
                {
                  role: "executor",
                  sessionID: executor.sessionID,
                  milestoneID: requirementsID,
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run()
        })

        process.env.OPENCODE_WORKFLOW_AUTORUN = "1"
        yield* workflow.intervene({
          workflowID: started.id,
          sourceSessionID: started.rootSessionID,
          targetSessionID: executor.sessionID,
          timing: "after-task",
          message: "after-task direction should wait until the executor milestone finishes",
        })
        yield* workflow.intervene({
          workflowID: started.id,
          sourceSessionID: started.rootSessionID,
          targetSessionID: executor.sessionID,
          timing: "temporary-interrupt",
          message: "temporary-interrupt direction should be delivered before the executor continues",
        })

        const beforeCompletion = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const afterTask = info.interventions.find((item) => item.message.includes("after-task direction"))
              const temporary = info.interventions.find((item) => item.message.includes("temporary-interrupt direction"))
              return afterTask?.status === "queued" && temporary?.status === "delivered"
                ? { afterTask, temporary }
                : undefined
            }),
          ),
          "temporary-interrupt did not deliver while after-task waited",
          "10 seconds",
        )
        expect(beforeCompletion.afterTask.timing).toBe("after-task")
        expect(beforeCompletion.temporary.timing).toBe("temporary-interrupt")

        Database.use((db) =>
          db.update(WorkflowMilestoneTable)
            .set({ status: "approved", time_updated: Date.now() })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )
        yield* workflow.dispatchCommand({
          action: "status",
          workflowID: started.id,
          sourceSessionID: started.rootSessionID,
          message: "active executor milestone finished; deliver queued after-task interventions",
        })

        const deliveredAfterCompletion = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const afterTask = info.interventions.find((item) => item.message.includes("after-task direction"))
              return afterTask?.status === "delivered" ? afterTask : undefined
            }),
          ),
          "after-task intervention did not deliver after target milestone completion",
          "10 seconds",
        )
        expect(deliveredAfterCompletion.response).toContain('<opencode-workflow-control action="resume">')
      }),
    25_000,
  )

  it.instance(
    "does not treat workflow-message assignment text as milestone dispatch",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Workflow message dispatch misuse workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const mainPM = (yield* workflow.graph(started.id)).members.find((item) => item.role === "main_pm")
        if (!mainPM) throw new Error("Expected a main PM employee")

        const messageID = MessageID.ascending()
        const now = Date.now()
        const message: MessageV2.Assistant = {
          id: messageID,
          sessionID: mainPM.sessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "workflow-main-pm",
          agent: "workflow-main-pm",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          finish: "stop",
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: mainPM.sessionID,
          messageID,
          type: "text",
          text: [
            "You're right that hammering the control plane hasn't drained the queue.",
            "But I realize I haven't used the documented dispatch channel yet.",
            "I'll now use the documented communication channel - raw <opencode-workflow-message> blocks directed at the executor roles by specialty.",
            "Per the requester's direction, I'm assigning the first wave directly.",
            '<opencode-workflow-message to-role="executor" specialty="graphics" timing="after-task" reason="first wave dispatch">Your assignment: audit-graphics-scene-renderer. Output audit-graphics-scene-renderer/report.md.</opencode-workflow-message>',
            '<opencode-workflow-message to-role="executor" specialty="core" timing="after-task" reason="first wave dispatch">Your assignment: audit-core-spine. Output audit-core-spine/report.md.</opencode-workflow-message>',
            '<opencode-workflow-message to-role="executor" specialty="ui" timing="after-task" reason="first wave dispatch">Your assignment: audit-ui. Output audit-ui/report.md.</opencode-workflow-message>',
            '<opencode-workflow-message to-role="executor" specialty="plans" timing="after-task" reason="first wave dispatch">Your assignment: audit-plans. Output audit-plans/report.md.</opencode-workflow-message>',
            "These four messages route through the workflow manager's communication channel and should prompt the executor sessions directly.",
          ].join("\n"),
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            type: MessageV2.Event.Updated.type,
            properties: { sessionID: mainPM.sessionID, info: message },
          },
        })

        const blocked = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.workflow.status === "blocked" && (info.workflow.error ?? "").includes("dispatch claim") ? info : undefined,
            ),
          ),
          "workflow-message assignment was not rejected as fake dispatch",
          "20 seconds",
        )

        expect(blocked.workflow.error).toContain("dispatch claim")
        expect(
          blocked.milestones.flatMap((milestone) => milestone.session).some((ref) => ref.role === "executor"),
        ).toBe(false)
        expect(
          blocked.consultations.some(
            (item) =>
              item.fromRole === "main_pm" &&
              item.toRole === "executor" &&
              item.question.includes("Your assignment: audit-graphics-scene-renderer"),
          ),
        ).toBe(false)
      }),
    25_000,
  )

  it.instance(
    "rejects natural-language dispatch claims without workflow control",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Natural dispatch claim workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const mainPM = (yield* workflow.graph(started.id)).members.find((item) => item.role === "main_pm")
        if (!mainPM) throw new Error("Expected a main PM employee")

        const messageID = MessageID.ascending()
        const now = Date.now()
        const message: MessageV2.Assistant = {
          id: messageID,
          sessionID: mainPM.sessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "workflow-main-pm",
          agent: "workflow-main-pm",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          finish: "stop",
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: mainPM.sessionID,
          messageID,
          type: "text",
          text: [
            "Still no pickup. I will use the documented communication channel now.",
            "Your assignment: audit-graphics-scene-renderer.",
            "These messages route through the workflow manager and should prompt executor sessions directly.",
            "",
            "## Handoff Summary",
            "- completed: queued first wave executor dispatch.",
            "- next: remaining tracks will be assigned in wave 2.",
          ].join("\n"),
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            type: MessageV2.Event.Updated.type,
            properties: { sessionID: mainPM.sessionID, info: message },
          },
        })

        const blocked = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.workflow.status === "blocked" && (info.workflow.error ?? "").includes("dispatch claim") ? info : undefined,
            ),
          ),
          "natural-language dispatch claim was not rejected",
          "20 seconds",
        )

        expect(blocked.workflow.error).toContain("dispatch claim")
        expect(blocked.consultations.some((item) => item.toRole === "executor")).toBe(false)
      }),
    25_000,
  )

  it.instance(
    "rejects resume messages that try to carry executor assignments",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Resume message dispatch misuse workflow", commandBusWorkflowXml].join("\n"),
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const mainPM = (yield* workflow.graph(started.id)).members.find((item) => item.role === "main_pm")
        if (!mainPM) throw new Error("Expected a main PM employee")

        const result = yield* workflow.dispatchCommand({
          action: "resume",
          workflowID: started.id,
          sourceSessionID: mainPM.sessionID,
          message: [
            "I'll now use the resume channel to dispatch executor roles.",
            "Your assignment: audit-graphics-scene-renderer. Output audit-graphics-scene-renderer/report.md.",
            "These messages should prompt executor sessions directly.",
          ].join("\n"),
        })
        expect(result.applied).toBe(false)
        expect(result.rejection?.code).toBe("precondition_failed")
        expect(result.message).toContain("resume message cannot assign")

        const graph = yield* workflow.graph(started.id)
        expect(
          graph.milestones.flatMap((milestone) => milestone.session).some((ref) => ref.role === "executor"),
        ).toBe(false)
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const command = journal.find((row) => row.action === "resume")
        expect(command?.outcome).toBe("rejected")
        expect(command?.rejection?.code).toBe("precondition_failed")
      }),
    20_000,
  )

  it.instance(
    "rejects raw workflow-message dispatch before consultation delivery",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "1"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Raw resolver misuse workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const journal = yield* pollWithTimeout(
          Effect.promise(() => Bun.file(path.join(instance.directory, started.path, "journal", "messages.jsonl")).text()).pipe(
            Effect.catchCause(() => Effect.succeed(undefined as string | undefined)),
            Effect.map((text) =>
              text?.includes('"action":"reject"') && text.includes("first wave dispatch") ? text : undefined,
            ),
          ),
          "raw workflow-message assignment was not journaled as rejected",
          "20 seconds",
        )
        const graph = yield* workflow.graph(started.id)

        expect(
          graph.consultations.some(
            (item) =>
              item.toRole === "executor" &&
              item.question.includes("Your assignment: audit-graphics-scene-renderer"),
          ),
        ).toBe(false)
        expect(journal).toContain('"action":"reject"')
        expect(journal).toContain("workflow-message is consultation/notification only")
        expect(journal).toContain("first wave dispatch")
      }),
    25_000,
  )

  it.instance(
    "applies managed prompt workflow control corrections",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "1"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Managed control correction workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const journal = yield* pollWithTimeout(
          Effect.promise(() => Bun.file(path.join(instance.directory, started.path, "journal", "commands.jsonl")).text()).pipe(
            Effect.catchCause(() => Effect.succeed(undefined as string | undefined)),
            Effect.map((text) =>
              text?.includes('"action":"plan_complete"') &&
              text.includes('"milestoneID":"requirements"') &&
              text.includes('"outcome":"applied"')
                ? text
                : undefined,
            ),
          ),
          "managed workflow control correction was not applied",
          "20 seconds",
        )
        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              return requirements?.status === "approved" &&
                implementation?.session.some((ref) => ref.role === "department_pm")
                ? info
                : undefined
            }),
          ),
          "managed workflow control correction did not dispatch downstream implementation",
          "20 seconds",
        )

        expect(journal).toContain('"action":"plan_complete"')
        expect(journal).toContain('"outcome":"applied"')
        expect(graph.workflow.status).not.toBe("blocked")
      }),
    25_000,
  )

  it.instance(
    "corrects main PM dispatch claims from idle sessions",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const bus = yield* Bus.Service
        const started = yield* workflow.start({
          prompt: ["Main PM idle dispatch correction workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const mainPM = (yield* workflow.graph(started.id)).members.find((item) => item.role === "main_pm")
        if (!mainPM) throw new Error("Expected a main PM employee")

        const messageID = MessageID.ascending()
        const now = Date.now()
        const message: MessageV2.Assistant = {
          id: messageID,
          sessionID: mainPM.sessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "workflow-main-pm",
          agent: "workflow-main-pm",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          finish: "stop",
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: mainPM.sessionID,
          messageID,
          type: "text",
          text: [
            "Requirements are ready and I have dispatched the implementation team through workflow messages.",
            "Queued milestone_status and resume; implementation should pick up now.",
            "Your assignment: implementation. Output implementation/plan.md.",
            "",
            "## Handoff Summary",
            "- completed: claimed downstream implementation dispatch.",
            "- next: implementation team should start.",
          ].join("\n"),
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)
        yield* bus.publish(testSessionIdleEvent, { sessionID: mainPM.sessionID })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              return requirements?.status === "done" && implementation?.session.some((ref) => ref.role === "department_pm")
                ? info
                : undefined
            }),
          ),
          "main PM idle dispatch correction did not unblock downstream dispatch",
          "20 seconds",
        )

        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        expect(journal.some((row) => row.action === "force_complete" && row.milestoneID === "requirements")).toBe(true)
        expect(graph.workflow.status).not.toBe("blocked")
      }),
    25_000,
  )

  it.instance(
    "converts main PM stalled queue fake dispatch into gated audit dispatch",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const bus = yield* Bus.Service
        const started = yield* workflow.start({
          prompt: ["Unreliable correction audit workflow", thirteenAuditWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 13,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const graph = yield* workflow.graph(started.id)
        const mainPM = graph.members.find((item) => item.role === "main_pm")
        if (!mainPM) throw new Error("Expected a main PM employee")
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              time_updated: Date.now(),
            })
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, started.id),
                eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("requirements")),
              ),
            )
            .run(),
        )

        const messageID = MessageID.ascending()
        const now = Date.now()
        const message: MessageV2.Assistant = {
          id: messageID,
          sessionID: mainPM.sessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "workflow-main-pm",
          agent: "workflow-main-pm",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          finish: "stop",
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: mainPM.sessionID,
          messageID,
          type: "text",
          text: [
            "Requirements is STILL planning; milestone_status=approval has not drained.",
            "I queued milestone_status=approved and two resume nudges, but nothing has transitioned.",
            "The control-plane queue appears stalled, so I will use the documented communication channel.",
            '<opencode-workflow-message to-role="executor" specialty="graphics" timing="temporary-interrupt" reason="first wave dispatch">Your assignment: audit-graphics-scene-renderer.</opencode-workflow-message>',
            "",
            "## Handoff Summary",
            "- completed: attempted first-wave executor dispatch through workflow-message.",
            "- next: executor sessions should pick up directly.",
          ].join("\n"),
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)
        yield* bus.publish(testSessionIdleEvent, { sessionID: mainPM.sessionID })

        const dispatched = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const tracks = thirteenAuditIDs.map((id) => info.milestones.find((milestone) => milestone.id === id))
              return requirements?.status === "done" &&
                tracks.every((track) => track?.session.some((ref) => ref.role === "department_pm"))
                ? info
                : undefined
            }),
          ),
          "main PM stalled queue fake dispatch did not dispatch gated audit tracks",
          "30 seconds",
        ).pipe(
          Effect.catchCause(() =>
            Effect.gen(function* () {
              const info = yield* workflow.graph(started.id)
              const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
              throw new Error(
                [
                  "main PM stalled queue fake dispatch did not dispatch gated audit tracks",
                  `workflow=${info.workflow.status}:error=${info.workflow.error ?? "none"}`,
                  ...info.milestones.map(
                    (milestone) =>
                      `${milestone.id}:${milestone.status}:waiting=${milestone.waitingFor ?? "none"}:sessions=${
                        milestone.session.map((ref) => `${ref.role}:${ref.sessionID}`).join(",") || "none"
                      }`,
                  ),
                  `commands=${journal
                    .map(
                      (row) =>
                        `${row.action}:${row.milestoneID ?? "none"}:${row.outcome}:${
                          "message" in row && typeof row.message === "string" ? row.message : ""
                        }`,
                    )
                    .join(" | ")}`,
                ].join("\n"),
              )
            }),
          ),
        )

        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const command = journal.find((row) => row.action === "force_complete" && row.milestoneID === "requirements")
        expect(command?.outcome).toBe("applied")
        expect(dispatched.workflow.status).not.toBe("blocked")
        expect(
          dispatched.consultations.some(
            (item) => item.toRole === "executor" && item.question.includes("audit-graphics-scene-renderer"),
          ),
        ).toBe(false)
      }),
    35_000,
  )

  it.instance(
    "corrects department PM stalled queue claims from idle sessions",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const bus = yield* Bus.Service
        const started = yield* workflow.start({
          prompt: ["Department PM idle queue correction workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const graph = yield* workflow.graph(started.id)
        const departmentPM = graph.members.find((item) => item.role === "department_pm")
        if (!departmentPM) throw new Error("Expected a department PM employee")

        const requirementsID = WorkflowMilestoneID.make("requirements")
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: 1,
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )

        const messageID = MessageID.ascending()
        const now = Date.now()
        const message: MessageV2.Assistant = {
          id: messageID,
          sessionID: departmentPM.sessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "workflow-department-pm",
          agent: "workflow-department-pm",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          finish: "stop",
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: departmentPM.sessionID,
          messageID,
          type: "text",
          text: [
            "Requirements is STILL planning; milestone_status=approval has not drained.",
            "I queued milestone_status=approved and two resume nudges, but nothing has transitioned.",
            "The control-plane queue appears stalled, so I will use the documented communication channel.",
            '<opencode-workflow-message to-role="executor" specialty="graphics" timing="temporary-interrupt" reason="first wave dispatch">Your assignment: audit-graphics-scene-renderer.</opencode-workflow-message>',
            "",
            "## Handoff Summary",
            "- completed: attempted first-wave executor dispatch through workflow-message.",
            "- next: executor sessions should pick up directly.",
          ].join("\n"),
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)
        yield* bus.publish(testSessionIdleEvent, { sessionID: departmentPM.sessionID })

        const updated = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              return requirements && requirements.attempt > 1 ? info : undefined
            }),
          ),
          "department PM idle stalled-queue correction did not restart the milestone through workflow control",
          "20 seconds",
        )

        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const command = journal.find((row) => row.action === "plan_complete" && row.milestoneID === "requirements")
        expect(command?.outcome).toBe("applied")
        expect(updated.workflow.status).not.toBe("blocked")
        expect(
          updated.consultations.some(
            (item) => item.toRole === "executor" && item.question.includes("audit-graphics-scene-renderer"),
          ),
        ).toBe(false)
      }),
    25_000,
  )

  it.instance(
    "applies workflow tool commands received through the global command bus",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Global command bus workflow", commandBusWorkflowXml].join("\n"),
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const journalReplayCommandID = Bus.createID()
        mkdirSync(path.join(instance.directory, started.path, "journal"), { recursive: true })
        writeFileSync(
          path.join(instance.directory, started.path, "journal", "commands.jsonl"),
          `${JSON.stringify({
            seq: 1,
            id: journalReplayCommandID,
            ts: new Date(0).toISOString(),
            source: { sessionID: started.rootSessionID, role: "requester" },
            action: "force_complete",
            milestoneID: "implementation",
            from: { milestoneStatus: "pending" },
            to: { milestoneStatus: "done" },
            outcome: "applied",
            message: "Recovered workflow command result from journal.",
          })}\n`,
        )
        const journalReplayPromise = waitForWorkflowCommandResult(journalReplayCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: journalReplayCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: journalReplayCommandID,
              action: "force_complete",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              milestoneID: "implementation",
              message: "this should replay the journal result instead of applying again",
            },
          },
        })
        const journalReplay = yield* Effect.promise(() => journalReplayPromise)
        expect(journalReplay?.applied).toBe(true)
        expect(journalReplay?.message).toBe("Recovered workflow command result from journal.")
        expect((yield* workflow.graph(started.id)).milestones.find((milestone) => milestone.id === "implementation")?.status).toBe(
          "pending",
        )

        const invalidXmlCommandID = Bus.createID()
        const invalidXmlResultPromise = waitForWorkflowCommandResult(invalidXmlCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: invalidXmlCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: invalidXmlCommandID,
              action: "update_xml",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              xml: [
                "<workflow>",
                "  <ordered>",
                '    <milestone id="requirements" title="Requirements" department="product">a</milestone>',
                "    <slug>literal placeholders are only valid inside milestone text</slug>",
                "  </ordered>",
                "</workflow>",
              ].join("\n"),
              message: "invalid structural slug should be rejected with structured reason",
            },
          },
        })
        const invalidXmlResult = yield* Effect.promise(() => invalidXmlResultPromise)
        expect(invalidXmlResult?.applied).toBe(false)
        expect(invalidXmlResult?.rejection?.code).toBe("invalid_xml")
        expect(invalidXmlResult?.rejection?.reason).toContain("unsupported workflow element <slug> at line 4, column")

        const unrelated = yield* sessions.create({ title: "Unrelated session" })
        const rejectedCommandID = Bus.createID()
        const rejectedResultPromise = waitForWorkflowCommandResult(rejectedCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: rejectedCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: rejectedCommandID,
              action: "force_complete",
              workflowID: started.id,
              sourceSessionID: unrelated.id,
              milestoneID: "requirements",
              message: "unrelated session should not close the gate",
            },
          },
        })

        const rejectedResult = yield* Effect.promise(() => rejectedResultPromise)
        expect(rejectedResult?.applied).toBe(false)
        expect(rejectedResult?.rejection?.code).toBe("not_authorized")

        const commandID = Bus.createID()
        const resultPromise = waitForWorkflowCommandResult(commandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: commandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: commandID,
              action: "force_complete",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              milestoneID: "requirements",
              message: "test closes requirements",
            },
          },
        })

        const result = yield* Effect.promise(() => resultPromise)
        expect(result?.applied).toBe(true)
        expect(result?.message).toContain("requirements")
        const replayPromise = waitForWorkflowCommandResult(commandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: commandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: commandID,
              action: "force_complete",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              milestoneID: "requirements",
              message: "test closes requirements",
            },
          },
        })
        const replay = yield* Effect.promise(() => replayPromise)
        expect(replay?.applied).toBe(true)
        expect(replay?.message).toBe(result?.message)
        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.milestones.find((milestone) => milestone.id === "requirements")?.status === "done"
                ? info
                : undefined,
            ),
          ),
          "global workflow command did not update the milestone",
          "5 seconds",
        )
        expect(graph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const rejectedJournal = journal.find((row) => row.id === rejectedCommandID)
        expect(rejectedJournal?.outcome).toBe("rejected")
        expect(rejectedJournal?.rejection?.code).toBe("not_authorized")
        expect(rejectedJournal?.source?.role).toBe("unknown")
        const invalidXmlJournal = journal.find((row) => row.id === invalidXmlCommandID)
        expect(invalidXmlJournal?.outcome).toBe("rejected")
        expect(invalidXmlJournal?.rejection?.code).toBe("invalid_xml")
        const appliedJournal = journal.find((row) => row.id === commandID)
        expect(appliedJournal?.outcome).toBe("applied")
        expect(appliedJournal?.source?.role).toBe("requester")
        expect(["pending", "planning"]).toContain(appliedJournal?.from?.milestoneStatus ?? "")
        expect(appliedJournal?.to?.milestoneStatus).toBe("done")
        expect(journal.filter((row) => row.id === commandID).length).toBe(1)
      }),
  )

  it.instance(
    "dispatches workflow tool commands through the workflow service",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Direct workflow tool workflow", commandBusWorkflowXml].join("\n"),
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const info = yield* WorkflowTool
        const tool = yield* info.init()
        const result = yield* tool.execute(
          {
            action: "force_complete",
            workflowID: started.id,
            milestoneID: WorkflowMilestoneID.make("requirements"),
            message: "requester closes requirements through the direct workflow tool",
          },
          workflowMessageContext(started.rootSessionID),
        )
        expect(result.metadata.confirmed).toBe(true)
        expect(result.metadata.applied).toBe(true)
        expect(result.output).toContain("Applied workflow command: force_complete")

        const graph = yield* workflow.graph(started.id)
        expect(graph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
      }),
    15_000,
  )

  it.instance(
    "replays one workflow tool command outcome for one hundred duplicate ids",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Duplicate command id workflow", commandBusWorkflowXml].join("\n"),
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const rootSessionID = started.rootSessionID

        const commandID = Bus.createID()
        const results = yield* Effect.all(
          Array.from({ length: 100 }, () =>
            workflow.dispatchCommand({
              id: commandID,
              action: "force_complete",
              workflowID: started.id,
              sourceSessionID: rootSessionID,
              milestoneID: WorkflowMilestoneID.make("requirements"),
              message: "requester closes requirements once despite duplicate command delivery",
            }),
          ),
          { concurrency: "unbounded" },
        )

        expect(results.every((result) => result.applied)).toBe(true)
        expect(new Set(results.map((result) => result.message)).size).toBe(1)
        expect((yield* workflow.graph(started.id)).milestones.find((milestone) => milestone.id === "requirements")?.status).toBe(
          "done",
        )
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        expect(journal.filter((row) => row.id === commandID).length).toBe(1)
        expect(journal.find((row) => row.id === commandID)?.outcome).toBe("applied")
      }),
    15_000,
  )

  it.instance(
    "returns structured rejection codes instead of queued command semantics",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Structured command rejection workflow", commandBusWorkflowXml].join("\n"),
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const unrelated = yield* sessions.create({ title: "Unrelated workflow command source" })
        const rejected = yield* Effect.all(
          [
            workflow.dispatchCommand({
              action: "update_xml",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              xml: "",
            }),
            workflow.dispatchCommand({
              action: "milestone_status",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
            }),
            workflow.dispatchCommand({
              action: "plan_complete",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
            }),
            workflow.dispatchCommand({
              action: "force_complete",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
            }),
            workflow.dispatchCommand({
              action: "force_skip",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
            }),
            workflow.dispatchCommand({
              action: "status_update",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
            }),
            workflow.dispatchCommand({
              action: "scheduling",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
            }),
            workflow.dispatchCommand({
              action: "workflow_status",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
            }),
            workflow.dispatchCommand({
              action: "force_complete",
              sourceSessionID: unrelated.id,
              milestoneID: WorkflowMilestoneID.make("requirements"),
            }),
          ],
          { concurrency: 1 },
        )

        expect(rejected.every((result) => result.applied === false)).toBe(true)
        expect(rejected.every((result) => !!result.rejection?.code)).toBe(true)
        expect(rejected.map((result) => result.message).join("\n")).not.toContain("queued")
      }),
    15_000,
  )

  it.instance(
    "turns requester direct execution intervention into auditable gate close and dispatch",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: ["Requester direct executor override workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const departmentPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected a department PM session")
        const requirementsID = WorkflowMilestoneID.make("requirements")
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: 1,
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )
        const protectedJobID = `${started.id}:requirements:requester-override-guard`
        yield* background.start({
          id: protectedJobID,
          type: "workflow.milestone",
          title: "requirements planning guard",
          metadata: { workflowID: started.id, milestoneID: "requirements" },
          run: Effect.never,
        })
        yield* Effect.addFinalizer(() => background.cancel(protectedJobID).pipe(Effect.ignore))

        yield* workflow.intervene({
          workflowID: started.id,
          sourceSessionID: started.rootSessionID,
          message: "忽视planning状态，直接继续到执行者",
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              if (requirements?.status !== "done") return undefined
              if (!implementation?.session.some((ref) => ref.role === "department_pm")) return undefined
              return info
            }),
          ),
          "requester direct execution override did not close planning and dispatch downstream work",
          "30 seconds",
        )

        expect((yield* background.get(protectedJobID))?.status).toBe("cancelled")
        expect(graph.interventions.at(-1)?.status).toBe("acked")
        expect(graph.interventions.at(-1)?.response).toContain("force-completed")
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const command = journal.find((row) => row.action === "force_complete" && row.milestoneID === "requirements")
        expect(command?.outcome).toBe("applied")
        expect(command?.source?.role).toBe("requester")

        const replay = yield* workflow.start({
          prompt: ["Requester queued direct override replay workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!replay.rootSessionID) throw new Error("Replay workflow did not create a requester session")
        const replayGraph = yield* workflow.graph(replay.id)
        const replayMainPM = replayGraph.members.find((member) => member.role === "main_pm")
        const replayDepartmentPM = replayGraph.members.find((member) => member.role === "department_pm")
        if (!replayMainPM || !replayDepartmentPM) throw new Error("Expected replay main PM and department PM sessions")
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: 1,
              session: [
                {
                  role: "department_pm",
                  sessionID: replayDepartmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, replay.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )
        Database.use((db) =>
          db
            .insert(WorkflowInterventionTable)
            .values({
              workflow_id: replay.id,
              id: "intervention_direct_override_replay",
              from_session_id: replay.rootSessionID,
              target_session_id: replayMainPM.sessionID,
              target_role: "main_pm",
              timing: "temporary-interrupt",
              message: "忽视planning状态，直接继续到执行者",
              path: path.join(replay.path, "interventions", "intervention_direct_override_replay.md"),
              status: "queued",
              time_created: Date.now(),
              time_updated: Date.now(),
            })
            .run(),
        )
        const replayProtectedJobID = `${replay.id}:requirements:queued-direct-override-guard`
        yield* background.start({
          id: replayProtectedJobID,
          type: "workflow.milestone",
          title: "requirements planning guard",
          metadata: { workflowID: replay.id, milestoneID: "requirements" },
          run: Effect.never,
        })
        yield* Effect.addFinalizer(() => background.cancel(replayProtectedJobID).pipe(Effect.ignore))

        const status = yield* workflow.dispatchCommand({
          action: "status",
          workflowID: replay.id,
          sourceSessionID: replay.rootSessionID,
          message: "refresh queued direct execution override",
        })
        expect(status.applied).toBe(true)
        const replayDispatched = yield* pollWithTimeout(
          workflow.graph(replay.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              const intervention = info.interventions.find((item) => item.id === "intervention_direct_override_replay")
              if (requirements?.status !== "done") return undefined
              if (intervention?.status !== "acked") return undefined
              if (!implementation?.session.some((ref) => ref.role === "department_pm")) return undefined
              return info
            }),
          ),
          "queued requester direct execution override did not convert to gate close and dispatch",
          "30 seconds",
        )
        expect((yield* background.get(replayProtectedJobID))?.status).toBe("cancelled")
        expect(replayDispatched.interventions.find((item) => item.id === "intervention_direct_override_replay")?.response).toContain(
          "force-completed",
        )
      }),
    20_000,
  )

  it.instance(
    "rejects unauthorized milestone transitions and dispatches after manager milestone_status closes a gate",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: ["Milestone status dispatch workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const initialGraph = yield* workflow.graph(started.id)
        const departmentPM = initialGraph.members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected a department PM session")
        const executor = initialGraph.members.find((member) => member.role === "executor")
        if (!executor) throw new Error("Expected an executor session")
        const requirementsID = WorkflowMilestoneID.make("requirements")

        const protectedJobID = `${started.id}:requirements:illegal-transition-guard`
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 0,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )
        yield* background.start({
          id: protectedJobID,
          type: "workflow.milestone",
          title: "requirements planning guard",
          metadata: { workflowID: started.id, milestoneID: "requirements" },
          run: Effect.never,
        })
        yield* Effect.addFinalizer(() => background.cancel(protectedJobID).pipe(Effect.ignore))

        const rejectedCommandID = Bus.createID()
        const rejectedResultPromise = waitForWorkflowCommandResult(rejectedCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: rejectedCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: rejectedCommandID,
              action: "milestone_status",
              workflowID: started.id,
              sourceSessionID: executor.sessionID,
              milestoneID: "requirements",
              milestoneStatus: "done",
              message: "unauthorized direct completion should be rejected",
            },
          },
        })
        const rejectedResult = yield* Effect.promise(() => rejectedResultPromise)
        expect(rejectedResult?.applied).toBe(false)
        expect(rejectedResult?.rejection?.code).toBe("illegal_transition")
        expect(rejectedResult?.message).toContain("Planning gates do not close through milestone_status")
        expect(rejectedResult?.message).toContain("action=plan_complete")
        expect(rejectedResult?.message).toContain("action=force_complete")
        expect(rejectedResult?.rejection?.allowedTransitions).toContain("executing")
        const rejectedJournal = (yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))).find(
          (row) => row.id === rejectedCommandID,
        )
        expect(rejectedJournal?.outcome).toBe("rejected")
        expect(rejectedJournal?.from?.milestoneStatus).toBe("planning")
        expect(rejectedJournal?.to?.milestoneStatus).toBe("planning")
        expect(rejectedJournal?.rejection?.code).toBe("illegal_transition")
        const statusSummary = workflowCommandJournalSummary(started.id).join("\n")
        expect(statusSummary).toContain("illegal_transition")
        expect(statusSummary).toContain("milestone planning")
        const dispatchSummary = workflowDispatchSummary(started.id).join("\n")
        expect(dispatchSummary).toContain("ready: none")
        expect(dispatchSummary).toContain("active: requirements:planning")
        expect((yield* background.get(protectedJobID))?.status).toBe("running")

        const ownerAliasCommandID = Bus.createID()
        const ownerAliasResultPromise = waitForWorkflowCommandResult(ownerAliasCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: ownerAliasCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: ownerAliasCommandID,
              action: "milestone_status",
              workflowID: started.id,
              sourceSessionID: departmentPM.sessionID,
              milestoneID: "requirements",
              milestoneStatus: "done",
              message: "owning department PM reports the plan gate is ready to continue",
            },
          },
        })
        const ownerAliasResult = yield* Effect.promise(() => ownerAliasResultPromise)
        expect(ownerAliasResult?.applied).toBe(true)
        expect(ownerAliasResult?.message).toContain("owning department PM")
        expect(ownerAliasResult?.message).toContain("plan_complete")
        expect((yield* background.get(protectedJobID))?.status).toBe("cancelled")

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              if (!requirements || requirements.attempt === 0) return undefined
              return info
            }),
          ),
          "owning department PM milestone_status did not cancel stale planning run and redispatch requirements",
          "12 seconds",
        )
        const requirements = graph.milestones.find((milestone) => milestone.id === "requirements")
        expect(requirements?.session.some((ref) => ref.role === "department_pm")).toBe(true)
      }),
    20_000,
  )

  it.instance(
    "main PM milestone_status approval drains a planning gate and dispatches thirteen audit tracks",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: ["Thirteen audit gate workflow", thirteenAuditWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 13,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const initialGraph = yield* workflow.graph(started.id)
        const mainPM = initialGraph.members.find((member) => member.role === "main_pm")
        if (!mainPM) throw new Error("Expected a main PM session")
        const executor = initialGraph.members.find((member) => member.role === "executor")
        if (!executor) throw new Error("Expected an executor session")
        yield* Effect.all(
          (yield* background.list())
            .filter((job) => job.metadata?.workflowID === started.id)
            .map((job) => background.cancel(job.id).pipe(Effect.ignore)),
          { discard: true },
        )
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              session: [],
              attempt: 0,
              time_updated: Date.now(),
            })
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, started.id),
                eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("requirements")),
              ),
            )
            .run(),
        )

        const protectedJobID = `${started.id}:requirements:t0-illegal-transition-guard`
        yield* background.start({
          id: protectedJobID,
          type: "workflow.milestone",
          title: "requirements T-0 planning guard",
          metadata: { workflowID: started.id, milestoneID: "requirements" },
          run: Effect.never,
        })
        yield* Effect.addFinalizer(() => background.cancel(protectedJobID).pipe(Effect.ignore))

        const rejectedCommandID = Bus.createID()
        const rejectedResultPromise = waitForWorkflowCommandResult(rejectedCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: rejectedCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: rejectedCommandID,
              action: "milestone_status",
              workflowID: started.id,
              sourceSessionID: executor.sessionID,
              milestoneID: "requirements",
              milestoneStatus: "done",
              message: "executor tries to close the planning gate directly before audits are ready",
            },
          },
        })
        const rejectedResult = yield* Effect.promise(() => rejectedResultPromise)
        expect(rejectedResult?.applied).toBe(false)
        expect(rejectedResult?.rejection?.code).toBe("illegal_transition")
        expect(rejectedResult?.rejection?.allowedTransitions).toContain("executing")
        expect(rejectedResult?.message).toContain("action=plan_complete")
        expect(rejectedResult?.message).toContain("action=force_complete")
        expect((yield* background.get(protectedJobID))?.status).toBe("running")

        const commandID = Bus.createID()
        const resultPromise = waitForWorkflowCommandResult(commandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: commandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: commandID,
              action: "milestone_status",
              workflowID: started.id,
              sourceSessionID: mainPM.sessionID,
              milestoneID: "requirements",
              milestoneStatus: "approved",
              message: "Main PM approves the requirements planning gate and unblocks all audit tracks.",
            },
          },
        })
        const result = yield* Effect.promise(() => resultPromise)
        expect(result?.applied).toBe(true)
        expect(result?.message).toContain("force_complete")
        expect((yield* background.get(protectedJobID))?.status).toBe("cancelled")

        const dispatched = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const tracks = thirteenAuditIDs.map((id) => info.milestones.find((milestone) => milestone.id === id))
              return requirements?.status === "done" &&
                tracks.length === thirteenAuditIDs.length &&
                tracks.every((track) => track?.session.some((ref) => ref.role === "department_pm"))
                ? info
                : undefined
            }),
          ),
          "main PM milestone_status approval did not dispatch all thirteen audit tracks",
          "30 seconds",
        ).pipe(
          Effect.catchCause(() =>
            Effect.gen(function* () {
              const info = yield* workflow.graph(started.id)
              const jobs = yield* background.list()
              throw new Error(
                [
                  "main PM milestone_status approval did not dispatch all thirteen audit tracks",
                  `workflow=${info.workflow.status}:scheduling=${info.workflow.scheduling?.mode ?? "none"}:error=${
                    info.workflow.error ?? "none"
                  }`,
                  ...info.milestones.map(
                    (milestone) =>
                      `${milestone.id}:${milestone.status}:waiting=${milestone.waitingFor ?? "none"}:sessions=${
                        milestone.session.map((ref) => `${ref.role}:${ref.sessionID}`).join(",") || "none"
                      }`,
                  ),
                  `jobs=${jobs
                    .filter((job) => job.metadata?.workflowID === started.id)
                    .map((job) => `${job.id}:${job.type}:${job.status}:${job.metadata?.milestoneID ?? ""}`)
                    .join("|")}`,
                ].join("\n"),
              )
            }),
          ),
        )
        expect(
          thirteenAuditIDs.every((id) =>
            dispatched.milestones.find((milestone) => milestone.id === id)?.session.some((ref) => ref.role === "department_pm"),
          ),
        ).toBe(true)
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const rejectedCommand = journal.find((row) => row.id === rejectedCommandID)
        expect(rejectedCommand?.outcome).toBe("rejected")
        expect(rejectedCommand?.from?.milestoneStatus).toBe("planning")
        expect(rejectedCommand?.to?.milestoneStatus).toBe("planning")
        expect(rejectedCommand?.rejection?.code).toBe("illegal_transition")
        const command = journal.find((row) => row.id === commandID)
        expect(command?.outcome).toBe("applied")
        expect(command?.from?.milestoneStatus).toBe("planning")
        expect(command?.to?.milestoneStatus).toBe("done")
      }),
    45_000,
  )

  it.instance(
    "requester plan_complete cancels stale planning runs and redispatches the milestone",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: ["Requester plan complete recovery workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const departmentPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected a department PM session")
        const requirementsID = WorkflowMilestoneID.make("requirements")
        const protectedJobID = `${started.id}:requirements:stale-plan-complete-guard`

        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 0,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )
        yield* background.start({
          id: protectedJobID,
          type: "workflow.milestone",
          title: "stale requirements planning guard",
          metadata: { workflowID: started.id, milestoneID: "requirements" },
          run: Effect.never,
        })
        yield* Effect.addFinalizer(() => background.cancel(protectedJobID).pipe(Effect.ignore))

        const commandID = Bus.createID()
        const resultPromise = waitForWorkflowCommandResult(commandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: commandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: commandID,
              action: "plan_complete",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              milestoneID: "requirements",
              message: "requester confirms the requirements plan and wants executor dispatch to continue",
            },
          },
        })
        const result = yield* Effect.promise(() => resultPromise)
        expect(result?.applied).toBe(true)
        expect(result?.message).toContain("stale active planning runs were cancelled")
        expect((yield* background.get(protectedJobID))?.status).toBe("cancelled")

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              if (!requirements || requirements.status === "pending" || requirements.attempt === 0) return undefined
              return info
            }),
          ),
          "requester plan_complete did not redispatch the stale planning milestone",
          "12 seconds",
        )
        const requirements = graph.milestones.find((milestone) => milestone.id === "requirements")
        expect(requirements?.attempt).toBeGreaterThan(0)
        expect(requirements?.session.some((ref) => ref.role === "department_pm")).toBe(true)
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const command = journal.find((row) => row.id === commandID)
        expect(command?.outcome).toBe("applied")
        expect(command?.from?.milestoneStatus).toBe("planning")
      }),
    20_000,
  )

  it.instance(
    "department PM plan_complete cancels stale planning runs and redispatches the milestone",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: ["Department PM plan complete recovery workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const departmentPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected a department PM session")
        const requirementsID = WorkflowMilestoneID.make("requirements")
        const protectedJobID = `${started.id}:requirements:stale-department-plan-complete-guard`

        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 0,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )
        yield* background.start({
          id: protectedJobID,
          type: "workflow.milestone",
          title: "stale department requirements planning guard",
          metadata: { workflowID: started.id, milestoneID: "requirements" },
          run: Effect.never,
        })
        yield* Effect.addFinalizer(() => background.cancel(protectedJobID).pipe(Effect.ignore))

        const commandID = Bus.createID()
        const resultPromise = waitForWorkflowCommandResult(commandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: commandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: commandID,
              action: "plan_complete",
              workflowID: started.id,
              sourceSessionID: departmentPM.sessionID,
              milestoneID: "requirements",
              message: "department PM plan is complete and stale runner should not hold the workflow",
            },
          },
        })
        const result = yield* Effect.promise(() => resultPromise)
        expect(result?.applied).toBe(true)
        expect(result?.message).toContain("stale active planning runs were cancelled")
        expect((yield* background.get(protectedJobID))?.status).toBe("cancelled")

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              if (!requirements || requirements.attempt === 0) return undefined
              return info
            }),
          ),
          "department PM plan_complete did not redispatch the stale planning milestone",
          "12 seconds",
        )
        const requirements = graph.milestones.find((milestone) => milestone.id === "requirements")
        expect(requirements?.session.some((ref) => ref.role === "department_pm")).toBe(true)
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const command = journal.find((row) => row.id === commandID)
        expect(command?.outcome).toBe("applied")
        expect(command?.from?.milestoneStatus).toBe("planning")
      }),
    20_000,
  )

  it.instance(
    "applies workflow control XML command blocks from workflow sessions",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Control XML command workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const mainPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "main_pm")
        if (!mainPM) throw new Error("Expected a main PM session")

        const now = Date.now()
        const message: MessageV2.Assistant = {
          id: MessageID.ascending(),
          sessionID: mainPM.sessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "workflow-main-pm",
          agent: "workflow-main-pm",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: {
              read: 0,
              write: 0,
            },
          },
          finish: "stop",
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: mainPM.sessionID,
          messageID: message.id,
          type: "text" as const,
          text: [
            "Requirements are approved as a gate-only planning milestone.",
            '<opencode-workflow-control action="force_complete" milestone="requirements">requirements charter is approved and should unblock implementation</opencode-workflow-control>',
            "",
            "## Handoff Summary",
            "- completed: approved requirements as a gate-only milestone.",
            "- next: implementation should dispatch from the workflow scheduler.",
          ].join("\n"),
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              if (requirements?.status !== "done") return undefined
              if (!implementation?.session.some((ref) => ref.role === "department_pm")) return undefined
              return info
            }),
          ),
          "workflow control XML force_complete did not dispatch the dependent implementation milestone",
          "12 seconds",
        )
        expect(graph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
        expect(
          graph.milestones.find((milestone) => milestone.id === "implementation")?.session.some((ref) => ref.role === "department_pm"),
        ).toBe(true)

        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        expect(journal.some((row) => row.action === "force_complete" && row.outcome === "applied")).toBe(true)
      }),
    20_000,
  )

  it.instance(
    "accepts milestone_status approval alias from workflow control and dispatches downstream work",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Milestone approval alias workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const graph = yield* workflow.graph(started.id)
        const mainPM = graph.members.find((member) => member.role === "main_pm")
        const departmentPM = graph.members.find((member) => member.role === "department_pm")
        if (!mainPM || !departmentPM) throw new Error("Expected main PM and department PM sessions")
        const requirementsID = WorkflowMilestoneID.make("requirements")

        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )

        const messageID = MessageID.ascending()
        const now = Date.now()
        const message: MessageV2.Assistant = {
          id: messageID,
          sessionID: mainPM.sessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "workflow-main-pm",
          agent: "workflow-main-pm",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: {
              read: 0,
              write: 0,
            },
          },
          finish: "stop",
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: mainPM.sessionID,
          messageID,
          type: "text" as const,
          text: [
            "Requirements is approved and should unblock the next milestone.",
            '<opencode-workflow-control action="milestone_status" milestone="requirements" status="approval">requirements approved</opencode-workflow-control>',
            "",
            "## Handoff Summary",
            "- completed: closed the requirements planning gate.",
            "- next: scheduler should dispatch downstream milestones.",
          ].join("\n"),
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            type: MessageV2.Event.Updated.type,
            properties: { sessionID: mainPM.sessionID, info: message },
          },
        })

        const updated = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              if (requirements?.status !== "done") return undefined
              if (!implementation || (implementation.status === "pending" && implementation.session.length === 0)) return undefined
              return info
            }),
          ),
          "milestone_status approval alias did not close requirements and dispatch downstream work",
          "20 seconds",
        )

        expect(updated.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const command = journal.find((row) => row.action === "milestone_status" && row.milestoneID === "requirements")
        expect(command?.outcome).toBe("applied")
        expect(command?.to?.milestoneStatus).toBe("done")
      }),
    45_000,
  )

  it.instance(
    "syncs queued requester interventions from workflow state even when workflow time is unchanged",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const bus = yield* Bus.Service
        const started = yield* workflow.start({
          prompt: ["State-only intervention workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
        })
        const graph = yield* workflow.graph(started.id)
        const mainPM = graph.members.find((member) => member.role === "main_pm")
        if (!started.rootSessionID || !mainPM) throw new Error("Expected requester and main PM sessions")
        const statePath = path.join(instance.directory, started.path, "workflow-state.json")
        const state = JSON.parse(readFileSync(statePath, "utf8"))
        state.interventions = [
          {
            id: "intervention_state_only_direct_override",
            workflowID: started.id,
            fromSessionID: started.rootSessionID,
            targetSessionID: mainPM.sessionID,
            targetRole: "main_pm",
            timing: "temporary-interrupt",
            message: "忽视planning状态，直接继续到执行者",
            path: path.join(started.path, "interventions", "intervention_state_only_direct_override.md"),
            status: "queued",
            time: { created: state.workflow.time.updated, updated: state.workflow.time.updated },
          },
        ]
        writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`)
        yield* bus.publish(FileWatcher.Event.Updated, {
          file: statePath,
          event: "change",
        })

        const synced = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.interventions.some((item) => item.id === "intervention_state_only_direct_override") ? info : undefined,
            ),
          ),
          "state-only queued intervention was not imported from workflow-state.json",
          "12 seconds",
        )
        expect(synced.interventions.find((item) => item.id === "intervention_state_only_direct_override")?.status).toBe("queued")
      }),
    20_000,
  )

  it.instance(
    "closes completed gate plan files through the workflow file watcher and dispatches dependents",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const bus = yield* Bus.Service
        const started = yield* workflow.start({
          prompt: ["Completed gate plan watcher workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const graph = yield* workflow.graph(started.id)
        const departmentPM = graph.members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected department PM session")
        const requirementsID = WorkflowMilestoneID.make("requirements")
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: 1,
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )
        const plan = path.join(instance.directory, started.path, "requirements", "plan.md")
        mkdirSync(path.dirname(plan), { recursive: true })
        writeFileSync(
          plan,
          [
            "All requirements-milestone work is complete. The workflow runtime is processing my queued `milestone_status=done` + `resume` commands asynchronously.",
            "",
            "## Handoff Summary",
            "- completed: requirements charter and downstream audit handoff.",
            "- next: dispatch dependent implementation milestones.",
          ].join("\n"),
        )
        yield* bus.publish(FileWatcher.Event.Updated, {
          file: plan,
          event: "change",
        })

        const updated = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              if (requirements?.status !== "done") return undefined
              if (!implementation?.session.some((ref) => ref.role === "department_pm")) return undefined
              return info
            }),
          ),
          "completed gate plan file did not close requirements and dispatch implementation",
          "12 seconds",
        )
        expect(updated.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("done")
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const command = journal.find((row) => row.action === "force_complete" && row.milestoneID === "requirements")
        expect(command?.outcome).toBe("applied")
        expect(command?.source?.agent).toBe("workflow-file-watcher")
      }),
    20_000,
  )

  it.instance(
    "retries orphaned active milestone sessions before blocking the workflow",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const session = yield* Session.Service
        const started = yield* workflow.start({
          prompt: ["Orphaned milestone watchdog workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        const initialGraph = yield* workflow.graph(started.id)
        const departmentPM = initialGraph.members.find((member) => member.role === "department_pm")
        const executor = initialGraph.members.find((member) => member.role === "executor")
        const requirements = initialGraph.milestones.find((milestone) => milestone.id === "requirements")
        if (!departmentPM) throw new Error("Expected a department PM session")
        if (!executor) throw new Error("Expected an executor session")
        if (!requirements) throw new Error("Expected requirements milestone")
        for (const delay of [0, 20, 40]) {
          if (delay > 0) yield* Effect.sleep(`${delay} millis`)
          for (const job of (yield* background.list()).filter((job) => job.metadata?.workflowID === started.id)) {
            yield* background.cancel(job.id).pipe(Effect.ignore)
          }
        }
        yield* pollWithTimeout(
          background.list().pipe(
            Effect.map((jobs) =>
              jobs.some((job) => job.status === "running" && job.metadata?.workflowID === started.id) ? undefined : true,
            ),
          ),
          "workflow jobs did not stop before orphan watchdog setup",
          "5 seconds",
        )

        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: 1,
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirements.id,
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirements.id)))
            .run(),
        )
        Database.use((db) =>
          db
            .update(WorkflowTable)
            .set({ status: "executing", error: "", time_updated: Date.now() })
            .where(eq(WorkflowTable.id, started.id))
            .run(),
        )
        for (const job of (yield* background.list()).filter(
          (job) =>
            job.status === "running" && job.metadata?.workflowID === started.id,
        )) {
          yield* background.cancel(job.id).pipe(Effect.ignore)
        }
        yield* pollWithTimeout(
          background.list().pipe(
            Effect.map((jobs) =>
              jobs.some((job) => job.status === "running" && job.metadata?.workflowID === started.id) ? undefined : true,
            ),
          ),
          "workflow jobs did not stop after orphan watchdog setup",
          "5 seconds",
        )

        const statusCommandID = Bus.createID()
        const statusResultPromise = waitForWorkflowCommandResult(statusCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: statusCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: statusCommandID,
              action: "status",
              workflowID: started.id,
              sourceSessionID: departmentPM.sessionID,
              message: "refresh status and run active milestone watchdog",
            },
          },
        })
        const statusResult = yield* Effect.promise(() => statusResultPromise)
        expect(statusResult?.applied).toBe(true)

        const retried = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              return info.workflow.status !== "blocked" &&
                requirements &&
                requirements.attempt >= 2 &&
                requirements.status !== "blocked"
                ? info
                : undefined
            }),
          ),
          "orphaned active milestone was not retried by the workflow watchdog",
          "10 seconds",
        )
        expect(retried.workflow.error ?? "").not.toContain("Workflow watchdog")
        const retryNotice = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.interventions.find((intervention) =>
                intervention.message.includes("Workflow watchdog found orphaned active milestone job(s) and queued retry"),
              ) ?? undefined,
            ),
          ),
          "main PM graph did not receive orphan retry warning",
          "10 seconds",
        )
        expect(retryNotice.targetRole).toBe("main_pm")
        expect(retryNotice.status).toBe("delivered")

        for (const job of (yield* background.list()).filter((job) => job.metadata?.workflowID === started.id)) {
          yield* background.cancel(job.id).pipe(Effect.ignore)
        }
        yield* pollWithTimeout(
          background.list().pipe(
            Effect.map((jobs) =>
              jobs.some((job) => job.status === "running" && job.metadata?.workflowID === started.id) ? undefined : true,
            ),
          ),
          "workflow jobs did not stop before exhausted orphan watchdog setup",
          "5 seconds",
        )
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "executing",
              attempt: 3,
              session: [
                {
                  role: "executor",
                  sessionID: executor.sessionID,
                  milestoneID: requirements.id,
                  attempt: 3,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirements.id)))
            .run(),
        )
        Database.use((db) =>
          db
            .update(WorkflowTable)
            .set({ status: "executing", error: "", time_updated: Date.now() })
            .where(eq(WorkflowTable.id, started.id))
            .run(),
        )

        const blockedCommandID = Bus.createID()
        const blockedResultPromise = waitForWorkflowCommandResult(blockedCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: blockedCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: blockedCommandID,
              action: "status",
              workflowID: started.id,
              sourceSessionID: departmentPM.sessionID,
              message: "refresh status and block exhausted orphan milestone",
            },
          },
        })
        const blockedResult = yield* Effect.promise(() => blockedResultPromise)
        expect(blockedResult?.applied).toBe(true)

        const blocked = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              return info.workflow.status === "blocked" &&
                requirements?.status === "blocked" &&
                (info.workflow.error ?? "").includes("Workflow watchdog detected orphaned active milestone job(s)")
                ? info
                : undefined
            }),
          ),
          "exhausted orphaned active milestone did not block the workflow",
          "10 seconds",
        )
        expect(blocked.workflow.error ?? "").toContain("scheduler/runtime dispatch problem")
        const blockedNotice = blocked.interventions.find((intervention) =>
          intervention.message.includes("Workflow watchdog detected orphaned active milestone job(s)"),
        )
        expect(blockedNotice?.targetRole).toBe("main_pm")
        expect(blockedNotice?.status).toBe("delivered")
      }),
    35_000,
  )

  it.instance(
    "blocks dependent milestones after a failed dependency and resumes them after force_skip",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: ["Failed dependency workflow", failedDependencyWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 2,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const requirements = (yield* workflow.graph(started.id)).milestones.find((milestone) => milestone.id === "requirements")
        if (!requirements) throw new Error("Expected requirements milestone")
        for (const delay of [0, 20, 40]) {
          if (delay > 0) yield* Effect.sleep(`${delay} millis`)
          for (const job of (yield* background.list()).filter((job) => job.metadata?.workflowID === started.id)) {
            yield* background.cancel(job.id).pipe(Effect.ignore)
          }
        }
        yield* pollWithTimeout(
          background.list().pipe(
            Effect.map((jobs) =>
              jobs.some((job) => job.status === "running" && job.metadata?.workflowID === started.id) ? undefined : true,
            ),
          ),
          "workflow jobs did not stop before failed dependency setup",
          "5 seconds",
        )
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({ status: "executing", attempt: 1, time_updated: Date.now() })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirements.id)))
            .run(),
        )
        Database.use((db) =>
          db
            .update(WorkflowTable)
            .set({ status: "executing", error: "", time_updated: Date.now() })
            .where(eq(WorkflowTable.id, started.id))
            .run(),
        )

        const failCommandID = Bus.createID()
        const failResultPromise = waitForWorkflowCommandResult(failCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: failCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: failCommandID,
              action: "milestone_status",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              milestoneID: requirements.id,
              milestoneStatus: "failed",
              message: "upstream requirements gate failed and should block dependents",
            },
          },
        })
        const failResult = yield* Effect.promise(() => failResultPromise)
        expect(failResult?.applied).toBe(true)

        const blocked = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              const verification = info.milestones.find((milestone) => milestone.id === "verification")
              const report = info.interventions.find(
                (intervention) =>
                  intervention.targetRole === "main_pm" &&
                  intervention.message.includes("Milestones blocked by failed dependencies"),
              )
              return info.workflow.status === "blocked" &&
                implementation?.status === "blocked" &&
                verification?.status === "blocked" &&
                report
                ? info
                : undefined
            }),
          ),
          "failed dependency did not block dependent milestones",
          "10 seconds",
        )
        expect(blocked.workflow.error).toContain("Milestones blocked by failed dependencies")
        const dependencyReport = blocked.interventions.find((intervention) =>
          intervention.message.includes("Milestones blocked by failed dependencies"),
        )
        expect(dependencyReport?.status).toBe("delivered")
        expect(dependencyReport?.targetRole).toBe("main_pm")
        expect(dependencyReport?.message).toContain("implementation, verification")

        const skipCommandID = Bus.createID()
        const skipResultPromise = waitForWorkflowCommandResult(skipCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: skipCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: skipCommandID,
              action: "force_skip",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              milestoneID: requirements.id,
              message: "requester accepts skipping the failed requirements gate so downstream work can continue",
            },
          },
        })
        expect((yield* Effect.promise(() => skipResultPromise))?.applied).toBe(true)

        const resumed = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              const verification = info.milestones.find((milestone) => milestone.id === "verification")
              const ready =
                implementation?.session.some((ref) => ref.role === "department_pm") &&
                verification?.session.some((ref) => ref.role === "department_pm") &&
                implementation.status !== "blocked" &&
                verification.status !== "blocked"
              return ready ? info : undefined
            }),
          ),
          "force_skip did not resume dependent milestones",
          "12 seconds",
        )
        expect(resumed.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("skipped")
      }),
    25_000,
  )

  it.instance(
    "blocks a milestone after repeated missing required handoff output",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "1"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Attempt limit workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })

        const blocked = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const report = info.interventions.find(
                (intervention) =>
                  intervention.targetRole === "main_pm" &&
                  intervention.message.includes("exceeded 3 attempts") &&
                  intervention.message.includes("without required plan Handoff Summary"),
              )
              return info.workflow.status === "blocked" &&
                requirements?.status === "blocked" &&
                requirements.attempt >= 3 &&
                (info.workflow.error ?? "").includes("exceeded 3 attempts") &&
                report
                ? info
                : undefined
            }),
          ),
          "milestone did not block after repeated missing handoff output",
          "20 seconds",
        )
        expect(blocked.workflow.error).toContain("without required plan Handoff Summary")
        const attemptReport = blocked.interventions.find((intervention) =>
          intervention.message.includes("without required plan Handoff Summary"),
        )
        expect(attemptReport?.status).toBe("delivered")
        expect(attemptReport?.targetRole).toBe("main_pm")
      }),
    25_000,
  )

  it.instance(
    "serializes conflicting workflow tool commands for the same workflow",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Conflicting command workflow", commandBusWorkflowXml].join("\n"),
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const skipCommandID = Bus.createID()
        const cancelCommandID = Bus.createID()
        const skipResultPromise = waitForWorkflowCommandResult(skipCommandID)
        const cancelResultPromise = waitForWorkflowCommandResult(cancelCommandID)
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: skipCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: skipCommandID,
              action: "milestone_status",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              milestoneID: "requirements",
              milestoneStatus: "skipped",
              message: "first command force-skips requirements",
            },
          },
        })
        GlobalBus.emit("event", {
          directory: instance.directory,
          payload: {
            id: cancelCommandID,
            type: WorkflowToolCommandEvent.type,
            properties: {
              id: cancelCommandID,
              action: "milestone_status",
              workflowID: started.id,
              sourceSessionID: started.rootSessionID,
              milestoneID: "requirements",
              milestoneStatus: "cancelled",
              message: "second command must see the terminal state",
            },
          },
        })

        const skipResult = yield* Effect.promise(() => skipResultPromise)
        const cancelResult = yield* Effect.promise(() => cancelResultPromise)
        const results = [
          { id: skipCommandID, target: "skipped", result: skipResult },
          { id: cancelCommandID, target: "cancelled", result: cancelResult },
        ]
        expect(results.filter((item) => item.result?.applied).length).toBe(1)
        expect(results.filter((item) => item.result?.rejection?.code === "illegal_transition").length).toBe(1)
        const applied = results.find((item) => item.result?.applied)
        const appliedTarget = applied?.target
        if (appliedTarget !== "skipped" && appliedTarget !== "cancelled") throw new Error("Expected one command target")
        const graph = yield* workflow.graph(started.id)
        expect(graph.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe(appliedTarget)
        const journal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        expect(results.map((item) => journal.find((row) => row.id === item.id)?.outcome).toSorted()).toEqual([
          "applied",
          "rejected",
        ])
      }),
    20_000,
  )

  it.instance(
    "automatically advances through PM, execution, review, testing, technical assessment, and acceptance",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Ship a small company-style workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "workflow did not finish",
          "45 seconds",
        )

        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected workflow to complete, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        expect(completed.status).toBe("completed")

        expect(graph.milestones.map((milestone) => milestone.status)).toEqual(["done", "done", "done"])
        expect(graph.members.map((member) => member.role).toSorted()).toEqual([
          "department_pm",
          "executor",
          "expert",
          "main_pm",
          "reviewer",
          "tester",
        ])
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "consultation" &&
              edge.to === started.id &&
              edge.question?.includes("matches the strategic request"),
          ),
        ).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "consultation" &&
              edge.summary?.includes("Main PM consulted Tester") &&
              edge.question?.includes("completeness review readiness"),
          ),
        ).toBe(true)
        expect(graph.nodes.map((node) => node.id)).toContain(`${started.id}:technical-assessment`)
        expect(graph.nodes.map((node) => node.id)).toContain(`${started.id}:requester-acceptance`)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "owns" &&
              edge.to === `${started.id}:test-plan` &&
              edge.summary?.includes("Tester owns completeness"),
          ),
        ).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "owns" &&
              edge.to === `${started.id}:technical-assessment` &&
              edge.summary?.includes("Technical advisor owns architecture"),
          ),
        ).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "owns" &&
              edge.to === `${started.id}:main-pm-acceptance` &&
              edge.summary?.includes("Main PM owns product acceptance"),
          ),
        ).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "owns" &&
              edge.from === started.id &&
              edge.to === `${started.id}:requester-acceptance` &&
              edge.summary?.includes("Requester owns final strategic acceptance"),
          ),
        ).toBe(true)
        expect(
          yield* Effect.promise(() =>
            Bun.file(path.join(instance.directory, completed.path, "reference", "index.md")).exists(),
          ),
        ).toBe(true)
        expect(
          yield* Effect.promise(() =>
            Bun.file(path.join(instance.directory, completed.path, "final", "test-plan.md")).exists(),
          ),
        ).toBe(true)
        expect(
          yield* Effect.promise(() =>
            Bun.file(path.join(instance.directory, completed.path, "final", "technical-assessment.md")).exists(),
          ),
        ).toBe(true)
        expect(
          yield* Effect.promise(() =>
            Bun.file(path.join(instance.directory, completed.path, "acceptance", "requester.md")).exists(),
          ),
        ).toBe(true)
        const referenceIndex = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, completed.path, "reference", "index.md")).text(),
        )
        const organizationText = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, completed.path, "organization.md")).text(),
        )
        const progressText = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, completed.path, "progress.md")).text(),
        )
        const requesterMemoryText = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, completed.path, "reference", "requester.md")).text(),
        )
        const deliverySummaryText = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, completed.path, "delivery-summary.md")).text(),
        )
        const standupIndex = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, completed.path, "standups", "index.md")).text(),
        )
        const consultationIndex = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, completed.path, "reference", "consultations", "index.md")).text(),
        )
        const supervisionLine = standupIndex
          .split(/\r?\n/)
          .find((line) => line.includes("main PM supervision:") && line.includes("supervision_"))
        const supervisionPath = supervisionLine?.slice(supervisionLine.indexOf("main PM supervision:") + "main PM supervision:".length).trim()
        if (!supervisionPath) throw new Error("Expected main PM supervision note in standup index")
        const supervisionNote = yield* Effect.promise(() => Bun.file(path.join(instance.directory, supervisionPath)).text())
        const supervisionNode = graph.nodes.find((node) => node.type === "document" && node.path === supervisionPath)
        if (!supervisionNode) throw new Error("Expected main PM supervision note document node")
        const reviewer = graph.members.find((member) => member.role === "reviewer")
        const executor = graph.members.find((member) => member.role === "executor")
        const expert = graph.members.find((member) => member.role === "expert")
        if (!executor) throw new Error("Expected executor staff member")
        if (!expert) throw new Error("Expected technical advisor staff member")
        const sessions = yield* Session.Service
        const executorPrompt = (yield* sessions.messages({ sessionID: executor.sessionID }))
          .filter((message) => message.info.role === "user")
          .flatMap((message) => message.parts)
          .filter((part) => part.type === "text")
          .map((part) => part.text)
          .join("\n")
          .replaceAll("\\", "/")
        expect(executorPrompt).toContain("## Workflow Employee Context")
        expect(executorPrompt).toContain("Company role: Executor")
        expect(executorPrompt).toContain("Staff memory:")
        expect(executorPrompt).toContain("Current milestone:")
        expect(executorPrompt).toContain("## Company Operating Snapshot")
        expect(executorPrompt).toContain("### Employees")
        expect(executorPrompt).toContain("<opencode-workflow-message")
        expect(executorPrompt).toContain("## Workflow File Isolation")
        expect(executorPrompt).toContain("Agent-authored deliverables")
        expect(executorPrompt).toContain("artifacts/")
        const staffMemory = graph.nodes.find(
          (node) =>
            node.type === "document" &&
            node.sessionID === executor.sessionID &&
            node.path?.includes(path.join("reference", "staff")),
        )
        const expertMemory = graph.nodes.find(
          (node) =>
            node.type === "document" &&
            node.sessionID === expert.sessionID &&
            node.path?.includes(path.join("reference", "staff")),
        )
        const advisorNote = graph.nodes.find(
          (node) =>
            node.type === "document" &&
            node.role === "expert" &&
            node.path?.includes("expert-") &&
            !node.path.includes(path.join("reference", "staff")),
        )
        const consultationDoc = graph.nodes.find(
          (node) =>
            node.type === "document" &&
            node.path?.includes(path.join("reference", "consultations")) &&
            node.path.includes("consult_"),
        )
        if (!staffMemory?.path) throw new Error("Expected executor staff memory document")
        if (!expertMemory?.path) throw new Error("Expected technical advisor staff memory document")
        if (!advisorNote?.path) throw new Error("Expected technical advisor note document")
        if (!consultationDoc?.path) throw new Error("Expected consultation archive document")
        const summaries = graph.nodes
          .filter((node) => node.type === "document" && node.path?.includes("reference") && node.path.includes("summary"))
          .map((node) => node.path!)
        expect(referenceIndex).toContain("## Session Summaries")
        expect(referenceIndex).toContain("## Company Operating Documents")
        expect(referenceIndex).toContain("- Company organization: organization.md")
        expect(referenceIndex).toContain("- Workflow progress: progress.md")
        expect(referenceIndex).toContain(`- Main PM plan: ${path.join("planning", "main-plan.md")}`)
        expect(referenceIndex).toContain("- Workflow XML: workflow.xml")
        expect(referenceIndex).toContain("- Delivery summary: delivery-summary.md")
        expect(referenceIndex).toContain("## Strategic Owner Memory")
        expect(referenceIndex).toContain(`Requester memory: ${path.join("reference", "requester.md")}`)
        expect(referenceIndex).toContain("## Staff Memory")
        expect(referenceIndex).toContain(executor.title)
        expect(referenceIndex).toContain("## Technical Advisor Notes")
        expect(referenceIndex).toContain("expert-")
        expect(referenceIndex).toContain("## Consultation History")
        expect(referenceIndex).toContain(`Consultation archive: ${path.join("reference", "consultations", "index.md")}`)
        expect(referenceIndex).toContain(
          `Reviewer: ${path.join("reference", `session_${String(reviewer!.sessionID).replace(/^ses_?/, "")}-summary.md`)}`,
        )
        expect(referenceIndex).toContain("Company standups: standups")
        expect(referenceIndex).toContain("## Main PM Supervision Notes")
        expect(referenceIndex).toContain(supervisionPath)
        expect(referenceIndex).toContain("Delivery summary: delivery-summary.md")
        expect(deliverySummaryText).toContain("## Final State")
        expect(deliverySummaryText).toContain("Status: completed")
        expect(deliverySummaryText).toContain("## Milestone Outcomes")
        expect(deliverySummaryText).toContain("Requester acceptance:")
        expect(graph.nodes.some((node) => node.type === "document" && node.path?.endsWith("delivery-summary.md"))).toBe(true)
        expect(standupIndex).toContain("main PM supervision")
        expect(progressText).toContain("## Recent Main PM Supervision")
        expect(progressText).toContain("main PM supervision:")
        expect(progressText).toContain(path.join("standups", "supervision_"))
        expect(organizationText).toContain("## Staffing Limits")
        expect(organizationText).toContain("Sessions are long-lived company members")
        expect(requesterMemoryText).toContain("## Strategic Direction")
        expect(requesterMemoryText).toContain("Ship a small company-style workflow")
        expect(requesterMemoryText).toContain("## Recent Requester Interventions")
        expect(requesterMemoryText).toContain("## Recent Main PM Supervision")
        expect(requesterMemoryText).toContain("to-role=\"requester\"")
        expect(consultationIndex).toContain("Consultation Archive")
        expect(consultationIndex).toContain("consult_")
        expect(supervisionNote).toContain("## Triggering Progress Update")
        expect(supervisionNote).toContain("## Main PM Response")
        expect(graph.edges.some((edge) => edge.kind === "consultation")).toBe(true)
        expect(
          graph.nodes.some(
            (node) =>
              node.type === "document" &&
              node.role === "requester" &&
              node.path?.includes(path.join("reference", "requester.md")),
          ),
        ).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "requester memory" &&
              edge.to === `${started.id}:requester-memory`,
          ),
        ).toBe(true)
        expect(graph.nodes.some((node) => node.type === "document" && node.id === `${started.id}:organization`)).toBe(true)
        expect(graph.nodes.some((node) => node.type === "document" && node.id === `${started.id}:progress`)).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "operations" &&
              edge.from === `${started.id}:reference` &&
              edge.to === `${started.id}:organization`,
          ),
        ).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "progress" &&
              edge.from === started.id &&
              edge.to === `${started.id}:progress`,
          ),
        ).toBe(true)
        expect(
          graph.edges.some((edge) => edge.kind === "document" && edge.label === "staff memory" && edge.to === staffMemory.id),
        ).toBe(true)
        expect(
          graph.edges.some((edge) => edge.kind === "document" && edge.label === "advisor note" && edge.to === advisorNote.id),
        ).toBe(true)
        expect(
          graph.edges.some((edge) => edge.kind === "document" && edge.label === "consultation" && edge.to === consultationDoc.id),
        ).toBe(true)
        expect(
          graph.edges.some((edge) => edge.kind === "document" && edge.label === "asked" && edge.to === consultationDoc.id),
        ).toBe(true)
        expect(
          graph.edges.some((edge) => edge.kind === "document" && edge.label === "answered by" && edge.from === consultationDoc.id),
        ).toBe(true)
        expect(supervisionNode.role).toBe("main_pm")
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "supervision note" &&
              edge.from === `${started.id}:standups` &&
              edge.to === supervisionNode.id,
          ),
        ).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "supervises" &&
              edge.to === supervisionNode.id &&
              edge.summary?.includes("Main PM supervision"),
          ),
        ).toBe(true)
        expect(yield* Effect.promise(() => Bun.file(path.join(instance.directory, advisorNote.path!)).text())).toContain(
          "Advisor note",
        )
        const consultationDocText = yield* Effect.promise(() => Bun.file(path.join(instance.directory, consultationDoc.path!)).text())
        expect(consultationDocText).toContain("## Question")
        expect(consultationDocText).toContain("## Response")
        expect(consultationDocText).toContain("## Reuse Notes")
        const staffMemoryText = yield* Effect.promise(() => Bun.file(path.join(instance.directory, staffMemory.path!)).text())
        const expertMemoryText = yield* Effect.promise(() => Bun.file(path.join(instance.directory, expertMemory.path!)).text())
        expect(staffMemoryText).toContain("## Current/Previous Assignments")
        expect(staffMemoryText).toContain("## Employee Profile")
        expect(staffMemoryText).toContain("Role responsibility: Implement assigned milestone work")
        expect(staffMemoryText).toContain("Stable consultation specialty:")
        expect(staffMemoryText).toContain("Direct consultation handle:")
        expect(staffMemoryText).toContain("## Active Responsibilities")
        expect(staffMemoryText).toContain("## Latest Handoff Memory")
        expect(staffMemoryText).toContain("- completed:")
        expect(staffMemoryText).toContain("## Recent Main PM Supervision")
        expect(staffMemoryText).toContain("main PM supervision:")
        expect(staffMemoryText).toContain("## Consultation History")
        expect(staffMemoryText).toContain("## Consultation Capabilities")
        expect(staffMemoryText).toContain("timing=\"temporary-interrupt\"")
        expect(staffMemoryText).toContain("Consult this employee directly")
        expect(staffMemoryText).toContain("Read this staff memory before assigning similar work")
        expect(expertMemoryText).toContain("## Technical Advisor Notes")
        expect(expertMemoryText).toContain("Role responsibility: Advise on architecture")
        expect(expertMemoryText).toContain("expert-")
        expect(summaries.length).toBeGreaterThan(0)
        expect(
          yield* Effect.promise(() => Bun.file(path.join(instance.directory, summaries[0]!)).text()),
        ).toContain("- completed:")
      }),
    60_000,
  )

  it.instance(
    "reopens targeted milestone work when tester feedback fails completeness review",
    () =>
      Effect.gen(function* () {
        failTesterOnce.delete("Reopen once workflow")
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Reopen once workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "workflow did not finish after tester feedback reopen",
          "70 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected workflow to complete after tester reopen, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const byID = new Map(graph.milestones.map((milestone) => [String(milestone.id), milestone]))
        expect(byID.get("requirements")?.attempt).toBe(1)
        expect(byID.get("implementation")?.attempt).toBeGreaterThan(1)
        expect(byID.get("verification")?.attempt).toBeGreaterThan(1)
        expect(graph.milestones.every((milestone) => milestone.status === "done")).toBe(true)
      }),
    90_000,
  )

  it.instance(
    "reuses an active requester workflow but allows a new workflow after completion",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Reusable requester workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const reused = yield* workflow.start({
          sessionID: started.rootSessionID,
          prompt: "This should not create a duplicate active workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
        })

        expect(reused.id).toBe(started.id)

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "reusable requester workflow did not finish",
          "35 seconds",
        )
        if (completed.status !== "completed") {
          throw new Error(`Expected reusable requester workflow to complete, got ${completed.status}: ${completed.error ?? "no error"}`)
        }

        const next = yield* workflow.start({
          sessionID: completed.rootSessionID!,
          prompt: "Second requester workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        expect(next.id).not.toBe(completed.id)
        expect(next.rootSessionID).toBe(completed.rootSessionID)
        expect(next.request).toBe("Second requester workflow")
        yield* workflow.cancel(next.id)

        const afterCancel = yield* workflow.start({
          sessionID: completed.rootSessionID!,
          prompt: "Third requester workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        expect(afterCancel.id).not.toBe(next.id)
        expect(afterCancel.rootSessionID).toBe(completed.rootSessionID)
        expect(afterCancel.request).toBe("Third requester workflow")
        yield* workflow.cancel(afterCancel.id)
      }),
    45_000,
  )

  it.instance(
    "drains waiting parallel milestones after executor assignment frees the department PM",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Parallel drain workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 2,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        yield* workflow.updateXml({
          workflowID: started.id,
          xml: [
            "<workflow>",
            "  <parallel>",
            '    <milestone id="parallel-a" title="Parallel A" department="engineering">First parallel milestone.</milestone>',
            '    <milestone id="parallel-b" title="Parallel B" department="engineering">Second parallel milestone.</milestone>',
            "  </parallel>",
            "</workflow>",
          ].join("\n"),
        })
        yield* workflow.resume(started.id)

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const first = info.milestones.find((milestone) => milestone.id === "parallel-a")
              const second = info.milestones.find((milestone) => milestone.id === "parallel-b")
              return first?.status === "executing" &&
                first.session.some((ref) => ref.role === "executor") &&
                second?.session.some((ref) => ref.role === "department_pm")
                ? info
                : undefined
            }),
          ),
          "parallel waiting milestone did not dispatch while the first executor was still running",
          "25 seconds",
        )

        expect(graph.milestones.find((milestone) => milestone.id === "parallel-b")?.waitingFor).toBeUndefined()
      }),
    35_000,
  )

  it.instance(
    "queues parallel milestones when company staffing has only one department PM",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Parallel staffing workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 2,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        yield* workflow.updateXml({
          workflowID: started.id,
          xml: [
            "<workflow>",
            "  <parallel>",
            '    <milestone id="parallel-a" title="Parallel A" department="engineering">First parallel milestone.</milestone>',
            '    <milestone id="parallel-b" title="Parallel B" department="engineering">Second parallel milestone.</milestone>',
            "  </parallel>",
            "</workflow>",
          ].join("\n"),
        })
        yield* workflow.resume(started.id)

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "parallel staffing workflow did not finish",
          "75 seconds",
        ).pipe(
          Effect.catch(() =>
            Effect.gen(function* () {
              const graph = yield* workflow.graph(started.id)
              throw new Error(
                [
                  "parallel staffing workflow did not finish",
                  `workflow=${graph.workflow.status}:${graph.workflow.error ?? "no error"}`,
                  ...graph.milestones.map((milestone) =>
                    `${milestone.id}:${milestone.status}:attempt${milestone.attempt}:waiting=${milestone.waitingFor ?? "none"}:sessions=${milestone.session.map((ref) => `${ref.role}:${ref.attempt ?? "?"}`).join(",") || "none"}`,
                  ),
                ].join("\n"),
              )
            }),
          ),
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected parallel staffing workflow to complete, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const departmentPMs = graph.members.filter((member) => member.role === "department_pm")
        const assignedPMs = new Set(
          graph.milestones.flatMap((milestone) =>
            milestone.session.filter((ref) => ref.role === "department_pm").map((ref) => ref.sessionID),
          ),
        )

        expect(graph.milestones.map((milestone) => milestone.status)).toEqual(["done", "done"])
        expect(departmentPMs).toHaveLength(1)
        expect(assignedPMs.size).toBe(1)
      }),
    90_000,
  )

  it.instance(
    "stops using extra employees after staffing limits are reduced",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Reduced staffing workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 1,
            tester: 1,
            expert: 2,
          },
        })
        yield* workflow.updateStaffing({
          workflowID: started.id,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        yield* workflow.updateXml({
          workflowID: started.id,
          xml: [
            "<workflow>",
            "  <ordered>",
            '    <milestone id="one" title="One" department="engineering">First serial milestone.</milestone>',
            '    <milestone id="two" title="Two" department="engineering">Second serial milestone.</milestone>',
            "  </ordered>",
            "</workflow>",
          ].join("\n"),
        })
        yield* workflow.resume(started.id)

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "reduced staffing workflow did not finish",
          "45 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected reduced staffing workflow to complete, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const executors = graph.members.filter((member) => member.role === "executor")
        const assignedExecutors = new Set(
          graph.milestones.flatMap((milestone) =>
            milestone.session.filter((ref) => ref.role === "executor").map((ref) => ref.sessionID),
          ),
        )

        expect(executors).toHaveLength(2)
        expect(executors[0]!.status).toBe("active")
        expect(executors[1]!.status).toBe("paused")
        expect(assignedExecutors.size).toBe(1)
        expect(assignedExecutors.has(executors[0]!.sessionID)).toBe(true)
        expect(assignedExecutors.has(executors[1]!.sessionID)).toBe(false)
        expect(yield* Effect.promise(() => Bun.file(path.join(instance.directory, completed.path, "organization.md")).text())).toContain(
          "  - status: paused",
        )
      }),
    55_000,
  )

  it.instance(
    "injects workflow-triggered peer handoff between executor employees",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: "Executor peer sync workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 1,
            tester: 1,
            expert: 2,
          },
        })
        yield* workflow.updateXml({
          workflowID: started.id,
          xml: [
            "<workflow>",
            "  <ordered>",
            "    <parallel>",
            '      <milestone id="alpha" title="Alpha API slice" department="engineering">Implement the first executor-owned API slice.</milestone>',
            '      <milestone id="beta" title="Beta UI slice" department="engineering">Implement the second executor-owned UI slice.</milestone>',
            "    </parallel>",
            '    <milestone id="integration" title="Integration handoff" department="engineering">Integrate the alpha and beta slices using peer handoff context.</milestone>',
            "  </ordered>",
            "</workflow>",
          ].join("\n"),
        })
        yield* workflow.resume(started.id)

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "executor peer sync workflow did not finish",
          "55 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected executor peer sync workflow to complete, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const peerEdge = graph.edges.find(
          (edge) =>
            edge.kind === "consultation" &&
            edge.summary?.includes("Executor consulted Executor") &&
            edge.question?.includes("Workflow-triggered executor peer sync before milestone integration"),
        )
        const integrationExecutor = graph.milestones
          .find((milestone) => String(milestone.id) === "integration")
          ?.session.find((ref) => ref.role === "executor")
        if (!integrationExecutor) throw new Error("Expected integration executor assignment")

        const integrationPrompt = (yield* sessions.messages({ sessionID: integrationExecutor.sessionID }))
          .filter((message) => message.info.role === "user")
          .flatMap((message) => message.parts)
          .filter((part) => part.type === "text")
          .map((part) => part.text)
          .join("\n")

        expect(peerEdge?.summary).toContain("workflow-triggered executor peer technical sync")
        expect(peerEdge?.answer).toContain("Peer handoff: prior milestone is complete")
        expect(integrationPrompt).toContain("Workflow-triggered executor peer sync:")
        expect(integrationPrompt).toContain("Peer handoff: prior milestone is complete")
        const integrationStaffMemory = graph.nodes.find(
          (node) =>
            node.type === "document" &&
            node.sessionID === integrationExecutor.sessionID &&
            node.path?.includes(path.join("reference", "staff")),
        )
        if (!integrationStaffMemory?.path) throw new Error("Expected integration executor staff memory")
        const integrationStaffMemoryText = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, integrationStaffMemory.path!)).text(),
        )
        expect(integrationStaffMemoryText).toContain("## Peer Executor Handoffs")
        expect(integrationStaffMemoryText).toContain("workflow-triggered executor peer technical sync")
        expect(integrationStaffMemoryText).toContain("Peer handoff: prior milestone is complete")
        expect(
          (yield* Effect.promise(() =>
            Bun.file(path.join(instance.directory, completed.path, "reference", "index.md")).text(),
          )).includes("workflow-triggered executor peer technical sync"),
        ).toBe(true)
      }),
    65_000,
  )

  it.instance(
    "processes consultation escalation emitted by a consulted employee",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Nested consult workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "nested consultation workflow did not finish",
          "45 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected nested consultation workflow to complete, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const nestedEdge = graph.edges.find(
          (edge) =>
            edge.kind === "consultation" &&
            edge.summary?.includes("Technical Advisor consulted Main PM") &&
            edge.question?.includes("nested technical escalation"),
        )
        if (!nestedEdge) throw new Error("Expected technical advisor to escalate consultation to Main PM")

        const nestedDoc = graph.nodes.find(
          (node) =>
            node.type === "document" &&
            node.title.includes("Technical Advisor to Main PM consultation") &&
            node.path?.includes(path.join("reference", "consultations")),
        )
        if (!nestedDoc?.path) throw new Error("Expected nested consultation document")

        const consultationIndex = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, completed.path, "reference", "consultations", "index.md")).text(),
        )
        const consultationDoc = yield* Effect.promise(() => Bun.file(path.join(instance.directory, nestedDoc.path!)).text())

        expect(nestedEdge.answer).toContain("PM alignment confirmed")
        expect(consultationIndex).toContain("nested scope escalation")
        expect(consultationDoc).toContain("nested technical escalation")
        expect(consultationDoc).toContain("PM alignment confirmed")
      }),
    55_000,
  )

  it.instance(
    "applies workflow control emitted by a consulted main PM",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "PM consult control workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const blocked = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (info.status === "blocked" ? info : undefined)),
          ),
          "workflow did not block from consulted main PM control",
          "35 seconds",
        )
        const graph = yield* workflow.graph(started.id)

        expect(blocked.error).toContain("consultation response to Executor")
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "consultation" &&
              edge.summary?.includes("Executor consulted Main PM") &&
              edge.question?.includes("Should this workflow pause for PM control"),
          ),
        ).toBe(true)
        expect(graph.milestones.some((milestone) => milestone.status === "blocked")).toBe(true)
      }),
    45_000,
  )

  it.instance(
    "reopens targeted milestone work when technical advisor fails architecture review",
    () =>
      Effect.gen(function* () {
        failTechnicalOnce.delete("Technical reopen workflow")
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Technical reopen workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "workflow did not finish after technical advisor reopen",
          "45 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected workflow to complete after technical reopen, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const byID = new Map(graph.milestones.map((milestone) => [String(milestone.id), milestone]))
        expect(byID.get("requirements")?.attempt).toBe(1)
        expect(byID.get("implementation")?.attempt).toBeGreaterThan(1)
        expect(byID.get("verification")?.attempt).toBeGreaterThan(1)
        expect(graph.nodes.map((node) => node.id)).toContain(`${started.id}:technical-assessment`)
        expect(completed.status).toBe("completed")
      }),
    55_000,
  )

  it.instance(
    "reopens targeted milestone work when main PM rejects final acceptance",
    () =>
      Effect.gen(function* () {
        failMainPmAcceptanceOnce.delete("Main PM acceptance reopen workflow")
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Main PM acceptance reopen workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "workflow did not finish after main PM acceptance reopen",
          "70 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected workflow to complete after main PM acceptance reopen, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const byID = new Map(graph.milestones.map((milestone) => [String(milestone.id), milestone]))
        expect(byID.get("requirements")?.attempt).toBe(1)
        expect(byID.get("implementation")?.attempt).toBeGreaterThan(1)
        expect(byID.get("verification")?.attempt).toBeGreaterThan(1)
        expect(graph.milestones.every((milestone) => milestone.status === "done")).toBe(true)
      }),
    85_000,
  )

  it.instance(
    "reopens targeted milestone work when requester rejects final acceptance",
    () =>
      Effect.gen(function* () {
        failRequesterAcceptanceOnce.delete("Requester acceptance reopen workflow")
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Requester acceptance reopen workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "workflow did not finish after requester acceptance reopen",
          "70 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected workflow to complete after requester acceptance reopen, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const byID = new Map(graph.milestones.map((milestone) => [String(milestone.id), milestone]))
        expect(byID.get("requirements")?.attempt).toBe(1)
        expect(byID.get("implementation")?.attempt).toBeGreaterThan(1)
        expect(byID.get("verification")?.attempt).toBeGreaterThan(1)
        expect(graph.milestones.every((milestone) => milestone.status === "done")).toBe(true)
      }),
    85_000,
  )

  it.instance(
    "lets a real requester message provide final acceptance",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: "Manual requester final acceptance workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const blocked = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) =>
              info.status === "blocked" && info.error?.includes("Requester did not provide a final acceptance decision")
                ? info
                : undefined,
            ),
          ),
          "workflow did not wait for real requester final acceptance",
          "35 seconds",
        )

        const messageID = MessageID.ascending()
        const now = Date.now()
        yield* sessions.updateMessage({
          id: messageID,
          sessionID: started.rootSessionID,
          role: "user",
          time: { created: now },
          agent: "build",
          model: { providerID: testProviderID, modelID: testModelID },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: started.rootSessionID,
          messageID,
          type: "text",
          text: [
            "Requester reviewed the delivery and approves it.",
            '<opencode-workflow-acceptance role="requester" decision="approve">human requester approves final delivery</opencode-workflow-acceptance>',
            '<opencode-workflow-complete status="complete">human requester accepts final workflow completion</opencode-workflow-complete>',
          ].join("\n"),
          time: { start: now, end: now },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(Effect.map((info) => (info.status === "completed" ? info : undefined))),
          "real requester final acceptance did not complete the workflow",
          "20 seconds",
        )
        const acceptance = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, blocked.path, "acceptance", "requester.md")).text(),
        )
        const graph = yield* workflow.graph(started.id)

        expect(completed.status).toBe("completed")
        expect(acceptance).toContain("human requester approves final delivery")
        expect(graph.milestones.every((milestone) => milestone.status === "done")).toBe(true)
        expect(
          graph.interventions.some((intervention) => intervention.message.includes("human requester approves final delivery")),
        ).toBe(false)
      }),
    60_000,
  )

  it.instance(
    "lets a real requester message reject final acceptance and reopen work",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: "Manual requester reject workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const blocked = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) =>
              info.status === "blocked" && info.error?.includes("Requester did not provide a final acceptance decision")
                ? info
                : undefined,
            ),
          ),
          "workflow did not wait for real requester final reject",
          "35 seconds",
        )

        const messageID = MessageID.ascending()
        const now = Date.now()
        yield* sessions.updateMessage({
          id: messageID,
          sessionID: started.rootSessionID,
          role: "user",
          time: { created: now },
          agent: "build",
          model: { providerID: testProviderID, modelID: testModelID },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: started.rootSessionID,
          messageID,
          type: "text",
          text: [
            "Requester reviewed the delivery and rejects the implementation milestone.",
            '<opencode-workflow-acceptance role="requester" decision="reject" milestones="implementation">human requester says implementation misses the strategic workflow outcome</opencode-workflow-acceptance>',
          ].join("\n"),
          time: { start: now, end: now },
        })

        const reopened = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const byID = new Map(info.milestones.map((milestone) => [String(milestone.id), milestone]))
              const verification = byID.get("verification")
              return (byID.get("implementation")?.attempt ?? 0) > 1 &&
                (verification?.status === "pending" || (verification?.attempt ?? 0) > 1)
                ? { info, byID }
                : undefined
            }),
          ),
          "real requester final reject did not reopen implementation",
          "30 seconds",
        )
        const acceptance = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, blocked.path, "acceptance", "requester.md")).text(),
        )

        expect(acceptance).toContain("human requester says implementation misses")
        expect(reopened.byID.get("requirements")?.attempt).toBe(1)
        expect(reopened.byID.get("implementation")?.attempt).toBeGreaterThan(1)
        const verification = reopened.byID.get("verification")
        if (!verification) throw new Error("Expected verification milestone")
        expect(["pending", "planning", "executing", "reviewing", "approved", "testing", "done"]).toContain(verification.status)
        expect(
          reopened.info.interventions.some((intervention) => intervention.message.includes("human requester says implementation")),
        ).toBe(false)
      }),
    70_000,
  )

  it.instance(
    "records requester interrupt intervention and lets main PM keep workflow blocked for clarification",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Interruptible workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        yield* workflow.intervene({
          workflowID: started.id,
          timing: "interrupt",
          targetRole: "main_pm",
          message: "pause for requester decision: change strategic direction before continuing",
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.interventions.some(
                (item) => item.message.includes("pause for requester decision") && item.status === "blocked",
              )
                ? info
                : undefined,
            ),
          ),
          "workflow intervention was not delivered and blocked by main PM",
          "15 seconds",
        )
        const blocked = yield* workflow.get(started.id)
        const intervention = graph.interventions.find((item) =>
          item.message.includes("pause for requester decision"),
        )

        expect(blocked.status).toBe("blocked")
        expect(blocked.error).toContain("pause for requester decision")
        expect(intervention?.status).toBe("blocked")
        expect(intervention?.response).toContain('<opencode-workflow-control action="block">')
        expect(intervention?.targetRole).toBe("main_pm")
        expect(intervention?.timing).toBe("interrupt")
        expect(intervention?.path).toContain("interventions")
        expect(
          yield* Effect.promise(() => Bun.file(path.join(instance.directory, intervention!.path)).exists()),
        ).toBe(true)
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "consultation" &&
              edge.question?.includes("pause for requester decision") &&
              edge.summary?.includes("Requester intervention"),
          ),
        ).toBe(true)
      }),
    25_000,
  )

  it.instance(
    "turns requester session messages into workflow interventions and lets main PM consult staff",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: "Requester message workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const messageID = MessageID.ascending()
        const now = Date.now()
        const message = {
          id: messageID,
          sessionID: started.rootSessionID,
          role: "user" as const,
          time: { created: now },
          agent: "build",
          model: { providerID: testProviderID, modelID: testModelID },
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: started.rootSessionID,
          messageID,
          type: "text",
          text: "Requester strategic update: ask the technical advisor before continuing.",
          time: { start: now, end: now },
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.interventions.some(
                (item) => item.message.includes("Requester strategic update") && item.status === "delivered",
              ) &&
              info.edges.some(
                (edge) =>
                  edge.kind === "consultation" &&
                  edge.summary?.includes("Main PM consulted Technical Advisor") &&
                  edge.question?.includes("architecture risk"),
              )
                ? info
                : undefined,
            ),
          ),
          "requester message did not become an intervention with staff consultation",
          "20 seconds",
        )
        const intervention = graph.interventions.find((item) => item.message.includes("Requester strategic update"))

        expect(intervention?.targetRole).toBe("main_pm")
        expect(intervention?.timing).toBe("temporary-interrupt")
        expect(intervention?.status).toBe("delivered")
        expect(intervention?.response).toContain('<opencode-workflow-control action="resume">')
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "consultation" &&
              edge.summary?.includes("Main PM consulted Technical Advisor") &&
              edge.answer?.includes("bounded implementation route"),
          ),
        ).toBe(true)
      }),
    30_000,
  )

  it.instance(
    "routes requester workflow communication XML directly to target staff",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: "Requester direct workflow message",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const messageID = MessageID.ascending()
        const now = Date.now()
        yield* sessions.updateMessage({
          id: messageID,
          sessionID: started.rootSessionID,
          role: "user",
          time: { created: now },
          agent: "build",
          model: { providerID: testProviderID, modelID: testModelID },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: started.rootSessionID,
          messageID,
          type: "text",
          text: '<opencode-workflow-message to-role="expert" specialty="routing" timing="temporary-interrupt" reason="direct requester route">Check whether this direct requester workflow message reaches staff.</opencode-workflow-message>',
          time: { start: now, end: now },
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.consultations.some(
                (item) =>
                  item.fromRole === "requester" &&
                  item.toRole === "expert" &&
                  item.status === "answered" &&
                  item.question.includes("direct requester workflow message"),
              )
                ? info
                : undefined,
            ),
          ),
          "requester workflow communication XML did not route to target staff",
          "10 seconds",
        )

        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "consultation" &&
              edge.summary?.includes("Requester consulted Technical Advisor") &&
              edge.question?.includes("direct requester workflow message"),
          ),
        ).toBe(true)
        expect(graph.interventions.some((item) => item.message.includes("direct requester workflow message"))).toBe(false)
      }),
    20_000,
  )

  it.instance(
    "converts requester workflow-message executor assignment into scheduler dispatch",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: ["Requester raw executor assignment workflow", commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const graph = yield* workflow.graph(started.id)
        const departmentPM = graph.members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected a department PM session")
        const requirementsID = WorkflowMilestoneID.make("requirements")
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: 1,
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: requirementsID,
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(and(eq(WorkflowMilestoneTable.workflow_id, started.id), eq(WorkflowMilestoneTable.id, requirementsID)))
            .run(),
        )
        const protectedJobID = `${started.id}:requirements:requester-raw-message-guard`
        yield* background.start({
          id: protectedJobID,
          type: "workflow.milestone",
          title: "requirements planning guard",
          metadata: { workflowID: started.id, milestoneID: "requirements" },
          run: Effect.never,
        })
        yield* Effect.addFinalizer(() => background.cancel(protectedJobID).pipe(Effect.ignore))

        const messageID = MessageID.ascending()
        const now = Date.now()
        const message = {
          id: messageID,
          sessionID: started.rootSessionID,
          role: "user" as const,
          time: { created: now },
          agent: "build",
          model: { providerID: testProviderID, modelID: testModelID },
        }
        yield* sessions.updateMessage(message)
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: started.rootSessionID,
          messageID,
          type: "text",
          text: '<opencode-workflow-message to-role="executor" specialty="graphics" timing="temporary-interrupt" reason="first wave dispatch">Your assignment: audit-graphics-scene-renderer. Output audit-graphics-scene-renderer/report.md.</opencode-workflow-message>',
          time: { start: now, end: now },
        })
        yield* sessions.updateMessage(message)

        const dispatched = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              if (requirements?.status !== "done") return undefined
              if (!implementation?.session.some((ref) => ref.role === "department_pm")) return undefined
              return info
            }),
          ),
          "requester workflow-message executor assignment did not convert to scheduler dispatch",
          "30 seconds",
        )

        const commandJournal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const messageJournalText = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, started.path, "journal", "messages.jsonl")).text(),
        )
        expect(commandJournal.find((row) => row.action === "force_complete" && row.milestoneID === "requirements")?.source?.role).toBe(
          "requester",
        )
        expect(messageJournalText).toContain('"action":"reject"')
        expect(messageJournalText).toContain("audit-graphics-scene-renderer")
        expect((yield* background.get(protectedJobID))?.status).toBe("cancelled")
        expect(
          dispatched.consultations.some(
            (item) => item.toRole === "executor" && item.question.includes("audit-graphics-scene-renderer"),
          ),
        ).toBe(false)
      }),
    35_000,
  )

  it.instance(
    "records employee questions to requester as pending human clarification",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        const started = yield* workflow.start({
          prompt: "Requester clarification workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")

        const blocked = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const pending = info.consultations.find(
                (item) =>
                  item.toRole === "requester" &&
                  item.status === "pending" &&
                  item.question.includes("Confirm whether the requester wants the safer API route"),
              )
              return info.workflow.status === "blocked" && pending ? { info, pending } : undefined
            }),
          ),
          "requester clarification was not recorded as pending and blocking",
          "25 seconds",
        )

        expect(blocked.pending.answer).toContain("Pending requester response")
        expect(blocked.pending.timing).toBe("temporary-interrupt")
        expect(blocked.info.milestones.find((milestone) => milestone.id === blocked.pending.milestoneID)?.status).toBe(
          "blocked",
        )

        const messageID = MessageID.ascending()
        const now = Date.now()
        yield* sessions.updateMessage({
          id: messageID,
          sessionID: started.rootSessionID,
          role: "user",
          time: { created: now },
          agent: "build",
          model: { providerID: testProviderID, modelID: testModelID },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: started.rootSessionID,
          messageID,
          type: "text",
          text: "Requester answer: use the safer API route and continue.",
          time: { start: now, end: now },
        })

        const answered = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.consultations.some(
                (item) =>
                  item.id === blocked.pending.id &&
                  item.status === "answered" &&
                  item.answer.includes("safer API route"),
              )
                ? info
                : undefined,
            ),
          ),
          "requester answer did not resolve the pending clarification",
          "20 seconds",
        )

        expect(
          answered.edges.some(
            (edge) =>
              edge.kind === "consultation" &&
              edge.to === started.id &&
              edge.answer?.includes("safer API route"),
          ),
        ).toBe(true)
        const sourcePrompt = (yield* sessions.messages({ sessionID: blocked.pending.fromSessionID }))
          .filter((message) => message.info.role === "user")
          .flatMap((message) => message.parts)
          .filter((part) => part.type === "text")
          .map((part) => part.text)
          .join("\n")
        expect(sourcePrompt).toContain("Requester clarification response received.")
        expect(sourcePrompt).toContain("Requester response:")
        expect(sourcePrompt).toContain("safer API route")
      }),
    40_000,
  )

  it.instance(
    "lets requester interventions to staff trigger workflow consultations",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Requester intervention consult workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        yield* workflow.intervene({
          workflowID: started.id,
          timing: "temporary-interrupt",
          targetRole: "executor",
          message: "requester asks executor to check expert route before continuing",
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.interventions.some(
                (item) => item.message.includes("requester asks executor") && item.status === "delivered",
              ) &&
              info.edges.some(
                (edge) =>
                  edge.kind === "consultation" &&
                  edge.summary?.includes("Executor consulted Technical Advisor") &&
                  edge.question?.includes("Confirm the expert route requested by requester intervention"),
              )
                ? info
                : undefined,
            ),
          ),
          "requester intervention to executor did not trigger expert consultation",
          "25 seconds",
        )

        const intervention = graph.interventions.find((item) => item.message.includes("requester asks executor"))
        expect(intervention?.targetRole).toBe("executor")
        expect(intervention?.status).toBe("delivered")
        expect(intervention?.response).toContain("Confirm the expert route requested by requester intervention")
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "document" &&
              edge.label === "consultation" &&
              edge.summary?.includes("Confirm the expert route requested by requester intervention"),
          ),
        ).toBe(true)
      }),
    35_000,
  )

  it.instance(
    "lets main PM block active work from supervision updates",
    () =>
      Effect.gen(function* () {
        pmSupervisionBlockOnce.delete("PM supervision control workflow")
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "PM supervision control workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const blocked = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (info.status === "blocked" || info.status === "failed" ? info : undefined)),
          ),
          "main PM supervision did not block the workflow",
          "25 seconds",
        )
        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => (info.milestones.some((milestone) => milestone.status === "blocked") ? info : undefined)),
          ),
          "main PM supervision did not mark active milestone blocked",
          "5 seconds",
        )

        expect(blocked.status).toBe("blocked")
        expect(blocked.error).toContain("main PM supervision")
        expect(graph.milestones.some((milestone) => milestone.status === "blocked")).toBe(true)
      }),
    35_000,
  )

  it.instance(
    "applies main PM company standup control to block active work",
    () =>
      Effect.gen(function* () {
        standupBlockOnce.delete("Standup control workflow")
        const workflow = yield* Workflow.Service
        const background = yield* BackgroundJob.Service
        const started = yield* workflow.start({
          prompt: "Standup control workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const blocked = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (info.status === "blocked" || info.status === "failed" ? info : undefined)),
          ),
          "company standup did not block the workflow",
          "25 seconds",
        )
        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => (info.milestones.some((milestone) => milestone.status === "blocked") ? info : undefined)),
          ),
          "company standup did not mark active milestone blocked",
          "5 seconds",
        )

        expect(blocked.status).toBe("blocked")
        expect(blocked.error).toContain("company standup")
        expect(graph.milestones.some((milestone) => milestone.status === "blocked")).toBe(true)
        expect(
          (yield* background.list()).some(
            (job) =>
              job.status === "running" &&
              job.type === "workflow.milestone" &&
              job.metadata?.workflowID === started.id,
          ),
        ).toBe(false)
      }),
    35_000,
  )

  it.instance(
    "queues company standup requests for active workflow members",
    () =>
      Effect.gen(function* () {
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Company standup request workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })

        const requests = yield* pollWithTimeout(
          Effect.sync(() =>
            Database.use((db) =>
              db
                .select()
                .from(WorkflowMessageTable)
                .where(and(eq(WorkflowMessageTable.workflow_id, started.id), eq(WorkflowMessageTable.kind, "standup")))
                .all(),
            ),
          ).pipe(Effect.map((rows) => (rows.length > 0 ? rows : undefined))),
          "company standup did not queue member standup requests",
          "25 seconds",
        )
        const request = requests.find((item) => item.to_role === "department_pm") ?? requests[0]
        if (!request.to_session_id) throw new Error("Expected standup request target session")
        expect(request.from_role).toBe("main_pm")
        expect(request.status).toBe("queued")
        expect(request.body).toContain("workflow action=status_update")
        expect(request.body).toContain("workflow_message action=ack")

        const info = yield* WorkflowMessageTool
        const tool = yield* info.init()
        const inbox = yield* tool.execute({ action: "inbox", workflowID: started.id }, workflowMessageContext(request.to_session_id))
        expect(inbox.output).toContain(request.id)
        expect(inbox.output).toContain("Workflow company standup request")
        const acked = yield* tool.execute(
          { action: "ack", workflowID: started.id, messageID: request.id },
          workflowMessageContext(request.to_session_id),
        )
        expect(acked.metadata.kind).toBe("standup")
        expect(acked.metadata.status).toBe("acked")
        const row = Database.use((db) =>
          db
            .select()
            .from(WorkflowMessageTable)
            .where(and(eq(WorkflowMessageTable.workflow_id, started.id), eq(WorkflowMessageTable.id, request.id)))
            .get(),
        )
        expect(row?.kind).toBe("standup")
        expect(row?.status).toBe("acked")

        const journal = yield* Effect.promise(() =>
          Bun.file(path.join(instance.directory, started.path, "journal", "messages.jsonl")).text(),
        )
        expect(journal).toContain(request.id)
        expect(journal).toContain('"kind":"standup"')
        expect(journal).toContain('"status":"acked"')
      }),
    35_000,
  )

  it.instance(
    "archives manual workflow session replies without consultation XML",
    () =>
      Effect.gen(function* () {
        const key = "Manual archive workflow"
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        pmSupervisionBlockOnce.delete(key)
        const started = yield* workflow.start({
          prompt: key,
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const blockedGraph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => (info.workflow.status === "blocked" ? info : undefined)),
          ),
          "manual archive workflow did not block for intervention test",
          "25 seconds",
        )
        const executorSessionID = blockedGraph.milestones
          .flatMap((milestone) => milestone.session)
          .find((ref) => ref.role === "executor")?.sessionID
        if (!executorSessionID) throw new Error("Expected an executor session to archive")
        const messageID = MessageID.ascending()
        const now = Date.now()
        yield* sessions.updateMessage({
          id: messageID,
          sessionID: executorSessionID,
          role: "assistant",
          time: { created: now, completed: now },
          parentID: MessageID.ascending(),
          modelID: testModelID,
          providerID: testProviderID,
          mode: "workflow-executor",
          agent: "workflow-executor",
          path: { cwd: "", root: "" },
          cost: 0,
          tokens: {
            input: 0,
            output: 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          finish: "stop",
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: executorSessionID,
          messageID,
          type: "text",
          text: [
            "Manual operator note from executor without consultation XML.",
            "",
            "## Handoff Summary",
            "- completed: manual design note saved.",
            "- next: future staff should reuse this note.",
          ].join("\n"),
          time: { start: now, end: now },
        })

        const archived = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.flatMap((info) => {
              const summary = info.nodes.find(
                (node) => node.type === "document" && node.sessionID === executorSessionID && node.path?.includes("summary"),
              )
              if (!summary?.path) return Effect.succeed(undefined)
              return Effect.promise(async () => {
                const file = Bun.file(path.join(instance.directory, summary.path!))
                if (!(await file.exists())) return undefined
                const text = await file.text()
                return text.includes("manual design note saved") ? { info, text } : undefined
              })
            }),
          ),
          "manual workflow session reply was not archived into reference library",
          "10 seconds",
        )

        expect(archived.text).toContain("manual design note saved")
        expect(archived.info.edges.some((edge) => edge.kind === "consultation" && edge.question?.includes("manual design"))).toBe(
          false,
        )
      }),
    40_000,
  )

  it.instance(
    "records user intervention messages sent inside workflow child sessions",
    () =>
      Effect.gen(function* () {
        const key = "Child session user intervention workflow"
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const sessions = yield* Session.Service
        pmSupervisionBlockOnce.delete(key)
        const started = yield* workflow.start({
          prompt: key,
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
        })

        const blockedGraph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => (info.workflow.status === "blocked" ? info : undefined)),
          ),
          "child intervention workflow did not block for intervention test",
          "25 seconds",
        )
        const executorSessionID = blockedGraph.milestones
          .flatMap((milestone) => milestone.session)
          .find((ref) => ref.role === "executor")?.sessionID
        if (!executorSessionID) throw new Error("Expected an executor session to receive user intervention")
        const messageID = MessageID.ascending()
        const now = Date.now()
        yield* sessions.updateMessage({
          id: messageID,
          sessionID: executorSessionID,
          role: "user",
          time: { created: now },
          agent: "workflow-executor",
          model: { providerID: testProviderID, modelID: testModelID },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: executorSessionID,
          messageID,
          type: "text",
          text: "Manual child session direction: use the safer API route and record the tradeoff.",
          time: { start: now, end: now },
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.flatMap((info) => {
              const intervention = info.interventions.find(
                (item) =>
                  item.targetSessionID === executorSessionID &&
                  item.targetRole === "executor" &&
                  item.status === "delivered" &&
                  item.message.includes("safer API route"),
              )
              if (!intervention) return Effect.succeed(undefined)
              return Effect.promise(async () => {
                const file = Bun.file(path.join(instance.directory, intervention.path))
                return (await file.exists()) ? info : undefined
              })
            }),
          ),
          "child session user intervention was not recorded in workflow",
          "10 seconds",
        )
        const intervention = graph.interventions.find((item) => item.message.includes("safer API route"))

        expect(intervention?.targetSessionID).toBe(executorSessionID)
        expect(intervention?.targetRole).toBe("executor")
        expect(
          yield* Effect.promise(() => Bun.file(path.join(instance.directory, intervention!.path)).text()),
        ).toContain("safer API route")
        expect(
          graph.edges.some(
            (edge) =>
              edge.kind === "consultation" &&
              edge.question?.includes("safer API route") &&
              edge.summary?.includes("Requester intervention to Executor"),
          ),
        ).toBe(true)
      }),
    40_000,
  )

  it.instance(
    "assigns parallel department PM milestones to distinct available employees",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Parallel PM assignment workflow", parallelDepartmentWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const assigned = info.milestones
                .filter((item) => item.id === "track-a" || item.id === "track-b")
                .flatMap((item) => item.session.filter((ref) => ref.role === "department_pm"))
              return assigned.length === 2 ? info : undefined
            }),
          ),
          "parallel milestones were not assigned to department PM sessions",
          "20 seconds",
        )
        const assignedPMs = graph.milestones
          .filter((item) => item.id === "track-a" || item.id === "track-b")
          .map((item) => item.session.find((ref) => ref.role === "department_pm")?.sessionID)
          .filter(Boolean)

        expect(assignedPMs.length).toBe(2)
        expect(new Set(assignedPMs).size).toBe(2)
      }),
    30_000,
  )

  it.instance(
    "retargets unused precreated staff to milestone departments",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Department specialty assignment workflow", mixedDepartmentWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const assigned = info.milestones
                .filter((item) => item.id === "graphics-track" || item.id === "audio-track")
                .flatMap((item) => item.session.filter((ref) => ref.role === "department_pm"))
              return assigned.length === 2 ? info : undefined
            }),
          ),
          "department milestones were not assigned to specialized PM sessions",
          "20 seconds",
        )
        const memberBySession = new Map(graph.members.map((member) => [member.sessionID, member]))
        const assignedSpecialties = graph.milestones
          .filter((item) => item.id === "graphics-track" || item.id === "audio-track")
          .map((item) => memberBySession.get(item.session.find((ref) => ref.role === "department_pm")!.sessionID)?.specialty)
          .toSorted()

        expect(assignedSpecialties).toEqual(["audio", "graphics"])
      }),
    30_000,
  )

  it.instance(
    "expands workflow pipeline items from the workflow artifact directory",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: "Pipeline item expansion workflow",
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const pipelineXml = `<workflow>
          <ordered>
            <milestone id="prepare" title="Prepare tracks" department="product">Prepare track inputs.</milestone>
            <pipeline items="work/shared/audit-tracks.json">
              <milestone id="audit" title="Audit {item.title}" department="{item.department}">Audit {item.id}: {item.path}</milestone>
              <milestone id="review" title="Review {item.title}" department="quality" depends="audit">Review {item.id}</milestone>
            </pipeline>
            <milestone id="finish" title="Finish synthesis" department="quality">Synthesize all reviews.</milestone>
          </ordered>
        </workflow>`
        yield* workflow.updateXml({
          workflowID: started.id,
          xml: pipelineXml,
        })

        const waitingGraph = yield* workflow.graph(started.id)
        expect(waitingGraph.milestones.map((milestone) => [String(milestone.id), milestone.waitingFor, milestone.dependsOn.map(String)])).toEqual([
          ["prepare", undefined, []],
          ["pipeline-items@work-shared-audit-tracks.json", "pipeline_items", ["prepare"]],
          ["finish", undefined, ["pipeline-items@work-shared-audit-tracks.json"]],
        ])
        expect(workflowDispatchSummary(started.id).join("\n")).toContain(
          "pipeline-items@work-shared-audit-tracks.json (pipeline_items)",
        )

        const bus = yield* Bus.Service
        mkdirSync(path.join(instance.directory, started.path, "work", "shared"), { recursive: true })
        writeFileSync(
          path.join(instance.directory, started.path, "work", "shared", "audit-tracks.json"),
          JSON.stringify([
            { id: "graphics", title: "Graphics", department: "engineering", path: "zircon_runtime/src/graphics" },
            { id: "ui", title: "UI", department: "product", path: "zircon_runtime/src/ui" },
          ]),
        )
        yield* bus.publish(FileWatcher.Event.Updated, {
          file: path.join(instance.directory, started.path, "work", "shared", "audit-tracks.json"),
          event: "add",
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => info.milestones.some((milestone) => milestone.id === "audit@graphics") ? info : undefined),
          ),
          "pipeline item file creation did not expand milestones",
          "12 seconds",
        )
        expect(graph.milestones.map((milestone) => [String(milestone.id), milestone.dependsOn.map(String)])).toEqual([
          ["prepare", []],
          ["audit@graphics", ["prepare"]],
          ["review@graphics", ["audit@graphics"]],
          ["audit@ui", ["prepare"]],
          ["review@ui", ["audit@ui"]],
          ["finish", ["review@graphics", "review@ui"]],
        ])
        expect(graph.milestones.find((milestone) => milestone.id === "audit@graphics")?.department).toBe("engineering")
        expect(graph.milestones.find((milestone) => milestone.id === "audit@ui")?.prompt).toBe(
          "Audit ui: zircon_runtime/src/ui",
        )
        writeFileSync(
          path.join(instance.directory, started.path, "work", "shared", "audit-tracks.json"),
          JSON.stringify([
            { id: "graphics", title: "Graphics", department: "engineering", path: "zircon_runtime/src/graphics" },
            { id: "ui", title: "UI", department: "product", path: "zircon_runtime/src/ui" },
            { id: "audio", title: "Audio", department: "audio", path: "zircon_runtime/src/audio" },
          ]),
        )
        yield* bus.publish(FileWatcher.Event.Updated, {
          file: path.join(instance.directory, started.path, "work", "shared", "audit-tracks.json"),
          event: "change",
        })
        const refreshed = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => info.milestones.some((milestone) => milestone.id === "audit@audio") ? info : undefined),
          ),
          "pipeline item file change did not refresh expanded milestones",
          "12 seconds",
        )
        expect(refreshed.milestones.find((milestone) => milestone.id === "review@audio")?.dependsOn.map(String)).toEqual([
          "audit@audio",
        ])
      }),
    20_000,
  )

  it.instance(
    "shows ready milestones waiting for department PM capacity",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Staffing waiting summary workflow", parallelDepartmentWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        yield* workflow.updateStaffing({
          workflowID: started.id,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const waitingGraph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const tracks = info.milestones.filter((item) => item.id === "track-a" || item.id === "track-b")
              return tracks.some((item) => item.session.some((ref) => ref.role === "department_pm")) &&
                tracks.some((item) => item.waitingFor === "staffing")
                ? info
                : undefined
            }),
          ),
          "department PM capacity did not leave a ready milestone waiting",
          "10 seconds",
        )
        const waitingTrack = waitingGraph.milestones.find((item) => item.waitingFor === "staffing")
        expect(waitingTrack?.id).toMatch(/^track-/)
        expect(workflowDispatchSummary(started.id).join("\n")).toContain(`waiting: ${waitingTrack?.id} (staffing)`)

        yield* workflow.updateStaffing({
          workflowID: started.id,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        const dispatched = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const track = info.milestones.find((item) => item.id === waitingTrack?.id)
              return track?.session.some((ref) => ref.role === "department_pm") && !track.waitingFor ? info : undefined
            }),
          ),
          "staffing capacity increase did not dispatch the waiting milestone",
          "10 seconds",
        )
        expect(dispatched.milestones.find((item) => item.id === waitingTrack?.id)?.waitingFor).toBeUndefined()
      }),
    30_000,
  )

  it.instance(
    "reminds main PM when ready milestones wait too long for staffing",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )

        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Waiting watchdog workflow", parallelDepartmentWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const staleTime = Date.now() - 31 * 60 * 1000
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({ waiting_for: "staffing", time_updated: staleTime })
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, started.id),
                eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("track-b")),
              ),
            )
            .run(),
        )

        const result = yield* workflow.dispatchCommand({
          action: "status",
          workflowID: started.id,
          sourceSessionID: started.rootSessionID,
          message: "refresh waiting watchdog",
        })
        expect(result.applied).toBe(true)

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.interventions.some(
                (item) =>
                  item.targetRole === "main_pm" &&
                  item.status === "queued" &&
                  item.message.includes("track-b") &&
                  item.message.includes("waiting too long"),
              )
                ? info
                : undefined,
            ),
          ),
          "waiting watchdog did not queue main PM reminder",
          "10 seconds",
        )
        const intervention = graph.interventions.find(
          (item) => item.targetRole === "main_pm" && item.message.includes("track-b"),
        )
        expect(intervention?.status).toBe("queued")
        const row = Database.use((db) =>
          db
            .select()
            .from(WorkflowMilestoneTable)
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, started.id),
                eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("track-b")),
              ),
            )
            .get(),
        )
        expect(row?.time_updated).toBeGreaterThan(staleTime)
      }),
    20_000,
  )

  it.instance(
    "limits ready milestone dispatch in economical scheduling mode",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Economical scheduling workflow", economicalWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 3,
            executor: 3,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
          scheduling: {
            mode: "economical",
            maxActive: 1,
          },
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const economical = info.milestones.filter((item) => item.id === "eco-a" || item.id === "eco-b" || item.id === "eco-c")
              const assigned = economical.flatMap((item) => item.session.filter((ref) => ref.role === "department_pm"))
              const waiting = economical.filter((item) => item.session.length === 0)
              return assigned.length === 1 && waiting.length === 2 && waiting.every((item) => item.waitingFor === "scheduling")
                ? info
                : undefined
            }),
          ),
          "economical scheduling did not limit the first dispatch wave",
          "10 seconds",
        )

        expect(graph.workflow.scheduling?.mode).toBe("economical")
        expect(graph.workflow.scheduling?.maxActive).toBe(1)
        expect(
          graph.milestones
            .filter((item) => item.id === "eco-a" || item.id === "eco-b" || item.id === "eco-c")
            .flatMap((item) => item.session.filter((ref) => ref.role === "department_pm")).length,
        ).toBe(1)
        expect(
          graph.milestones
            .filter((item) => (item.id === "eco-a" || item.id === "eco-b" || item.id === "eco-c") && item.session.length === 0)
            .map((item) => item.waitingFor),
        ).toEqual(["scheduling", "scheduling"])
      }),
    20_000,
  )

  it.instance(
    "stages ordered milestone dispatch until workflow resume",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Staged scheduling workflow", reviewSkipWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
          scheduling: {
            mode: "staged",
          },
        })

        const blocked = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const tiny = info.milestones.find((item) => item.id === "tiny")
              const after = info.milestones.find((item) => item.id === "after")
              return info.workflow.status === "blocked" &&
                (info.workflow.error ?? "").includes("Staged scheduling gate") &&
                tiny?.status === "approved" &&
                after?.session.length === 0
                ? info
                : undefined
            }),
          ),
          "staged scheduling did not pause before the next ordered milestone",
          "20 seconds",
        )
        expect(blocked.workflow.scheduling?.mode).toBe("staged")

        yield* workflow.resume(started.id)
        const resumed = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.milestones
                .find((item) => item.id === "after")
                ?.session.some((ref) => ref.role === "department_pm")
                ? info
                : undefined,
            ),
          ),
          "workflow resume did not dispatch the next staged milestone",
          "10 seconds",
        )
        expect(resumed.milestones.find((item) => item.id === "after")?.session.length).toBeGreaterThan(0)
      }),
    35_000,
  )

  it.instance(
    "switches staged scheduling to eager and dispatches the blocked next stage",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Switch scheduling workflow", reviewSkipWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
          scheduling: {
            mode: "staged",
          },
        })
        const requesterSessionID = started.rootSessionID
        if (!requesterSessionID) throw new globalThis.Error("workflow requester session was not created")

        yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.workflow.status === "blocked" &&
              (info.workflow.error ?? "").includes("Staged scheduling gate") &&
              info.milestones.find((item) => item.id === "after")?.session.length === 0
                ? info
                : undefined,
            ),
          ),
          "staged scheduling did not pause before switching mode",
          "20 seconds",
        )

        const result = yield* workflow.dispatchCommand({
          action: "scheduling",
          workflowID: started.id,
          sourceSessionID: requesterSessionID,
          schedulingMode: "eager",
          message: "requester wants the remaining ready work to continue without the staged gate",
        })
        expect(result.applied).toBe(true)
        expect(result.message).toContain("scheduling changed to eager")

        const resumed = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) =>
              info.workflow.scheduling?.mode === "eager" &&
              info.milestones
                .find((item) => item.id === "after")
                ?.session.some((ref) => ref.role === "department_pm")
                ? info
                : undefined,
            ),
          ),
          "switching staged scheduling to eager did not dispatch the next milestone",
          "10 seconds",
        )
        expect(resumed.workflow.status).not.toBe("blocked")
      }),
    35_000,
  )

  it.instance(
    "skips functional review when milestone review is disabled",
    () =>
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["Review skip workflow", reviewSkipWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 1,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })

        const graph = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const tiny = info.milestones.find((item) => item.id === "tiny")
              const after = info.milestones.find((item) => item.id === "after")
              if (!tiny || !after) return undefined
              return tiny.status !== "reviewing" &&
                ["approved", "testing", "done", "completed"].includes(tiny.status) &&
                after.session.some((ref) => ref.role === "department_pm")
                ? info
                : undefined
            }),
          ),
          "review=skip milestone did not approve and dispatch downstream",
          "20 seconds",
        )
        const tiny = graph.milestones.find((item) => item.id === "tiny")
        const after = graph.milestones.find((item) => item.id === "after")

        expect(tiny?.reviewPath).toBeUndefined()
        expect(tiny?.status).not.toBe("reviewing")
        expect(after?.session.some((ref) => ref.role === "department_pm")).toBe(true)
      }),
    30_000,
  )

  it.instance(
    "refreshes workflow XML after main PM supervision changes the plan",
    () =>
      Effect.gen(function* () {
        const key = "PM refreshes workflow XML"
        const instance = yield* TestInstance
        pmRefreshWorkflowXmlOnce.delete(key)
        workflowXmlRewriteDirectories.set(key, instance.directory)
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: key,
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 1,
            tester: 1,
            expert: 2,
          },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "workflow did not finish after main PM XML refresh",
          "45 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        workflowXmlRewriteDirectories.delete(key)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected workflow to complete after PM XML refresh, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const byID = new Map(graph.milestones.map((milestone) => [String(milestone.id), milestone]))
        expect(byID.get("pm-added")?.status).toBe("done")
        expect([...byID.keys()]).toEqual(["requirements", "pm-added", "implementation", "verification"])
      }),
    55_000,
  )

  it.instance(
    "applies workflow update XML emitted by a department PM",
    () =>
      Effect.gen(function* () {
        const key = "Department PM XML update workflow"
        departmentPmWorkflowUpdateOnce.delete(key)
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: key,
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 1,
            tester: 1,
            expert: 2,
          },
        })

        const completed = yield* pollWithTimeout(
          workflow.get(started.id).pipe(
            Effect.map((info) => (["completed", "blocked", "failed"].includes(info.status) ? info : undefined)),
          ),
          "workflow did not finish after department PM XML update",
          "45 seconds",
        )
        const graph = yield* workflow.graph(started.id)
        if (completed.status !== "completed") {
          throw new Error(
            [
              `Expected workflow to complete after department PM XML update, got ${completed.status}: ${completed.error ?? "no error"}`,
              ...graph.milestones.map((milestone) => `${milestone.id}:${milestone.status}:attempt${milestone.attempt}`),
            ].join("\n"),
          )
        }

        const byID = new Map(graph.milestones.map((milestone) => [String(milestone.id), milestone]))
        expect(byID.get("department-added")?.status).toBe("done")
        expect([...byID.keys()]).toEqual(["requirements", "department-added", "implementation", "verification"])
      }),
    55_000,
  )

  it.instance(
    "cancels active milestones removed by workflow update XML",
    () =>
      Effect.gen(function* () {
        const previousAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
        process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (previousAutorun === undefined) {
              delete process.env.OPENCODE_WORKFLOW_AUTORUN
              return
            }
            process.env.OPENCODE_WORKFLOW_AUTORUN = previousAutorun
          }),
        )
        const key = "Update XML delete active workflow"
        const instance = yield* TestInstance
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: [key, commandBusWorkflowXml].join("\n"),
          agent: "build",
          model: `${testProviderID}/${testModelID}`,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 1,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        if (!started.rootSessionID) throw new Error("Workflow did not create a requester session")
        const departmentPM = (yield* workflow.graph(started.id)).members.find((member) => member.role === "department_pm")
        if (!departmentPM) throw new Error("Expected a department PM session")
        Database.use((db) =>
          db
            .update(WorkflowMilestoneTable)
            .set({
              status: "planning",
              attempt: 1,
              session: [
                {
                  role: "department_pm",
                  sessionID: departmentPM.sessionID,
                  milestoneID: WorkflowMilestoneID.make("requirements"),
                  attempt: 1,
                },
              ],
              time_updated: Date.now(),
            })
            .where(
              and(
                eq(WorkflowMilestoneTable.workflow_id, started.id),
                eq(WorkflowMilestoneTable.id, WorkflowMilestoneID.make("requirements")),
              ),
            )
            .run(),
        )

        const result = yield* workflow.dispatchCommand({
          action: "update_xml",
          workflowID: started.id,
          sourceSessionID: started.rootSessionID,
          xml: deleteActiveWorkflowXml,
          message: "remove the active requirements milestone and continue the revised graph",
        })
        expect(result.applied).toBe(true)
        const commandJournal = yield* Effect.promise(() => readWorkflowCommandJournal(instance.directory, started.path))
        const updateCommand = commandJournal.find((row) => row.action === "update_xml")
        expect(updateCommand?.outcome).toBe("applied")

        const updated = yield* pollWithTimeout(
          workflow.graph(started.id).pipe(
            Effect.map((info) => {
              const requirements = info.milestones.find((milestone) => milestone.id === "requirements")
              const implementation = info.milestones.find((milestone) => milestone.id === "implementation")
              return requirements?.status === "cancelled" &&
                requirements.session.length > 0 &&
                implementation?.session.some((session) => session.role === "department_pm")
                ? info
                : undefined
            }),
          ),
          "deleted active milestone was not cancelled or revised graph did not dispatch",
          "25 seconds",
        )
        expect(updated.milestones.find((milestone) => milestone.id === "requirements")?.status).toBe("cancelled")
        expect(updated.milestones.find((milestone) => milestone.id === "implementation")?.dependsOn).not.toContain(
          "requirements",
        )

        yield* Effect.sleep("2300 millis")
        expect((yield* workflow.graph(started.id)).milestones.find((milestone) => milestone.id === "requirements")?.status).toBe(
          "cancelled",
        )
      }),
    70_000,
  )
})

function waitForWorkflowCommandResult(commandID: string) {
  return new Promise<WorkflowToolCommandResult | undefined>((resolve) => {
    let settled = false
    let timer: ReturnType<typeof setTimeout>
    const done = (result: WorkflowToolCommandResult | undefined) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      GlobalBus.off("event", handler)
      resolve(result)
    }
    const handler = (event: GlobalEvent) => {
      const result = workflowCommandResultFromEvent(event, commandID)
      if (result) done(result)
    }
    timer = setTimeout(() => done(undefined), 10_000)
    GlobalBus.on("event", handler)
  })
}

function workflowCommandResultFromEvent(event: GlobalEvent, commandID: string) {
  if (event.payload?.type !== WorkflowToolCommandResultEvent.type) return
  const properties = event.payload.properties
  if (!properties || typeof properties !== "object" || properties.id !== commandID) return
  return properties as WorkflowToolCommandResult
}

type WorkflowCommandJournalRow = {
  seq?: number
  id?: string
  action?: string
  milestoneID?: string
  outcome?: string
  source?: {
    role?: string
    agent?: string
  }
  from?: {
    milestoneStatus?: string
  }
  to?: {
    milestoneStatus?: string
  }
  rejection?: {
    code?: string
  }
}

type WorkflowMessageJournalRow = {
  seq?: number
  ts?: string
  action?: string
  messageID?: string
  status?: string
}

type DirectorySnapshotEntry = {
  path: string
  kind: "directory" | "file"
  content?: string
}

function snapshotDirectory(root: string, current = root): DirectorySnapshotEntry[] {
  if (!existsSync(current)) return []
  return readdirSync(current, { withFileTypes: true })
    .flatMap((entry) => {
      const file = path.join(current, entry.name)
      const relative = path.relative(root, file).split(path.sep).join("/")
      if (entry.isDirectory()) {
        return [{ path: relative, kind: "directory" as const }, ...snapshotDirectory(root, file)]
      }
      if (entry.isFile()) {
        return [{ path: relative, kind: "file" as const, content: readFileSync(file).toString("base64") }]
      }
      return []
    })
    .sort((a, b) => a.path.localeCompare(b.path) || a.kind.localeCompare(b.kind))
}

type WorkflowStateSnapshotJson = {
  schema?: number
  version?: number
  manifest?: unknown
  journal?: {
    commands?: {
      highWater?: number
    }
    messages?: {
      highWater?: number
    }
    events?: {
      highWater?: number
    }
  }
  workflow: {
    directory?: string
    path?: string
    status?: string
  }
  members: Array<{
    role?: string
    sessionID?: SessionID
    model?: {
      providerID?: string
      modelID?: string
    }
    modelWeight?: number
    modelCacheUntil?: number
  }>
  milestones: Array<{
    id: string
    status?: string
    session?: unknown[]
  }>
  consultations?: unknown[]
  interventions?: unknown[]
  sessions: Array<{
    id: string
    path?: string
    contentHash?: string
    messages?: unknown
  }>
}

async function readWorkflowCommandJournal(instanceDirectory: string, workflowPath: string) {
  const text = await Bun.file(path.join(instanceDirectory, workflowPath, "journal", "commands.jsonl")).text()
  return text
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => JSON.parse(line) as WorkflowCommandJournalRow)
}

async function readWorkflowMessageJournal(instanceDirectory: string, workflowPath: string) {
  const text = await Bun.file(path.join(instanceDirectory, workflowPath, "journal", "messages.jsonl")).text()
  return text
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => JSON.parse(line) as WorkflowMessageJournalRow)
}

type FakePromptInput = {
  sessionID: SessionID
  agent?: string
  model?: { providerID: ProviderID; modelID: ModelID }
  variant?: string
  messageID?: MessageID
  parts?: ReadonlyArray<SessionPrompt.PromptInput["parts"][number]>
}

function fakeMessage(input: FakePromptInput): MessageV2.WithParts {
  const now = Date.now()
  const model = input.model ?? { providerID: testProviderID, modelID: testModelID }
  const info: MessageV2.Assistant = {
    id: MessageID.ascending(),
    sessionID: input.sessionID,
    role: "assistant",
    time: { created: now, completed: now },
    parentID: input.messageID ?? MessageID.ascending(),
    modelID: model.modelID,
    providerID: model.providerID,
    mode: input.agent ?? "build",
    agent: input.agent ?? "build",
    path: { cwd: "", root: "" },
    cost: 0,
    tokens: {
      input: 0,
      output: 0,
      reasoning: 0,
      cache: {
        read: 0,
        write: 0,
      },
    },
    finish: "stop",
  }
  const part: MessageV2.TextPart = {
    id: PartID.ascending(),
    sessionID: input.sessionID,
    messageID: info.id,
    type: "text",
    text: replyFor(promptText(input.parts ?? [])),
    time: { start: now, end: now },
  }
  return { info, parts: [part] }
}

function promptText(parts: ReadonlyArray<SessionPrompt.PromptInput["parts"][number]>) {
  return textPromptParts({ parts })
    .map((part) => part.text)
    .join("\n")
}

function textPromptParts(input: { parts?: ReadonlyArray<SessionPrompt.PromptInput["parts"][number]> }) {
  return (input.parts ?? []).filter((part): part is MessageV2.TextPartInput => part.type === "text")
}

function replyFor(prompt: string) {
  if (prompt.includes("Required output: executor completion XML for milestone")) {
    const milestoneID = /milestone ([^\s]+)/i.exec(prompt)?.[1] ?? "requirements"
    return [
      "Executor completed the assigned scope after automatic continuation.",
      `<opencode-workflow-result milestone="${milestoneID}" status="complete">implementation evidence recorded</opencode-workflow-result>`,
      "",
      "## Handoff Summary",
      "- completed: produced the required executor completion marker.",
      "- next: department PM review.",
    ].join("\n")
  }
  if (prompt.includes("Requester intervention received for this workflow company.")) {
    if (prompt.includes("pause for requester decision")) {
      return [
        "The requester changed strategy and the company should wait for clarification before more work.",
        '<opencode-workflow-control action="block">waiting for requester to settle the new direction</opencode-workflow-control>',
        "",
        "## Handoff Summary",
        "- completed: recorded requester interrupt and kept workflow blocked.",
        "- next: requester or main PM must clarify before resuming.",
      ].join("\n")
    }
    if (prompt.includes("requester asks executor to check expert route")) {
      return [
        "Requester direction noted. I need expert confirmation before continuing.",
        '<opencode-workflow-message to-role="expert" timing="temporary-interrupt" reason="requester intervention technical route">Confirm the expert route requested by requester intervention before the executor continues.</opencode-workflow-message>',
        '<opencode-workflow-control action="resume">ready to continue after the expert route is recorded</opencode-workflow-control>',
        "",
        "## Handoff Summary",
        "- completed: routed requester intervention through executor to expert consultation.",
        "- next: continue after expert advice is archived.",
      ].join("\n")
    }
    return [
      "Direction incorporated.",
      '<opencode-workflow-control action="resume">ready to continue with revised direction</opencode-workflow-control>',
      "",
      "## Handoff Summary",
      "- completed: incorporated requester intervention.",
      "- next: workflow can continue.",
    ].join("\n")
  }
  if (prompt.includes("Requester sent a new strategic direction in the requester session.")) {
    return [
      "Direction noted. Ask the advisor before continuing the company plan.",
      '<opencode-workflow-message to-role="expert" timing="temporary-interrupt" reason="architecture risk">Check the architecture risk from the requester strategic update before the company continues.</opencode-workflow-message>',
      '<opencode-workflow-control action="resume">ready to continue with the requester update</opencode-workflow-control>',
      "",
      "## Handoff Summary",
      "- completed: routed requester update through main PM supervision.",
      "- next: continue after technical advisor context is recorded.",
    ].join("\n")
  }
  if (prompt.includes("A peer employee session in the same workflow company is asking for consultation")) {
    if (prompt.includes("PM consult control workflow") && prompt.includes("Should this workflow pause for PM control")) {
      return [
        "Main PM says this workflow should pause before the executor continues.",
        '<opencode-workflow-control action="block">pause from consulted main PM control response</opencode-workflow-control>',
        "",
        "## Handoff Summary",
        "- completed: main PM blocked the workflow from a consultation response.",
        "- next: requester or PM should clarify before resuming.",
      ].join("\n")
    }
    if (prompt.includes("Nested consult workflow") && prompt.includes("Confirm PM alignment for nested technical escalation")) {
      return [
        "PM alignment confirmed for the nested technical escalation.",
        "",
        "## Handoff Summary",
        "- completed: main PM confirmed the advisor escalation.",
        "- next: technical advisor can answer the executor with this alignment.",
      ].join("\n")
    }
    if (prompt.includes("Nested consult workflow") && prompt.includes("Confirm the technical route is acceptable before review")) {
      return [
        "The technical route looks acceptable, but the advisor needs one Main PM alignment check first.",
        '<opencode-workflow-message to-role="main_pm" timing="temporary-interrupt" reason="nested scope escalation">Confirm PM alignment for nested technical escalation before the executor review proceeds.</opencode-workflow-message>',
        "",
        "## Handoff Summary",
        "- completed: advisor escalated the technical route to main PM for alignment.",
        "- next: continue after PM alignment is recorded.",
      ].join("\n")
    }
    return [
      "Use the bounded implementation route and preserve the review evidence.",
      "",
      "## Handoff Summary",
      "- completed: answered peer technical consultation.",
      "- next: executor can continue with the clarified approach.",
    ].join("\n")
  }
  if (prompt.includes("Workflow consultation response received")) {
    return [
      "Consultation received and applied to the execution record.",
      "",
      "## Handoff Summary",
      "- completed: applied expert consultation response.",
      "- next: department PM review.",
    ].join("\n")
  }
  if (
    prompt.includes("Run a short workflow company standup.") &&
    prompt.includes("Standup control workflow") &&
    !standupBlockOnce.has("Standup control workflow")
  ) {
    standupBlockOnce.add("Standup control workflow")
    return [
      "Company standup sees a strategy mismatch before the active milestone should continue.",
      '<opencode-workflow-control action="block">pause active work from company standup</opencode-workflow-control>',
      "",
      "## Handoff Summary",
      "- completed: company standup stopped active work.",
      "- next: requester or main PM should clarify before resuming.",
    ].join("\n")
  }
  if (prompt.includes("Workflow progress update for main PM supervision.") && prompt.includes("Executor finished milestone")) {
    const supervisionBlockKey = [
      "PM supervision control workflow",
      "Manual archive workflow",
      "Child session user intervention workflow",
    ].find((item) => prompt.includes(item))
    if (supervisionBlockKey && !pmSupervisionBlockOnce.has(supervisionBlockKey)) {
      pmSupervisionBlockOnce.add(supervisionBlockKey)
      return [
        "Main PM sees a strategic mismatch and stops active work before review.",
        '<opencode-workflow-control action="block">pause active work for main PM supervision</opencode-workflow-control>',
        "",
        "## Handoff Summary",
        "- completed: stopped the active milestone from supervision.",
        "- next: requester or main PM should clarify before resuming.",
      ].join("\n")
    }
    return [
      "Main PM wants the tester to prepare completeness focus before the formal test gate.",
      '<opencode-workflow-message to-role="tester" timing="after-task" reason="prepare completeness review">Check whether the executor evidence is enough for final completeness review readiness.</opencode-workflow-message>',
      "",
      "## Handoff Summary",
      "- completed: supervised executor completion and asked tester for readiness focus.",
      "- next: department PM review and tester preparation.",
    ].join("\n")
  }
  if (
    prompt.includes("Workflow progress update for main PM supervision.") &&
    prompt.includes("Department PM approved milestone requirements") &&
    prompt.includes("PM refreshes workflow XML") &&
    !pmRefreshWorkflowXmlOnce.has("PM refreshes workflow XML")
  ) {
    pmRefreshWorkflowXmlOnce.add("PM refreshes workflow XML")
    rewriteWorkflowXmlFromPrompt(
      prompt,
      workflowXmlRewriteDirectories.get("PM refreshes workflow XML"),
      [
        "<workflow>",
        "  <ordered>",
        '    <milestone id="requirements" title="Clarify requirements" department="product">Clarify the user request, constraints, and acceptance criteria.</milestone>',
        '    <milestone id="pm-added" title="PM added alignment check" department="product">Main PM added a strategy alignment check after reviewing requirements.</milestone>',
        '    <milestone id="implementation" title="Implement solution" department="engineering">Implement the requested change end to end.</milestone>',
        '    <milestone id="verification" title="Verify solution" department="quality">Verify behavior with focused tests and checks.</milestone>',
        "  </ordered>",
        "</workflow>",
      ].join("\n"),
    )
    return [
      "Main PM adjusted workflow.xml with a new strategy alignment milestone.",
      "",
      "## Handoff Summary",
      "- completed: added pm-added milestone after requirements.",
      "- next: dispatch the updated workflow graph.",
    ].join("\n")
  }
  if (prompt.includes("<opencode-workflow-review")) {
    return [
      "Functional scope is acceptable.",
      '<opencode-workflow-review milestone="requirements" decision="approve">approved</opencode-workflow-review>',
      "",
      "## Handoff Summary",
      "- completed: functional review approved the milestone.",
      "- next: downstream workflow may continue.",
    ].join("\n")
  }
  if (prompt.includes("<opencode-workflow-acceptance")) {
    if (
      prompt.includes("Main PM acceptance reopen workflow") &&
      prompt.includes("You are the main product manager") &&
      !failMainPmAcceptanceOnce.has("Main PM acceptance reopen workflow")
    ) {
      failMainPmAcceptanceOnce.add("Main PM acceptance reopen workflow")
      return [
        "Main PM acceptance rejects implementation evidence and dependent verification.",
        '<opencode-workflow-acceptance role="main_pm" decision="reject" milestones="implementation">implementation needs another pass before requester acceptance</opencode-workflow-acceptance>',
        "",
        "## Handoff Summary",
        "- completed: main PM rejected final acceptance with targeted feedback.",
        "- next: reopen implementation and dependent verification.",
      ].join("\n")
    }
    if (
      prompt.includes("Requester acceptance reopen workflow") &&
      prompt.includes("You are the requester and strategic owner") &&
      !failRequesterAcceptanceOnce.has("Requester acceptance reopen workflow")
    ) {
      failRequesterAcceptanceOnce.add("Requester acceptance reopen workflow")
      return [
        "Requester acceptance rejects implementation fit and dependent verification.",
        '<opencode-workflow-acceptance role="requester" decision="reject" milestones="implementation">implementation does not yet satisfy the strategic request</opencode-workflow-acceptance>',
        "",
        "## Handoff Summary",
        "- completed: requester rejected final acceptance with targeted feedback.",
        "- next: reopen implementation and dependent verification.",
      ].join("\n")
    }
    if (
      (prompt.includes("Manual requester final acceptance workflow") || prompt.includes("Manual requester reject workflow")) &&
      prompt.includes("You are the requester and strategic owner")
    ) {
      return [
        "Requester final acceptance is waiting for real user confirmation.",
        "",
        "## Handoff Summary",
        "- completed: final delivery is ready for human requester acceptance.",
        "- next: requester must approve or reject.",
      ].join("\n")
    }
    return [
      "Final acceptance approved.",
      '<opencode-workflow-acceptance role="requester" decision="approve">approved</opencode-workflow-acceptance>',
      '<opencode-workflow-complete status="complete">final accepted outcome</opencode-workflow-complete>',
      "",
      "## Handoff Summary",
      "- completed: final acceptance approved.",
      "- next: workflow can close.",
    ].join("\n")
  }
  if (prompt.includes("<opencode-workflow-technical")) {
    if (prompt.includes("Technical reopen workflow") && !failTechnicalOnce.has("Technical reopen workflow")) {
      failTechnicalOnce.add("Technical reopen workflow")
      return [
        "Architecture risk remains in implementation and verification must be rerun after it changes.",
        '<opencode-workflow-technical decision="fail" milestones="implementation">implementation needs another architecture pass before acceptance</opencode-workflow-technical>',
        "",
        "## Handoff Summary",
        "- completed: technical advisor found architecture risk.",
        "- next: reopen implementation and dependent verification.",
      ].join("\n")
    }
    return [
      "Architecture and performance are acceptable.",
      '<opencode-workflow-technical decision="pass">architecture and performance are acceptable</opencode-workflow-technical>',
      "",
      "## Handoff Summary",
      "- completed: final technical assessment passed.",
      "- next: acceptance.",
    ].join("\n")
  }
  if (prompt.includes("<opencode-workflow-test")) {
    if (prompt.includes("Reopen once workflow") && !failTesterOnce.has("Reopen once workflow")) {
      failTesterOnce.add("Reopen once workflow")
      return [
        "Implementation evidence is incomplete and verification depends on it.",
        '<opencode-workflow-test decision="fail" milestones="implementation">implementation needs another execution pass before verification can be trusted</opencode-workflow-test>',
        "",
        "## Handoff Summary",
        "- completed: tester found incomplete implementation evidence.",
        "- next: reopen implementation and dependent verification work.",
      ].join("\n")
    }
    return [
      "Targeted workflow tests passed.",
      '<opencode-workflow-test decision="pass">all required workflow checks passed</opencode-workflow-test>',
      "",
      "## Handoff Summary",
      "- completed: tester passed completeness and regression review.",
      "- next: technical advisor assessment.",
    ].join("\n")
  }
  if (
    prompt.includes("Another executor is starting") ||
    prompt.includes("The workflow manager is asking you for peer technical handoff to another executor.")
  ) {
    return [
      "Peer handoff: prior milestone is complete.",
      "",
      "## Handoff Summary",
      "- completed: shared prior context.",
      "- next: new executor can proceed.",
    ].join("\n")
  }
  if (prompt.includes("technical advisor for workflow milestone")) {
    return [
      "Advisor note: keep scope bounded and verify handoff evidence.",
      "",
      "## Handoff Summary",
      "- completed: architecture and risk advice.",
      "- next: executor implementation.",
    ].join("\n")
  }
  if (
    prompt.includes("Attempt limit workflow") &&
    (prompt.includes("long-lived department product manager") ||
      prompt.includes("Required output: department PM execution plan"))
  ) {
    return "Still drafting the plan, but no recognized handoff marker is ready yet."
  }
  if (prompt.includes("Main PM idle dispatch correction workflow") && prompt.includes("Workflow dispatch correction required.")) {
    return [
      "The previous response described dispatch without using the workflow control plane.",
      '<opencode-workflow-control action="force_complete" milestone="requirements">requirements are approved; unblock implementation dispatch</opencode-workflow-control>',
      "",
      "## Handoff Summary",
      "- completed: corrected the idle main PM dispatch claim through workflow control.",
      "- next: workflow runtime can dispatch implementation from the approved requirements gate.",
    ].join("\n")
  }
  if (prompt.includes("Department PM idle queue correction workflow") && prompt.includes("Workflow dispatch correction required.")) {
    return [
      "The previous response described queued control-plane work without a confirmed workflow command result.",
      '<opencode-workflow-control action="plan_complete" milestone="requirements">requirements plan is ready; continue through the scheduler</opencode-workflow-control>',
      "",
      "## Handoff Summary",
      "- completed: corrected the idle department PM stalled-queue claim through workflow control.",
      "- next: workflow runtime can continue the requirements milestone from the real command journal.",
    ].join("\n")
  }
  if (prompt.includes("Managed control correction workflow") && prompt.includes("Workflow dispatch correction required.")) {
    return [
      "The previous response described dispatch without using the workflow control plane.",
      '<opencode-workflow-control action="plan_complete" milestone="requirements">requirements plan is complete and ready for executor dispatch</opencode-workflow-control>',
      "",
      "## Handoff Summary",
      "- completed: corrected the department PM plan closure through workflow control.",
      "- next: workflow runtime can continue from the acknowledged planning gate.",
    ].join("\n")
  }
  if (prompt.includes("Managed control correction workflow") && prompt.includes("main product manager")) {
    return [
      "# Main Product Plan",
      "- Use a single requirements milestone so department PM correction can be inspected.",
      "",
      '<opencode-workflow-update reason="managed correction test">',
      "<workflow>",
      "  <ordered>",
      '    <milestone id="requirements" title="Clarify requirements" department="product">Clarify the request and produce a concrete handoff.</milestone>',
      '    <milestone id="implementation" title="Implement solution" department="engineering">Implement the approved requirements after the department PM plan gate is closed.</milestone>',
      "  </ordered>",
      "</workflow>",
      "</opencode-workflow-update>",
      "",
      "## Handoff Summary",
      "- completed: created the dispatchable workflow XML.",
      "- next: run the department PM milestone.",
    ].join("\n")
  }
  if (prompt.includes("Managed control correction workflow") && prompt.includes("department product manager")) {
    return [
      "Still no pickup. I am assigning the first wave directly.",
      '<opencode-workflow-message to-role="executor" specialty="graphics" timing="after-task" reason="first wave dispatch">Your assignment: audit-graphics-scene-renderer. Output audit-graphics-scene-renderer/report.md.</opencode-workflow-message>',
      "",
      "## Handoff Summary",
      "- completed: attempted to send executor work through workflow-message.",
      "- next: workflow runtime must request a control-plane correction.",
    ].join("\n")
  }
  if (prompt.includes("Raw resolver misuse workflow") && prompt.includes("main product manager")) {
    return [
      "# Main Product Plan",
      "- Use a single requirements milestone so department PM output can be inspected.",
      "",
      '<opencode-workflow-update reason="raw resolver misuse test">',
      "<workflow>",
      "  <ordered>",
      '    <milestone id="requirements" title="Clarify requirements" department="product">Clarify the request and produce a concrete handoff.</milestone>',
      "  </ordered>",
      "</workflow>",
      "</opencode-workflow-update>",
      "",
      "## Handoff Summary",
      "- completed: created the dispatchable workflow XML.",
      "- next: run the department PM milestone.",
    ].join("\n")
  }
  if (prompt.includes("Raw resolver misuse workflow") && prompt.includes("department product manager")) {
    return [
      "Still no pickup. I am assigning the first wave directly.",
      '<opencode-workflow-message to-role="executor" specialty="graphics" timing="after-task" reason="first wave dispatch">Your assignment: audit-graphics-scene-renderer. Output audit-graphics-scene-renderer/report.md.</opencode-workflow-message>',
      "",
      "## Handoff Summary",
      "- completed: attempted to send executor work through workflow-message.",
      "- next: workflow runtime must reject this as a fake dispatch.",
    ].join("\n")
  }
  if (prompt.includes("department product manager")) {
    if (
      prompt.includes("Department PM XML update workflow") &&
      !departmentPmWorkflowUpdateOnce.has("Department PM XML update workflow")
    ) {
      departmentPmWorkflowUpdateOnce.add("Department PM XML update workflow")
      return [
        "# Milestone Plan",
        "- Requirements need an additional department-owned alignment step before implementation.",
        "",
        '<opencode-workflow-update reason="department PM split broad requirements handoff">',
        "<workflow>",
        "  <ordered>",
        '    <milestone id="requirements" title="Clarify requirements" department="product">Clarify the user request, constraints, and acceptance criteria.</milestone>',
        '    <milestone id="department-added" title="Department PM alignment check" department="product">Confirm the requirements handoff is concrete before implementation starts.</milestone>',
        '    <milestone id="implementation" title="Implement solution" department="engineering">Implement the requested change end to end.</milestone>',
        '    <milestone id="verification" title="Verify solution" department="quality">Verify behavior with focused tests and checks.</milestone>',
        "  </ordered>",
        "</workflow>",
        "</opencode-workflow-update>",
        "",
        "## Handoff Summary",
        "- completed: split requirements with an additional alignment milestone.",
        "- next: finish current requirements then dispatch department-added.",
      ].join("\n")
    }
    return [
      "# Milestone Plan",
      "- Define concrete scope.",
      "- Execute the smallest complete change.",
      "- Record evidence for review.",
      "",
      "## Handoff Summary",
      "- completed: detailed milestone plan.",
      "- next: expert and executor.",
    ].join("\n")
  }
  if (prompt.includes("executor employee assigned to workflow milestone")) {
    if (prompt.includes("PM consult control workflow")) {
      return [
        "Executor found a scope issue that needs PM control.",
        '<opencode-workflow-message to-role="main_pm" timing="temporary-interrupt" reason="pm control">Should this workflow pause for PM control before continuing?</opencode-workflow-message>',
        "",
        "## Handoff Summary",
        "- completed: asked main PM for control decision.",
        "- next: wait for PM control response.",
      ].join("\n")
    }
    if (prompt.includes("Requester clarification workflow")) {
      return [
        "Executor needs the requester to choose the strategy before continuing.",
        '<opencode-workflow-message to-role="requester" timing="temporary-interrupt" reason="strategy choice">Confirm whether the requester wants the safer API route before implementation continues.</opencode-workflow-message>',
        "",
        "## Handoff Summary",
        "- completed: paused for requester strategy clarification.",
        "- next: continue only after requester answers.",
      ].join("\n")
    }
    return [
      "Executor completed the assigned scope.",
      '<opencode-workflow-message to-role="expert" timing="temporary-interrupt" reason="confirm implementation route">Confirm the technical route is acceptable before review.</opencode-workflow-message>',
      '<opencode-workflow-message to-role="requester" timing="after-task" reason="confirm strategy fit">Confirm whether the completed milestone still matches the strategic request.</opencode-workflow-message>',
      "",
      "## Handoff Summary",
      "- completed: implementation evidence recorded.",
      "- next: department PM review.",
    ].join("\n")
  }
  return [
    "# Main Product Plan",
    "- Maintain the default requirements, implementation, and verification milestones.",
    "- Supervise progress through completion.",
    "",
    "## Handoff Summary",
    "- completed: high-level workflow plan.",
    "- next: milestone dispatch.",
  ].join("\n")
}

function rewriteWorkflowXmlFromPrompt(prompt: string, directory: string | undefined, xml: string) {
  const root = /^Workflow root:\s*(.+)$/m.exec(prompt)?.[1]?.trim()
  if (!root || !directory) return
  mkdirSync(path.join(directory, root), { recursive: true })
  writeFileSync(path.join(directory, root, "workflow.xml"), xml)
}
