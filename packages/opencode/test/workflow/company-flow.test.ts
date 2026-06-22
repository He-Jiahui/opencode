import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { mkdirSync, writeFileSync } from "fs"
import path from "path"

import { BackgroundJob } from "@/background/job"
import { Bus } from "@/bus"
import { EventV2Bridge } from "@/event-v2-bridge"
import { ModelID, ProviderID } from "@/provider/schema"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { Workflow } from "@/workflow/workflow"
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
        if (
          text.includes("Executor peer sync workflow") &&
          text.includes("long-lived executor employee assigned to workflow milestone alpha")
        ) {
          yield* Effect.sleep("1 second")
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
    Layer.provideMerge(Layer.mergeAll(BackgroundJob.defaultLayer, Bus.layer, EventV2Bridge.defaultLayer, Session.defaultLayer)),
  ),
)

describe("company workflow execution", () => {
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
        expect(yield* Effect.promise(() => Bun.file(path.join(instance.directory, completed.path, "test-plan.md")).exists())).toBe(
          true,
        )
        expect(
          yield* Effect.promise(() =>
            Bun.file(path.join(instance.directory, completed.path, "technical-assessment.md")).exists(),
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
        expect(referenceIndex).toContain("- Main PM plan: main-plan.md")
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
          "45 seconds",
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
    55_000,
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
})

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
