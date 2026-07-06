// @ts-nocheck
import { afterEach, describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { HttpRouter } from "effect/unstable/http"
import path from "path"
import { mkdir } from "fs/promises"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { createOpencodeClient } from "@opencode-ai/sdk/v2"
import { InstanceBootstrap } from "../../src/project/bootstrap-service"
import { InstanceStore } from "../../src/project/instance-store"
import { disposeMiddleware } from "../../src/server/routes/instance/httpapi/lifecycle"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { WorkflowPaths } from "../../src/server/routes/instance/httpapi/groups/workflow"
import { Database } from "../../src/storage/db"
import { WorkflowID } from "../../src/workflow/schema"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"

const noopBootstrapLayer = Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))
const it = testEffect(
  LayerNode.compile(
    LayerNode.group([CrossSpawnSpawner.node, InstanceStore.node]),
    [[InstanceStore.bootstrapNode, noopBootstrapLayer]],
  ),
)

function testApp() {
  const handler = HttpRouter.toWebHandler(HttpApiApp.createRoutes(), {
    disableLogger: true,
    memoMap: Layer.makeMemoMapUnsafe(),
    middleware: disposeMiddleware,
  }).handler
  return {
    fetch: (request: Request) => handler(request, HttpApiApp.context),
    request(input: string | URL | Request, init?: RequestInit) {
      return this.fetch(input instanceof Request ? input : new Request(new URL(input, "http://localhost"), init))
    },
  }
}

function serverFetch() {
  const app = testApp()
  return Object.assign((request: RequestInfo | URL, init?: RequestInit) => Promise.resolve(app.request(request, init)), {
    preconnect: globalThis.fetch.preconnect,
  }) satisfies typeof globalThis.fetch
}

function client(directory: string) {
  return createOpencodeClient({
    baseUrl: "http://localhost",
    directory,
    fetch: serverFetch(),
    throwOnError: true,
  })
}

function requestWithTimeout<T>(request: () => Promise<T>, message: string) {
  return Effect.promise(request).pipe(
    Effect.timeoutOrElse({
      duration: "5 seconds",
      orElse: () => Effect.fail(new Error(message)),
    }),
  )
}

describe("workflow HttpApi", () => {
  afterEach(async () => {
    await disposeAllInstances()
    await resetDatabase()
  })

  it.instance("manages workflow company state through the generated SDK", () =>
    Effect.gen(function* () {
      const workflowAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
      process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (workflowAutorun === undefined) return delete process.env.OPENCODE_WORKFLOW_AUTORUN
          process.env.OPENCODE_WORKFLOW_AUTORUN = workflowAutorun
        }),
      )
      const test = yield* TestInstance
      const sdk = client(test.directory)
      const session = yield* Effect.promise(() => sdk.session.create({ title: "Workflow source" }))
      if (!session.data) throw new Error("session create returned no data")
      const workflow = yield* requestWithTimeout(() =>
        sdk.workflow.start({
          workflowStartInput: {
            sessionID: session.data.id,
            prompt: "Build a targeted workflow",
          },
        }),
        "workflow start timed out",
      )
      if (!workflow.data) throw new Error("workflow start returned no data")

      expect(workflow.data.request).toBe("Build a targeted workflow")
      expect(workflow.data.rootSessionID).toBe(session.data.id)
      expect(workflow.data.path.startsWith(`${path.join(".opencode", "workflows")}${path.sep}`)).toBe(true)
      expect(path.basename(workflow.data.path)).toBe(workflow.data.id)

      const graph = yield* pollWithTimeout(
        Effect.gen(function* () {
          const result = yield* requestWithTimeout(() => sdk.workflow.graph({ workflowID: workflow.data!.id }), "workflow graph timed out")
          if (!result.data) return
          const planPath = result.data.milestones[0]?.planPath
          if (!planPath) return
          if (!(yield* Effect.promise(() => Bun.file(path.join(test.directory, planPath)).exists()))) return
          return result
        }),
        "workflow initialization did not write milestone plan files",
        "10 seconds",
      )
      if (!graph.data) throw new Error("workflow graph returned no data")
      expect(graph.data.nodes.map((node) => node.id)).toContain("requirements")
      expect(graph.data.milestones.map((milestone) => milestone.id)).toEqual([
        "requirements",
        "implementation",
        "verification",
      ])
      expect(yield* Effect.promise(() => Bun.file(path.join(test.directory, graph.data!.milestones[0]!.planPath!)).exists())).toBe(
        true,
      )

      const projectWorkflow = yield* requestWithTimeout(() =>
        sdk.workflow.start({
          workflowStartInput: {
            prompt: "Run a workflow independent from the current chat",
          },
        }),
        "project workflow start timed out",
      )
      if (!projectWorkflow.data) throw new Error("project workflow start returned no data")

      expect(projectWorkflow.data.rootSessionID).toBeDefined()
      expect(projectWorkflow.data.request).toBe("Run a workflow independent from the current chat")
      expect(path.basename(projectWorkflow.data.path)).toBe(projectWorkflow.data.id)

      const list = yield* Effect.promise(() => sdk.workflow.list())
      expect(list.data?.map((item) => item.id)).toContain(projectWorkflow.data.id)

      const staffedWorkflow = yield* requestWithTimeout(() =>
        sdk.workflow.start({
          workflowStartInput: {
            prompt: "Coordinate staffing and requester intervention",
            staffing: {
              mainPM: 1,
              departmentPM: 1,
              executor: 1,
              reviewer: 1,
              tester: 1,
              expert: 1,
            },
            modelWhitelist: {
              mainPM: [{ providerID: "github-copilot", modelID: "gpt-5.5", variant: "high", weight: 90, cacheMinutes: 480 }],
              executor: [{ providerID: "github-copilot", modelID: "gpt-5.5", variant: "medium", weight: 60, cacheMinutes: 0 }],
            },
          },
        }),
        "staffed workflow start timed out",
      )
      if (!staffedWorkflow.data) throw new Error("staffed workflow start returned no data")

      const updated = yield* Effect.promise(() =>
        sdk.workflow.updateStaffing({
          workflowID: staffedWorkflow.data!.id,
          staffing: {
            mainPM: 1,
            departmentPM: 2,
            executor: 2,
            reviewer: 1,
            tester: 1,
            expert: 1,
          },
          modelWhitelist: {
            mainPM: [{ providerID: "github-copilot", modelID: "gpt-5.5", variant: "xhigh", weight: 95, cacheMinutes: 720 }],
            executor: [{ providerID: "github-copilot", modelID: "gpt-5.5", variant: "medium", weight: 55, cacheMinutes: 0 }],
            tester: [{ providerID: "github-copilot", modelID: "gpt-5.5", weight: 70, cacheMinutes: 120 }],
          },
        }),
      )
      if (!updated.data) throw new Error("workflow staffing update returned no data")

      expect(updated.data.staffing).toMatchObject({
        mainPM: 1,
        departmentPM: 2,
        executor: 2,
        reviewer: 1,
        tester: 1,
        expert: 1,
      })
      expect(updated.data.modelWhitelist?.mainPM?.[0]).toMatchObject({
        providerID: "github-copilot",
        modelID: "gpt-5.5",
        variant: "xhigh",
        weight: 95,
        cacheMinutes: 720,
      })
      expect(updated.data.modelWhitelist?.executor?.[0]).toMatchObject({
        providerID: "github-copilot",
        modelID: "gpt-5.5",
        variant: "medium",
        weight: 55,
        cacheMinutes: 0,
      })
      expect(updated.data.modelWhitelist?.tester?.[0]).toMatchObject({
        providerID: "github-copilot",
        modelID: "gpt-5.5",
        weight: 70,
        cacheMinutes: 120,
      })

      const staffed = yield* requestWithTimeout(
        () => sdk.workflow.graph({ workflowID: staffedWorkflow.data!.id }),
        "staffed workflow graph timed out",
      )
      if (!staffed.data) throw new Error("workflow graph returned no data")
      expect(staffed.data.members.filter((member) => member.role === "department_pm" && member.status === "active")).toHaveLength(2)
      expect(staffed.data.members.filter((member) => member.role === "executor" && member.status === "active")).toHaveLength(2)

      const intervened = yield* Effect.promise(() =>
        sdk.workflow.intervene({
          workflowID: staffedWorkflow.data!.id,
          message: "Requester changes priority: validate the riskiest path first.",
          timing: "interrupt",
          targetRole: "main_pm",
        }),
      )
      if (!intervened.data) throw new Error("workflow intervention returned no data")
      expect(intervened.data.status).toBe("blocked")

      const intervenedGraph = yield* requestWithTimeout(
        () => sdk.workflow.graph({ workflowID: staffedWorkflow.data!.id }),
        "intervened workflow graph timed out",
      )
      if (!intervenedGraph.data) throw new Error("workflow graph returned no data")
      const intervention = intervenedGraph.data.interventions.find((item) =>
        item.message.includes("validate the riskiest path first"),
      )
      expect(intervention).toBeDefined()
      expect(intervention?.targetRole).toBe("main_pm")
      expect(intervention?.timing).toBe("interrupt")
      expect(intervention?.targetSessionID).toBeDefined()
      expect(
        yield* Effect.promise(() => Bun.file(path.join(test.directory, intervention!.path)).exists()),
      ).toBe(true)

      yield* Effect.promise(() => sdk.workflow.cancel({ workflowID: WorkflowID.make(workflow.data!.id) }))
      yield* Effect.promise(() => sdk.workflow.cancel({ workflowID: WorkflowID.make(projectWorkflow.data!.id) }))
      yield* Effect.promise(() => sdk.workflow.cancel({ workflowID: WorkflowID.make(staffedWorkflow.data!.id) }))
    }),
    30_000,
  )

  it.instance("starts workflow from an attachment-style request and explicit model settings", () =>
    Effect.gen(function* () {
      const workflowAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
      process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (workflowAutorun === undefined) return delete process.env.OPENCODE_WORKFLOW_AUTORUN
          process.env.OPENCODE_WORKFLOW_AUTORUN = workflowAutorun
        }),
      )
      const test = yield* TestInstance
      const sdk = client(test.directory)
      const session = yield* Effect.promise(() => sdk.session.create({ title: "Workflow source" }))
      if (!session.data) throw new Error("session create returned no data")
      const request =
        "完善ECS到渲染工作流，你可以参照dev/下面graphics的unity的SRP工作流以及unrealEngine虚幻源码渲染能力、bevy fyrox等对wgpu架构的设计[image:ZirconEngine ECS 到渲染链路完善里程碑计划.md]"

      const workflow = yield* requestWithTimeout(() =>
        sdk.workflow.start({
          workflowStartInput: {
            sessionID: session.data!.id,
            prompt: request,
            model: "github-copilot/gpt-5.5",
            variant: "Xhigh",
            agent: "build",
            staffing: {
              mainPM: 1,
              departmentPM: 2,
              executor: 4,
              reviewer: 2,
              tester: 1,
              expert: 1,
            },
          },
        }),
        "attachment workflow start timed out",
      )
      if (!workflow.data) throw new Error("workflow start returned no data")

      expect(workflow.data.request).toBe(request)
      expect(workflow.data.rootSessionID).toBe(session.data.id)
      expect(workflow.data.model).toMatchObject({
        providerID: "github-copilot",
        modelID: "gpt-5.5",
        variant: "Xhigh",
      })
      expect(path.basename(workflow.data.path)).not.toContain("/")
      expect(path.basename(workflow.data.path)).not.toContain(":")

      yield* Effect.promise(() => sdk.workflow.cancel({ workflowID: WorkflowID.make(workflow.data!.id) }))
    }),
    30_000,
  )

  it.instance("returns an existing session workflow without waiting for path migration", () =>
    Effect.gen(function* () {
      const workflowAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
      process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (workflowAutorun === undefined) return delete process.env.OPENCODE_WORKFLOW_AUTORUN
          process.env.OPENCODE_WORKFLOW_AUTORUN = workflowAutorun
        }),
      )
      const test = yield* TestInstance
      const sdk = client(test.directory)
      const session = yield* Effect.promise(() => sdk.session.create({ title: "Existing workflow source" }))
      if (!session.data) throw new Error("session create returned no data")
      const workflow = yield* requestWithTimeout(() =>
        sdk.workflow.start({
          workflowStartInput: {
            sessionID: session.data!.id,
            prompt: "Build an existing workflow",
          },
        }),
        "workflow start timed out",
      )
      if (!workflow.data) throw new Error("workflow start returned no data")

      const legacyPath = path.join(".opencode", "workflows", "20260704_064850_329_legacy-long-prompt-path")
      yield* Effect.promise(async () => {
        await mkdir(path.join(test.directory, legacyPath), { recursive: true })
        await Bun.write(path.join(test.directory, legacyPath, "marker.txt"), "legacy")
      })
      Database.Client().$client.prepare("UPDATE workflow SET path = ? WHERE id = ?").run(legacyPath, workflow.data.id)

      const existing = yield* requestWithTimeout(() =>
        sdk.workflow.start({
          workflowStartInput: {
            sessionID: session.data!.id,
            prompt: "Build an existing workflow",
          },
        }),
        "existing workflow start timed out",
      )
      if (!existing.data) throw new Error("existing workflow start returned no data")

      expect(existing.data.id).toBe(workflow.data.id)
      expect(existing.data.path).toBe(legacyPath)

      const migratedPath = yield* pollWithTimeout(
        Effect.sync(() => {
          const row = Database.Client().$client.prepare("SELECT path FROM workflow WHERE id = ?").get(workflow.data!.id) as
            | { path: string }
            | undefined
          return row?.path === workflow.data!.path ? row.path : undefined
        }),
        "legacy workflow path migration did not complete",
        "5 seconds",
      )
      expect(migratedPath).toBe(workflow.data.path)

      yield* Effect.promise(() => sdk.workflow.cancel({ workflowID: WorkflowID.make(workflow.data!.id) }))
    }),
    30_000,
  )

  it.instance("upgrades a legacy workflow table before starting with new fields", () =>
    Effect.gen(function* () {
      const workflowAutorun = process.env.OPENCODE_WORKFLOW_AUTORUN
      process.env.OPENCODE_WORKFLOW_AUTORUN = "0"
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (workflowAutorun === undefined) return delete process.env.OPENCODE_WORKFLOW_AUTORUN
          process.env.OPENCODE_WORKFLOW_AUTORUN = workflowAutorun
        }),
      )
      const test = yield* TestInstance
      Database.Client().$client.exec(`
        DROP TABLE IF EXISTS workflow_intervention;
        DROP TABLE IF EXISTS workflow_consultation;
        DROP TABLE IF EXISTS workflow_edge;
        DROP TABLE IF EXISTS workflow_milestone;
        DROP TABLE IF EXISTS workflow_member;
        DROP TABLE IF EXISTS workflow;
        CREATE TABLE workflow (
          id text PRIMARY KEY NOT NULL,
          project_id text NOT NULL,
          root_session_id text,
          request text NOT NULL,
          title text NOT NULL,
          directory text NOT NULL,
          path text NOT NULL,
          xml text NOT NULL,
          status text NOT NULL,
          time_created integer NOT NULL,
          time_updated integer NOT NULL
        );
      `)
      const sdk = client(test.directory)
      const session = yield* Effect.promise(() => sdk.session.create({ title: "Legacy workflow source" }))
      if (!session.data) throw new Error("session create returned no data")

      const workflow = yield* requestWithTimeout(() =>
        sdk.workflow.start({
          workflowStartInput: {
            sessionID: session.data!.id,
            prompt: "Start after legacy workflow schema",
            model: "github-copilot/gpt-5.5",
            agent: "build",
          },
        }),
        "legacy workflow start timed out",
      )
      if (!workflow.data) throw new Error("workflow start returned no data")

      expect(workflow.data.model).toMatchObject({
        providerID: "github-copilot",
        modelID: "gpt-5.5",
      })

      yield* Effect.promise(() => sdk.workflow.cancel({ workflowID: WorkflowID.make(workflow.data!.id) }))
    }),
    30_000,
  )

  it.instance("returns declared invalid request errors", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const response = yield* Effect.promise(() =>
        Promise.resolve(
          testApp().request(WorkflowPaths.start, {
            method: "POST",
            headers: {
              "content-type": "application/json",
              "x-opencode-directory": test.directory,
            },
            body: JSON.stringify({ sessionID: "ses_missing", prompt: "missing" }),
          }),
        ),
      )

      expect(response.status).toBe(400)
      expect(yield* Effect.promise(() => response.json())).toMatchObject({
        _tag: "InvalidRequestError",
        message: "Session not found: ses_missing",
      })
    }),
  )

  it.instance("returns declared not found errors", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const response = yield* Effect.promise(() =>
        Promise.resolve(
          testApp().request(WorkflowPaths.graph.replace(":workflowID", WorkflowID.descending()), {
            headers: { "x-opencode-directory": test.directory },
          }),
        ),
      )

      expect(response.status).toBe(404)
      expect(yield* Effect.promise(() => response.json())).toMatchObject({
        name: "NotFoundError",
      })
    }),
  )
})
