import { afterEach, describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import path from "path"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { createOpencodeClient } from "@opencode-ai/sdk/v2"
import { InstanceBootstrap } from "../../src/project/bootstrap-service"
import { InstanceStore } from "../../src/project/instance-store"
import { Server } from "../../src/server/server"
import { WorkflowPaths } from "../../src/server/routes/instance/httpapi/groups/workflow"
import { WorkflowID } from "../../src/workflow/schema"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(
  Layer.mergeAll(
    AppFileSystem.defaultLayer,
    CrossSpawnSpawner.defaultLayer,
    InstanceStore.defaultLayer.pipe(
      Layer.provide(Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))),
    ),
  ),
)

function serverFetch() {
  const app = Server.Default().app
  return Object.assign(
    (request: RequestInfo | URL, init?: RequestInit) =>
      Promise.resolve(app.fetch(request instanceof Request ? request : new Request(request, init))),
    { preconnect: globalThis.fetch.preconnect },
  ) satisfies typeof globalThis.fetch
}

function client(directory: string) {
  return createOpencodeClient({
    baseUrl: "http://localhost",
    directory,
    fetch: serverFetch(),
    throwOnError: true,
  })
}

describe("workflow HttpApi", () => {
  afterEach(async () => {
    await disposeAllInstances()
    await resetDatabase()
  })

  it.instance("starts workflow through the generated SDK", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const sdk = client(test.directory)
      const session = yield* Effect.promise(() => sdk.session.create({ title: "Workflow source" }))
      if (!session.data) throw new Error("session create returned no data")
      const workflow = yield* Effect.promise(() =>
        sdk.workflow.start({
          workflowStartInput: {
            sessionID: session.data.id,
            prompt: "Build a targeted workflow",
          },
        }),
      )
      if (!workflow.data) throw new Error("workflow start returned no data")

      expect(workflow.data.request).toBe("Build a targeted workflow")
      expect(workflow.data.rootSessionID).toBe(session.data.id)
      expect(workflow.data.path).toBe(`.opencode\\workflows\\${workflow.data.id}`)

      const graph = yield* Effect.promise(() => sdk.workflow.graph({ workflowID: workflow.data.id }))
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
      yield* Effect.promise(() => sdk.workflow.cancel({ workflowID: workflow.data!.id }))
    }),
    15_000,
  )

  it.instance("starts workflow as a project-level manager without a source session", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const sdk = client(test.directory)
      const workflow = yield* Effect.promise(() =>
        sdk.workflow.start({
          workflowStartInput: {
            prompt: "Run a workflow independent from the current chat",
          },
        }),
      )
      if (!workflow.data) throw new Error("workflow start returned no data")

      expect(workflow.data.rootSessionID).toBeUndefined()
      expect(workflow.data.request).toBe("Run a workflow independent from the current chat")

      const list = yield* Effect.promise(() => sdk.workflow.list())
      expect(list.data?.map((item) => item.id)).toContain(workflow.data.id)
      yield* Effect.promise(() => sdk.workflow.cancel({ workflowID: WorkflowID.make(workflow.data!.id) }))
    }),
  )

  it.instance("returns declared invalid request errors", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const response = yield* Effect.promise(() =>
        Promise.resolve(
          Server.Default().app.request(WorkflowPaths.start, {
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
          Server.Default().app.request(WorkflowPaths.graph.replace(":workflowID", WorkflowID.descending()), {
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
