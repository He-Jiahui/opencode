import * as InstanceState from "@/effect/instance-state"
import { File } from "@/file"
import { FileIgnore } from "@/file/ignore"
import * as PlanFile from "@/file/plan"
import { Ripgrep } from "@/file/ripgrep"
import { Effect } from "effect"
import path from "path"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"

export const fileHandlers = HttpApiBuilder.group(InstanceHttpApi, "file", (handlers) =>
  Effect.gen(function* () {
    const svc = yield* File.Service
    const ignore = yield* FileIgnore.Service
    const ripgrep = yield* Ripgrep.Service

    const findText = Effect.fn("FileHttpApi.findText")(function* (ctx: { query: { pattern: string } }) {
      const instance = yield* InstanceState.context
      const ignored = yield* ignore.patterns()
      return (yield* ripgrep
        .search({ cwd: instance.directory, pattern: ctx.query.pattern, ignore: ignored, limit: 10 })
        .pipe(Effect.orDie)).items.filter((item) => {
        const file = path.isAbsolute(item.path.text)
          ? path.relative(instance.directory, item.path.text)
          : item.path.text
        return !FileIgnore.matchWithPatterns(file, ignored)
      })
    })

    const findFile = Effect.fn("FileHttpApi.findFile")(function* (ctx: {
      query: { query: string; dirs?: "true" | "false"; type?: "file" | "directory"; limit?: number }
    }) {
      return yield* svc.search({
        query: ctx.query.query,
        limit: ctx.query.limit ?? 10,
        dirs: ctx.query.dirs !== "false",
        type: ctx.query.type,
      })
    })

    const findSymbol = Effect.fn("FileHttpApi.findSymbol")(function* () {
      return []
    })

    const list = Effect.fn("FileHttpApi.list")(function* (ctx: { query: { path: string } }) {
      return yield* svc.list(ctx.query.path)
    })

    const content = Effect.fn("FileHttpApi.content")(function* (ctx: { query: { path: string } }) {
      return yield* svc.read(ctx.query.path)
    })

    const savePlan = Effect.fn("FileHttpApi.savePlan")(function* (ctx: {
      payload: { title?: string; content: string }
    }) {
      const instance = yield* InstanceState.context
      return yield* Effect.promise(() =>
        PlanFile.save({
          worktree: instance.worktree,
          title: ctx.payload.title,
          content: ctx.payload.content,
        }),
      )
    })

    const ignoreGet = Effect.fn("FileHttpApi.ignoreGet")(function* () {
      return yield* ignore.get()
    })

    const ignoreUpdate = Effect.fn("FileHttpApi.ignoreUpdate")(function* (ctx: { payload: { content: string } }) {
      return yield* ignore.write(ctx.payload.content)
    })

    const status = Effect.fn("FileHttpApi.status")(function* () {
      return yield* svc.status()
    })

    return handlers
      .handle("findText", findText)
      .handle("findFile", findFile)
      .handle("findSymbol", findSymbol)
      .handle("list", list)
      .handle("content", content)
      .handle("savePlan", savePlan)
      .handle("ignoreGet", ignoreGet)
      .handle("ignoreUpdate", ignoreUpdate)
      .handle("status", status)
  }),
)
