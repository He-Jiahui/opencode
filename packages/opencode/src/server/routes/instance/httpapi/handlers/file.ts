import * as InstanceState from "@/effect/instance-state"
import { FileSystem } from "@opencode-ai/core/filesystem"
import * as FileIgnore from "@opencode-ai/core/filesystem/ignore"
import * as PlanFile from "@opencode-ai/core/filesystem/plan"
import { LocationServiceMap, locationServiceMapLayer } from "@opencode-ai/core/location-services"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath, RelativePath } from "@opencode-ai/core/schema"
import { Effect, Layer, Option } from "effect"
import createIgnore from "ignore"
import path from "path"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"

export const fileHandlers = HttpApiBuilder.group(InstanceHttpApi, "file", (handlers) =>
  Effect.gen(function* () {
    const ripgrep = yield* Ripgrep.Service
    const locations = yield* LocationServiceMap.Service

    const filesystem = Effect.fnUntraced(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
      return yield* effect.pipe(
        Effect.provide(
          locations.get(Location.Ref.make({ directory: AbsolutePath.make((yield* InstanceState.context).directory) })),
        ),
      )
    })

    const ignorePath = (instance: { directory: string; worktree: string }) =>
      FileIgnore.projectPath(
        instance.worktree === path.parse(instance.worktree).root ? instance.directory : instance.worktree,
      )

    const ignoreGet = Effect.fn("FileHttpApi.ignoreGet")(function* () {
      const instance = yield* InstanceState.context
      const fs = yield* FSUtil.Service
      const project = ignorePath(instance)
      const projectContent = yield* fs.readFileStringSafe(project).pipe(Effect.orDie)
      if (projectContent !== undefined) return { path: project, source: "project" as const, content: projectContent }

      const user = FileIgnore.defaultPath()
      const userContent = yield* fs.readFileStringSafe(user).pipe(Effect.orDie)
      if (userContent !== undefined) return { path: user, source: "user" as const, content: userContent }

      return { path: project, source: "default" as const, content: FileIgnore.DEFAULT }
    })

    const ignoreUpdate = Effect.fn("FileHttpApi.ignoreUpdate")(function* (ctx: { payload: { content: string } }) {
      const instance = yield* InstanceState.context
      const fs = yield* FSUtil.Service
      const root = instance.worktree === path.parse(instance.worktree).root ? instance.directory : instance.worktree
      const file = ignorePath(instance)
      if (!FSUtil.contains(root, file)) {
        return yield* Effect.die(new Error("Access denied: ignore file path escapes project directory"))
      }
      yield* fs.writeWithDirs(file, ctx.payload.content).pipe(Effect.orDie)
      return { path: file, source: "project" as const, content: ctx.payload.content }
    })

    const findText = Effect.fn("FileHttpApi.findText")(function* (ctx: { query: { pattern: string } }) {
      const instance = yield* InstanceState.context
      const ignored = FileIgnore.parse((yield* ignoreGet()).content)
      return (yield* ripgrep.grep({ cwd: instance.directory, pattern: ctx.query.pattern, limit: 10 }).pipe(Effect.orDie))
        .map((match) => ({
          path: { text: match.entry.path },
          lines: { text: match.text },
          line_number: match.line,
          absolute_offset: match.offset,
          submatches: match.submatches.map((submatch) => ({
            match: { text: submatch.text },
            start: submatch.start,
            end: submatch.end,
          })),
        }))
        .filter((item) => {
          const file = path.isAbsolute(item.path.text) ? path.relative(instance.directory, item.path.text) : item.path.text
          return !FileIgnore.matchWithPatterns(file, ignored)
        })
    })

    const findFile = Effect.fn("FileHttpApi.findFile")(function* (ctx: {
      query: { query: string; dirs?: "true" | "false"; type?: "file" | "directory"; limit?: number }
    }) {
      const directory = (yield* InstanceState.context).directory
      const limit = ctx.query.limit ?? 10
      const type = ctx.query.type ?? (ctx.query.dirs === "false" ? "file" : undefined)
      const started = performance.now()
      const found = yield* filesystem(FileSystem.Service.use((fs) => fs.find({ query: ctx.query.query, limit, type })))
      yield* Effect.logInfo("find file", {
        query: ctx.query.query,
        type,
        directory,
        limit,
        results: found.length,
        duration: Math.round(performance.now() - started),
      })
      return found.map((item) => item.path)
    })

    const findSymbol = Effect.fn("FileHttpApi.findSymbol")(function* () {
      return []
    })

    const list = Effect.fn("FileHttpApi.list")(function* (ctx: { query: { path: string } }) {
      const instance = yield* InstanceState.context
      return yield* filesystem(
        Effect.gen(function* () {
          const fs = yield* FileSystem.Service
          const raw = yield* FSUtil.Service
          const location = yield* Location.Service
          const ignored = createIgnore()
          const gitignore = yield* raw
            .readFileString(path.join(location.project.directory, ".gitignore"))
            .pipe(Effect.catch(() => Effect.succeed("")))
          if (gitignore) ignored.add(gitignore)
          const ignorefile = yield* raw.readFileStringSafe(ignorePath(instance)).pipe(Effect.orDie)
          if (ignorefile) ignored.add(ignorefile)
          return (yield* fs.list({ path: RelativePath.make(ctx.query.path) })).map((item) => ({
            name: path.basename(item.path),
            path: item.path,
            absolute: path.resolve(location.directory, item.path),
            type: item.type,
            ignored: ignored.ignores(
              path.relative(location.project.directory, path.resolve(location.directory, item.path)) +
                (item.type === "directory" ? "/" : ""),
            ),
          }))
        }),
      )
    })

    const content = Effect.fn("FileHttpApi.content")(function* (ctx: { query: { path: string } }) {
      const directory = (yield* InstanceState.context).directory
      const file = path.resolve(directory, ctx.query.path)
      if (!FSUtil.contains(directory, file)) return yield* Effect.die(new Error("Path escapes the location"))
      if (!(yield* FSUtil.Service.use((fs) => fs.existsSafe(file)))) return { type: "text" as const, content: "" }
      return yield* filesystem(
        FileSystem.Service.use((fs) => fs.read({ path: RelativePath.make(ctx.query.path) })),
      ).pipe(
        Effect.flatMap((item) =>
          Effect.gen(function* () {
            const text = item.content.includes(0)
              ? Option.none<string>()
              : yield* Effect.sync(() => new TextDecoder("utf-8", { fatal: true }).decode(item.content)).pipe(
                  Effect.option,
                )
            return { item, text }
          }),
        ),
        Effect.map(({ item, text }) =>
          Option.isSome(text)
            ? { type: "text" as const, content: text.value.trim() }
            : {
                type: "binary" as const,
                content: Buffer.from(item.content).toString("base64"),
                encoding: "base64" as const,
                mimeType: item.mime,
              },
        ),
      )
    })

    const status = Effect.fn("FileHttpApi.status")(function* () {
      return []
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
).pipe(Layer.provide(locationServiceMapLayer))
