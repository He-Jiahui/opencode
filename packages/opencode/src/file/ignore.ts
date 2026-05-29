import path from "path"
import { Glob } from "@opencode-ai/core/util/glob"
import { Global } from "@opencode-ai/core/global"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Effect, Context, Layer, Schema } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { containsPath } from "@/project/instance-context"

export const DEFAULT = [
  "node_modules",
  "bower_components",
  ".pnpm-store",
  "vendor",
  ".npm",
  "dist",
  "build",
  "out",
  ".next",
  "target",
  "bin",
  "obj",
  ".git",
  ".svn",
  ".hg",
  ".vscode",
  ".idea",
  ".turbo",
  ".output",
  "desktop",
  ".sst",
  ".cache",
  ".webkit-cache",
  "__pycache__",
  ".pytest_cache",
  "mypy_cache",
  ".history",
  ".gradle",
  "**/*.swp",
  "**/*.swo",
  "**/*.pyc",
  "**/.DS_Store",
  "**/Thumbs.db",
  "**/logs/**",
  "**/tmp/**",
  "**/temp/**",
  "**/*.log",
  "**/coverage/**",
  "**/.nyc_output/**",
].join("\n")

export const Info = Schema.Struct({
  path: Schema.String,
  source: Schema.Literals(["project", "user", "default"]),
  content: Schema.String,
}).annotate({ identifier: "IgnoreFile" })
export type Info = typeof Info.Type

export interface Interface {
  readonly get: () => Effect.Effect<Info>
  readonly write: (content: string) => Effect.Effect<Info>
  readonly patterns: () => Effect.Effect<string[]>
  readonly filter: (files: string[]) => Effect.Effect<string[]>
  readonly match: (file: string) => Effect.Effect<boolean>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/FileIgnore") {}

export function defaultPath() {
  return path.join(Global.Path.home, ".opencode", ".ignore")
}

export function projectPath(worktree: string) {
  return path.join(worktree, ".opencode", ".ignore")
}

export function parse(content: string) {
  return content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"))
}

export const PATTERNS = parse(DEFAULT)

export function matches(pattern: string, file: string) {
  const normalized = file.replaceAll("\\", "/").replace(/\/$/, "")
  const clean = pattern.replaceAll("\\", "/").replace(/\/$/, "").replace(/^\//, "")
  if (clean.includes("*")) {
    if (Glob.match(clean, normalized)) return true
    if (clean.startsWith("**/")) {
      if (Glob.match(clean.slice(3), normalized)) return true
      if (Glob.match(clean.replace(/^\*\*\//, "**/"), normalized)) return true
    }
    if (!clean.includes("/")) return normalized.split("/").some((part) => Glob.match(clean, part))
    return false
  }
  if (!clean.includes("/")) return normalized.split("/").includes(clean)
  if (normalized === clean) return true
  return normalized.startsWith(clean + "/")
}

export function matchWithPatterns(file: string, patterns: string[]) {
  return patterns.reduce((ignored, pattern) => {
    const negated = pattern.startsWith("!")
    const clean = negated ? pattern.slice(1) : pattern
    if (!clean) return ignored
    if (!matches(clean, file)) return ignored
    return !negated
  }, false)
}

export function match(
  filepath: string,
  opts?: {
    extra?: string[]
    whitelist?: string[]
  },
) {
  const patterns = [...parse(DEFAULT), ...(opts?.extra ?? [])]
  const whitelist = opts?.whitelist ?? []
  if (whitelist.some((pattern) => matches(pattern, filepath))) return false
  return matchWithPatterns(filepath, patterns)
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* AppFileSystem.Service

    const get = Effect.fn("FileIgnore.get")(function* () {
      const ctx = yield* InstanceState.context
      const project = projectPath(ctx.worktree === path.parse(ctx.worktree).root ? ctx.directory : ctx.worktree)
      const projectContent = yield* fs.readFileStringSafe(project).pipe(Effect.orDie)
      if (projectContent !== undefined) return { path: project, source: "project" as const, content: projectContent }

      const user = defaultPath()
      const userContent = yield* fs.readFileStringSafe(user).pipe(Effect.orDie)
      if (userContent !== undefined) return { path: user, source: "user" as const, content: userContent }

      return { path: project, source: "default" as const, content: DEFAULT }
    })

    const write = Effect.fn("FileIgnore.write")(function* (content: string) {
      const ctx = yield* InstanceState.context
      const file = projectPath(ctx.worktree === path.parse(ctx.worktree).root ? ctx.directory : ctx.worktree)
      if (!containsPath(file, ctx)) {
        throw new Error("Access denied: ignore file path escapes project directory")
      }
      yield* fs.writeWithDirs(file, content).pipe(Effect.orDie)
      return { path: file, source: "project" as const, content }
    })

    const patterns = Effect.fn("FileIgnore.patterns")(function* () {
      return parse((yield* get()).content)
    })

    const isIgnored = Effect.fn("FileIgnore.match")(function* (file: string) {
      return matchWithPatterns(file, yield* patterns())
    })

    const filter = Effect.fn("FileIgnore.filter")(function* (files: string[]) {
      const parsed = yield* patterns()
      return files.filter((file) => !matchWithPatterns(file, parsed))
    })

    return Service.of({ get, write, patterns, filter, match: isIgnored })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(AppFileSystem.defaultLayer))

export * as FileIgnore from "./ignore"
