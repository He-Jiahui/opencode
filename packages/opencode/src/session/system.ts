import path from "path"
import { Context, Effect, Layer } from "effect"

import { InstanceState } from "@/effect/instance-state"

import PROMPT_ANTHROPIC from "./prompt/anthropic.txt"
import PROMPT_DEFAULT from "./prompt/default.txt"
import PROMPT_BEAST from "./prompt/beast.txt"
import PROMPT_GEMINI from "./prompt/gemini.txt"
import PROMPT_GPT from "./prompt/gpt.txt"
import PROMPT_KIMI from "./prompt/kimi.txt"

import PROMPT_CODEX from "./prompt/codex.txt"
import PROMPT_TRINITY from "./prompt/trinity.txt"
import type { Provider } from "@/provider/provider"
import type { Agent } from "@/agent/agent"
import { Permission } from "@/permission"
import { Skill } from "@/skill"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { FileIgnore } from "@/file/ignore"

const OVERVIEW_DIRS = new Set([
  "app",
  "apps",
  "bin",
  "cmd",
  "config",
  "docs",
  "lib",
  "package",
  "packages",
  "src",
  "test",
  "tests",
])
const OVERVIEW_ROOT_LIMIT = 80
const OVERVIEW_CHILD_LIMIT = 25

export function provider(model: Provider.Model) {
  if (model.api.id.includes("gpt-4") || model.api.id.includes("o1") || model.api.id.includes("o3"))
    return [PROMPT_BEAST]
  if (model.api.id.includes("gpt")) {
    if (model.api.id.includes("codex")) {
      return [PROMPT_CODEX]
    }
    return [PROMPT_GPT]
  }
  if (model.api.id.includes("gemini-")) return [PROMPT_GEMINI]
  if (model.api.id.includes("claude")) return [PROMPT_ANTHROPIC]
  if (model.api.id.toLowerCase().includes("trinity")) return [PROMPT_TRINITY]
  if (model.api.id.toLowerCase().includes("kimi")) return [PROMPT_KIMI]
  return [PROMPT_DEFAULT]
}

export interface Interface {
  readonly environment: (model: Provider.Model) => Effect.Effect<string[]>
  readonly skills: (agent: Agent.Info) => Effect.Effect<string | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SystemPrompt") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* AppFileSystem.Service
    const ignore = yield* FileIgnore.Service
    const skill = yield* Skill.Service

    const countChildren = Effect.fn("SystemPrompt.countChildren")(function* (dir: string, ignored: string[]) {
      const ctx = yield* InstanceState.context
      const entries = yield* fs.readDirectoryEntries(dir).pipe(Effect.orElseSucceed(() => []))
      return entries.filter((entry) => {
        if (entry.name === ".DS_Store") return false
        return !FileIgnore.matchWithPatterns(path.relative(ctx.directory, path.join(dir, entry.name)), ignored)
      }).length
    })

    const projectFiles = Effect.fn("SystemPrompt.projectFiles")(function* () {
      const ctx = yield* InstanceState.context
      if (ctx.directory === path.parse(ctx.directory).root) return

      const ignored = yield* ignore.patterns()
      const entries = yield* fs.readDirectoryEntries(ctx.directory).pipe(Effect.orElseSucceed(() => []))
      const visible = entries
        .filter((entry) => {
          if (entry.name === ".DS_Store") return false
          return !FileIgnore.matchWithPatterns(entry.name, ignored)
        })
        .sort((a, b) => {
          if (a.type !== b.type) return a.type === "directory" ? -1 : 1
          return a.name.localeCompare(b.name)
        })

      if (visible.length === 0) return

      const root = yield* Effect.forEach(
        visible.slice(0, OVERVIEW_ROOT_LIMIT),
        Effect.fnUntraced(function* (entry) {
          if (entry.type !== "directory") return entry.name
          return `${entry.name}/ (${yield* countChildren(path.join(ctx.directory, entry.name), ignored)} entries)`
        }),
        { concurrency: 16 },
      )
      const dirs = visible.filter((entry) => entry.type === "directory" && OVERVIEW_DIRS.has(entry.name.toLowerCase()))
      const children = yield* Effect.forEach(
        dirs,
        Effect.fnUntraced(function* (dir) {
          const full = path.join(ctx.directory, dir.name)
          const items = (yield* fs.readDirectoryEntries(full).pipe(Effect.orElseSucceed(() => [])))
            .filter((entry) => {
              if (entry.name === ".DS_Store") return false
              return !FileIgnore.matchWithPatterns(path.join(dir.name, entry.name), ignored)
            })
            .sort((a, b) => {
              if (a.type !== b.type) return a.type === "directory" ? -1 : 1
              return a.name.localeCompare(b.name)
            })
          if (items.length === 0) return

          return [
            `${dir.name}/:`,
            ...items.slice(0, OVERVIEW_CHILD_LIMIT).map((entry) => `  ${entry.name}${entry.type === "directory" ? "/" : ""}`),
            ...(items.length > OVERVIEW_CHILD_LIMIT
              ? [`  ... (${items.length - OVERVIEW_CHILD_LIMIT} more entries)`]
              : []),
          ].join("\n")
        }),
        { concurrency: 8 },
      )
      return [
        "Project file overview:",
        "<files>",
        ...root,
        ...(visible.length > OVERVIEW_ROOT_LIMIT ? [`... (${visible.length - OVERVIEW_ROOT_LIMIT} more entries)`] : []),
        ...children.filter((item): item is string => Boolean(item)),
        "</files>",
      ].join("\n")
    })

    return Service.of({
      environment: Effect.fn("SystemPrompt.environment")(function* (model: Provider.Model) {
        const ctx = yield* InstanceState.context
        const files = yield* projectFiles().pipe(Effect.orElseSucceed(() => undefined))
        return [
          [
            `You are powered by the model named ${model.api.id}. The exact model ID is ${model.providerID}/${model.api.id}`,
            `Here is some useful information about the environment you are running in:`,
            `<env>`,
            `  Working directory: ${ctx.directory}`,
            `  Workspace root folder: ${ctx.worktree}`,
            `  Is directory a git repo: ${ctx.project.vcs === "git" ? "yes" : "no"}`,
            `  Platform: ${process.platform}`,
            `  Today's date: ${new Date().toDateString()}`,
            `</env>`,
          ].join("\n"),
          ...(files ? [files] : []),
        ]
      }),

      skills: Effect.fn("SystemPrompt.skills")(function* (agent: Agent.Info) {
        if (Permission.disabled(["skill"], agent.permission).has("skill")) return

        const list = yield* skill.available(agent)

        return [
          "Skills provide specialized instructions and workflows for specific tasks.",
          "Use the skill tool to load a skill when a task matches its description.",
          // the agents seem to ingest the information about skills a bit better if we present a more verbose
          // version of them here and a less verbose version in tool description, rather than vice versa.
          Skill.fmt(list, { verbose: true }),
        ].join("\n")
      }),
    })
  }),
)

export const defaultLayer = layer.pipe(
  Layer.provide(FileIgnore.defaultLayer),
  Layer.provide(AppFileSystem.defaultLayer),
  Layer.provide(Skill.defaultLayer),
)

export * as SystemPrompt from "./system"
