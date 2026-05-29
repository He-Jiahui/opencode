import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import fs from "fs/promises"
import path from "path"
import type { Agent } from "../../src/agent/agent"
import { NamedError } from "@opencode-ai/core/util/error"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { FileIgnore } from "@/file/ignore"
import { Skill } from "../../src/skill"
import { Permission } from "../../src/permission"
import { SystemPrompt } from "../../src/session/system"
import { Provider } from "@/provider/provider"
import { TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const skills: Skill.Info[] = [
  {
    name: "zeta-skill",
    description: "Zeta skill.",
    location: "/tmp/zeta-skill/SKILL.md",
    content: "# zeta-skill",
  },
  {
    name: "alpha-skill",
    description: "Alpha skill.",
    location: "/tmp/alpha-skill/SKILL.md",
    content: "# alpha-skill",
  },
  {
    name: "middle-skill",
    description: "Middle skill.",
    location: "/tmp/middle-skill/SKILL.md",
    content: "# middle-skill",
  },
  {
    name: "manual-skill",
    location: "/tmp/manual-skill/SKILL.md",
    content: "# manual-skill",
  },
]

const build: Agent.Info = {
  name: "build",
  mode: "primary",
  permission: Permission.fromConfig({ "*": "allow" }),
  options: {},
}

const it = testEffect(
  SystemPrompt.layer.pipe(
    Layer.provide(FileIgnore.defaultLayer),
    Layer.provide(AppFileSystem.defaultLayer),
    Layer.provide(
      Layer.succeed(
        Skill.Service,
        Skill.Service.of({
          get: (name) => Effect.succeed(skills.find((skill) => skill.name === name)),
          require: (name) => {
            const info = skills.find((skill) => skill.name === name)
            if (info) return Effect.succeed(info)
            return Effect.fail(new Skill.NotFoundError({ name, available: skills.map((skill) => skill.name) }))
          },
          all: () => Effect.succeed(skills),
          dirs: () => Effect.succeed([]),
          available: () => Effect.succeed(skills),
        }),
      ),
    ),
  ),
)

describe("session.system", () => {
  it.instance(
    "environment includes project file overview with directory child counts",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        yield* Effect.promise(() => fs.mkdir(path.join(test.directory, "src"), { recursive: true }))
        yield* Effect.promise(() => fs.mkdir(path.join(test.directory, ".opencode"), { recursive: true }))
        yield* Effect.promise(() => fs.mkdir(path.join(test.directory, "ignored"), { recursive: true }))
        yield* Effect.promise(() => fs.writeFile(path.join(test.directory, "src", "index.ts"), ""))
        yield* Effect.promise(() => fs.writeFile(path.join(test.directory, "src", "main.ts"), ""))
        yield* Effect.promise(() => fs.writeFile(path.join(test.directory, ".opencode", ".ignore"), "ignored\n"))
        yield* Effect.promise(() => fs.writeFile(path.join(test.directory, "ignored", "hidden.ts"), ""))

        const prompt = yield* SystemPrompt.Service
        const result = (yield* prompt.environment({
          api: { id: "gpt-test" },
          providerID: "test",
        } as Provider.Model)).join("\n")

        expect(result).toContain("Project file overview:")
        expect(result).toContain("src/ (2 entries)")
        expect(result).toContain("  index.ts")
        expect(result).not.toContain("ignored/")
      }),
    { git: true },
  )

  it.effect("skills output is sorted by name and stable across calls", () =>
    Effect.gen(function* () {
      const prompt = yield* SystemPrompt.Service
      const first = yield* prompt.skills(build)
      const second = yield* prompt.skills(build)
      const output = first ?? (yield* Effect.fail(new NamedError.Unknown({ message: "missing skills output" })))

      expect(first).toBe(second)

      const alpha = output.indexOf("<name>alpha-skill</name>")
      const middle = output.indexOf("<name>middle-skill</name>")
      const zeta = output.indexOf("<name>zeta-skill</name>")

      expect(alpha).toBeGreaterThan(-1)
      expect(middle).toBeGreaterThan(alpha)
      expect(zeta).toBeGreaterThan(middle)
      expect(output).not.toContain("manual-skill")
    }),
  )
})
