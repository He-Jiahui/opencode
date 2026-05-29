import path from "path"
import { describe, expect, test } from "bun:test"
import { filename, save } from "../../src/file/plan"
import { tmpdir } from "../fixture/fixture"

describe("plan files", () => {
  test("sanitizes plan titles into workspace filenames", () => {
    expect(filename({ title: 'CON<>:"/\\|?* .', content: "# ignored" })).toBe("CON-plan.md")
  })

  test("saves plans under .opencode/plans with a unique name", async () => {
    await using tmp = await tmpdir()

    const first = await save({ worktree: tmp.path, title: "Particle/Plan", content: "# Particle Plan" })
    const second = await save({ worktree: tmp.path, title: "Particle/Plan", content: "# Particle Plan v2" })

    expect(first).toEqual({ title: "Particle/Plan", path: path.join(".opencode", "plans", "Particle Plan.md") })
    expect(second.path).toBe(path.join(".opencode", "plans", "Particle Plan-1.md"))
    expect(await Bun.file(path.join(tmp.path, first.path)).text()).toBe("# Particle Plan")
    expect(await Bun.file(path.join(tmp.path, second.path)).text()).toBe("# Particle Plan v2")
  })
})
