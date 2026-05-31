import path from "path"
import { describe, expect, test } from "bun:test"

const promptDir = path.join(import.meta.dir, "..", "..", "src", "session", "prompt")

describe("plan mode prompts", () => {
  test("treats .codex skills and plans as project planning context", async () => {
    for (const name of ["plan.txt", "plan-mode.txt", "plan-reminder-anthropic.txt"]) {
      const prompt = await Bun.file(path.join(promptDir, name)).text()

      expect(prompt).toContain(".opencode/plans/**")
      expect(prompt).toContain(".opencode/skill/**")
      expect(prompt).toContain(".opencode/skills/**")
      expect(prompt).toContain(".codex/plans/**")
      expect(prompt).toContain(".codex/skill/**")
      expect(prompt).toContain(".codex/skills/**")
      expect(prompt).toContain(".codex/AGENTS.md")
    }
  })

  test("requires reusable markdown plans inside opencode_plan tags", async () => {
    for (const name of ["plan.txt", "plan-mode.txt", "plan-reminder-anthropic.txt"]) {
      const prompt = await Bun.file(path.join(promptDir, name)).text()

      expect(prompt).toContain('<opencode_plan title="Short filename-friendly plan title">')
      expect(prompt).toContain("</opencode_plan>")
      expect(prompt).toContain("exact valid markdown plan")
      expect(prompt).toContain("Do not put these tags inside a code fence")
    }
  })
})
