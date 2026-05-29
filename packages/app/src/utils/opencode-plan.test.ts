import { describe, expect, test } from "bun:test"
import { parseOpencodePlanBlocks, parseOpencodePlanSegments } from "./opencode-plan"

describe("opencode plan parsing", () => {
  test("extracts plan blocks and preserves surrounding markdown", () => {
    const segments = parseOpencodePlanSegments([
      "before",
      '<opencode_plan title="Particle Roadmap">',
      "# Ignored Heading",
      "- Build CPU particles",
      "</opencode_plan>",
      "after",
    ].join("\n"))

    expect(segments).toEqual([
      { type: "text", text: "before\n" },
      {
        type: "plan",
        id: "0",
        title: "Particle Roadmap",
        content: "# Ignored Heading\n- Build CPU particles",
      },
      { type: "text", text: "\nafter" },
    ])
  })

  test("uses the first markdown heading as the fallback title", () => {
    expect(
      parseOpencodePlanBlocks("<opencode_plan>\n## Hub Completion Plan\n- task\n</opencode_plan>")[0]?.title,
    ).toBe("Hub Completion Plan")
  })
})
