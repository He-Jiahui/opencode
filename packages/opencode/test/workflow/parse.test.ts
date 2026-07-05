import { describe, expect, test } from "bun:test"

import { parseWorkflowXml, workflowPipelineItemsMissing, WorkflowMilestoneID, WorkflowParseError } from "@/workflow"

describe("parseWorkflowXml", () => {
  test("parses milestones with metadata and prompt text", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <milestone id="plan" title="Plan" agent="planner">Write a plan</milestone>
      </workflow>
    `)

    expect(workflow.milestones).toEqual([
      {
        type: "milestone",
        id: WorkflowMilestoneID.make("plan"),
        title: "Plan",
        department: "planner",
        prompt: "Write a plan",
        dependsOn: [],
      },
    ])
  })

  test("turns ordered groups into dependency edges", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <ordered>
          <milestone id="plan">Plan</milestone>
          <parallel>
            <milestone id="api">API</milestone>
            <milestone id="tests">Tests</milestone>
          </parallel>
          <milestone id="finish">Finish</milestone>
        </ordered>
      </workflow>
    `)

    expect(workflow.milestones.map((milestone) => [String(milestone.id), milestone.dependsOn.map(String)])).toEqual([
      ["plan", []],
      ["api", ["plan"]],
      ["tests", ["plan"]],
      ["finish", ["api", "tests"]],
    ])
  })

  test("treats pipeline groups as ordered structural stages", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <pipeline>
          <milestone id="audit">Audit one item</milestone>
          <parallel>
            <milestone id="fix">Fix item</milestone>
            <milestone id="docs">Document item</milestone>
          </parallel>
          <milestone id="verify">Verify item</milestone>
        </pipeline>
      </workflow>
    `)

    expect(workflow.steps.type).toBe("pipeline")
    expect(workflow.milestones.map((milestone) => [String(milestone.id), milestone.dependsOn.map(String)])).toEqual([
      ["audit", []],
      ["fix", ["audit"]],
      ["docs", ["audit"]],
      ["verify", ["fix", "docs"]],
    ])
  })

  test("expands pipeline items into per-item stages without a batch barrier", () => {
    const workflow = parseWorkflowXml(
      `
        <workflow>
          <ordered>
            <milestone id="prepare">Prepare shared inputs</milestone>
            <pipeline items="work/shared/audit-tracks.json">
              <milestone id="audit" title="Audit {item.title}" department="{item.department}">Audit {item.id}: {item.path}</milestone>
              <milestone id="review" title="Review {item.title}" department="quality" depends="audit">Review {item.id}</milestone>
            </pipeline>
            <milestone id="finish">Finish synthesis</milestone>
          </ordered>
        </workflow>
      `,
      {
        readPipelineItems: () => [
          { id: "graphics", title: "Graphics", department: "engineering", path: "src/graphics" },
          { id: "ui", title: "UI", department: "product", path: "src/ui" },
        ],
      },
    )

    expect(workflow.milestones.map((milestone) => [String(milestone.id), milestone.dependsOn.map(String)])).toEqual([
      ["prepare", []],
      ["audit@graphics", ["prepare"]],
      ["review@graphics", ["audit@graphics"]],
      ["audit@ui", ["prepare"]],
      ["review@ui", ["audit@ui"]],
      ["finish", ["review@graphics", "review@ui"]],
    ])
    expect(workflow.milestones.find((milestone) => milestone.id === "audit@graphics")?.title).toBe("Audit Graphics")
    expect(workflow.milestones.find((milestone) => milestone.id === "audit@graphics")?.department).toBe("engineering")
    expect(workflow.milestones.find((milestone) => milestone.id === "audit@ui")?.prompt).toBe("Audit ui: src/ui")
  })

  test("expands thirteen pipeline items into twenty-six independent milestones", () => {
    const workflow = parseWorkflowXml(
      `
        <workflow>
          <ordered>
            <milestone id="prepare">Prepare shared inputs</milestone>
            <pipeline items="work/shared/audit-tracks.json">
              <milestone id="audit">Audit {item.id}</milestone>
              <milestone id="review" depends="audit">Review {item.id}</milestone>
            </pipeline>
            <milestone id="finish">Finish synthesis</milestone>
          </ordered>
        </workflow>
      `,
      {
        readPipelineItems: () =>
          Array.from({ length: 13 }, (_, index) => ({
            id: `track-${String(index + 1).padStart(2, "0")}`,
          })),
      },
    )
    const audit = workflow.milestones.filter((milestone) => String(milestone.id).startsWith("audit@"))
    const review = workflow.milestones.filter((milestone) => String(milestone.id).startsWith("review@"))
    const finish = workflow.milestones.find((milestone) => milestone.id === "finish")

    expect(audit).toHaveLength(13)
    expect(review).toHaveLength(13)
    expect(workflow.milestones).toHaveLength(28)
    expect(workflow.milestones.find((milestone) => milestone.id === "review@track-01")?.dependsOn.map(String)).toEqual([
      "audit@track-01",
    ])
    expect(workflow.milestones.find((milestone) => milestone.id === "review@track-02")?.dependsOn.map(String)).toEqual([
      "audit@track-02",
    ])
    expect(finish?.dependsOn.map(String)).toEqual(review.map((milestone) => String(milestone.id)))
  })

  test("uses a waiting milestone when pipeline items are not available yet", () => {
    const workflow = parseWorkflowXml(
      `
        <workflow>
          <ordered>
            <milestone id="prepare">Prepare shared inputs</milestone>
            <pipeline items="work/shared/audit-tracks.json">
              <milestone id="audit">Audit {item}</milestone>
              <milestone id="review">Review {item}</milestone>
            </pipeline>
            <milestone id="finish">Finish synthesis</milestone>
          </ordered>
        </workflow>
      `,
      {
        readPipelineItems: (itemsPath) => workflowPipelineItemsMissing(itemsPath),
      },
    )

    expect(workflow.milestones.map((milestone) => [String(milestone.id), milestone.waitingFor, milestone.dependsOn.map(String)])).toEqual([
      ["prepare", undefined, []],
      ["pipeline-items@work-shared-audit-tracks.json", "pipeline_items", ["prepare"]],
      ["finish", undefined, ["pipeline-items@work-shared-audit-tracks.json"]],
    ])
  })

  test("preserves explicit dependencies and removes duplicates", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <ordered>
          <milestone id="a">A</milestone>
          <milestone id="b" depends="a">B</milestone>
          <milestone id="c" depends="a, b">C</milestone>
        </ordered>
      </workflow>
    `)

    expect(workflow.milestones.map((milestone) => [String(milestone.id), milestone.dependsOn.map(String)])).toEqual([
      ["a", []],
      ["b", ["a"]],
      ["c", ["b", "a"]],
    ])
  })

  test("parses milestone review policy", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <milestone id="small" title="Small" department="engineering" review="skip">Do focused work.</milestone>
      </workflow>
    `)

    expect(workflow.milestones[0]?.review).toBe("skip")
  })

  test("rejects invalid milestone review policy", () => {
    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <milestone id="small" review="later">Do focused work.</milestone>
        </workflow>
      `),
    ).toThrow('<milestone review> must be "required" or "skip"')
  })

  test("treats unknown angle-bracket placeholders in milestone text as literal text", () => {
    const workflow = parseWorkflowXml(`
      <workflow>
        <milestone id="audit" title="Audit" department="engineering">Write implementation/audit-<slug>-findings.md and keep <generic name="T"> examples </generic>literal.</milestone>
      </workflow>
    `)

    expect(workflow.milestones[0]?.prompt).toBe(
      'Write implementation/audit-<slug>-findings.md and keep <generic name="T"> examples </generic>literal.',
    )
  })

  test("rejects unknown elements and missing milestone ids", () => {
    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <task id="a">A</task>
        </workflow>
      `),
    ).toThrow(WorkflowParseError)

    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <milestone>Missing id</milestone>
        </workflow>
      `),
    ).toThrow(WorkflowParseError)
  })

  test("reports line and column for unsupported structural elements", () => {
    let error: unknown
    try {
      parseWorkflowXml(`
        <workflow>
          <ordered>
            <milestone id="plan">Plan</milestone>
            <slug>placeholder must not be a workflow step</slug>
          </ordered>
        </workflow>
      `)
    } catch (caught) {
      error = caught
    }

    expect(error).toBeInstanceOf(WorkflowParseError)
    expect(error instanceof Error ? error.message : "").toContain("unsupported workflow element <slug> at line 5, column")
  })

  test("rejects empty groups with no milestones", () => {
    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <ordered></ordered>
        </workflow>
      `),
    ).toThrow("<ordered> must contain at least one step")
  })

  test("rejects duplicate ids, unknown dependencies, and cycles", () => {
    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <parallel>
            <milestone id="a">A</milestone>
            <milestone id="a">Duplicate</milestone>
          </parallel>
        </workflow>
      `),
    ).toThrow("workflow milestone ids must be unique")

    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <milestone id="a" depends="missing">A</milestone>
        </workflow>
      `),
    ).toThrow('workflow milestone depends on unknown id "missing"')

    expect(() =>
      parseWorkflowXml(`
        <workflow>
          <parallel>
            <milestone id="a" depends="b">A</milestone>
            <milestone id="b" depends="a">B</milestone>
          </parallel>
        </workflow>
      `),
    ).toThrow("workflow milestone dependencies cannot contain a cycle")
  })
})
