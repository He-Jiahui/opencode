import { describe, expect, test } from "bun:test"

import {
  extractHandoffSummary,
  parseAcceptanceDecision,
  parseConsultRequests,
  parseGateMilestoneIDs,
  parseTechnicalDecision,
  parseTestDecision,
  parseWorkflowControlAction,
  parseWorkflowUpdateXml,
  temporaryInterruptPauseStatus,
  workflowEmployeeTaskPrompt,
  workflowReferencePrompt,
} from "@/workflow/workflow"
import { ProjectID } from "@/project/schema"
import { SessionID } from "@/session/schema"
import { WorkflowID, WorkflowMilestoneID, type WorkflowInfo, type WorkflowMemberInfo, type WorkflowMilestoneInfo } from "@/workflow"

describe("parseConsultRequests", () => {
  test("parses direct session consultations with timing and reason", () => {
    const requests = parseConsultRequests(`
        <opencode-workflow-consult target-session="ses_target" timing="temporary-interrupt" reason="needs exact API contract">
          What did you decide for the shared plugin contract?
        </opencode-workflow-consult>
      `)

    expect(requests).toHaveLength(1)
    expect(String(requests[0]?.targetSessionID)).toBe("ses_target")
    expect(requests[0]?.timing).toBe("temporary-interrupt")
    expect(requests[0]?.reason).toBe("needs exact API contract")
    expect(requests[0]?.question).toBe("What did you decide for the shared plugin contract?")
  })

  test("parses role-based workflow messages with specialty", () => {
    expect(
      parseConsultRequests(`
        <opencode-workflow-message to-role="expert" specialty="graphics" timing="interrupt" reason="performance risk">
          Review the GPU particle path before executor continues.
        </opencode-workflow-message>
      `),
    ).toEqual([
      {
        targetRole: "expert",
        targetSpecialty: "graphics",
        timing: "interrupt",
        reason: "performance risk",
        question: "Review the GPU particle path before executor continues.",
      },
    ])
  })

  test("ignores empty messages and invalid roles or timings", () => {
    expect(
      parseConsultRequests(`
        <opencode-workflow-consult target-session="ses_target"></opencode-workflow-consult>
        <opencode-workflow-message to-role="unknown">Should be ignored.</opencode-workflow-message>
        <opencode-workflow-message to-role="main_pm" timing="later">Use default timing.</opencode-workflow-message>
      `),
    ).toEqual([
      {
        targetRole: "main_pm",
        question: "Use default timing.",
      },
    ])
  })
})

describe("parseAcceptanceDecision", () => {
  test("requires explicit final acceptance XML", () => {
    expect(
      parseAcceptanceDecision(
        '<opencode-workflow-acceptance role="requester" decision="approve">ship it</opencode-workflow-acceptance>',
      ),
    ).toBe("approve")
    expect(
      parseAcceptanceDecision(
        '<opencode-workflow-acceptance decision="reject" role="main_pm">missing acceptance evidence</opencode-workflow-acceptance>',
      ),
    ).toBe("reject")
    expect(parseAcceptanceDecision("Approved in prose, but no machine-readable decision.")).toBeUndefined()
  })
})

describe("parseTestDecision", () => {
  test("requires explicit tester gate XML", () => {
    expect(parseTestDecision('<opencode-workflow-test decision="pass">all targeted checks passed</opencode-workflow-test>')).toBe(
      "pass",
    )
    expect(parseTestDecision('<opencode-workflow-test decision="fail">missing regression coverage</opencode-workflow-test>')).toBe(
      "fail",
    )
    expect(parseTestDecision("Tests look okay in prose, but no gate XML.")).toBeUndefined()
  })
})

describe("parseGateMilestoneIDs", () => {
  test("extracts explicit milestone ids from gate failure XML", () => {
    expect(
      parseGateMilestoneIDs(
        '<opencode-workflow-test decision="fail" milestones="implementation, verification">coverage missing</opencode-workflow-test>',
        "test",
      ).map(String),
    ).toEqual(["implementation", "verification"])
    expect(
      parseGateMilestoneIDs(
        '<opencode-workflow-technical decision="fail" milestone="gpu-particles">hot path risk</opencode-workflow-technical>',
        "technical",
      ).map(String),
    ).toEqual(["gpu-particles"])
  })
})

describe("parseTechnicalDecision", () => {
  test("requires explicit technical advisor gate XML", () => {
    expect(
      parseTechnicalDecision(
        '<opencode-workflow-technical decision="pass">architecture and performance are acceptable</opencode-workflow-technical>',
      ),
    ).toBe("pass")
    expect(
      parseTechnicalDecision(
        '<opencode-workflow-technical decision="fail">hot path risk must be reopened</opencode-workflow-technical>',
      ),
    ).toBe("fail")
    expect(parseTechnicalDecision("Architecture looks acceptable in prose, but no gate XML.")).toBeUndefined()
  })
})

describe("parseWorkflowControlAction", () => {
  test("requires explicit main PM workflow control XML", () => {
    expect(parseWorkflowControlAction('<opencode-workflow-control action="resume">continue</opencode-workflow-control>')).toBe(
      "resume",
    )
    expect(parseWorkflowControlAction('<opencode-workflow-control action="block">needs clarification</opencode-workflow-control>')).toBe(
      "block",
    )
    expect(parseWorkflowControlAction("Continue in prose without control XML.")).toBeUndefined()
  })
})

describe("parseWorkflowUpdateXml", () => {
  test("extracts workflow XML from update blocks and fenced content", () => {
    expect(
      parseWorkflowUpdateXml(`
        <opencode-workflow-update reason="split broad scope">
          <workflow><ordered><milestone id="one">One</milestone></ordered></workflow>
        </opencode-workflow-update>
      `),
    ).toBe('<workflow><ordered><milestone id="one">One</milestone></ordered></workflow>')
    expect(
      parseWorkflowUpdateXml([
        '<opencode-workflow-update reason="fenced">',
        "```xml",
        '<workflow><parallel><milestone id="a">A</milestone></parallel></workflow>',
        "```",
        "</opencode-workflow-update>",
      ].join("\n")),
    ).toBe('<workflow><parallel><milestone id="a">A</milestone></parallel></workflow>')
    expect(parseWorkflowUpdateXml("<opencode-workflow-update>no graph</opencode-workflow-update>")).toBeUndefined()
  })
})

describe("temporaryInterruptPauseStatus", () => {
  test("pauses only active milestone work", () => {
    expect(temporaryInterruptPauseStatus("planning")).toBe("planning")
    expect(temporaryInterruptPauseStatus("executing")).toBe("executing")
    expect(temporaryInterruptPauseStatus("reviewing")).toBe("reviewing")
    expect(temporaryInterruptPauseStatus("pending")).toBeUndefined()
    expect(temporaryInterruptPauseStatus("approved")).toBeUndefined()
    expect(temporaryInterruptPauseStatus("done")).toBeUndefined()
  })
})

describe("extractHandoffSummary", () => {
  test("extracts the structured session handoff section", () => {
    expect(
      extractHandoffSummary([
        "Implementation notes",
        "",
        "## Handoff Summary",
        "- completed: CPU particle lifecycle",
        "- next: GPU path",
        "",
        "## Extra Details",
        "ignored",
      ].join("\n")),
    ).toBe("- completed: CPU particle lifecycle\n- next: GPU path")
    expect(extractHandoffSummary("No structured handoff.")).toBeUndefined()
  })
})

describe("workflowReferencePrompt", () => {
  test("instructs staff to read the workflow memory library before acting", () => {
    const workflow: WorkflowInfo = {
      id: WorkflowID.ascending("wfl_memory"),
      projectID: ProjectID.global,
      request: "Build the thing",
      title: "Memory workflow",
      directory: ".opencode/workflows/20260531_memory-workflow",
      path: ".opencode/workflows/20260531_memory-workflow",
      xml: "<workflow />",
      status: "executing",
      time: {
        created: 1,
        updated: 1,
      },
    }
    const prompt = workflowReferencePrompt(workflow)

    expect(prompt).toContain("progress.md for live state")
    expect(prompt).toContain("organization.md for owners and capacities")
    expect(prompt).toContain("reference/index.md for session summaries, staff memory, and consultation history")
    expect(prompt).toContain("reference/consultations/index.md and reference/consultations/*.md")
    expect(prompt).toContain("reference/staff/*.md")
    expect(prompt).toContain("standups/index.md and standups/*.md for recent supervision details")
    expect(prompt).toContain("interventions/index.md for requester direction changes")
    expect(prompt).toContain(".opencode/plans/**")
    expect(prompt).toContain(".codex/plans/**")
    expect(prompt).toContain(".codex/skills/**")
    expect(prompt).toContain(".codex/AGENTS.md")
    expect(prompt).toContain("Use prior session summaries as company memory")
    expect(prompt).toContain("## Handoff Summary")
    expect(prompt).toContain("<opencode-workflow-update")
  })
})

describe("workflowEmployeeTaskPrompt", () => {
  test("wraps assigned work with long-lived employee context and communication rules", () => {
    const workflow: WorkflowInfo = {
      id: WorkflowID.ascending("wfl_employee"),
      projectID: ProjectID.global,
      rootSessionID: SessionID.make("ses_requester"),
      request: "Build particle tooling",
      title: "Particle workflow",
      directory: ".opencode/workflows/20260531_particle-workflow",
      path: ".opencode/workflows/20260531_particle-workflow",
      xml: "<workflow />",
      status: "executing",
      staffing: {
        mainPM: 1,
        departmentPM: 2,
        executor: 4,
        reviewer: 2,
        tester: 1,
        expert: 1,
      },
      time: {
        created: 1,
        updated: 1,
      },
    }
    const sessionID = SessionID.make("ses_executor")
    const expertSessionID = SessionID.make("ses_expert")
    const member: WorkflowMemberInfo = {
      id: "mem_executor",
      workflowID: workflow.id,
      role: "executor",
      specialty: "particles",
      title: "Particle executor",
      sessionID,
      capacity: 1,
      status: "active",
      time: {
        created: 1,
        updated: 1,
      },
    }
    const expert: WorkflowMemberInfo = {
      id: "mem_expert",
      workflowID: workflow.id,
      role: "expert",
      specialty: "graphics",
      title: "Graphics advisor",
      sessionID: expertSessionID,
      capacity: 1,
      status: "active",
      time: {
        created: 2,
        updated: 2,
      },
    }
    const milestone: WorkflowMilestoneInfo = {
      id: WorkflowMilestoneID.make("particles-gpu"),
      title: "GPU particles",
      department: "graphics",
      prompt: "Implement GPU particle path",
      dependsOn: [],
      status: "executing",
      attempt: 2,
      planPath: ".opencode/workflows/20260531_particle-workflow/particles-gpu/plan.md",
      session: [
        { role: "expert", sessionID: expertSessionID, milestoneID: WorkflowMilestoneID.make("particles-gpu"), attempt: 1 },
        { role: "executor", sessionID, milestoneID: WorkflowMilestoneID.make("particles-gpu"), attempt: 2 },
      ],
    }
    const waitingMilestone: WorkflowMilestoneInfo = {
      id: WorkflowMilestoneID.make("particles-materials"),
      title: "Particle materials",
      department: "graphics",
      prompt: "Finish particle materials",
      dependsOn: [milestone.id],
      status: "pending",
      attempt: 1,
      session: [],
    }

    const prompt = workflowEmployeeTaskPrompt(
      workflow,
      {
        sessionID,
        role: "executor",
        milestoneID: milestone.id,
        attempt: 2,
        member,
        milestone,
      },
      "Carry out the implementation plan.",
      { members: [member, expert], milestones: [milestone, waitingMilestone] },
    )

    const normalized = prompt.replaceAll("\\", "/")
    expect(normalized).toContain("## Workflow Employee Context")
    expect(normalized).toContain("long-lived employee")
    expect(normalized).toContain("Company role: Executor")
    expect(normalized).toContain("Role responsibility: Implement assigned milestone work")
    expect(normalized).toContain("Staff memory: .opencode/workflows/20260531_particle-workflow/reference/staff/mem_executor.md")
    expect(normalized).toContain("Current milestone title: GPU particles")
    expect(normalized).toContain("Current department: graphics")
    expect(normalized).toContain("Current plan file: .opencode/workflows/20260531_particle-workflow/particles-gpu/plan.md")
    expect(normalized).toContain(".opencode/plans/**")
    expect(normalized).toContain(".codex/plans/**")
    expect(normalized).toContain(".codex/skills/**")
    expect(normalized).toContain("## Company Operating Snapshot")
    expect(normalized).toContain("### Staffing Limits")
    expect(normalized).toContain("Executor: staffed 1/4, busy 1")
    expect(normalized).toContain("Technical Advisor: staffed 1/1, busy 0")
    expect(normalized).toContain("### Strategic Owner")
    expect(normalized).toContain("Requester: Build particle tooling [requester/strategy] session=ses_requester")
    expect(normalized).toContain("responsibility=strategic direction, intervention, and final acceptance")
    expect(normalized).toContain("### Review Boundaries")
    expect(normalized).toContain("Department PM owns milestone functional approval or rejection.")
    expect(normalized).toContain("Tester owns completeness, regression, and feedback-loop review.")
    expect(normalized).toContain("Reviewer staff, when present, is optional audit/support context")
    expect(normalized).toContain("(you) Particle executor [executor/particles] session=ses_executor status=active capacity=1")
    expect(normalized).toContain("Graphics advisor [expert/graphics] session=ses_expert status=active capacity=1")
    expect(normalized).toContain("particles-gpu [executing] as Executor attempt 2")
    expect(normalized).toContain("particles-materials [pending] department=graphics deps=particles-gpu owners=unassigned")
    expect(normalized).toContain('<opencode-workflow-consult target-session="ses_xxx"')
    expect(normalized).toContain(
      '<opencode-workflow-message to-role="expert|main_pm|department_pm|executor|reviewer|tester|requester"',
    )
    expect(normalized).toContain("## Assigned Workflow Task")
    expect(normalized).toContain("Carry out the implementation plan.")
  })
})
