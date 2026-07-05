import { Effect, Layer } from "effect"

import { Agent } from "@/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Bus } from "@/bus"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionPrompt } from "@/session/prompt"
import { SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { Truncate } from "@/tool/truncate"
import type { WorkflowToolCommand } from "@/workflow/command"
import { WorkflowID } from "@/workflow/schema"
import { Workflow } from "@/workflow/workflow"
import { provideInstanceEffect, testInstanceStoreLayer } from "../fixture/fixture"

const seedXml = `<workflow>
  <ordered>
    <milestone id="seed" title="Seed" department="product">Seed workflow before the kill canary update.</milestone>
  </ordered>
</workflow>`

const killCanaryXml = `<workflow>
  <ordered>
    <milestone id="kill-canary-a" title="Kill canary A" department="product">Exercise external kill recovery before downstream dispatch.</milestone>
    <milestone id="kill-canary-b" title="Kill canary B" department="engineering">Verify workflow XML update survives process termination.</milestone>
    <milestone id="kill-canary-c" title="Kill canary C" department="quality">Verify doctor fix and retry can continue from the same workflow.</milestone>
  </ordered>
</workflow>`

const promptLayer = Layer.succeed(
  SessionPrompt.Service,
  SessionPrompt.Service.of({
    cancel: () => Effect.void,
    prompt: () => Effect.die(new Error("kill canary helper does not expect model prompts")),
    loop: () => Effect.die(new Error("kill canary helper does not expect model prompts")),
    shell: () => Effect.die(new Error("kill canary helper does not expect model prompts")),
    command: () => Effect.die(new Error("kill canary helper does not expect model prompts")),
    resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
  }),
)

const workflowLayer = Workflow.layer.pipe(
  Layer.provideMerge(promptLayer),
  Layer.provideMerge(
    Layer.mergeAll(
      BackgroundJob.defaultLayer,
      Bus.layer,
      EventV2Bridge.defaultLayer,
      Session.defaultLayer,
      Truncate.defaultLayer,
      Agent.defaultLayer,
    ),
  ),
  Layer.provideMerge(testInstanceStoreLayer),
)

function runWorkflow<A>(directory: string, effect: Effect.Effect<A, Error, Workflow.Service>) {
  return Effect.runPromise(effect.pipe(provideInstanceEffect(directory), Effect.provide(workflowLayer)))
}

function updateCommand(workflowID: WorkflowID, sourceSessionID: SessionID): WorkflowToolCommand {
  return {
    id: `kill-canary-update:${workflowID}`,
    action: "update_xml" as const,
    workflowID,
    sourceSessionID,
    xml: killCanaryXml,
    message: "external kill canary XML update",
  }
}

async function main() {
  const mode = process.argv[2]
  const directory = process.argv[3]
  if (!mode || !directory) throw new Error("Usage: kill-canary-child <start|crash|doctor|retry> <directory> [workflowID]")

  if (mode === "start") {
    const result = await runWorkflow(
      directory,
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const started = yield* workflow.start({
          prompt: ["External kill canary workflow", seedXml].join("\n"),
          agent: "build",
          model: "test/workflow-test-model",
          staffing: {
            mainPM: 0,
            departmentPM: 0,
            executor: 0,
            reviewer: 0,
            tester: 0,
            expert: 0,
          },
        })
        return { workflowID: started.id, rootSessionID: started.rootSessionID }
      }),
    )
    console.log(JSON.stringify(result))
    return
  }

  const workflowID = process.argv[4] ? WorkflowID.make(process.argv[4]) : undefined
  if (!workflowID) throw new Error(`Missing workflowID for ${mode}`)

  if (mode === "crash") {
    const result = await runWorkflow(
      directory,
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const current = yield* workflow.get(workflowID)
        if (!current.rootSessionID) throw new Error("workflow has no requester session")
        return yield* workflow.dispatchCommand(updateCommand(workflowID, current.rootSessionID))
      }),
    )
    console.log(JSON.stringify(result))
    return
  }

  if (mode === "doctor") {
    const result = await runWorkflow(
      directory,
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        return yield* workflow.doctor({ workflowID, fix: true })
      }),
    )
    console.log(JSON.stringify(result))
    return
  }

  if (mode === "retry") {
    const result = await runWorkflow(
      directory,
      Effect.gen(function* () {
        const workflow = yield* Workflow.Service
        const doctor = yield* workflow.doctor({ workflowID, fix: true })
        const current = yield* workflow.get(workflowID)
        if (!current.rootSessionID) throw new Error("workflow has no requester session")
        const dispatched = yield* workflow.dispatchCommand(updateCommand(workflowID, current.rootSessionID))
        const graph = yield* workflow.graph(workflowID)
        return {
          doctor,
          dispatched,
          milestoneIDs: graph.milestones.map((milestone) => milestone.id),
          report: yield* workflow.doctor({ workflowID, fix: true }),
        }
      }),
    )
    console.log(JSON.stringify(result))
    return
  }

  throw new Error(`Unknown mode: ${mode}`)
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.stack ?? error.message : String(error))
  process.exit(1)
})
