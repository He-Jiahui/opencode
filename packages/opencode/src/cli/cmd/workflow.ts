import type { Argv } from "yargs"
import { Effect } from "effect"
import { cmd } from "./cmd"
import { CliError, effectCmd } from "../effect-cmd"
import { Workflow } from "@/workflow/workflow"
import { WorkflowID } from "@/workflow/schema"
import { SessionID } from "@/session/schema"
import { UI } from "../ui"

const workflowCliError = (error: Workflow.Error) => new CliError({ message: error.message })

export const WorkflowCommand = cmd({
  command: "workflow",
  describe: "manage agent workflows",
  builder: (yargs: Argv) =>
    yargs
      .command(WorkflowStartCommand)
      .command(WorkflowListCommand)
      .command(WorkflowStatusCommand)
      .command(WorkflowGraphCommand)
      .command(WorkflowDoctorCommand)
      .command(WorkflowResumeCommand)
      .command(WorkflowCancelCommand)
      .demandCommand(),
  async handler() {},
})

export const WorkflowStartCommand = effectCmd({
  command: "start [prompt]",
  describe: "start an agent workflow",
  builder: (yargs) =>
    yargs
      .positional("prompt", {
        describe: "workflow request",
        type: "string",
      })
      .option("session", {
        describe: "existing root session id",
        type: "string",
      })
      .option("model", {
        describe: "model in provider/model format",
        type: "string",
      })
      .option("variant", {
        describe: "model variant / reasoning effort",
        type: "string",
      })
      .option("format", {
        describe: "output format",
        type: "string",
        choices: ["text", "json"],
        default: "text",
      }),
  handler: Effect.fn("Cli.workflow.start")(function* (args) {
    const workflow = yield* Workflow.Service
    const info = yield* workflow
      .start({
        prompt: args.prompt,
        sessionID: args.session ? SessionID.make(args.session) : undefined,
        model: args.model,
        variant: args.variant,
      })
      .pipe(Effect.mapError(workflowCliError))
    if (args.format === "json") {
      console.log(JSON.stringify(info, null, 2))
      return
    }
    UI.println(UI.Style.TEXT_SUCCESS_BOLD + `Workflow ${info.id} started` + UI.Style.TEXT_NORMAL)
  }),
})

export const WorkflowListCommand = effectCmd({
  command: "list",
  describe: "list workflows",
  builder: (yargs) =>
    yargs.option("format", {
      describe: "output format",
      type: "string",
      choices: ["text", "json"],
      default: "text",
    }),
  handler: Effect.fn("Cli.workflow.list")(function* (args) {
    const list = yield* Workflow.Service.use((workflow) => workflow.list())
    if (args.format === "json") {
      console.log(JSON.stringify(list, null, 2))
      return
    }
    for (const item of list) {
      UI.println(`${item.id}  ${item.status.padEnd(12)}  ${item.title}`)
    }
  }),
})

export const WorkflowStatusCommand = effectCmd({
  command: "status <workflowID>",
  describe: "show workflow status",
  builder: (yargs) =>
    yargs
      .positional("workflowID", {
        describe: "workflow id",
        type: "string",
        demandOption: true,
      })
      .option("format", {
        describe: "output format",
        type: "string",
        choices: ["text", "json"],
        default: "text",
      }),
  handler: Effect.fn("Cli.workflow.status")(function* (args) {
    const graph = yield* Workflow.Service.use((workflow) => workflow.graph(WorkflowID.make(args.workflowID))).pipe(
      Effect.mapError(workflowCliError),
    )
    if (args.format === "json") {
      console.log(JSON.stringify(graph, null, 2))
      return
    }
    UI.println(`${graph.workflow.id}  ${graph.workflow.status}  ${graph.workflow.title}`)
    for (const item of graph.milestones) {
      UI.println(`  ${item.id}  ${item.status}  ${item.title ?? item.prompt}`)
    }
  }),
})

export const WorkflowGraphCommand = effectCmd({
  command: "graph <workflowID>",
  describe: "print workflow graph",
  builder: (yargs) =>
    yargs
      .positional("workflowID", {
        describe: "workflow id",
        type: "string",
        demandOption: true,
      })
      .option("format", {
        describe: "output format",
        type: "string",
        choices: ["text", "json", "mermaid"],
        default: "text",
      }),
  handler: Effect.fn("Cli.workflow.graph")(function* (args) {
    const graph = yield* Workflow.Service.use((workflow) => workflow.graph(WorkflowID.make(args.workflowID))).pipe(
      Effect.mapError(workflowCliError),
    )
    if (args.format === "json") {
      console.log(JSON.stringify(graph, null, 2))
      return
    }
    if (args.format === "mermaid") {
      console.log(["flowchart LR", ...graph.edges.map((edge) => `  ${edge.from} --> ${edge.to}`)].join("\n"))
      return
    }
    for (const node of graph.nodes) {
      UI.println(`${node.id}  ${node.status ?? node.role ?? node.type}  ${node.title}`)
    }
    for (const edge of graph.edges) {
      UI.println(`  ${edge.from} -> ${edge.to}`)
    }
  }),
})

export const WorkflowDoctorCommand = effectCmd({
  command: "doctor [workflowID]",
  describe: "diagnose workflow persistence and dispatch state",
  builder: (yargs) =>
    yargs
      .positional("workflowID", {
        describe: "workflow id",
        type: "string",
      })
      .option("format", {
        describe: "output format",
        type: "string",
        choices: ["text", "json"],
        default: "text",
      })
      .option("fix", {
        describe: "rename duplicate workflow directories out of the active workflow set",
        type: "boolean",
        default: false,
      })
      .option("migrate", {
        describe: "move legacy workflow directories to the canonical workflow id path before checking",
        type: "boolean",
        default: false,
      }),
  handler: Effect.fn("Cli.workflow.doctor")(function* (args) {
    const report = yield* Workflow.Service.use((workflow) =>
      workflow.doctor({
        workflowID: args.workflowID ? WorkflowID.make(args.workflowID) : undefined,
        fix: args.fix,
        migrate: args.migrate,
      }),
    ).pipe(Effect.mapError(workflowCliError))
    if (args.format === "json") {
      console.log(JSON.stringify(report, null, 2))
      return
    }
    UI.println(
      `${report.ok ? UI.Style.TEXT_SUCCESS_BOLD : UI.Style.TEXT_DANGER_BOLD}Workflow doctor checked ${report.checked} workflow${report.checked === 1 ? "" : "s"}: ${
        report.ok ? "ok" : "issues found"
      }${UI.Style.TEXT_NORMAL}`,
    )
    for (const issue of report.issues) {
      UI.println(
        `  [${issue.severity}] ${issue.code}${issue.workflowID ? ` ${issue.workflowID}` : ""}${issue.path ? ` ${issue.path}` : ""}: ${issue.message}`,
      )
    }
  }),
})

export const WorkflowResumeCommand = effectCmd({
  command: "resume <workflowID>",
  describe: "resume a workflow",
  builder: (yargs) =>
    yargs.positional("workflowID", {
      describe: "workflow id",
      type: "string",
      demandOption: true,
    }),
  handler: Effect.fn("Cli.workflow.resume")(function* (args) {
    const info = yield* Workflow.Service.use((workflow) => workflow.resume(WorkflowID.make(args.workflowID))).pipe(
      Effect.mapError(workflowCliError),
    )
    UI.println(UI.Style.TEXT_SUCCESS_BOLD + `Workflow ${info.id} resumed` + UI.Style.TEXT_NORMAL)
  }),
})

export const WorkflowCancelCommand = effectCmd({
  command: "cancel <workflowID>",
  describe: "cancel a workflow",
  builder: (yargs) =>
    yargs.positional("workflowID", {
      describe: "workflow id",
      type: "string",
      demandOption: true,
    }),
  handler: Effect.fn("Cli.workflow.cancel")(function* (args) {
    const info = yield* Workflow.Service.use((workflow) => workflow.cancel(WorkflowID.make(args.workflowID))).pipe(
      Effect.mapError(workflowCliError),
    )
    UI.println(UI.Style.TEXT_SUCCESS_BOLD + `Workflow ${info.id} cancelled` + UI.Style.TEXT_NORMAL)
  }),
})
