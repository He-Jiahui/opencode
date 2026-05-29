import { TextAttributes } from "@opentui/core"
import { For, Show, createMemo, createResource, createSignal } from "solid-js"
import { useDialog } from "@tui/ui/dialog"
import { useTheme } from "../context/theme"
import { useSDK } from "../context/sdk"
import { useSync } from "../context/sync"
import { useLocal } from "../context/local"
import { useToast } from "../ui/toast"
import type { WorkflowGraph, WorkflowMilestone } from "@opencode-ai/sdk/v2"

function lineFor(milestone: WorkflowMilestone) {
  const deps = milestone.dependsOn.length ? ` <- ${milestone.dependsOn.join(", ")}` : ""
  return `${milestone.id}${deps}`
}

export function DialogWorkflow(props: { sessionID: string }) {
  const dialog = useDialog()
  const sdk = useSDK()
  const sync = useSync()
  const local = useLocal()
  const toast = useToast()
  const { theme } = useTheme()
  const [selected, setSelected] = createSignal<string>()

  const [workflows, { refetch: refetchWorkflows }] = createResource(
    () => props.sessionID,
    async (sessionID) => sdk.client.workflow.list({ sessionID }).then((result) => result.data ?? []),
  )
  const current = createMemo(() => workflows()?.at(-1))
  const [graph, { refetch: refetchGraph }] = createResource(
    () => current()?.id,
    async (workflowID) => (workflowID ? sdk.client.workflow.graph({ workflowID }).then((result) => result.data) : undefined),
  )
  const selectedMilestone = createMemo(() => graph()?.milestones.find((milestone) => milestone.id === selected()))

  const start = async () => {
    const model = local.model.current()
    await sdk.client.workflow
      .start({
        workflowStartInput: {
          sessionID: props.sessionID,
          prompt: graph()?.workflow.request ?? "Workflow request",
          model: model ? `${model.providerID}/${model.modelID}` : undefined,
          variant: local.model.variant.current(),
          agent: local.agent.current()?.name,
        },
      })
      .then(async () => {
        await refetchWorkflows()
        await sync.session.workflow(props.sessionID)
        toast.show({ variant: "success", message: "Workflow started" })
      })
      .catch((error) => toast.show({ variant: "error", message: error instanceof Error ? error.message : String(error) }))
  }

  const refresh = async () => {
    await refetchWorkflows()
    await refetchGraph()
    const workflowID = current()?.id
    if (workflowID) await sync.session.workflowGraph(workflowID)
  }

  return (
    <box paddingLeft={2} paddingRight={2} gap={1} paddingBottom={1}>
      <box flexDirection="row" justifyContent="space-between">
        <text fg={theme.text} attributes={TextAttributes.BOLD}>
          Workflow
        </text>
        <text fg={theme.textMuted} onMouseUp={() => dialog.clear()}>
          esc
        </text>
      </box>
      <box flexDirection="row" gap={1}>
        <text fg={theme.primary} onMouseUp={() => void start()}>
          start
        </text>
        <text fg={theme.textMuted}>|</text>
        <text fg={theme.primary} onMouseUp={() => void refresh()}>
          refresh
        </text>
      </box>
      <Show when={current()} fallback={<text fg={theme.textMuted}>No workflow for this session yet.</text>}>
        {(workflow) => (
          <box gap={1}>
            <text fg={theme.text} wrapMode="word">
              <b>{workflow().status}</b> <span style={{ fg: theme.textMuted }}>{workflow().id}</span>
            </text>
            <Show when={graph()} fallback={<text fg={theme.textMuted}>Loading graph...</text>}>
              {(g) => (
                <box gap={1}>
                  <For each={g().milestones}>
                    {(milestone) => (
                      <box flexDirection="row" gap={1}>
                        <text
                          flexShrink={0}
                          fg={
                            milestone.status === "approved" || milestone.status === "done"
                              ? theme.success
                              : milestone.status === "failed" || milestone.status === "rejected"
                                ? theme.error
                                : theme.warning
                          }
                        >
                          •
                        </text>
                        <text
                          fg={selected() === milestone.id ? theme.primary : theme.text}
                          wrapMode="word"
                          onMouseUp={() => setSelected(milestone.id)}
                        >
                          <b>{lineFor(milestone)}</b>{" "}
                          <span style={{ fg: theme.textMuted }}>
                            {milestone.status} {milestone.session.at(-1)?.role ?? ""}
                          </span>
                        </text>
                      </box>
                    )}
                  </For>
                  <Show when={selectedMilestone()}>
                    {(milestone) => (
                      <box paddingTop={1}>
                        <text fg={theme.text} wrapMode="word">
                          {milestone().title ?? milestone().prompt}
                        </text>
                        <text fg={theme.textMuted} wrapMode="word">
                          {milestone().planPath ?? ""}
                        </text>
                      </box>
                    )}
                  </Show>
                </box>
              )}
            </Show>
          </box>
        )}
      </Show>
    </box>
  )
}
