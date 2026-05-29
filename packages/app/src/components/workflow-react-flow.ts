import type { WorkflowGraph } from "@opencode-ai/sdk/v2"
import dagre from "@dagrejs/dagre"
import * as React from "react"
import { createRoot, type Root } from "react-dom/client"
import {
  Background,
  BaseEdge,
  Controls,
  EdgeLabelRenderer,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeProps,
  type NodeTypes,
  getBezierPath,
} from "@xyflow/react"
import "@xyflow/react/dist/style.css"

export type WorkflowReactFlowTarget = {
  sessionID?: string
  planPath?: string
}

export type WorkflowReactFlowContextTarget = WorkflowReactFlowTarget & {
  x: number
  y: number
}

export type WorkflowReactFlowSessionState = "running" | "completed"

export type WorkflowReactFlowProps = {
  graph: WorkflowGraph
  milestoneLabel: string
  viewport?: "panel" | "fullscreen"
  currentSessionID?: string
  sessionState?: Record<string, WorkflowReactFlowSessionState | undefined>
  onNodeSelect: (target: WorkflowReactFlowTarget) => void
  onNodeContextMenu?: (target: WorkflowReactFlowContextTarget) => void
}

export type WorkflowReactFlowInstance = {
  update: (props: WorkflowReactFlowProps) => void
  dispose: () => void
}

type WorkflowFlowNodeData = {
  kind: "workflow" | "milestone" | "session" | "document"
  title: string
  label: string
  status?: string
  summary?: string
  prompt?: string
  sessionID?: string
  sessionState?: WorkflowReactFlowSessionState
  currentSession?: boolean
  planPath?: string
  width: number
  height: number
}

type WorkflowFlowNode = Node<WorkflowFlowNodeData, "workflowNode">
type WorkflowFlowEdgeKind = "entry" | "dependency" | "session" | "tester" | "consultation" | "document"
type WorkflowFlowEdgeData = {
  kind: WorkflowFlowEdgeKind
  label?: string
  tooltip?: string
}
type WorkflowFlowEdge = Edge<WorkflowFlowEdgeData, "workflowEdge">
type WorkflowDisplayEdge = {
  id: string
  from: string
  to: string
  kind: WorkflowFlowEdgeKind
  label?: string
  tooltip?: string
}
type WorkflowMilestone = WorkflowGraph["milestones"][number]
type WorkflowMilestoneSession = WorkflowMilestone["session"][number]

const roleTitles = {
  requester: "Requester",
  main_pm: "Main PM",
  department_pm: "Department PM",
  executor: "Executor",
  reviewer: "Reviewer",
  tester: "Tester",
}

const nodeTypes: NodeTypes = {
  workflowNode: WorkflowFlowNodeView,
}

const edgeTypes = {
  workflowEdge: WorkflowFlowEdgeView,
}

const fitViewOptions = {
  padding: 0.18,
  minZoom: 0.55,
  maxZoom: 1.15,
}

export function mountWorkflowReactFlow(container: HTMLElement, props: WorkflowReactFlowProps): WorkflowReactFlowInstance {
  const root = createRoot(container)
  renderWorkflow(root, props)
  return {
    update: (next) => renderWorkflow(root, next),
    dispose: () => root.unmount(),
  }
}

function renderWorkflow(root: Root, props: WorkflowReactFlowProps) {
  root.render(React.createElement(WorkflowReactFlow, props))
}

function WorkflowReactFlow(props: WorkflowReactFlowProps) {
  const model = React.useMemo(
    () => workflowModel(props.graph, props.milestoneLabel, props.currentSessionID, props.sessionState ?? {}),
    [props.graph, props.milestoneLabel, props.currentSessionID, props.sessionState],
  )
  return React.createElement(ReactFlow, {
    key: `${props.graph.workflow.id}:${props.graph.workflow.time.updated}:${props.viewport ?? "panel"}:${props.currentSessionID ?? ""}:${model.nodes.length}:${model.edges.length}:${model.stateKey}`,
    className: "workflow-react-flow",
    nodes: model.nodes,
    edges: model.edges,
    nodeTypes,
    edgeTypes,
    fitView: true,
    fitViewOptions,
    minZoom: 0.35,
    maxZoom: 1.6,
    nodesDraggable: true,
    nodesConnectable: false,
    edgesFocusable: false,
    deleteKeyCode: null,
    onNodeClick: (_event, node) => {
      const data = node.data as WorkflowFlowNodeData
      props.onNodeSelect({ sessionID: data.sessionID, planPath: data.planPath })
    },
    onNodeContextMenu: (event, node) => {
      event.preventDefault()
      const data = node.data as WorkflowFlowNodeData
      props.onNodeContextMenu?.({
        sessionID: data.sessionID,
        planPath: data.planPath,
        x: event.clientX,
        y: event.clientY,
      })
    },
    children: [
      React.createElement(Background, {
        key: "background",
        gap: 22,
        size: 1,
        color: "var(--border-weaker-base)",
      }),
      React.createElement(Controls, {
        key: "controls",
        showInteractive: false,
        position: "bottom-right",
      }),
    ],
  })
}

function workflowModel(
  graph: WorkflowGraph,
  milestoneLabel: string,
  currentSessionID: string | undefined,
  sessionState: Record<string, WorkflowReactFlowSessionState | undefined>,
) {
  const milestones = new Map(graph.milestones.map((milestone) => [milestone.id, milestone]))
  const sessions = new Map(
    graph.milestones.flatMap((milestone) =>
      milestone.session.map((session) => [
        `${milestone.id}:${session.role}:${session.attempt ?? 0}`,
        {
          milestone,
          session,
        },
      ]),
    ),
  )
  const displayEdges = workflowDisplayEdges(graph)
  const currentNodeIDs = workflowCurrentNodeIDs(graph, currentSessionID)
  const nodes = layoutNodes(
    graph.nodes.map((node, index) => {
      const milestone = node.milestoneID ? milestones.get(node.milestoneID) : undefined
      const session = sessions.get(node.id)
      const data = workflowNodeData(
        node,
        milestone,
        session,
        graph.workflow.status,
        graph.workflow.request,
        milestoneLabel,
        sessionState,
        currentNodeIDs.has(node.id),
      )
      return {
        id: node.id,
        type: "workflowNode",
        data,
        position: { x: index * 220, y: 0 },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        style: {
          width: data.width,
          height: data.height,
        },
      } satisfies WorkflowFlowNode
    }),
    displayEdges,
  )
  const nodeIDs = new Set(nodes.map((node) => node.id))
  return {
    nodes,
    stateKey: Object.entries(sessionState)
      .map(([sessionID, state]) => `${sessionID}:${state}`)
      .sort()
      .join("|"),
    edges: displayEdges
      .filter((edge) => nodeIDs.has(edge.from) && nodeIDs.has(edge.to))
      .map(
        (edge) =>
          ({
            id: edge.id,
            source: edge.from,
            target: edge.to,
            type: "workflowEdge",
            className: `workflow-rf-edge workflow-rf-edge--${edge.kind}`,
            data: {
              kind: edge.kind,
              label: edge.label,
              tooltip: edge.tooltip,
            },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              width: 14,
              height: 14,
            },
            style: {
              strokeWidth: edge.kind === "dependency" || edge.kind === "entry" || edge.kind === "consultation" ? 1.7 : 1.2,
            },
          }) satisfies WorkflowFlowEdge,
      ),
  }
}

function workflowDisplayEdges(graph: WorkflowGraph) {
  const nodeIDs = new Set(graph.nodes.map((node) => node.id))
  const mainPMID = `${graph.workflow.id}:main_pm`
  const testerID = `${graph.workflow.id}:tester`
  const startID = nodeIDs.has(mainPMID) ? mainPMID : graph.workflow.id
  return uniqueEdges(
    [
      ...(nodeIDs.has(mainPMID)
        ? [
            displayEdge("entry", graph.workflow.id, mainPMID),
          ]
        : []),
      ...graph.milestones.flatMap((milestone) => [
        ...(milestone.dependsOn.length === 0
          ? [displayEdge("entry", startID, String(milestone.id))]
          : milestone.dependsOn.map((dependency) => displayEdge("dependency", String(dependency), String(milestone.id)))),
        ...milestoneSessionEdges(milestone),
      ]),
      ...(nodeIDs.has(testerID)
        ? terminalMilestones(graph.milestones).map((milestone) => displayEdge("tester", String(milestone.id), testerID))
        : []),
      ...graph.edges
        .filter(
          (edge): edge is WorkflowGraph["edges"][number] & { kind: "consultation" | "document" } =>
            edge.kind === "consultation" || edge.kind === "document",
        )
        .map((edge) => ({
          id: edge.id,
          from: edge.from,
          to: edge.to,
          kind: edge.kind,
          label: edge.label,
          tooltip: edgeTooltip(edge),
        })),
    ].filter((edge) => nodeIDs.has(edge.from) && nodeIDs.has(edge.to) && edge.from !== edge.to),
  )
}

function milestoneSessionEdges(milestone: WorkflowMilestone) {
  return Array.from(new Set(milestone.session.map((session) => Number(session.attempt ?? 0))))
    .sort((a, b) => a - b)
    .flatMap((attempt) => {
      const chain = [
        milestone.session.find((session) => (session.attempt ?? 0) === attempt && session.role === "department_pm"),
        milestone.session.find((session) => (session.attempt ?? 0) === attempt && session.role === "executor"),
        milestone.session.find((session) => (session.attempt ?? 0) === attempt && session.role === "reviewer"),
      ].filter((session): session is WorkflowMilestoneSession => Boolean(session))
      return chain.map((session, index) =>
        displayEdge(
          "session",
          index === 0 ? String(milestone.id) : milestoneSessionID(milestone, chain[index - 1]),
          milestoneSessionID(milestone, session),
        ),
      )
    })
}

function terminalMilestones(milestones: WorkflowMilestone[]) {
  const dependencies = new Set(milestones.flatMap((milestone) => milestone.dependsOn.map((dependency) => String(dependency))))
  return milestones.filter((milestone) => !dependencies.has(String(milestone.id)))
}

function milestoneSessionID(milestone: WorkflowMilestone, session: WorkflowMilestoneSession) {
  return `${milestone.id}:${session.role}:${session.attempt ?? 0}`
}

function displayEdge(kind: WorkflowFlowEdgeKind, from: string, to: string): WorkflowDisplayEdge {
  return {
    id: `${from}->${to}:${kind}`,
    from,
    to,
    kind,
  }
}

function edgeTooltip(edge: WorkflowGraph["edges"][number]) {
  if (edge.kind === "consultation") {
    return [
      edge.summary,
      edge.question ? `Question:\n${edge.question}` : undefined,
      edge.answer ? `Answer:\n${edge.answer}` : undefined,
    ]
      .filter(Boolean)
      .join("\n\n")
  }
  if (edge.kind === "document") return [edge.summary, edge.path].filter(Boolean).join("\n\n")
  return edge.summary
}

function uniqueEdges(edges: WorkflowDisplayEdge[]) {
  const result = new Map<string, WorkflowDisplayEdge>()
  edges.forEach((edge) => {
    result.set(edge.id, edge)
  })
  return Array.from(result.values())
}

function workflowCurrentNodeIDs(graph: WorkflowGraph, currentSessionID: string | undefined) {
  if (!currentSessionID) return new Set<string>()
  const matches = graph.nodes.filter((node) => node.sessionID === currentSessionID)
  const exact = matches.filter((node) => node.type !== "milestone")
  return new Set((exact.length > 0 ? exact : matches).map((node) => node.id))
}

function workflowNodeData(
  node: WorkflowGraph["nodes"][number],
  milestone: WorkflowGraph["milestones"][number] | undefined,
  session: { milestone: WorkflowGraph["milestones"][number]; session: WorkflowGraph["milestones"][number]["session"][number] } | undefined,
  workflowStatus: WorkflowGraph["workflow"]["status"],
  workflowRequest: string,
  milestoneLabel: string,
  sessionState: Record<string, WorkflowReactFlowSessionState | undefined>,
  currentSession: boolean,
): WorkflowFlowNodeData {
  const state = nodeSessionState(node, milestone, session, workflowStatus, sessionState)
  if (node.type === "milestone") {
    return {
      kind: "milestone",
      title: milestone?.title ?? node.title,
      label: milestone?.department ?? milestoneLabel,
      status: milestone?.status ?? node.status,
      summary: milestone?.attempt ? `#${milestone.attempt}` : undefined,
      prompt: milestone?.prompt,
      sessionID: node.sessionID,
      sessionState: state,
      currentSession,
      planPath: milestone?.planPath ?? node.path,
      width: 214,
      height: 92,
    }
  }
  if (node.type === "workflow") {
    return {
      kind: "workflow",
      title: node.title,
      label: roleLabel(node.role ?? "requester"),
      status: node.status ?? workflowStatus,
      prompt: workflowRequest,
      sessionID: node.sessionID,
      sessionState: state,
      currentSession,
      width: 178,
      height: 58,
    }
  }
  if (node.type === "document") {
    return {
      kind: "document",
      title: node.title,
      label: "Document",
      summary: node.role ? roleLabel(node.role) : undefined,
      prompt: [node.summary, node.path].filter(Boolean).join("\n\n"),
      sessionID: node.sessionID,
      sessionState: state,
      currentSession,
      planPath: node.path,
      width: 184,
      height: 56,
    }
  }
  return {
    kind: "session",
    title: session?.milestone.title ?? node.title,
    label: roleLabel(node.role ?? session?.session.role ?? "main_pm"),
    summary: session?.session.attempt ? `#${session.session.attempt}` : undefined,
    prompt: session?.milestone.prompt ?? workflowRequest,
    sessionID: node.sessionID,
    sessionState: state,
    currentSession,
    planPath: node.path,
    width: 170,
    height: 50,
  }
}

function WorkflowFlowEdgeView(props: EdgeProps<WorkflowFlowEdge>) {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX: props.sourceX,
    sourceY: props.sourceY,
    sourcePosition: props.sourcePosition,
    targetX: props.targetX,
    targetY: props.targetY,
    targetPosition: props.targetPosition,
  })
  return React.createElement(React.Fragment, null, [
    React.createElement(BaseEdge, {
      key: "edge",
      id: props.id,
      path: edgePath,
      markerEnd: props.markerEnd,
      style: props.style,
    }),
    props.data?.tooltip
      ? React.createElement(
          EdgeLabelRenderer,
          {
            key: "label",
            children: React.createElement(
              "div",
              {
                className: `workflow-rf-edge-label workflow-rf-edge-label--${props.data.kind}`,
                style: {
                  transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
                },
                title: props.data.tooltip,
              },
              props.data.label ?? (props.data.kind === "consultation" ? "consult" : "doc"),
            ),
          },
        )
      : null,
  ])
}

function layoutNodes(nodes: WorkflowFlowNode[], edges: WorkflowDisplayEdge[]) {
  const graph = new dagre.graphlib.Graph()
  graph.setDefaultEdgeLabel(() => ({}))
  graph.setGraph({
    rankdir: "LR",
    ranker: "network-simplex",
    nodesep: 46,
    ranksep: 86,
    marginx: 28,
    marginy: 24,
  })
  nodes.forEach((node) => {
    graph.setNode(node.id, {
      width: node.data.width,
      height: node.data.height,
    })
  })
  const nodeIDs = new Set(nodes.map((node) => node.id))
  edges
    .filter((edge) => nodeIDs.has(edge.from) && nodeIDs.has(edge.to))
    .forEach((edge) => graph.setEdge(edge.from, edge.to))
  dagre.layout(graph)
  return nodes.map((node) => {
    const position = graph.node(node.id)
    return {
      ...node,
      position: {
        x: position.x - node.data.width / 2,
        y: position.y - node.data.height / 2,
      },
    }
  })
}

function WorkflowFlowNodeView(props: NodeProps<WorkflowFlowNode>) {
  const data = props.data
  return React.createElement(
    "div",
    {
      className: `workflow-rf-node workflow-rf-node--${data.kind} workflow-rf-node--${statusTone(data.status)} workflow-rf-node--${
        data.sessionID ? "linked" : "unlinked"
      }${data.sessionState ? ` workflow-rf-node--session-${data.sessionState}` : ""}${
        data.currentSession ? " workflow-rf-node--current-session" : ""
      }`,
      title: data.prompt,
    },
    [
      React.createElement(Handle, {
        key: "target",
        type: "target",
        position: Position.Left,
        className: "workflow-rf-handle",
      }),
      React.createElement(
        "div",
        {
          key: "body",
          className: "workflow-rf-node__body",
        },
        [
          React.createElement(
            "div",
            {
              key: "meta",
              className: "workflow-rf-node__meta",
            },
            [
              React.createElement(
                "span",
                {
                  key: "label",
                  className: "workflow-rf-node__label",
                },
                data.label,
              ),
              data.summary
                ? React.createElement(
                    "span",
                    {
                      key: "summary",
                      className: "workflow-rf-node__summary",
                    },
                    data.summary,
                  )
                : null,
            ],
          ),
          React.createElement(
            "div",
            {
              key: "title",
              className: "workflow-rf-node__title",
            },
            data.title,
          ),
          data.status
            ? React.createElement(
                "span",
                {
                  key: "status",
                  className: "workflow-rf-node__status",
                },
                data.status,
              )
            : null,
        ],
      ),
      React.createElement(Handle, {
        key: "source",
        type: "source",
        position: Position.Right,
        className: "workflow-rf-handle",
      }),
      data.currentSession
        ? React.createElement("span", {
            key: "locator",
            className: "workflow-rf-node__locator",
            "aria-hidden": true,
          })
        : null,
      data.prompt
        ? React.createElement(
            "div",
            {
              key: "prompt",
              className: "workflow-rf-node__prompt",
            },
            data.prompt,
          )
        : null,
    ],
  )
}

function nodeSessionState(
  node: WorkflowGraph["nodes"][number],
  milestone: WorkflowGraph["milestones"][number] | undefined,
  session: { milestone: WorkflowGraph["milestones"][number]; session: WorkflowGraph["milestones"][number]["session"][number] } | undefined,
  workflowStatus: WorkflowGraph["workflow"]["status"],
  sessionState: Record<string, WorkflowReactFlowSessionState | undefined>,
) {
  if (!node.sessionID) return undefined
  if (node.type === "milestone" && milestoneFailed(milestone?.status ?? node.status)) return undefined
  if (node.type === "workflow" && workflowFailed(workflowStatus)) return undefined
  const explicit = sessionState[node.sessionID]
  if (explicit) return explicit
  if (node.type === "session" || session) return "completed"
  if (node.type === "milestone" && milestoneComplete(milestone?.status ?? node.status)) return "completed"
  if (node.type === "workflow" && workflowStatus === "completed") return "completed"
  return undefined
}

function milestoneComplete(status: string | undefined) {
  return status === "approved" || status === "done" || status === "completed"
}

function milestoneFailed(status: string | undefined) {
  return status === "rejected" || status === "blocked" || status === "failed" || status === "cancelled"
}

function workflowFailed(status: string | undefined) {
  return status === "blocked" || status === "failed" || status === "cancelled"
}

function roleLabel(role: string) {
  if (role in roleTitles) return roleTitles[role as keyof typeof roleTitles]
  return role
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
}

function statusTone(status: string | undefined) {
  if (!status) return "neutral"
  if (status === "approved" || status === "completed" || status === "done") return "success"
  if (status === "rejected" || status === "blocked" || status === "failed" || status === "cancelled") return "danger"
  if (status === "pending") return "pending"
  return "active"
}
