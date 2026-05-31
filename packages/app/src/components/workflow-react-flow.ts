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
  documentKind?: "reference" | "intervention" | "standup" | "summary" | "document"
  role?: string
  department?: string
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
  expert: "Technical Advisor",
}

const nodeTypes: NodeTypes = {
  workflowNode: WorkflowFlowNodeView,
}

const edgeTypes = {
  workflowEdge: WorkflowFlowEdgeView,
}

const panelFitViewOptions = {
  padding: 0.18,
  minZoom: 0.3,
  maxZoom: 1.15,
}

const fullscreenFitViewOptions = {
  padding: 0.24,
  minZoom: 0.14,
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
  const fullscreen = props.viewport === "fullscreen"
  const model = React.useMemo(
    () => workflowModel(props.graph, props.milestoneLabel, props.currentSessionID, props.sessionState ?? {}, props.viewport ?? "panel"),
    [props.graph, props.milestoneLabel, props.currentSessionID, props.sessionState, props.viewport],
  )
  return React.createElement(ReactFlow, {
    key: `${props.graph.workflow.id}:${props.graph.workflow.time.updated}:${props.viewport ?? "panel"}:${props.currentSessionID ?? ""}:${model.nodes.length}:${model.edges.length}:${model.stateKey}`,
    className: "workflow-react-flow",
    nodes: model.nodes,
    edges: model.edges,
    nodeTypes,
    edgeTypes,
    fitView: true,
    fitViewOptions: fullscreen ? fullscreenFitViewOptions : panelFitViewOptions,
    minZoom: fullscreen ? 0.12 : 0.25,
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
  viewport: WorkflowReactFlowProps["viewport"],
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
  const rawNodes = graph.nodes.map((node, index) => {
    const milestone = (node.milestoneID ? milestones.get(node.milestoneID) : undefined) ?? workflowMilestoneFromPath(node.path, milestones)
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
  })
  const nodes = layoutNodes(rawNodes, displayEdges, viewport ?? "panel", workflowFunctionColumns(rawNodes))
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
  return uniqueEdges(
    graph.edges
      .map((edge) => ({
        id: edge.id,
        from: edge.from,
        to: edge.to,
        kind: edge.kind ?? ("dependency" as const),
        label: edge.label,
        tooltip: edgeTooltip(edge),
      }))
      .filter((edge) => nodeIDs.has(edge.from) && nodeIDs.has(edge.to) && edge.from !== edge.to),
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

function workflowMilestoneFromPath(filePath: string | undefined, milestones: Map<string, WorkflowMilestone>) {
  if (!filePath) return undefined
  return filePath
    .split(/[\\/]+/)
    .map((part) => milestones.get(part))
    .find((milestone): milestone is WorkflowMilestone => Boolean(milestone))
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
      role: node.role,
      department: milestone?.department ?? milestoneLabel,
      title: milestone?.title ?? node.title,
      label: milestone?.department ?? milestoneLabel,
      status: milestone?.status ?? node.status,
      summary: milestone?.attempt ? `#${milestone.attempt}` : undefined,
      prompt: milestone?.prompt,
      sessionID: node.sessionID,
      sessionState: state,
      currentSession,
      planPath: milestone?.planPath ?? node.path,
      width: 236,
      height: 104,
    }
  }
  if (node.type === "workflow") {
    return {
      kind: "workflow",
      role: node.role,
      title: node.title,
      label: roleLabel(node.role ?? "requester"),
      status: node.status ?? workflowStatus,
      prompt: workflowRequest,
      sessionID: node.sessionID,
      sessionState: state,
      currentSession,
      width: 206,
      height: 68,
    }
  }
  if (node.type === "document") {
    const documentKind = workflowDocumentKind(node)
    return {
      kind: "document",
      title: node.title,
      documentKind,
      department: milestone?.department ?? session?.milestone.department,
      label: workflowDocumentLabel(documentKind),
      summary: node.role ? roleLabel(node.role) : undefined,
      prompt: [node.summary, node.path].filter(Boolean).join("\n\n"),
      sessionID: node.sessionID,
      sessionState: state,
      currentSession,
      planPath: node.path,
      width: 196,
      height: 62,
    }
  }
  return {
    kind: "session",
    role: node.role ?? session?.session.role,
    department: session?.milestone.department ?? milestone?.department,
    title: session?.milestone.title ?? node.title,
    label: roleLabel(node.role ?? session?.session.role ?? "main_pm"),
    summary: node.summary ?? (session?.session.attempt ? `#${session.session.attempt}` : undefined),
    prompt: node.summary ?? session?.milestone.prompt ?? workflowRequest,
    sessionID: node.sessionID,
    sessionState: state,
    currentSession,
    planPath: node.path,
    width: 198,
    height: 58,
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

function layoutNodes(
  nodes: WorkflowFlowNode[],
  edges: WorkflowDisplayEdge[],
  viewport: "panel" | "fullscreen",
  functionColumns: Map<string, number>,
) {
  const graph = new dagre.graphlib.Graph()
  graph.setDefaultEdgeLabel(() => ({}))
  graph.setGraph({
    rankdir: "LR",
    ranker: "network-simplex",
    nodesep: viewport === "fullscreen" ? 170 : 110,
    ranksep: viewport === "fullscreen" ? 320 : 220,
    edgesep: viewport === "fullscreen" ? 62 : 42,
    marginx: viewport === "fullscreen" ? 120 : 64,
    marginy: viewport === "fullscreen" ? 80 : 42,
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
  const positioned = nodes.map((node, index) => {
    const position = graph.node(node.id)
    return {
      node,
      index,
      column: workflowNodeColumn(node.data, functionColumns),
      order: position?.y ?? index,
    }
  })
  const columnGap = viewport === "fullscreen" ? 340 : 250
  const rowGap = viewport === "fullscreen" ? 150 : 108
  const marginX = viewport === "fullscreen" ? 120 : 64
  const marginY = viewport === "fullscreen" ? 80 : 42
  const columns = new Map<number, typeof positioned>()
  positioned.forEach((item) => {
    columns.set(item.column, [...(columns.get(item.column) ?? []), item])
  })
  const maxRows = Math.max(1, ...Array.from(columns.values()).map((items) => items.length))
  const output = new Map<string, WorkflowFlowNode>()
  Array.from(columns.entries()).forEach(([column, items]) => {
    const sorted = items.toSorted((a, b) => a.order - b.order || a.index - b.index || a.node.id.localeCompare(b.node.id))
    const yStart = marginY + ((maxRows - sorted.length) * rowGap) / 2
    sorted.forEach((item, index) => {
      output.set(item.node.id, {
        ...item.node,
        position: {
          x: marginX + column * columnGap,
          y: yStart + index * rowGap,
        },
      })
    })
  })
  return nodes.map((node) => output.get(node.id) ?? node)
}

function workflowFunctionColumns(nodes: WorkflowFlowNode[]) {
  const firstSeen = new Map<string, number>()
  nodes
    .map((node) => workflowFunctionKeyForData(node.data))
    .filter((key): key is string => Boolean(key))
    .forEach((key, index) => {
      if (!firstSeen.has(key)) firstSeen.set(key, index)
    })
  return new Map(
    Array.from(firstSeen.keys())
      .toSorted((a, b) => workflowFunctionGroup(a) - workflowFunctionGroup(b) || (firstSeen.get(a) ?? 0) - (firstSeen.get(b) ?? 0))
      .map((key, index) => [key, index]),
  )
}

function workflowNodeColumn(data: WorkflowFlowNodeData, functionColumns: Map<string, number>) {
  if (data.role === "requester") return 0
  if (data.role === "main_pm" || data.kind === "workflow") return 1
  const functionKey = workflowFunctionKeyForData(data)
  const functionIndex = functionKey ? functionColumns.get(functionKey) ?? functionColumns.size : functionColumns.size
  return 2 + functionIndex * 5 + workflowFunctionStage(data)
}

function workflowFunctionStage(data: WorkflowFlowNodeData) {
  if (data.kind === "milestone") return 0
  if (data.role === "department_pm" || data.role === "expert") return 1
  if (data.role === "executor") return 2
  if (data.role === "reviewer" || data.role === "tester") return 3
  if (data.kind === "document") return 4
  if (data.kind === "session") return 2
  return 0
}

function workflowFunctionKeyForData(data: WorkflowFlowNodeData) {
  if (data.role === "requester" || data.role === "main_pm" || data.kind === "workflow") return undefined
  if (data.department) return workflowFunctionKey(data.department)
  if (data.kind === "document") return "documentation"
  if (data.role === "reviewer" || data.role === "tester") return "quality"
  if (data.role === "executor") return "engineering"
  return data.role ? workflowFunctionKey(data.role) : "general"
}

function workflowFunctionKey(value: string) {
  return value
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, "-")
    .replace(/^-+|-+$/g, "")
    .trim() || "general"
}

function workflowFunctionGroup(key: string) {
  if (/(product|requirement|requirements|request|scope)/.test(key)) return 0
  if (/(architecture|design|system|contract|profile|composition)/.test(key)) return 1
  if (/(integration|release|build|ci|staged)/.test(key)) return 3
  if (/(quality|test|tests|verify|verification|acceptance|review)/.test(key)) return 4
  if (/(doc|docs|document|reference|summary)/.test(key)) return 5
  return 2
}

function WorkflowFlowNodeView(props: NodeProps<WorkflowFlowNode>) {
  const data = props.data
  return React.createElement(
    "div",
    {
      className: `workflow-rf-node workflow-rf-node--${data.kind} workflow-rf-node--${statusTone(data.status)} workflow-rf-node--${
        data.sessionID ? "linked" : "unlinked"
      }${data.role ? ` workflow-rf-node--role workflow-rf-node--role-${roleClass(data.role)}` : ""}${
        data.documentKind ? ` workflow-rf-node--doc-${data.documentKind}` : ""}${
        data.sessionState ? ` workflow-rf-node--session-${data.sessionState}` : ""
      }${data.currentSession ? " workflow-rf-node--current-session" : ""}`,
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
              data.role
                ? React.createElement("span", {
                    key: "role-icon",
                    className: "workflow-rf-node__role-icon",
                    "aria-hidden": true,
                  })
                : null,
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

function roleClass(role: string) {
  return role.replace(/[^a-z0-9]+/gi, "-").toLowerCase()
}

function workflowDocumentKind(node: WorkflowGraph["nodes"][number]): WorkflowFlowNodeData["documentKind"] {
  if (node.path?.includes("/standups/") || node.path?.includes("\\standups\\") || node.title.toLowerCase().includes("standup"))
    return "standup"
  if (
    node.path?.includes("/interventions/") ||
    node.path?.includes("\\interventions\\") ||
    node.title.toLowerCase().includes("intervention")
  )
    return "intervention"
  if (node.title.toLowerCase().includes("summary") || node.path?.toLowerCase().includes("-summary.md")) return "summary"
  if (node.path?.includes("/reference/") || node.path?.includes("\\reference\\") || node.title.toLowerCase().includes("reference"))
    return "reference"
  return "document"
}

function workflowDocumentLabel(kind: WorkflowFlowNodeData["documentKind"]) {
  if (kind === "reference") return "Reference"
  if (kind === "intervention") return "Intervention"
  if (kind === "standup") return "Standup"
  if (kind === "summary") return "Summary"
  return "Document"
}

function statusTone(status: string | undefined) {
  if (!status) return "neutral"
  if (status === "approved" || status === "completed" || status === "done") return "success"
  if (status === "rejected" || status === "blocked" || status === "failed" || status === "cancelled") return "danger"
  if (status === "pending") return "pending"
  return "active"
}
