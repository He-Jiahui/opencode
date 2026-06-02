import type { AssistantMessage, Project, UserMessage, WorkflowGraph } from "@opencode-ai/sdk/v2"
import { useDialog } from "@opencode-ai/ui/context/dialog"
import { createQuery, skipToken, useMutation, useQueryClient } from "@tanstack/solid-query"
import {
  batch,
  onCleanup,
  Show,
  Match,
  Switch,
  createMemo,
  createEffect,
  createComputed,
  createSignal,
  on,
  onMount,
  untrack,
  createResource,
  For,
} from "solid-js"
import { Portal } from "solid-js/web"
import { makeEventListener } from "@solid-primitives/event-listener"
import { createMediaQuery } from "@solid-primitives/media"
import { createResizeObserver } from "@solid-primitives/resize-observer"
import { debounce } from "@solid-primitives/scheduled"
import { useLocal } from "@/context/local"
import { selectionFromLines, useFile, type FileSelection, type SelectedLineRange } from "@/context/file"
import { createStore } from "solid-js/store"
import { ResizeHandle } from "@opencode-ai/ui/resize-handle"
import { Select } from "@opencode-ai/ui/select"
import { Tabs } from "@opencode-ai/ui/tabs"
import { createAutoScroll } from "@opencode-ai/ui/hooks"
import { previewSelectedLines } from "@opencode-ai/ui/pierre/selection-bridge"
import { Button } from "@opencode-ai/ui/button"
import { IconButton } from "@opencode-ai/ui/icon-button"
import { TextField } from "@opencode-ai/ui/text-field"
import { Tooltip } from "@opencode-ai/ui/tooltip"
import { showToast } from "@opencode-ai/ui/toast"
import { checksum } from "@opencode-ai/core/util/encode"
import { getFilename } from "@opencode-ai/core/util/path"
import { useLocation, useNavigate, useSearchParams } from "@solidjs/router"
import { NewSessionDesignView, NewSessionView, SessionHeader } from "@/components/session"
import { useComments } from "@/context/comments"
import { getSessionPrefetch, SESSION_PREFETCH_TTL } from "@/context/global-sync/session-prefetch"
import { useServerSync } from "@/context/server-sync"
import { useLanguage } from "@/context/language"
import { useLayout } from "@/context/layout"
import { usePrompt } from "@/context/prompt"
import { useSDK } from "@/context/sdk"
import { useSettings } from "@/context/settings"
import { useSync } from "@/context/sync"
import { useTerminal } from "@/context/terminal"
import { type FollowupDraft, sendFollowupDraft } from "@/components/prompt-input/submit"
import type { OpencodePlanBlock } from "@/utils/opencode-plan"
import { createSessionComposerState, SessionComposerRegion } from "@/pages/session/composer"
import {
  createOpenReviewFile,
  createSessionTabs,
  createSizing,
  focusTerminalById,
  shouldFocusTerminalOnKeyDown,
} from "@/pages/session/helpers"
import { MessageTimeline } from "@/pages/session/message-timeline"
import { type DiffStyle, SessionReviewTab, type SessionReviewTabProps } from "@/pages/session/review-tab"
import { useSessionLayout } from "@/pages/session/session-layout"
import { syncSessionModel } from "@/pages/session/session-model-helpers"
import { SessionSidePanel } from "@/pages/session/session-side-panel"
import { TerminalPanel } from "@/pages/session/terminal-panel"
import { useSessionCommands } from "@/pages/session/use-session-commands"
import { useSessionHashScroll } from "@/pages/session/use-session-hash-scroll"
import { shouldUseV2NewSessionPage } from "@/pages/session/new-session-layout"
import {
  mountWorkflowReactFlow,
  type WorkflowReactFlowContextTarget,
  type WorkflowReactFlowInstance,
  type WorkflowReactFlowProps,
  type WorkflowReactFlowSessionState,
  type WorkflowReactFlowTarget,
} from "@/components/workflow-react-flow"
import { Identifier } from "@/utils/id"
import { diffs as list } from "@/utils/diffs"
import { Persist, persisted } from "@/utils/persist"
import { extractPromptFromParts } from "@/utils/prompt"
import { same } from "@/utils/same"
import { formatServerError } from "@/utils/server-errors"
import { useUsageExceededDialogs } from "./session/usage-exceeded-dialogs"

const emptyUserMessages: UserMessage[] = []
type FollowupItem = FollowupDraft & { id: string }
type FollowupEdit = Pick<FollowupItem, "id" | "prompt" | "context">
const emptyFollowups: FollowupItem[] = []
const PLAN_COMPLETE_MARKER = "OPENCODE_PLAN_COMPLETE"
const PLAN_MILESTONE_TAG = "opencode-plan-milestone"
const PLAN_MILESTONE_OPEN = `<${PLAN_MILESTONE_TAG}>`
const PLAN_MILESTONE_CLOSE = `</${PLAN_MILESTONE_TAG}>`
const PLAN_MILESTONE_PATTERN = /<opencode-plan-milestone>([\s\S]*?)<\/opencode-plan-milestone>/g
const PLAN_FOLLOWUP_PREFIX = "plan:"
const PLAN_FOLLOWUP_CONTENT = [
  "Continue plan mode.",
  "",
  "Re-read the attached plan file and compare it against the work completed in this session.",
  "At the start of every response, report the current plan milestone in this exact block:",
  PLAN_MILESTONE_OPEN,
  "completed: <what you completed in this response, or none>",
  "current: <current milestone or feature task>",
  "remaining: <what planned work remains after this response>",
  PLAN_MILESTONE_CLOSE,
  "Do not omit this block; the desktop app reads it to show plan progress.",
  "Then return a concise status update, including whether any planned work remains.",
  "If any planned work remains incomplete, continue with the next unfinished item now.",
  `If the entire plan is complete, reply with ${PLAN_COMPLETE_MARKER} on its own line and do not start new work.`,
].join("\n")

type PlanMilestone = {
  id: string
  messageID: string
  parentID: string
  at: number
  text: string
}

type PlanSessionState = {
  enabled: Record<string, boolean | undefined>
  file: Record<string, string | undefined>
  lastAssistant: Record<string, string | undefined>
  milestones: Record<string, PlanMilestone[] | undefined>
}
const emptyPlanMilestones: PlanMilestone[] = []

const emptyWorkflowList: WorkflowGraph["workflow"][] = []
type WorkflowStaffing = NonNullable<WorkflowGraph["workflow"]["staffing"]>
const defaultWorkflowStaffing: Required<WorkflowStaffing> = {
  mainPM: 1,
  departmentPM: 2,
  executor: 4,
  reviewer: 2,
  tester: 1,
  expert: 1,
}
const workflowStaffingFields = [
  ["mainPM", "session.workflow.staffing.mainPM"],
  ["departmentPM", "session.workflow.staffing.departmentPM"],
  ["executor", "session.workflow.staffing.executor"],
  ["reviewer", "session.workflow.staffing.reviewer"],
  ["tester", "session.workflow.staffing.tester"],
  ["expert", "session.workflow.staffing.expert"],
] as const
const workflowStaffingValue = (value: number | undefined, fallback: number) =>
  Number.isFinite(value) ? Math.max(1, Math.min(12, Math.trunc(value!))) : fallback
const normalizeWorkflowStaffing = (input?: WorkflowStaffing): Required<WorkflowStaffing> => ({
  mainPM: workflowStaffingValue(input?.mainPM, defaultWorkflowStaffing.mainPM),
  departmentPM: workflowStaffingValue(input?.departmentPM, defaultWorkflowStaffing.departmentPM),
  executor: workflowStaffingValue(input?.executor, defaultWorkflowStaffing.executor),
  reviewer: workflowStaffingValue(input?.reviewer, defaultWorkflowStaffing.reviewer),
  tester: workflowStaffingValue(input?.tester, defaultWorkflowStaffing.tester),
  expert: workflowStaffingValue(input?.expert, defaultWorkflowStaffing.expert),
})
type WorkflowInterventionTiming = "after-task" | "temporary-interrupt" | "interrupt"
const workflowInterventionTimingOptions: WorkflowInterventionTiming[] = [
  "temporary-interrupt",
  "after-task",
  "interrupt",
]
type WorkflowInterventionTargetRole = "main_pm" | "department_pm" | "executor" | "reviewer" | "tester" | "expert"
const workflowInterventionTargetRoleOptions: WorkflowInterventionTargetRole[] = [
  "main_pm",
  "department_pm",
  "executor",
  "reviewer",
  "tester",
  "expert",
]

const workflowStatusTone = (status: WorkflowGraph["workflow"]["status"] | WorkflowGraph["milestones"][number]["status"]) => {
  if (status === "approved" || status === "completed" || status === "done") return "bg-success/10 text-success"
  if (status === "rejected" || status === "blocked" || status === "failed" || status === "cancelled")
    return "bg-danger/10 text-danger"
  if (status === "pending") return "bg-surface-element text-text-weak"
  return "bg-accent/10 text-accent"
}

type WorkflowSessionLogRole = NonNullable<WorkflowGraph["nodes"][number]["role"]>
type WorkflowSessionLogSource = "workflow" | "trigger" | "staff"
type WorkflowSessionLogEntry = {
  role: WorkflowSessionLogRole
  source: WorkflowSessionLogSource
  title: string
  status: string
  sessionID?: string
  specialty?: string
  milestoneID?: string
  milestoneTitle?: string
  attempt?: number
  current: boolean
}
type WorkflowSessionLogGroup = {
  role: WorkflowSessionLogRole
  entries: WorkflowSessionLogEntry[]
}
const workflowSessionLogRoleOrder: WorkflowSessionLogRole[] = [
  "requester",
  "main_pm",
  "department_pm",
  "expert",
  "executor",
  "reviewer",
  "tester",
]
const workflowSessionRefKey = (role: WorkflowSessionLogRole, sessionID: string) => `${role}:${sessionID}`
const workflowSessionLogStatusTone = (status: string) => {
  if (status === "approved" || status === "completed" || status === "done" || status === "active")
    return "bg-success/10 text-success"
  if (
    status === "rejected" ||
    status === "blocked" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "paused"
  )
    return "bg-danger/10 text-danger"
  if (status === "pending" || status === "untriggered") return "bg-surface-element text-text-weak"
  return "bg-accent/10 text-accent"
}
const workflowSessionLogAttempt = (attempt: WorkflowGraph["milestones"][number]["attempt"] | undefined) =>
  typeof attempt === "number" ? attempt : undefined
const workflowSessionLogGroups = (
  graph: WorkflowGraph,
  currentSessionID: string | undefined,
  sessionRunning: (sessionID: string) => boolean,
): WorkflowSessionLogGroup[] => {
  const memberBySessionRole = new Map(
    graph.members.map((member) => [workflowSessionRefKey(member.role, member.sessionID), member] as const),
  )
  const baseEntryCandidates: (WorkflowSessionLogEntry | undefined)[] = [
    graph.workflow.rootSessionID
      ? {
          role: "requester" as const,
          source: "workflow" as const,
          title: graph.workflow.title,
          status: sessionRunning(graph.workflow.rootSessionID) ? "running" : graph.workflow.status,
          sessionID: graph.workflow.rootSessionID,
          current: graph.workflow.rootSessionID === currentSessionID,
        }
      : undefined,
    graph.workflow.pmSessionID
      ? {
          role: "main_pm" as const,
          source: "workflow" as const,
          title:
            memberBySessionRole.get(workflowSessionRefKey("main_pm", graph.workflow.pmSessionID))?.title ??
            "Main product manager",
          status: sessionRunning(graph.workflow.pmSessionID) ? "running" : graph.workflow.status,
          sessionID: graph.workflow.pmSessionID,
          current: graph.workflow.pmSessionID === currentSessionID,
        }
      : undefined,
    graph.workflow.testerSessionID
      ? {
          role: "tester" as const,
          source: "workflow" as const,
          title:
            memberBySessionRole.get(workflowSessionRefKey("tester", graph.workflow.testerSessionID))?.title ??
            "Workflow tester",
          status: sessionRunning(graph.workflow.testerSessionID) ? "running" : graph.workflow.status,
          sessionID: graph.workflow.testerSessionID,
          current: graph.workflow.testerSessionID === currentSessionID,
        }
      : undefined,
  ]
  const baseEntries = baseEntryCandidates.filter((entry): entry is WorkflowSessionLogEntry => !!entry)
  const triggerEntries = graph.milestones.flatMap((milestone) =>
    milestone.session.map((ref): WorkflowSessionLogEntry => {
      const member = memberBySessionRole.get(workflowSessionRefKey(ref.role, ref.sessionID))
      return {
        role: ref.role,
        source: "trigger",
        title: member?.title ?? `${ref.role} ${milestone.title ?? milestone.id}`,
        status: sessionRunning(ref.sessionID) ? "running" : milestone.status,
        sessionID: ref.sessionID,
        specialty: member?.specialty ?? milestone.department,
        milestoneID: String(ref.milestoneID ?? milestone.id),
        milestoneTitle: milestone.title,
        attempt: workflowSessionLogAttempt(ref.attempt ?? milestone.attempt),
        current: ref.sessionID === currentSessionID,
      }
    }),
  )
  const usedSessions = new Set(
    [...baseEntries, ...triggerEntries]
      .filter((entry): entry is WorkflowSessionLogEntry & { sessionID: string } => !!entry.sessionID)
      .map((entry) => workflowSessionRefKey(entry.role, entry.sessionID)),
  )
  const staffEntries = graph.members
    .filter((member) => !usedSessions.has(workflowSessionRefKey(member.role, member.sessionID)))
    .map(
      (member): WorkflowSessionLogEntry => ({
        role: member.role,
        source: "staff",
        title: member.title,
        status: sessionRunning(member.sessionID) ? "running" : "untriggered",
        sessionID: member.sessionID,
        specialty: member.specialty,
        current: member.sessionID === currentSessionID,
      }),
    )
  const entries = [...baseEntries, ...triggerEntries, ...staffEntries]
  return workflowSessionLogRoleOrder
    .map((role) => ({
      role,
      entries: entries.filter((entry) => entry.role === role),
    }))
    .filter((group) => group.entries.length > 0)
}

type ChangeMode = "git" | "branch" | "turn"
type VcsMode = "git" | "branch"

type SessionHistoryWindowInput = {
  sessionID: () => string | undefined
  loaded: () => number
  visibleUserMessages: () => UserMessage[]
  historyMore: () => boolean
  historyLoading: () => boolean
  loadMore: (sessionID: string) => Promise<void>
  userScrolled: () => boolean
  scroller: () => HTMLDivElement | undefined
}

function createSessionHistoryLoader(input: SessionHistoryWindowInput) {
  const historyScrollThreshold = 200
  let shiftFrame: number | undefined

  const [state, setState] = createStore({
    shift: false,
  })

  const userMessages = createMemo(() => input.visibleUserMessages(), emptyUserMessages, {
    equals: same,
  })

  const cancelShiftReset = () => {
    if (shiftFrame === undefined) return
    cancelAnimationFrame(shiftFrame)
    shiftFrame = undefined
  }

  const scheduleShiftReset = () => {
    cancelShiftReset()
    shiftFrame = requestAnimationFrame(() => {
      shiftFrame = undefined
      setState("shift", false)
    })
  }

  const fetchOlderMessages = async () => {
    const id = input.sessionID()
    if (!id) return
    if (!input.historyMore() || input.historyLoading()) return

    // TODO(session-timeline): switch this to core cursor-based part pagination when that API lands.
    const beforeVisible = input.visibleUserMessages().length
    let loaded = input.loaded()
    let growth = 0

    cancelShiftReset()
    setState("shift", true)

    while (true) {
      await input.loadMore(id)
      if (input.sessionID() !== id) return

      const nextLoaded = input.loaded()
      const raw = nextLoaded - loaded
      loaded = nextLoaded
      growth = input.visibleUserMessages().length - beforeVisible

      if (growth > 0) break
      if (raw <= 0) break
      if (!input.historyMore()) break
    }

    if (growth > 0) {
      scheduleShiftReset()
      return
    }

    setState("shift", false)
  }

  const loadAndReveal = () => fetchOlderMessages()

  const onScrollerScroll = () => {
    if (!input.userScrolled()) return
    const el = input.scroller()
    if (!el) return
    if (el.scrollTop >= historyScrollThreshold) return

    void fetchOlderMessages()
  }

  createEffect(
    on(
      input.sessionID,
      () => {
        cancelShiftReset()
        setState({ shift: false })
      },
      { defer: true },
    ),
  )

  onCleanup(cancelShiftReset)

  return {
    userMessages,
    shift: () => state.shift,
    loadAndReveal,
    onScrollerScroll,
  }
}

export default function Page() {
  const serverSync = useServerSync()
  const layout = useLayout()
  const local = useLocal()
  const file = useFile()
  const sync = useSync()
  const queryClient = useQueryClient()
  const dialog = useDialog()
  const language = useLanguage()
  const sdk = useSDK()
  const settings = useSettings()
  const prompt = usePrompt()
  const comments = useComments()
  const terminal = useTerminal()
  const [searchParams, setSearchParams] = useSearchParams<{ prompt?: string }>()
  const location = useLocation()
  const navigate = useNavigate()
  const { params, sessionKey, tabs, view } = useSessionLayout()
  const newSessionDesign = createMemo(() => settings.general.newLayoutDesigns())

  createEffect(() => {
    if (!prompt.ready()) return
    untrack(() => {
      if (params.id) return
      const text = searchParams.prompt
      if (!text) return
      prompt.set([{ type: "text", content: text, start: 0, end: text.length }], text.length)
      setSearchParams({ ...searchParams, prompt: undefined })
    })
  })

  const [ui, setUi] = createStore({
    pendingMessage: undefined as string | undefined,
    reviewSnap: false,
    scrollGesture: 0,
    scroll: {
      overflow: false,
      bottom: true,
      jump: false,
    },
  })

  const composer = createSessionComposerState()

  const workspaceKey = createMemo(() => params.dir ?? "")
  const workspaceTabs = createMemo(() => layout.tabs(workspaceKey))

  createEffect(
    on(
      () => params.id,
      (id, prev) => {
        if (!id) return
        if (prev) return

        const pending = layout.handoff.tabs()
        if (!pending) return
        if (Date.now() - pending.at > 60_000) {
          layout.handoff.clearTabs()
          return
        }

        if (pending.id !== id) return
        layout.handoff.clearTabs()
        if (pending.dir !== (params.dir ?? "")) return

        const from = workspaceTabs().tabs()
        if (from.all.length === 0 && !from.active) return

        const current = tabs().tabs()
        if (current.all.length > 0 || current.active) return

        const all = normalizeTabs(from.all)
        const active = from.active ? normalizeTab(from.active) : undefined
        tabs().setAll(all)
        tabs().setActive(active && all.includes(active) ? active : all[0])

        workspaceTabs().setAll([])
        workspaceTabs().setActive(undefined)
      },
      { defer: true },
    ),
  )

  const isDesktop = createMediaQuery("(min-width: 768px)")
  const size = createSizing()
  const isV2NewSessionPage = () =>
    shouldUseV2NewSessionPage({ newLayoutDesigns: newSessionDesign(), sessionID: params.id })
  const desktopReviewOpen = createMemo(() => isDesktop() && view().reviewPanel.opened() && !isV2NewSessionPage())
  const desktopFileTreeOpen = createMemo(() => isDesktop() && layout.fileTree.opened() && !isV2NewSessionPage())
  const desktopSidePanelOpen = createMemo(() => desktopReviewOpen() || desktopFileTreeOpen())
  const sessionPanelWidth = createMemo(() => {
    if (!desktopSidePanelOpen()) return "100%"
    if (desktopReviewOpen()) return `${layout.session.width()}px`
    return `calc(100% - ${layout.fileTree.width()}px)`
  })
  const centered = createMemo(() => isDesktop() && !desktopReviewOpen())

  function normalizeTab(tab: string) {
    if (!tab.startsWith("file://")) return tab
    return file.tab(tab)
  }

  function normalizeTabs(list: string[]) {
    const seen = new Set<string>()
    const next: string[] = []
    for (const item of list) {
      const value = normalizeTab(item)
      if (seen.has(value)) continue
      seen.add(value)
      next.push(value)
    }
    return next
  }

  const openReviewPanel = () => {
    if (!view().reviewPanel.opened()) view().reviewPanel.open()
  }

  const info = createMemo(() => (params.id ? sync.session.get(params.id) : undefined))
  const diffs = createMemo(() => (params.id ? list(sync.data.session_diff[params.id]) : []))
  const canReview = createMemo(() => !!sync.project)
  const reviewTab = createMemo(() => isDesktop())
  const tabState = createSessionTabs({
    tabs,
    pathFromTab: file.pathFromTab,
    normalizeTab,
    review: reviewTab,
    hasReview: canReview,
  })
  const activeTab = tabState.activeTab
  const activeFileTab = tabState.activeFileTab
  const revertMessageID = createMemo(() => info()?.revert?.messageID)
  const messages = createMemo(() => (params.id ? (sync.data.message[params.id] ?? []) : []))
  const messagesReady = createMemo(() => {
    const id = params.id
    if (!id) return true
    return sync.data.message[id] !== undefined
  })
  const historyMore = createMemo(() => {
    const id = params.id
    if (!id) return false
    return sync.session.history.more(id)
  })
  const historyLoading = createMemo(() => {
    const id = params.id
    if (!id) return false
    return sync.session.history.loading(id)
  })
  const userMessages = createMemo(
    () => messages().filter((m) => m.role === "user") as UserMessage[],
    emptyUserMessages,
    { equals: same },
  )
  const visibleUserMessages = createMemo(
    () => {
      const revert = revertMessageID()
      if (!revert) return userMessages()
      return userMessages().filter((m) => m.id < revert)
    },
    emptyUserMessages,
    {
      equals: same,
    },
  )
  const lastUserMessage = createMemo(() => visibleUserMessages().at(-1))
  const workflowReferencesSession = (workflow: WorkflowGraph["workflow"], sessionID: string) => {
    if (workflow.rootSessionID === sessionID || workflow.pmSessionID === sessionID || workflow.testerSessionID === sessionID)
      return true
    const graph = sync.data.workflow_graph[workflow.id]
    return (
      graph?.members?.some((member) => member.sessionID === sessionID) ||
      graph?.milestones?.some((milestone) => milestone.session?.some((ref) => ref.sessionID === sessionID)) ||
      false
    )
  }
  const sessionWorkflows = createMemo(() => {
    if (!params.id) return emptyWorkflowList
    return sync.data.workflow.filter((workflow) => workflowReferencesSession(workflow, params.id!))
  })
  const activeWorkflow = createMemo(() => sessionWorkflows().at(-1))
  const activeWorkflowGraph = createMemo(() => {
    const workflow = activeWorkflow()
    if (!workflow) return
    return sync.data.workflow_graph[workflow.id]
  })
  const sessionBelongsToWorkflow = createMemo(() => {
    const id = params.id
    const workflow = activeWorkflow()
    if (!id || !workflow) return false
    return workflow.rootSessionID !== id && workflowReferencesSession(workflow, id)
  })
  const [workflowUi, setWorkflowUi] = createStore({
    expanded: false,
    fullscreen: false,
    sessionLog: false,
    xml: "",
    editingXml: false,
    interventionMessage: "",
    interventionTargetRole: "main_pm" as WorkflowInterventionTargetRole,
    interventionTiming: "temporary-interrupt" as WorkflowInterventionTiming,
  })
  const [workflowStaffing, setWorkflowStaffing] = createStore(normalizeWorkflowStaffing())
  const [workflowMenu, setWorkflowMenu] = createSignal<WorkflowReactFlowContextTarget>()
  const workflowSessionLog = createMemo(() => {
    const graph = activeWorkflowGraph()
    if (!graph) return []
    return workflowSessionLogGroups(graph, params.id, (sessionID) => sync.data.session_working(sessionID))
  })
  const lastCompletedAssistant = createMemo(
    () => messages().findLast((m) => m.role === "assistant" && m.time.completed) as AssistantMessage | undefined,
  )

  createEffect(
    on(
      () => activeWorkflowGraph()?.workflow.xml ?? activeWorkflow()?.xml,
      (xml) => {
        if (!xml || workflowUi.editingXml) return
        setWorkflowUi("xml", xml)
      },
    ),
  )

  createEffect(
    on(
      () => activeWorkflowGraph()?.workflow.staffing ?? activeWorkflow()?.staffing,
      (staffing) => {
        setWorkflowStaffing(normalizeWorkflowStaffing(staffing))
      },
    ),
  )

  createEffect(() => {
    const tab = activeFileTab()
    if (!tab) return

    const path = file.pathFromTab(tab)
    if (path) void file.load(path)
  })

  createEffect(
    on(
      () => lastUserMessage()?.id,
      () => {
        const msg = lastUserMessage()
        if (!msg) return
        syncSessionModel(local, msg)
      },
    ),
  )

  createEffect(
    on(
      () => [sdk.directory, params.id] as const,
      ([, id]) => {
        if (!id) return
        void sync.session.workflow(id).catch((error) => {
          console.debug("[workflow] failed to load workflows", error)
        })
      },
    ),
  )

  createEffect(
    on(
      () => activeWorkflow()?.id,
      (workflowID) => {
        if (!workflowID) return
        void sync.session.workflowGraph(workflowID).catch((error) => {
          console.debug("[workflow] failed to load graph", error)
        })
      },
    ),
  )

  const stopWorkflowEvents = sdk.event.listen((event) => {
    const type = event.details.type
    if (
      type !== "workflow.created" &&
      type !== "workflow.updated" &&
      type !== "workflow.node.updated" &&
      type !== "workflow.graph.updated"
    )
      return
    const id = params.id
    if (!id) return
    if (type === "workflow.created" || type === "workflow.updated") void sync.session.workflow(id, { force: true })
    void sync.session.fetch(0)
    const workflowID =
      "workflowID" in event.details.properties ? event.details.properties.workflowID : activeWorkflow()?.id
    if (workflowID && (type === "workflow.node.updated" || type === "workflow.graph.updated"))
      void sync.session.workflowGraph(workflowID, { force: true })
  })
  onCleanup(stopWorkflowEvents)

  createEffect(
    on(
      () => ({ dir: params.dir, id: params.id }),
      (next, prev) => {
        if (!prev) return
        if (next.dir === prev.dir && next.id === prev.id) return
        if (prev.id && !next.id) local.session.reset()
      },
      { defer: true },
    ),
  )

  const [store, setStore] = createStore({
    messageId: undefined as string | undefined,
    mobileTab: "session" as "session" | "changes",
    changes: "git" as ChangeMode,
    newSessionWorktree: "main",
    deferRender: false,
  })

  const [followup, setFollowup] = persisted(
    Persist.workspace(sdk.directory, "followup", ["followup.v1"]),
    createStore<{
      items: Record<string, FollowupItem[] | undefined>
      failed: Record<string, string | undefined>
      paused: Record<string, boolean | undefined>
      edit: Record<string, FollowupEdit | undefined>
    }>({
      items: {},
      failed: {},
      paused: {},
      edit: {},
    }),
  )

  const [planSession, setPlanSession] = persisted(
    Persist.workspace(sdk.directory, "session-plan", ["session-plan.v1"]),
    createStore<PlanSessionState>({
      enabled: {},
      file: {},
      lastAssistant: {},
      milestones: {},
    }),
  )

  createComputed((prev) => {
    const key = sessionKey()
    if (key !== prev) {
      setStore("deferRender", true)
      requestAnimationFrame(() => {
        setTimeout(() => setStore("deferRender", false), 0)
      })
    }
    return key
  }, sessionKey())

  let reviewFrame: number | undefined
  let refreshFrame: number | undefined
  let refreshTimer: number | undefined
  let todoFrame: number | undefined
  let todoTimer: number | undefined
  let diffFrame: number | undefined
  let diffTimer: number | undefined

  createComputed((prev) => {
    const open = desktopReviewOpen()
    if (prev === undefined || prev === open) return open

    if (reviewFrame !== undefined) cancelAnimationFrame(reviewFrame)
    setUi("reviewSnap", true)
    reviewFrame = requestAnimationFrame(() => {
      reviewFrame = undefined
      setUi("reviewSnap", false)
    })
    return open
  }, desktopReviewOpen())

  const turnDiffs = createMemo(() => list(lastUserMessage()?.summary?.diffs))
  const nogit = createMemo(() => !!sync.project && sync.project.vcs !== "git")
  const changesOptions = createMemo<ChangeMode[]>(() => {
    const list: ChangeMode[] = []
    if (sync.project?.vcs === "git") list.push("git")
    if (
      sync.project?.vcs === "git" &&
      sync.data.vcs?.branch &&
      sync.data.vcs?.default_branch &&
      sync.data.vcs.branch !== sync.data.vcs.default_branch
    ) {
      list.push("branch")
    }
    list.push("turn")
    return list
  })
  const mobileChanges = createMemo(() => !isDesktop() && store.mobileTab === "changes")
  const planFile = createMemo(() => {
    const id = params.id
    if (!id) return
    return planSession.file[id]
  })
  const planActive = createMemo(() => {
    const id = params.id
    return !!id && !!planSession.enabled[id] && !!planSession.file[id]
  })
  const wantsReview = createMemo(() =>
    isDesktop()
      ? desktopFileTreeOpen() || (desktopReviewOpen() && activeTab() === "review")
      : store.mobileTab === "changes",
  )
  const vcsMode = createMemo<VcsMode | undefined>(() => {
    if (store.changes === "git" || store.changes === "branch") return store.changes
  })
  const vcsKey = createMemo(
    () => ["session-vcs", sdk.directory, sync.data.vcs?.branch ?? "", sync.data.vcs?.default_branch ?? ""] as const,
  )
  const vcsQuery = createQuery(() => {
    const mode = vcsMode()
    const enabled = wantsReview() && sync.project?.vcs === "git"

    return {
      queryKey: [...vcsKey(), mode] as const,
      enabled,
      staleTime: Number.POSITIVE_INFINITY,
      gcTime: 60 * 1000,
      queryFn: mode
        ? () =>
            sdk.client.vcs
              .diff({ mode })
              .then((result) => list(result.data))
              .catch((error) => {
                console.debug("[session-review] failed to load vcs diff", { mode, error })
                return []
              })
        : skipToken,
    }
  })
  const refreshVcs = debounce(() => void queryClient.invalidateQueries({ queryKey: vcsKey() }), 100)
  const reviewDiffs = () => {
    if (store.changes === "git" || store.changes === "branch")
      // avoids suspense
      return vcsQuery.isFetched ? (vcsQuery.data ?? []) : []
    return turnDiffs()
  }
  const reviewCount = () => reviewDiffs().length
  const hasReview = () => reviewCount() > 0
  const reviewReady = () => {
    if (store.changes === "git" || store.changes === "branch") return !vcsQuery.isPending
    return true
  }

  const newSessionWorktree = createMemo(() => {
    if (store.newSessionWorktree === "create") return "create"
    const project = sync.project
    if (project && sdk.directory !== project.worktree) return sdk.directory
    return "main"
  })

  const setActiveMessage = (message: UserMessage | undefined) => {
    messageMark = scrollMark
    setStore("messageId", message?.id)
  }

  const anchor = (id: string) => `message-${id}`

  const cursor = () => {
    const root = scroller
    if (!root) return store.messageId

    const box = root.getBoundingClientRect()
    const line = box.top + 100
    const list = [...root.querySelectorAll<HTMLElement>("[data-message-id]")]
      .map((el) => {
        const id = el.dataset.messageId
        if (!id) return

        const rect = el.getBoundingClientRect()
        return { id, top: rect.top, bottom: rect.bottom }
      })
      .filter((item): item is { id: string; top: number; bottom: number } => !!item)

    const shown = list.filter((item) => item.bottom > box.top && item.top < box.bottom)
    const hit = shown.find((item) => item.top <= line && item.bottom >= line)
    if (hit) return hit.id

    const near = [...shown].sort((a, b) => {
      const da = Math.abs(a.top - line)
      const db = Math.abs(b.top - line)
      if (da !== db) return da - db
      return a.top - b.top
    })[0]
    if (near) return near.id

    return list.filter((item) => item.top <= line).at(-1)?.id ?? list[0]?.id ?? store.messageId
  }

  function navigateMessageByOffset(offset: number) {
    const msgs = visibleUserMessages()
    if (msgs.length === 0) return

    const current = store.messageId && messageMark === scrollMark ? store.messageId : cursor()
    const base = current ? msgs.findIndex((m) => m.id === current) : msgs.length
    const currentIndex = base === -1 ? msgs.length : base
    const targetIndex = currentIndex + offset
    if (targetIndex < 0 || targetIndex > msgs.length) return

    if (targetIndex === msgs.length) {
      resumeScroll()
      return
    }

    autoScroll.pause()
    scrollToMessage(msgs[targetIndex], "auto")
  }

  function upsert(next: Project) {
    const list = serverSync.data.project
    sync.set("project", next.id)
    const idx = list.findIndex((item) => item.id === next.id)
    if (idx >= 0) {
      serverSync.set(
        "project",
        list.map((item, i) => (i === idx ? { ...item, ...next } : item)),
      )
      return
    }
    const at = list.findIndex((item) => item.id > next.id)
    if (at >= 0) {
      serverSync.set("project", [...list.slice(0, at), next, ...list.slice(at)])
      return
    }
    serverSync.set("project", [...list, next])
  }

  const gitMutation = useMutation(() => ({
    mutationFn: () => sdk.client.project.initGit(),
    onSuccess: (x) => {
      if (!x.data) return
      upsert(x.data)
    },
    onError: (err) => {
      showToast({
        variant: "error",
        title: language.t("common.requestFailed"),
        description: formatServerError(err, language.t),
      })
    },
  }))

  function initGit() {
    if (gitMutation.isPending) return
    gitMutation.mutate()
  }

  let inputRef!: HTMLDivElement
  let promptDock: HTMLDivElement | undefined
  let dockHeight = 0
  let scroller: HTMLDivElement | undefined
  let content: HTMLDivElement | undefined
  let revealMessage = (_id: string) => {}
  let scrollMark = 0
  let messageMark = 0

  const scrollGestureWindowMs = 250

  const markScrollGesture = (target?: EventTarget | null) => {
    const root = scroller
    if (!root) return

    const el = target instanceof Element ? target : undefined
    const nested = el?.closest("[data-scrollable]")
    if (nested && nested !== root) return

    setUi("scrollGesture", Date.now())
  }

  const hasScrollGesture = () => Date.now() - ui.scrollGesture < scrollGestureWindowMs

  const [sessionSync] = createResource(
    () => [sdk.directory, params.id] as const,
    ([directory, id]) => {
      if (refreshFrame !== undefined) cancelAnimationFrame(refreshFrame)
      if (refreshTimer !== undefined) window.clearTimeout(refreshTimer)
      refreshFrame = undefined
      refreshTimer = undefined
      if (!id) return

      const cached = untrack(() => sync.data.message[id] !== undefined)
      const stale = !cached
        ? false
        : (() => {
            const info = getSessionPrefetch(directory, id)
            if (!info) return true
            return Date.now() - info.at > SESSION_PREFETCH_TTL
          })()

      refreshFrame = requestAnimationFrame(() => {
        refreshFrame = undefined
        refreshTimer = window.setTimeout(() => {
          refreshTimer = undefined
          if (params.id !== id) return
          untrack(() => {
            if (stale) void sync.session.sync(id, { force: true })
          })
        }, 0)
      })

      return sync.session.sync(id)
    },
  )

  createEffect(
    on(
      () => {
        const id = params.id
        return [
          sdk.directory,
          id,
          id ? (sync.data.session_status[id]?.type ?? "idle") : "idle",
          id ? composer.blocked() : false,
        ] as const
      },
      ([dir, id, status, blocked]) => {
        if (todoFrame !== undefined) cancelAnimationFrame(todoFrame)
        if (todoTimer !== undefined) window.clearTimeout(todoTimer)
        todoFrame = undefined
        todoTimer = undefined
        if (!id) return
        if (status === "idle" && !blocked) return
        const cached = untrack(() => sync.data.todo[id] !== undefined || serverSync.data.session_todo[id] !== undefined)

        todoFrame = requestAnimationFrame(() => {
          todoFrame = undefined
          todoTimer = window.setTimeout(() => {
            todoTimer = undefined
            if (sdk.directory !== dir || params.id !== id) return
            untrack(() => {
              void sync.session.todo(id, cached ? { force: true } : undefined)
            })
          }, 0)
        })
      },
      { defer: true },
    ),
  )

  createEffect(
    on(
      () => visibleUserMessages().at(-1)?.id,
      (lastId, prevLastId) => {
        if (lastId && prevLastId && lastId > prevLastId) {
          setStore("messageId", undefined)
        }
      },
      { defer: true },
    ),
  )

  createEffect(
    on(
      sessionKey,
      () => {
        setStore("messageId", undefined)
        setStore("changes", "git")
        setUi("pendingMessage", undefined)
      },
      { defer: true },
    ),
  )

  const stopVcs = sdk.event.listen((evt) => {
    if (evt.details.type !== "file.watcher.updated") return
    const props =
      typeof evt.details.properties === "object" && evt.details.properties
        ? (evt.details.properties as Record<string, unknown>)
        : undefined
    const file = typeof props?.file === "string" ? props.file : undefined
    if (!file || file.startsWith(".git/")) return
    refreshVcs()
  })
  onCleanup(stopVcs)

  createEffect(
    on(
      () => params.dir,
      (dir) => {
        if (!dir) return
        setStore("newSessionWorktree", "main")
      },
      { defer: true },
    ),
  )

  const selectionPreview = (path: string, selection: FileSelection) => {
    const content = file.get(path)?.content?.content
    if (!content) return undefined
    return previewSelectedLines(content, { start: selection.startLine, end: selection.endLine })
  }

  const addCommentToContext = (input: {
    file: string
    selection: SelectedLineRange
    comment: string
    preview?: string
    origin?: "review" | "file"
  }) => {
    const selection = selectionFromLines(input.selection)
    const preview = input.preview ?? selectionPreview(input.file, selection)
    const saved = comments.add({
      file: input.file,
      selection: input.selection,
      comment: input.comment,
    })
    prompt.context.add({
      type: "file",
      path: input.file,
      selection,
      comment: input.comment,
      commentID: saved.id,
      commentOrigin: input.origin,
      preview,
    })
  }

  const updateCommentInContext = (input: {
    id: string
    file: string
    selection: SelectedLineRange
    comment: string
    preview?: string
  }) => {
    comments.update(input.file, input.id, input.comment)
    prompt.context.updateComment(input.file, input.id, {
      comment: input.comment,
      ...(input.preview ? { preview: input.preview } : {}),
    })
  }

  const removeCommentFromContext = (input: { id: string; file: string }) => {
    comments.remove(input.file, input.id)
    prompt.context.removeComment(input.file, input.id)
  }

  const reviewCommentActions = createMemo(() => ({
    moreLabel: language.t("common.moreOptions"),
    editLabel: language.t("common.edit"),
    deleteLabel: language.t("common.delete"),
    saveLabel: language.t("common.save"),
  }))

  const isEditableTarget = (target: EventTarget | null | undefined) => {
    if (!(target instanceof HTMLElement)) return false
    return /^(INPUT|TEXTAREA|SELECT|BUTTON)$/.test(target.tagName) || target.isContentEditable
  }

  const deepActiveElement = () => {
    let current: Element | null = document.activeElement
    while (current instanceof HTMLElement && current.shadowRoot?.activeElement) {
      current = current.shadowRoot.activeElement
    }
    return current instanceof HTMLElement ? current : undefined
  }

  const handleKeyDown = (event: KeyboardEvent) => {
    const path = event.composedPath()
    const target = path.find((item): item is HTMLElement => item instanceof HTMLElement)
    const activeElement = deepActiveElement()

    const protectedTarget = path.some(
      (item) => item instanceof HTMLElement && item.closest("[data-prevent-autofocus]") !== null,
    )
    if (protectedTarget || isEditableTarget(target)) return

    if (activeElement) {
      const isProtected = activeElement.closest("[data-prevent-autofocus]")
      const isInput = isEditableTarget(activeElement)
      if (isProtected || isInput) return
    }
    if (dialog.active) return

    if (activeElement === inputRef) {
      if (event.key === "Escape") inputRef?.blur()
      return
    }

    // Prefer the open terminal over the composer when it can take focus
    if (view().terminal.opened()) {
      const id = terminal.active()
      if (id && shouldFocusTerminalOnKeyDown(event) && focusTerminalById(id)) return
    }

    // Only treat explicit scroll keys as potential "user scroll" gestures.
    if (event.key === "PageUp" || event.key === "PageDown" || event.key === "Home" || event.key === "End") {
      markScrollGesture()
      return
    }

    if (event.key.length === 1 && event.key !== "Unidentified" && !(event.ctrlKey || event.metaKey)) {
      if (composer.blocked()) return
      inputRef?.focus()
    }
  }

  createEffect(() => {
    const list = changesOptions()
    if (list.includes(store.changes)) return
    const next = list[0]
    if (!next) return
    setStore("changes", next)
  })

  createEffect(
    on(
      () => sync.data.session_status[params.id ?? ""]?.type,
      (next, prev) => {
        if (next !== "idle" || prev === undefined || prev === "idle") return
        refreshVcs()
      },
      { defer: true },
    ),
  )

  const fileTreeTab = () => layout.fileTree.tab()
  const setFileTreeTab = (value: "changes" | "all") => layout.fileTree.setTab(value)

  const [tree, setTree] = createStore({
    reviewScroll: undefined as HTMLDivElement | undefined,
    pendingDiff: undefined as string | undefined,
    activeDiff: undefined as string | undefined,
  })

  createEffect(
    on(
      sessionKey,
      () => {
        setTree({
          reviewScroll: undefined,
          pendingDiff: undefined,
          activeDiff: undefined,
        })
      },
      { defer: true },
    ),
  )

  const showAllFiles = () => {
    if (fileTreeTab() !== "changes") return
    setFileTreeTab("all")
  }

  const focusInput = () => {
    inputRef?.focus()
  }

  useSessionCommands({
    navigateMessageByOffset,
    setActiveMessage,
    focusInput,
    review: reviewTab,
  })

  const openReviewFile = createOpenReviewFile({
    showAllFiles,
    tabForPath: file.tab,
    openTab: tabs().open,
    setActive: tabs().setActive,
    loadFile: file.load,
  })

  const changesTitle = () => {
    if (!canReview()) {
      return null
    }

    const label = (option: ChangeMode) => {
      if (option === "git") return language.t("ui.sessionReview.title.git")
      if (option === "branch") return language.t("ui.sessionReview.title.branch")
      return language.t("ui.sessionReview.title.lastTurn")
    }

    return (
      <Select
        options={changesOptions()}
        current={store.changes}
        label={label}
        onSelect={(option) => option && setStore("changes", option)}
        variant="ghost"
        size="small"
        valueClass="text-14-medium"
      />
    )
  }

  const empty = (text: string) => (
    <div class="h-full pb-64 -mt-4 flex flex-col items-center justify-center text-center gap-6">
      <div class="text-14-regular text-text-weak max-w-56">{text}</div>
    </div>
  )

  const createGit = (input: { emptyClass: string }) => (
    <div class={input.emptyClass}>
      <div class="flex flex-col gap-3">
        <div class="text-14-medium text-text-strong">{language.t("session.review.noVcs.createGit.title")}</div>
        <div class="text-14-regular text-text-base max-w-md" style={{ "line-height": "var(--line-height-normal)" }}>
          {language.t("session.review.noVcs.createGit.description")}
        </div>
      </div>
      <Button size="large" disabled={gitMutation.isPending} onClick={initGit}>
        {gitMutation.isPending
          ? language.t("session.review.noVcs.createGit.actionLoading")
          : language.t("session.review.noVcs.createGit.action")}
      </Button>
    </div>
  )

  const reviewEmptyText = createMemo(() => {
    if (store.changes === "git") return language.t("session.review.noUncommittedChanges")
    if (store.changes === "branch") return language.t("session.review.noBranchChanges")
    return language.t("session.review.noChanges")
  })

  const reviewEmpty = (input: { loadingClass: string; emptyClass: string }) => {
    if (store.changes === "git" || store.changes === "branch") {
      if (!reviewReady()) return <div class={input.loadingClass}>{language.t("session.review.loadingChanges")}</div>
      return empty(reviewEmptyText())
    }

    if (store.changes === "turn") {
      if (nogit()) return createGit(input)
      return empty(reviewEmptyText())
    }

    return (
      <div class={input.emptyClass}>
        <div class="text-14-regular text-text-weak max-w-56">{reviewEmptyText()}</div>
      </div>
    )
  }

  const reviewContent = (input: {
    diffStyle: DiffStyle
    onDiffStyleChange?: (style: DiffStyle) => void
    classes?: SessionReviewTabProps["classes"]
    loadingClass: string
    emptyClass: string
  }) => (
    <Show when={!store.deferRender}>
      <SessionReviewTab
        title={changesTitle()}
        empty={reviewEmpty(input)}
        diffs={reviewDiffs}
        view={view}
        diffStyle={input.diffStyle}
        onDiffStyleChange={input.onDiffStyleChange}
        onScrollRef={(el) => setTree("reviewScroll", el)}
        focusedFile={tree.activeDiff}
        onLineComment={(comment) => addCommentToContext({ ...comment, origin: "review" })}
        onLineCommentUpdate={updateCommentInContext}
        onLineCommentDelete={removeCommentFromContext}
        lineCommentActions={reviewCommentActions()}
        commentMentions={{
          items: file.searchFilesAndDirectories,
        }}
        comments={comments.all()}
        focusedComment={comments.focus()}
        onFocusedCommentChange={comments.setFocus}
        onViewFile={openReviewFile}
        classes={input.classes}
      />
    </Show>
  )

  const reviewPanel = () => (
    <div class="flex flex-col h-full overflow-hidden bg-background-stronger contain-strict">
      <div class="relative pt-2 flex-1 min-h-0 overflow-hidden">
        {reviewContent({
          diffStyle: layout.review.diffStyle(),
          onDiffStyleChange: layout.review.setDiffStyle,
          loadingClass: "px-6 py-4 text-text-weak",
          emptyClass: "h-full pb-64 -mt-4 flex flex-col items-center justify-center text-center gap-6",
        })}
      </div>
    </div>
  )

  createEffect(
    on(
      activeFileTab,
      (active) => {
        if (!active) return
        if (fileTreeTab() !== "changes") return
        showAllFiles()
      },
      { defer: true },
    ),
  )

  const reviewDiffId = (path: string) => {
    const sum = checksum(path)
    if (!sum) return
    return `session-review-diff-${sum}`
  }

  const reviewDiffTop = (path: string) => {
    const root = tree.reviewScroll
    if (!root) return

    const id = reviewDiffId(path)
    if (!id) return

    const el = document.getElementById(id)
    if (!(el instanceof HTMLElement)) return
    if (!root.contains(el)) return

    const a = el.getBoundingClientRect()
    const b = root.getBoundingClientRect()
    return a.top - b.top + root.scrollTop
  }

  const scrollToReviewDiff = (path: string) => {
    const root = tree.reviewScroll
    if (!root) return false

    const top = reviewDiffTop(path)
    if (top === undefined) return false

    view().setScroll("review", { x: root.scrollLeft, y: top })
    root.scrollTo({ top, behavior: "auto" })
    return true
  }

  const focusReviewDiff = (path: string) => {
    openReviewPanel()
    view().review.openPath(path)
    setTree({ activeDiff: path, pendingDiff: path })
  }

  createEffect(() => {
    const pending = tree.pendingDiff
    if (!pending) return
    if (!tree.reviewScroll) return
    if (!reviewReady()) return

    const attempt = (count: number) => {
      if (tree.pendingDiff !== pending) return
      if (count > 60) {
        setTree("pendingDiff", undefined)
        return
      }

      const root = tree.reviewScroll
      if (!root) {
        requestAnimationFrame(() => attempt(count + 1))
        return
      }

      if (!scrollToReviewDiff(pending)) {
        requestAnimationFrame(() => attempt(count + 1))
        return
      }

      const top = reviewDiffTop(pending)
      if (top === undefined) {
        requestAnimationFrame(() => attempt(count + 1))
        return
      }

      if (Math.abs(root.scrollTop - top) <= 1) {
        setTree("pendingDiff", undefined)
        return
      }

      requestAnimationFrame(() => attempt(count + 1))
    }

    requestAnimationFrame(() => attempt(0))
  })

  createEffect(() => {
    const id = params.id
    if (!id) return

    if (!wantsReview()) return
    if (sync.data.session_diff[id] !== undefined) return
    if (sync.status === "loading") return

    void sync.session.diff(id)
  })

  createEffect(
    on(
      () => [sessionKey(), wantsReview()] as const,
      ([key, wants]) => {
        if (diffFrame !== undefined) cancelAnimationFrame(diffFrame)
        if (diffTimer !== undefined) window.clearTimeout(diffTimer)
        diffFrame = undefined
        diffTimer = undefined
        if (!wants) return

        const id = params.id
        if (!id) return
        if (!untrack(() => sync.data.session_diff[id] !== undefined)) return

        diffFrame = requestAnimationFrame(() => {
          diffFrame = undefined
          diffTimer = window.setTimeout(() => {
            diffTimer = undefined
            if (sessionKey() !== key) return
            void sync.session.diff(id, { force: true })
          }, 0)
        })
      },
      { defer: true },
    ),
  )

  let treeDir: string | undefined
  createEffect(() => {
    const dir = sdk.directory
    if (!isDesktop()) return
    if (!layout.fileTree.opened()) return
    if (sync.status === "loading") return

    fileTreeTab()
    const refresh = treeDir !== dir
    treeDir = dir
    void (refresh ? file.tree.refresh("") : file.tree.list(""))
  })

  createEffect(
    on(
      () => sdk.directory,
      () => {
        const tab = activeFileTab()
        if (!tab) return
        const path = file.pathFromTab(tab)
        if (!path) return
        void file.load(path, { force: true })
      },
      { defer: true },
    ),
  )

  const autoScroll = createAutoScroll({
    working: () => true,
    overflowAnchor: "dynamic",
  })

  let scrollStateFrame: number | undefined
  let scrollStateTarget: HTMLDivElement | undefined
  let fillFrame: number | undefined

  const jumpThreshold = (el: HTMLDivElement) => Math.max(400, el.clientHeight)

  const updateScrollState = (el: HTMLDivElement) => {
    const max = el.scrollHeight - el.clientHeight
    const distance = max - el.scrollTop
    const overflow = max > 1
    const bottom = !overflow || distance <= 2
    const jump = overflow && distance > jumpThreshold(el)

    if (ui.scroll.overflow === overflow && ui.scroll.bottom === bottom && ui.scroll.jump === jump) return
    setUi("scroll", { overflow, bottom, jump })
  }

  const scheduleScrollState = (el: HTMLDivElement) => {
    scrollStateTarget = el
    if (scrollStateFrame !== undefined) return

    scrollStateFrame = requestAnimationFrame(() => {
      scrollStateFrame = undefined

      const target = scrollStateTarget
      scrollStateTarget = undefined
      if (!target) return

      updateScrollState(target)
    })
  }

  const resumeScroll = () => {
    setStore("messageId", undefined)
    autoScroll.forceScrollToBottom()
    clearMessageHash()

    const el = scroller
    if (el) scheduleScrollState(el)
  }

  // When the user returns to the bottom, treat the active message as "latest".
  createEffect(
    on(
      autoScroll.userScrolled,
      (scrolled) => {
        if (scrolled) return
        setStore("messageId", undefined)
        clearMessageHash()
      },
      { defer: true },
    ),
  )

  let fill = () => {}

  const setScrollRef = (el: HTMLDivElement | undefined) => {
    scroller = el
    autoScroll.scrollRef(el)
    if (!el) return
    scheduleScrollState(el)
    fill()
  }

  const markUserScroll = () => {
    scrollMark += 1
  }

  createResizeObserver(
    () => content,
    () => {
      const el = scroller
      if (el) scheduleScrollState(el)
      fill()
    },
  )

  const historyLoader = createSessionHistoryLoader({
    sessionID: () => params.id,
    loaded: () => messages().length,
    visibleUserMessages,
    historyMore,
    historyLoading,
    loadMore: (sessionID) => sync.session.history.loadMore(sessionID),
    userScrolled: autoScroll.userScrolled,
    scroller: () => scroller,
  })

  fill = () => {
    if (fillFrame !== undefined) return

    fillFrame = requestAnimationFrame(() => {
      fillFrame = undefined

      if (!params.id || !messagesReady()) return
      if (autoScroll.userScrolled() || historyLoading()) return

      const el = scroller
      if (!el) return
      if (el.scrollHeight > el.clientHeight + 1) return
      if (!historyMore()) return

      void historyLoader.loadAndReveal()
    })
  }

  createEffect(
    on(
      () =>
        [
          params.id,
          messagesReady(),
          historyMore(),
          historyLoading(),
          autoScroll.userScrolled(),
          visibleUserMessages().length,
        ] as const,
      ([id, ready, more, loading, scrolled]) => {
        if (!id || !ready || loading || scrolled) return
        if (!more) return
        fill()
      },
      { defer: true },
    ),
  )

  const draft = (id: string) =>
    extractPromptFromParts(sync.data.part[id] ?? [], {
      directory: sdk.directory,
      attachmentName: language.t("common.attachment"),
    })

  const line = (id: string) => {
    const text = draft(id)
      .map((part) => (part.type === "image" ? `[image:${part.filename}]` : part.content))
      .join("")
      .replace(/\s+/g, " ")
      .trim()
    if (text) return text
    return `[${language.t("common.attachment")}]`
  }

  const fail = (err: unknown) => {
    showToast({
      variant: "error",
      title: language.t("common.requestFailed"),
      description: formatServerError(err, language.t),
    })
  }

  const workflowPrompt = () => {
    const message = lastUserMessage()
    if (message) {
      const text = extractPromptFromParts(sync.data.part[message.id] ?? [], {
        directory: sdk.directory,
        attachmentName: language.t("common.attachment"),
      })
        .map((part) => {
          if (part.type === "image") return `[image:${part.filename}]`
          if (part.type === "file") return `@${part.path}`
          if (part.type === "agent") return `@${part.name}`
          return part.content
        })
        .join("")
        .trim()
      if (text) return text
    }
    return info()?.title ?? ""
  }

  const workflowModel = () => {
    const model = local.model.current()
    if (!model) return
    return `${model.provider.id}/${model.id}`
  }

  const workflowVariants = () => {
    const variants = local.model.variant.list()
    if (variants.length === 0) return []
    return variants.includes("default") ? variants : ["default", ...variants]
  }

  const startWorkflowMutation = useMutation(() => ({
    mutationFn: async (input: { request: string; variant?: string; staffing: WorkflowStaffing }) => {
      const sessionID = params.id
      if (!sessionID) return
      const result = await sdk.client.workflow.start({
        workflowStartInput: {
          sessionID,
          prompt: input.request,
          model: workflowModel(),
          variant: input.variant,
          agent: local.agent.current()?.name,
          staffing: input.staffing,
        },
      })
      if (result.data) {
        sync.set("workflow", (items) => [...items.filter((item) => item.id !== result.data!.id), result.data!])
      }
      return result.data
    },
    onSuccess: (workflow) => {
      if (!workflow) return
      setWorkflowUi("expanded", true)
      showToast({
        variant: "success",
        icon: "circle-check",
        title: language.t("session.workflow.started.title"),
        description: workflow.id,
      })
      void sync.session.workflow(params.id!, { force: true })
      void sync.session.workflowGraph(workflow.id, { force: true })
      void sync.session.fetch(0)
      dialog.close()
    },
    onError: fail,
  }))

  const openStartWorkflowDialog = () => {
    void import("@/components/dialog-start-workflow").then((x) => {
      dialog.show(() => (
        <x.DialogStartWorkflow
          initialRequest={workflowPrompt()}
          variants={workflowVariants()}
          initialVariant={local.model.variant.current() ?? "default"}
          pending={startWorkflowMutation.isPending}
          onStart={(input) => startWorkflowMutation.mutate(input)}
        />
      ))
    })
  }

  const updateWorkflowXmlMutation = useMutation(() => ({
    mutationFn: async () => {
      const workflow = activeWorkflow()
      if (!workflow) return
      return sdk.client.workflow.updateXml({ workflowID: workflow.id, xml: workflowUi.xml }).then((result) => result.data)
    },
    onSuccess: (graph) => {
      if (!graph) return
      sync.set("workflow_graph", graph.workflow.id, graph)
      setWorkflowUi("editingXml", false)
      showToast({
        variant: "success",
        icon: "circle-check",
        title: language.t("session.workflow.xmlSaved.title"),
      })
    },
    onError: fail,
  }))

  const updateWorkflowStaffingMutation = useMutation(() => ({
    mutationFn: async () => {
      const workflow = activeWorkflow()
      if (!workflow) return
      return sdk.client.workflow
        .updateStaffing({ workflowID: workflow.id, staffing: { ...workflowStaffing } })
        .then((result) => result.data)
    },
    onSuccess: (workflow) => {
      if (!workflow) return
      sync.set("workflow", (items) => [...items.filter((item) => item.id !== workflow.id), workflow])
      void sync.session.workflow(params.id, { force: true })
      void sync.session.workflowGraph(workflow.id, { force: true })
      void sync.session.fetch(0)
      showToast({
        variant: "success",
        icon: "circle-check",
        title: language.t("session.workflow.staffing.saved"),
      })
    },
    onError: fail,
  }))

  const workflowInterventionMutation = useMutation(() => ({
    mutationFn: async () => {
      const workflow = activeWorkflow()
      const message = workflowUi.interventionMessage.trim()
      if (!workflow || !message) return
      return sdk.client.workflow
        .intervene({
          workflowID: workflow.id,
          message,
          timing: workflowUi.interventionTiming,
          targetRole: workflowUi.interventionTargetRole,
        })
        .then((result) => result.data)
    },
    onSuccess: (workflow) => {
      if (!workflow) return
      sync.set("workflow", (items) => [...items.filter((item) => item.id !== workflow.id), workflow])
      setWorkflowUi("interventionMessage", "")
      void sync.session.workflow(params.id, { force: true })
      void sync.session.workflowGraph(workflow.id, { force: true })
      void sync.session.fetch(0)
      showToast({
        variant: "success",
        icon: "circle-check",
        title: language.t("session.workflow.intervention.sent"),
      })
    },
    onError: fail,
  }))

  const resumeWorkflowMutation = useMutation(() => ({
    mutationFn: async () => {
      const workflow = activeWorkflow()
      if (!workflow) return
      return sdk.client.workflow.resume({ workflowID: workflow.id }).then((result) => result.data)
    },
    onSuccess: (workflow) => {
      if (!workflow) return
      sync.set("workflow", (items) => [...items.filter((item) => item.id !== workflow.id), workflow])
      void sync.session.workflow(params.id, { force: true })
      void sync.session.workflowGraph(workflow.id, { force: true })
      void sync.session.fetch(0)
    },
    onError: fail,
  }))

  const cancelWorkflowMutation = useMutation(() => ({
    mutationFn: async () => {
      const workflow = activeWorkflow()
      if (!workflow) return
      return sdk.client.workflow.cancel({ workflowID: workflow.id }).then((result) => result.data)
    },
    onSuccess: (workflow) => {
      if (!workflow) return
      void sync.session.workflow(params.id, { force: true })
      void sync.session.workflowGraph(workflow.id, { force: true })
      void sync.session.fetch(0)
    },
    onError: fail,
  }))

  const openWorkflowPath = (path: string | undefined) => {
    if (!path) return
    setWorkflowMenu(undefined)
    setWorkflowUi("fullscreen", false)
    openReviewFile(path)
    openReviewPanel()
  }

  const openWorkflowSession = (sessionID: string | undefined) => {
    if (!sessionID) return
    setWorkflowMenu(undefined)
    setWorkflowUi("fullscreen", false)
    setWorkflowUi("sessionLog", false)
    navigate(`/${params.dir}/session/${sessionID}`)
  }

  const workflowNodeClick = (target: WorkflowReactFlowTarget) => {
    openWorkflowSession(target.sessionID)
  }

  const workflowNodeContextMenu = (target: WorkflowReactFlowContextTarget) => {
    setWorkflowMenu(target)
  }

  const workflowSessionState = (graph: WorkflowGraph) =>
    Object.fromEntries(
      graph.nodes
        .map((node): [string, WorkflowReactFlowSessionState] | undefined => {
          if (!node.sessionID) return
          if (sync.data.session_working(node.sessionID)) return [node.sessionID, "running"]
          if (node.type === "session") return [node.sessionID, "completed"]
          return
        })
        .filter((entry): entry is [string, WorkflowReactFlowSessionState] => !!entry),
    )

  const workflowSessionLogRoleLabel = (role: WorkflowSessionLogRole) => {
    if (role === "requester") return language.t("session.workflow.sessionLog.requester")
    if (role === "main_pm") return language.t("session.workflow.staffing.mainPM")
    if (role === "department_pm") return language.t("session.workflow.staffing.departmentPM")
    if (role === "executor") return language.t("session.workflow.staffing.executor")
    if (role === "reviewer") return language.t("session.workflow.staffing.reviewer")
    if (role === "tester") return language.t("session.workflow.staffing.tester")
    return language.t("session.workflow.staffing.expert")
  }
  const workflowSessionLogSourceLabel = (source: WorkflowSessionLogSource) => {
    if (source === "workflow") return language.t("session.workflow.sessionLog.source.workflow")
    if (source === "trigger") return language.t("session.workflow.sessionLog.source.trigger")
    return language.t("session.workflow.sessionLog.source.staff")
  }
  const workflowSessionLogStatusLabel = (status: string) =>
    status === "untriggered" ? language.t("session.workflow.sessionLog.untriggered") : status

  makeEventListener(window, "click", () => setWorkflowMenu(undefined))
  makeEventListener(window, "keydown", (event) => {
    if (event.key !== "Escape") return
    setWorkflowMenu(undefined)
    setWorkflowUi("fullscreen", false)
    setWorkflowUi("sessionLog", false)
  })

  const WorkflowGraphView = (props: { graph: WorkflowGraph; fullscreen?: boolean }) => {
    const [host, setHost] = createSignal<HTMLDivElement>()
    let flow: WorkflowReactFlowInstance | undefined
    createEffect(() => {
      const element = host()
      if (!element) return
      const next: WorkflowReactFlowProps = {
        graph: props.graph,
        milestoneLabel: language.t("session.workflow.milestone"),
        viewport: props.fullscreen ? "fullscreen" : "panel",
        currentSessionID: params.id,
        sessionState: workflowSessionState(props.graph),
        onNodeSelect: workflowNodeClick,
        onNodeContextMenu: workflowNodeContextMenu,
      }
      if (!flow) {
        flow = mountWorkflowReactFlow(element, next)
        return
      }
      flow.update(next)
    })
    onCleanup(() => flow?.dispose())
    return (
      <div
        ref={setHost}
        class={
          props.fullscreen
            ? "h-full min-h-0 w-full overflow-hidden rounded-md border border-border-weak-base bg-surface-panel/60"
            : "h-[280px] min-h-[220px] overflow-hidden rounded-md border border-border-weak-base bg-surface-panel/60"
        }
      />
    )
  }

  const workflowContextMenu = () => (
    <Show when={workflowMenu()}>
      {(target) => (
        <Portal>
          <div
            class="fixed z-[100] flex min-w-44 flex-col gap-1 rounded-md border border-border-weak-base bg-surface-panel p-1 shadow-lg"
            style={{
              left: `${Math.max(8, Math.min(target().x, window.innerWidth - 184))}px`,
              top: `${Math.max(8, Math.min(target().y, window.innerHeight - 88))}px`,
            }}
            onClick={(event) => event.stopPropagation()}
            onContextMenu={(event) => event.preventDefault()}
          >
            <button
              type="button"
              class="rounded px-2 py-1.5 text-left text-12-regular text-text-strong hover:bg-surface-hover disabled:cursor-not-allowed disabled:text-text-disabled"
              disabled={!target().sessionID}
              onClick={() => {
                openWorkflowSession(target().sessionID)
                setWorkflowMenu(undefined)
              }}
            >
              {language.t("session.workflow.openSession")}
            </button>
            <button
              type="button"
              class="rounded px-2 py-1.5 text-left text-12-regular text-text-strong hover:bg-surface-hover disabled:cursor-not-allowed disabled:text-text-disabled"
              disabled={!target().planPath}
              onClick={() => {
                openWorkflowPath(target().planPath)
                setWorkflowMenu(undefined)
              }}
            >
              {language.t("session.workflow.openPlan")}
            </button>
          </div>
        </Portal>
      )}
    </Show>
  )

  const workflowSessionLogWindow = () => (
    <Show when={workflowUi.sessionLog && activeWorkflowGraph()}>
      {(graph) => (
        <Portal>
          <div
            class="fixed inset-0 z-[1000] isolate flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm"
            onClick={() => setWorkflowUi("sessionLog", false)}
          >
            <div
              class="relative z-[1] flex max-h-[82vh] w-full max-w-5xl flex-col overflow-hidden rounded-lg border border-border-strong bg-surface-raised-stronger-non-alpha shadow-xl"
              onClick={(event) => event.stopPropagation()}
            >
              <div class="flex items-center gap-3 border-b border-border-weak-base bg-surface-raised-stronger-non-alpha px-4 py-3">
                <div class="min-w-0 flex-1">
                  <div class="text-15-medium text-text-strong">{language.t("session.workflow.sessionLog")}</div>
                  <div class="truncate text-12-regular text-text-weak">
                    {graph().workflow.title} · {graph().workflow.id}
                  </div>
                </div>
                <IconButton
                  type="button"
                  size="small"
                  variant="ghost"
                  icon="close-small"
                  onClick={() => setWorkflowUi("sessionLog", false)}
                  aria-label={language.t("common.close")}
                />
              </div>
              <div class="min-h-0 overflow-y-auto bg-background-base p-4">
                <div class="mb-3 text-12-regular text-text-weak">
                  {language.t("session.workflow.sessionLog.description")}
                </div>
                <div class="flex flex-col gap-3">
                  <For
                    each={workflowSessionLog()}
                    fallback={
                      <div class="rounded-md border border-border-weak-base bg-background px-3 py-4 text-12-regular text-text-weak">
                        {language.t("session.workflow.sessionLog.empty")}
                      </div>
                    }
                  >
                    {(group) => (
                      <section class="overflow-hidden rounded-md border border-border-weak-base bg-background-base">
                        <div class="flex items-center gap-2 border-b border-border-weak-base bg-surface-raised-stronger-non-alpha px-3 py-2">
                          <div class="min-w-0 flex-1">
                            <div class="text-13-medium text-text-strong">{workflowSessionLogRoleLabel(group.role)}</div>
                            <div class="text-11-regular text-text-weaker">
                              {language.t("session.workflow.sessionLog.count", { count: group.entries.length })}
                            </div>
                          </div>
                        </div>
                        <div class="divide-y divide-border-weak-base">
                          <For each={group.entries}>
                            {(entry) => (
                              <div
                                class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 bg-background-base px-3 py-2"
                                classList={{
                                  "border-l-2 border-accent bg-accent/5": entry.current,
                                }}
                              >
                                <div class="min-w-0">
                                  <div class="flex min-w-0 flex-wrap items-center gap-1.5">
                                    <span class="truncate text-12-medium text-text-strong">{entry.title}</span>
                                    <span
                                      class={`rounded px-1.5 py-0.5 text-[10px] leading-4 ${workflowSessionLogStatusTone(
                                        entry.status,
                                      )}`}
                                    >
                                      {workflowSessionLogStatusLabel(entry.status)}
                                    </span>
                                  </div>
                                  <div class="mt-1 flex min-w-0 flex-wrap gap-x-3 gap-y-1 text-11-regular text-text-weak">
                                    <span>{workflowSessionLogSourceLabel(entry.source)}</span>
                                    <Show when={entry.specialty}>
                                      {(specialty) => <span>{specialty()}</span>}
                                    </Show>
                                    <Show when={entry.milestoneID}>
                                      {(milestoneID) => (
                                        <span class="truncate">
                                          {milestoneID()}
                                          <Show when={entry.milestoneTitle}> · {entry.milestoneTitle}</Show>
                                        </span>
                                      )}
                                    </Show>
                                    <Show when={entry.attempt !== undefined}>
                                      <span>
                                        {language.t("session.workflow.sessionLog.attempt", {
                                          count: entry.attempt ?? 0,
                                        })}
                                      </span>
                                    </Show>
                                    <Show when={entry.sessionID}>
                                      {(sessionID) => (
                                        <span class="max-w-60 truncate font-mono text-text-weaker">{sessionID()}</span>
                                      )}
                                    </Show>
                                  </div>
                                </div>
                                <Button
                                  type="button"
                                  size="small"
                                  variant="ghost"
                                  icon="enter"
                                  disabled={!entry.sessionID}
                                  onClick={() => openWorkflowSession(entry.sessionID)}
                                >
                                  {language.t("session.workflow.openSession")}
                                </Button>
                              </div>
                            )}
                          </For>
                        </div>
                      </section>
                    )}
                  </For>
                </div>
              </div>
            </div>
          </div>
        </Portal>
      )}
    </Show>
  )

  const workflowFullscreenPreview = () => (
    <Show when={workflowUi.fullscreen && activeWorkflowGraph()}>
      {(graph) => (
        <Portal>
          <div class="workflow-fullscreen-preview fixed inset-0 z-[90] flex flex-col gap-3 p-4">
            <div class="flex min-h-8 items-center gap-2">
              <div class="min-w-0 flex-1">
                <div class="truncate text-13-medium text-text-strong">{graph().workflow.title}</div>
                <div class="truncate text-11-regular text-text-weaker">{graph().workflow.id}</div>
              </div>
              <Tooltip placement="bottom" value={language.t("session.workflow.exitFullscreen")}>
                <IconButton
                  type="button"
                  size="small"
                  variant="secondary"
                  icon="collapse"
                  onClick={() => setWorkflowUi("fullscreen", false)}
                  aria-label={language.t("session.workflow.exitFullscreen")}
                />
              </Tooltip>
              <Tooltip placement="bottom" value={language.t("session.workflow.sessionLog")}>
                <IconButton
                  type="button"
                  size="small"
                  variant="secondary"
                  icon="bullet-list"
                  onClick={() => setWorkflowUi("sessionLog", true)}
                  aria-label={language.t("session.workflow.sessionLog")}
                />
              </Tooltip>
            </div>
            <div class="workflow-fullscreen-preview__graph min-h-0 flex-1 overflow-hidden rounded-md">
              <WorkflowGraphView graph={graph()} fullscreen />
            </div>
          </div>
        </Portal>
      )}
    </Show>
  )

  const workflowSessionControl = () => (
    <>
      <Show when={params.id && !mobileChanges()}>
        <div class="shrink-0 px-3 pt-2 pb-1 flex max-w-full flex-col gap-2">
        <div class="rounded-md border border-border-weak-base bg-surface-panel shadow-sm">
          <div class="flex items-center gap-2 px-2 py-1.5">
            <Show
              when={sessionBelongsToWorkflow()}
              fallback={
                <Button
                  type="button"
                  size="small"
                  variant={activeWorkflow() ? "secondary" : "primary"}
                  icon="branch"
                  disabled={startWorkflowMutation.isPending || !params.id}
                  onClick={openStartWorkflowDialog}
                >
                  {activeWorkflow() ? language.t("session.workflow.startAnother") : language.t("session.workflow.start")}
                </Button>
              }
            >
              <span class="shrink-0 rounded border border-border-weak-base px-2 py-1 text-12-medium text-text-weak">
                {language.t("session.workflow.belongsTo")}
              </span>
            </Show>
            <Show when={activeWorkflow()} keyed>
              {(workflow) => (
                <>
                  <button
                    type="button"
                    class="min-w-0 flex-1 text-left"
                    onClick={() => setWorkflowUi("expanded", !workflowUi.expanded)}
                  >
                    <div class="flex min-w-0 items-center gap-2">
                      <span
                        class={`shrink-0 rounded px-1.5 py-0.5 text-[10px] leading-4 ${workflowStatusTone(
                          workflow.status,
                        )}`}
                      >
                        {workflow.status}
                      </span>
                      <span class="truncate text-12-medium text-text-strong">{workflow.title}</span>
                    </div>
                    <div class="truncate text-11-regular text-text-weaker">{workflow.id}</div>
                  </button>
                  <Tooltip placement="bottom" value={language.t("session.workflow.fullscreen")}>
                    <IconButton
                      type="button"
                      size="small"
                      variant="ghost"
                      icon="expand"
                      disabled={!activeWorkflowGraph()}
                      onClick={() => setWorkflowUi("fullscreen", true)}
                      aria-label={language.t("session.workflow.fullscreen")}
                    />
                  </Tooltip>
                  <Tooltip placement="bottom" value={language.t("session.workflow.sessionLog")}>
                    <IconButton
                      type="button"
                      size="small"
                      variant="ghost"
                      icon="bullet-list"
                      disabled={!activeWorkflowGraph()}
                      onClick={() => setWorkflowUi("sessionLog", true)}
                      aria-label={language.t("session.workflow.sessionLog")}
                    />
                  </Tooltip>
                  <Tooltip placement="bottom" value={language.t("session.workflow.resume")}>
                    <IconButton
                      type="button"
                      size="small"
                      variant="ghost"
                      icon="circle-check"
                      disabled={resumeWorkflowMutation.isPending || workflow.status === "completed"}
                      onClick={() => resumeWorkflowMutation.mutate()}
                      aria-label={language.t("session.workflow.resume")}
                    />
                  </Tooltip>
                  <Tooltip placement="bottom" value={language.t("session.workflow.cancel")}>
                    <IconButton
                      type="button"
                      size="small"
                      variant="ghost"
                      icon="close-small"
                      disabled={cancelWorkflowMutation.isPending || workflow.status === "cancelled"}
                      onClick={() => cancelWorkflowMutation.mutate()}
                      aria-label={language.t("session.workflow.cancel")}
                    />
                  </Tooltip>
                </>
              )}
            </Show>
          </div>
          <Show when={workflowUi.expanded && activeWorkflow()}>
            <div class="border-t border-border-weak-base p-2 flex flex-col gap-2">
              <Show when={activeWorkflowGraph()} fallback={<div class="text-12-regular text-text-weak px-1 py-2">{language.t("common.loading")}</div>}>
                {(graph) => (
                  <>
                    <WorkflowGraphView graph={graph()} />
                    <div class="flex flex-wrap gap-1.5">
                      <For each={graph().milestones}>
                        {(milestone) => (
                          <>
                            <Show when={milestone.planPath}>
                              {(path) => (
                                <Button
                                  type="button"
                                  size="small"
                                  variant="ghost"
                                  icon="open-file"
                                  onClick={() => openWorkflowPath(path())}
                                >
                                  {milestone.id}
                                </Button>
                              )}
                            </Show>
                          </>
                        )}
                      </For>
                    </div>
                    <details class="rounded-md border border-border-weak-base bg-background">
                      <summary class="cursor-pointer px-2 py-1.5 text-12-medium text-text-weak">
                        {language.t("session.workflow.intervention.title")}
                      </summary>
                      <div class="border-t border-border-weak-base p-2 flex flex-col gap-2">
                        <textarea
                          class="min-h-24 w-full resize-y rounded-md border border-border-weak-base bg-surface-panel p-2 text-12-regular text-text-strong outline-none focus:border-border-strong"
                          value={workflowUi.interventionMessage}
                          placeholder={language.t("session.workflow.intervention.placeholder")}
                          onInput={(event) => setWorkflowUi("interventionMessage", event.currentTarget.value)}
                          spellcheck={false}
                        />
                        <div class="flex flex-wrap items-center justify-end gap-1.5">
                          <Select
                            options={workflowInterventionTargetRoleOptions}
                            current={workflowUi.interventionTargetRole}
                            label={(option) => language.t(`session.workflow.intervention.target.${option}`)}
                            onSelect={(option) => option && setWorkflowUi("interventionTargetRole", option)}
                            variant="secondary"
                            size="small"
                            aria-label={language.t("session.workflow.intervention.target")}
                          />
                          <Select
                            options={workflowInterventionTimingOptions}
                            current={workflowUi.interventionTiming}
                            label={(option) => language.t(`session.workflow.intervention.timing.${option}`)}
                            onSelect={(option) => option && setWorkflowUi("interventionTiming", option)}
                            variant="secondary"
                            size="small"
                            aria-label={language.t("session.workflow.intervention.timing")}
                          />
                          <Button
                            type="button"
                            size="small"
                            variant="primary"
                            icon="arrow-up"
                            disabled={
                              workflowInterventionMutation.isPending || workflowUi.interventionMessage.trim().length === 0
                            }
                            onClick={() => workflowInterventionMutation.mutate()}
                          >
                            {language.t("session.workflow.intervention.send")}
                          </Button>
                        </div>
                      </div>
                    </details>
                    <details class="rounded-md border border-border-weak-base bg-background">
                      <summary class="cursor-pointer px-2 py-1.5 text-12-medium text-text-weak">
                        {language.t("session.workflow.staffing.title")}
                      </summary>
                      <div class="border-t border-border-weak-base p-2 flex flex-col gap-2">
                        <div class="grid grid-cols-2 gap-2 md:grid-cols-3">
                          <For each={workflowStaffingFields}>
                            {([key, label]) => (
                              <TextField
                                type="number"
                                min="1"
                                max="12"
                                step="1"
                                label={language.t(label)}
                                value={String(workflowStaffing[key])}
                                onChange={(value) =>
                                  setWorkflowStaffing(
                                    key,
                                    workflowStaffingValue(Number(value), defaultWorkflowStaffing[key]),
                                  )
                                }
                              />
                            )}
                          </For>
                        </div>
                        <div class="flex justify-end">
                          <Button
                            type="button"
                            size="small"
                            variant="primary"
                            icon="check-small"
                            disabled={updateWorkflowStaffingMutation.isPending}
                            onClick={() => updateWorkflowStaffingMutation.mutate()}
                          >
                            {language.t("session.workflow.staffing.save")}
                          </Button>
                        </div>
                      </div>
                    </details>
                    <details class="rounded-md border border-border-weak-base bg-background">
                      <summary class="cursor-pointer px-2 py-1.5 text-12-medium text-text-weak">
                        {language.t("session.workflow.xml")}
                      </summary>
                      <div class="border-t border-border-weak-base p-2 flex flex-col gap-2">
                        <textarea
                          class="min-h-40 w-full resize-y rounded-md border border-border-weak-base bg-surface-panel p-2 font-mono text-12-regular text-text-strong outline-none focus:border-border-strong"
                          value={workflowUi.xml}
                          onInput={(event) => {
                            setWorkflowUi("editingXml", true)
                            setWorkflowUi("xml", event.currentTarget.value)
                          }}
                          spellcheck={false}
                        />
                        <div class="flex justify-end gap-1.5">
                          <Button
                            type="button"
                            size="small"
                            variant="secondary"
                            onClick={() => {
                              setWorkflowUi("xml", graph().workflow.xml)
                              setWorkflowUi("editingXml", false)
                            }}
                          >
                            {language.t("common.cancel")}
                          </Button>
                          <Button
                            type="button"
                            size="small"
                            variant="primary"
                            icon="check-small"
                            disabled={updateWorkflowXmlMutation.isPending}
                            onClick={() => updateWorkflowXmlMutation.mutate()}
                          >
                            {language.t("session.workflow.saveXml")}
                          </Button>
                        </div>
                      </div>
                    </details>
                  </>
                )}
              </Show>
            </div>
          </Show>
        </div>
        </div>
      </Show>
      {workflowContextMenu()}
      {workflowFullscreenPreview()}
      {workflowSessionLogWindow()}
    </>
  )

  const merge = (next: NonNullable<ReturnType<typeof info>>) =>
    sync.set("session", (list) => {
      const idx = list.findIndex((item) => item.id === next.id)
      if (idx < 0) return list
      const out = list.slice()
      out[idx] = next
      return out
    })

  const roll = (sessionID: string, next: NonNullable<ReturnType<typeof info>>["revert"]) =>
    sync.set("session", (list) => {
      const idx = list.findIndex((item) => item.id === sessionID)
      if (idx < 0) return list
      const out = list.slice()
      out[idx] = { ...out[idx], revert: next }
      return out
    })

  const busy = (sessionID: string) => sync.data.session_working(sessionID)

  const queuedFollowups = createMemo(() => {
    const id = params.id
    if (!id) return emptyFollowups
    return followup.items[id] ?? emptyFollowups
  })

  const editingFollowup = createMemo(() => {
    const id = params.id
    if (!id) return
    return followup.edit[id]
  })

  const followupMutation = useMutation(() => ({
    mutationFn: async (input: { sessionID: string; id: string; manual?: boolean }) => {
      const item = (followup.items[input.sessionID] ?? []).find((entry) => entry.id === input.id)
      if (!item) return

      if (input.manual) setFollowup("paused", input.sessionID, undefined)
      setFollowup("failed", input.sessionID, undefined)

      const ok = await sendFollowupDraft({
        client: sdk.client,
        sync,
        serverSync,
        draft: item,
        optimisticBusy: item.sessionDirectory === sdk.directory,
        defaultPrompt: settings.general.defaultPrompt(),
      }).catch((err) => {
        setFollowup("failed", input.sessionID, input.id)
        fail(err)
        return false
      })
      if (!ok) return

      setFollowup("items", input.sessionID, (items) => (items ?? []).filter((entry) => entry.id !== input.id))
      if (input.manual) resumeScroll()
    },
  }))

  const followupBusy = (sessionID: string) =>
    followupMutation.isPending && followupMutation.variables?.sessionID === sessionID

  const sendingFollowup = createMemo(() => {
    const id = params.id
    if (!id) return
    if (!followupBusy(id)) return
    return followupMutation.variables?.id
  })

  const queueEnabled = createMemo(() => {
    const id = params.id
    if (!id) return false
    return (
      settings.general.followup() === "queue" &&
      busy(id) &&
      !composer.blocked()
    )
  })

  const followupText = (item: FollowupDraft) => {
    const text = item.prompt
      .map((part) => {
        if (part.type === "image") return `[image:${part.filename}]`
        if (part.type === "file") return `[file:${part.path}]`
        if (part.type === "agent") return `@${part.name}`
        return part.content
      })
      .join("")
      .split(/\r?\n/)
      .map((line) => line.trim())
      .find((line) => !!line)

    if (text) return text
    return `[${language.t("common.attachment")}]`
  }

  const queueFollowup = (draft: FollowupDraft, id = Identifier.ascending("message")) => {
    setFollowup("items", draft.sessionID, (items) => [...(items ?? []), { id, ...draft }])
    setFollowup("failed", draft.sessionID, undefined)
    setFollowup("paused", draft.sessionID, undefined)
  }

  const removeQueuedPlanFollowups = (sessionID: string) => {
    setFollowup("items", sessionID, (items) =>
      (items ?? []).filter((entry) => !entry.id.startsWith(PLAN_FOLLOWUP_PREFIX)),
    )
    setFollowup("failed", sessionID, (value) => (value?.startsWith(PLAN_FOLLOWUP_PREFIX) ? undefined : value))
  }

  const assistantOutput = (messageID: string) => {
    const parts = sync.data.part[messageID]
    if (!parts) return
    return parts
      .filter((part) => part.type === "text")
      .map((part) => part.text)
      .join("\n")
  }

  const hasPlanCompleteMarker = (output: string) =>
    output.split(/\r?\n/).some((line) => line.trim() === PLAN_COMPLETE_MARKER)

  const planMilestones = createMemo(() => {
    const id = params.id
    if (!id) return emptyPlanMilestones
    return planSession.milestones[id] ?? emptyPlanMilestones
  })

  const savePlanBlock = async (block: OpencodePlanBlock) => {
    return sdk.client.file
      .plan.save({
        title: block.title,
        content: block.content,
      })
      .then((result) => result.data?.path)
      .catch((err) => {
        showToast({
          variant: "error",
          title: language.t("common.requestFailed"),
          description: formatServerError(err, language.t, language.t("common.requestFailed")),
        })
        return undefined
      })
  }

  const parsePlanMilestones = (output: string) => {
    const matches = output.matchAll(PLAN_MILESTONE_PATTERN)
    return [...matches].map((match) => match[1]?.trim()).filter((text): text is string => !!text)
  }

  const recordPlanMilestones = (sessionID: string, assistant: AssistantMessage, output: string) => {
    const parsed = parsePlanMilestones(output)
    if (parsed.length === 0) return

    setPlanSession("milestones", sessionID, (items) => {
      const existing = items ?? []
      if (existing.some((item) => item.messageID === assistant.id)) return existing
      return [
        ...existing,
        ...parsed.map((text, index) => ({
          id: `${assistant.id}:${index}`,
          messageID: assistant.id,
          parentID: assistant.parentID,
          at: assistant.time.completed ?? assistant.time.created,
          text,
        })),
      ]
    })
  }

  const queuePlanFollowup = (sessionID: string, path: string, opts?: { notify?: boolean }) => {
    if (followupBusy(sessionID)) return false
    if ((followup.items[sessionID] ?? []).length > 0) return false
    if (followup.paused[sessionID]) return false
    if (composer.blocked()) return false
    if (busy(sessionID)) return false

    const currentModel = local.model.current()
    const currentAgent = local.agent.current()
    if (!currentModel || !currentAgent) {
      if (opts?.notify) {
        showToast({
          title: language.t("prompt.toast.modelAgentRequired.title"),
          description: language.t("prompt.toast.modelAgentRequired.description"),
        })
      }
      return false
    }

    const content = PLAN_FOLLOWUP_CONTENT + "\n\n"
    const fileContent = `@${path}`
    queueFollowup(
      {
        sessionID,
        sessionDirectory: sdk.directory,
        prompt: [
          { type: "text", content, start: 0, end: content.length },
          {
            type: "file",
            path,
            content: fileContent,
            start: content.length,
            end: content.length + fileContent.length,
          },
        ],
        context: [],
        agent: currentAgent.name,
        model: { providerID: currentModel.provider.id, modelID: currentModel.id },
        variant: local.model.variant.current(),
      },
      PLAN_FOLLOWUP_PREFIX + Identifier.ascending("message"),
    )
    return true
  }

  const usePlanBlock = async (block: OpencodePlanBlock) => {
    const sessionID = params.id
    if (!sessionID) return
    const path = await savePlanBlock(block)
    if (!path) return
    batch(() => {
      setPlanSession("file", sessionID, path)
      setPlanSession("enabled", sessionID, false)
      setPlanSession("lastAssistant", sessionID, undefined)
      setPlanSession("milestones", sessionID, [])
      setFollowup("paused", sessionID, undefined)
      removeQueuedPlanFollowups(sessionID)
    })
    if (queuePlanFollowup(sessionID, path, { notify: true })) {
      setPlanSession("enabled", sessionID, true)
      return
    }
    showToast({
      variant: "success",
      icon: "circle-check",
      title: language.t("session.plan.block.ready.title"),
      description: language.t("session.plan.block.ready.description"),
    })
  }

  const enablePlanSession = (sessionID: string, path: string, opts?: { resetMilestones?: boolean }) => {
    batch(() => {
      setPlanSession("file", sessionID, path)
      setPlanSession("enabled", sessionID, false)
      setPlanSession("lastAssistant", sessionID, undefined)
      if (opts?.resetMilestones) setPlanSession("milestones", sessionID, [])
      setFollowup("paused", sessionID, undefined)
      removeQueuedPlanFollowups(sessionID)
    })
    if (queuePlanFollowup(sessionID, path, { notify: true })) {
      setPlanSession("enabled", sessionID, true)
    }
  }

  const choosePlanFile = () => {
    const sessionID = params.id
    if (!sessionID) return
    void import("@/components/dialog-select-file").then((x) => {
      dialog.show(() => (
        <x.DialogSelectFile
          mode="files"
          nativeFilePicker
          onSelectFile={(path) => {
            const id = params.id
            if (!id) return
            enablePlanSession(id, path, { resetMilestones: true })
          }}
        />
      ))
    })
  }

  const pausePlanSession = () => {
    const sessionID = params.id
    if (!sessionID) return
    setPlanSession("enabled", sessionID, false)
    removeQueuedPlanFollowups(sessionID)
  }

  const clearPlanSession = () => {
    const sessionID = params.id
    if (!sessionID) return
    batch(() => {
      setPlanSession("enabled", sessionID, undefined)
      setPlanSession("file", sessionID, undefined)
      setPlanSession("lastAssistant", sessionID, undefined)
      setPlanSession("milestones", sessionID, undefined)
      removeQueuedPlanFollowups(sessionID)
    })
  }

  const togglePlanSession = () => {
    const sessionID = params.id
    if (!sessionID) return
    if (planActive()) {
      pausePlanSession()
      return
    }
    const path = planSession.file[sessionID]
    if (!path) {
      choosePlanFile()
      return
    }
    enablePlanSession(sessionID, path)
  }

  const planSessionControl = () => (
    <Show when={params.id && !mobileChanges()}>
      <div class="shrink-0 px-3 pt-3 pb-1 flex max-w-full flex-col gap-2">
        <div class="flex max-w-full items-center gap-1">
          <Tooltip
            placement="bottom"
            value={
              <div class="flex flex-col gap-1">
                <span>{language.t(planFile() ? "session.plan.change" : "session.plan.choose")}</span>
                <span class="text-text-weaker">
                  {language.t("session.plan.marker", {
                    marker: PLAN_COMPLETE_MARKER,
                  })}
                </span>
                <span class="text-text-weaker">
                  {language.t("session.plan.milestoneTag", {
                    open: PLAN_MILESTONE_OPEN,
                    close: PLAN_MILESTONE_CLOSE,
                  })}
                </span>
              </div>
            }
          >
            <Button
              type="button"
              size="small"
              variant={planActive() ? "primary" : "secondary"}
              icon={planFile() ? (planActive() ? "circle-ban-sign" : "arrow-up") : "checklist"}
              class="max-w-[260px] justify-start border border-border-weak-base bg-surface-panel shadow-sm"
              onClick={() => (planFile() ? togglePlanSession() : choosePlanFile())}
              aria-label={language.t(
                planFile()
                  ? planActive()
                    ? "session.plan.disable"
                    : "session.plan.enable"
                  : "session.plan.choose",
              )}
            >
              <span class="truncate">
                {planFile()
                  ? planActive()
                    ? getFilename(planFile()!)
                    : language.t("session.plan.enable")
                  : language.t("session.plan.choose")}
              </span>
            </Button>
          </Tooltip>
          <Show when={planFile()}>
            <Tooltip placement="bottom" value={language.t("session.plan.change")}>
              <IconButton
                type="button"
                size="small"
                variant="secondary"
                icon="folder"
                class="border border-border-weak-base bg-surface-panel shadow-sm"
                onClick={choosePlanFile}
                aria-label={language.t("session.plan.change")}
              />
            </Tooltip>
            <Tooltip placement="bottom" value={language.t("session.plan.clear")}>
              <IconButton
                type="button"
                size="small"
                variant="secondary"
                icon="close-small"
                class="border border-border-weak-base bg-surface-panel shadow-sm"
                onClick={clearPlanSession}
                aria-label={language.t("session.plan.clear")}
              />
            </Tooltip>
          </Show>
        </div>
        <Show when={planMilestones().length > 0}>
          <div class="max-w-full overflow-x-auto">
            <div class="flex items-stretch gap-1.5">
              <div class="shrink-0 self-center text-11-medium text-text-weaker">
                {language.t("session.plan.milestones")}
              </div>
              <For each={planMilestones()}>
                {(item, index) => (
                  <button
                    type="button"
                    class="shrink-0 max-w-[280px] rounded-md border border-border-weak-base bg-surface-panel px-2 py-1 text-left shadow-sm hover:bg-surface-hover"
                    onClick={() => {
                      const message = visibleUserMessages().find((value) => value.id === item.parentID)
                      if (message) scrollToMessage(message)
                    }}
                  >
                    <div class="text-11-medium text-text-weaker">
                      {language.t("session.plan.milestone", { count: index() + 1 })}
                    </div>
                    <div class="text-12-regular text-text-strong whitespace-pre-wrap break-words max-h-14 overflow-hidden">
                      {item.text}
                    </div>
                  </button>
                )}
              </For>
            </div>
          </div>
        </Show>
      </div>
    </Show>
  )

  createEffect(() => {
    const sessionID = params.id
    const path = planSession.file[sessionID ?? ""]
    const assistant = lastCompletedAssistant()
    if (!sessionID || !path || !planSession.enabled[sessionID] || !assistant) return
    if (planSession.lastAssistant[sessionID] === assistant.id) return
    if (assistant.error) return

    const output = assistantOutput(assistant.id)
    if (output === undefined) return

    recordPlanMilestones(sessionID, assistant, output)

    if (hasPlanCompleteMarker(output)) {
      batch(() => {
        setPlanSession("enabled", sessionID, false)
        setPlanSession("lastAssistant", sessionID, assistant.id)
        removeQueuedPlanFollowups(sessionID)
      })
      showToast({
        variant: "success",
        icon: "circle-check",
        title: language.t("session.plan.complete.title"),
        description: language.t("session.plan.complete.description"),
      })
      return
    }

    if (queuePlanFollowup(sessionID, path)) {
      setPlanSession("lastAssistant", sessionID, assistant.id)
    }
  })

  const followupDock = createMemo(() => queuedFollowups().map((item) => ({ id: item.id, text: followupText(item) })))

  const sendFollowup = (sessionID: string, id: string, opts?: { manual?: boolean }) => {
    const item = (followup.items[sessionID] ?? []).find((entry) => entry.id === id)
    if (!item) return Promise.resolve()
    if (followupBusy(sessionID)) return Promise.resolve()

    return followupMutation.mutateAsync({ sessionID, id, manual: opts?.manual })
  }

  const editFollowup = (id: string) => {
    const sessionID = params.id
    if (!sessionID) return
    if (followupBusy(sessionID)) return

    const item = queuedFollowups().find((entry) => entry.id === id)
    if (!item) return

    setFollowup("items", sessionID, (items) => (items ?? []).filter((entry) => entry.id !== id))
    setFollowup("failed", sessionID, (value) => (value === id ? undefined : value))
    setFollowup("edit", sessionID, {
      id: item.id,
      prompt: item.prompt,
      context: item.context,
    })
  }

  const clearFollowupEdit = () => {
    const id = params.id
    if (!id) return
    setFollowup("edit", id, undefined)
  }

  const halt = (sessionID: string) =>
    busy(sessionID) ? sdk.client.session.abort({ sessionID }).catch(() => {}) : Promise.resolve()

  const revertMutation = useMutation(() => ({
    mutationFn: async (input: { sessionID: string; messageID: string }) => {
      const prev = prompt.current().slice()
      const last = info()?.revert
      const value = draft(input.messageID)
      batch(() => {
        roll(input.sessionID, { messageID: input.messageID })
        prompt.set(value)
      })
      await halt(input.sessionID)
        .then(() => sdk.client.session.revert(input))
        .then((result) => {
          if (result.data) merge(result.data)
        })
        .catch((err) => {
          batch(() => {
            roll(input.sessionID, last)
            prompt.set(prev)
          })
          fail(err)
        })
    },
  }))

  const restoreMutation = useMutation(() => ({
    mutationFn: async (id: string) => {
      const sessionID = params.id
      if (!sessionID) return

      const next = userMessages().find((item) => item.id > id)
      const prev = prompt.current().slice()
      const last = info()?.revert

      batch(() => {
        roll(sessionID, next ? { messageID: next.id } : undefined)
        if (next) {
          prompt.set(draft(next.id))
          return
        }
        prompt.reset()
      })

      const task = !next
        ? halt(sessionID).then(() => sdk.client.session.unrevert({ sessionID }))
        : halt(sessionID).then(() =>
            sdk.client.session.revert({
              sessionID,
              messageID: next.id,
            }),
          )

      await task
        .then((result) => {
          if (result.data) merge(result.data)
        })
        .catch((err) => {
          batch(() => {
            roll(sessionID, last)
            prompt.set(prev)
          })
          fail(err)
        })
    },
  }))

  const reverting = createMemo(() => revertMutation.isPending || restoreMutation.isPending)
  const restoring = createMemo(() => (restoreMutation.isPending ? restoreMutation.variables : undefined))

  const revert = (input: { sessionID: string; messageID: string }) => {
    if (reverting()) return
    return revertMutation.mutateAsync(input)
  }

  const restore = (id: string) => {
    if (!params.id || reverting()) return
    return restoreMutation.mutateAsync(id)
  }

  const rolled = createMemo(() => {
    const id = revertMessageID()
    if (!id) return []
    return userMessages()
      .filter((item) => item.id >= id)
      .map((item) => ({ id: item.id, text: line(item.id) }))
  })

  const actions = { revert }

  createEffect(() => {
    const sessionID = params.id
    if (!sessionID) return

    const item = queuedFollowups()[0]
    if (!item) return
    if (followupBusy(sessionID)) return
    if (followup.failed[sessionID] === item.id) return
    if (followup.paused[sessionID]) return
    if (composer.blocked()) return
    if (busy(sessionID)) return

    void sendFollowup(sessionID, item.id)
  })

  createResizeObserver(
    () => promptDock,
    ({ height }) => {
      const next = Math.ceil(height)

      if (next === dockHeight) return

      const el = scroller
      const delta = next - dockHeight
      const stick = el
        ? !autoScroll.userScrolled() || el.scrollHeight - el.clientHeight - el.scrollTop < 10 + Math.max(0, delta)
        : false

      dockHeight = next

      if (stick) autoScroll.forceScrollToBottom()

      if (el) scheduleScrollState(el)
      fill()
    },
  )

  const { clearMessageHash, scrollToMessage } = useSessionHashScroll({
    sessionKey,
    sessionID: () => params.id,
    messagesReady,
    visibleUserMessages,
    historyMore,
    historyLoading,
    loadMore: (sessionID) => sync.session.history.loadMore(sessionID),
    currentMessageId: () => store.messageId,
    pendingMessage: () => ui.pendingMessage,
    setPendingMessage: (value) => setUi("pendingMessage", value),
    setActiveMessage,
    autoScroll,
    scroller: () => scroller,
    anchor,
    revealMessage: (id) => revealMessage(id),
    scheduleScrollState,
    consumePendingMessage: layout.pendingMessage.consume,
  })

  createEffect(
    on(
      () => params.id,
      (id) => {
        if (!id) requestAnimationFrame(() => inputRef?.focus())
      },
    ),
  )

  onMount(() => {
    makeEventListener(document, "keydown", handleKeyDown)
  })

  onCleanup(() => {
    if (reviewFrame !== undefined) cancelAnimationFrame(reviewFrame)
    if (refreshFrame !== undefined) cancelAnimationFrame(refreshFrame)
    if (refreshTimer !== undefined) window.clearTimeout(refreshTimer)
    if (todoFrame !== undefined) cancelAnimationFrame(todoFrame)
    if (todoTimer !== undefined) window.clearTimeout(todoTimer)
    if (diffFrame !== undefined) cancelAnimationFrame(diffFrame)
    if (diffTimer !== undefined) window.clearTimeout(diffTimer)
    if (scrollStateFrame !== undefined) cancelAnimationFrame(scrollStateFrame)
    if (fillFrame !== undefined) cancelAnimationFrame(fillFrame)
  })

  useUsageExceededDialogs()

  const composerRegion = (placement: "dock" | "inline") => (
    <SessionComposerRegion
      state={composer}
      ready={!store.deferRender && messagesReady()}
      centered={placement === "dock" && centered()}
      placement={placement}
      inputRef={(el) => {
        inputRef = el
      }}
      newSessionWorktree={newSessionWorktree()}
      onNewSessionWorktreeReset={() => setStore("newSessionWorktree", "main")}
      onSubmit={() => {
        comments.clear()
        resumeScroll()
      }}
      onResponseSubmit={resumeScroll}
      followup={
        params.id
          ? {
              queue: queueEnabled,
              items: followupDock(),
              sending: sendingFollowup(),
              edit: editingFollowup(),
              onQueue: queueFollowup,
              onAbort: () => {
                const id = params.id
                if (!id) return
                setFollowup("paused", id, true)
              },
              onSend: (id) => {
                void sendFollowup(params.id!, id, { manual: true })
              },
              onEdit: editFollowup,
              onEditLoaded: clearFollowupEdit,
            }
          : undefined
      }
      revert={
        rolled().length > 0
          ? {
              items: rolled(),
              restoring: restoring(),
              disabled: reverting(),
              onRestore: restore,
            }
          : undefined
      }
      setPromptDockRef={(el) => {
        promptDock = el
      }}
    />
  )

  return (
    <div class="relative bg-background-base size-full overflow-hidden flex flex-col">
      {sessionSync() ?? ""}
      <SessionHeader />
      <div class="flex-1 min-h-0 flex flex-col md:flex-row">
        <Show when={!isDesktop() && !!params.id}>
          <Tabs value={store.mobileTab} class="h-auto">
            <Tabs.List>
              <Tabs.Trigger
                value="session"
                class="!w-1/2 !max-w-none"
                classes={{ button: "w-full" }}
                onClick={() => setStore("mobileTab", "session")}
              >
                {language.t("session.tab.session")}
              </Tabs.Trigger>
              <Tabs.Trigger
                value="changes"
                class="!w-1/2 !max-w-none !border-r-0"
                classes={{ button: "w-full" }}
                onClick={() => setStore("mobileTab", "changes")}
              >
                {hasReview()
                  ? language.t("session.review.filesChanged", { count: reviewCount() })
                  : language.t("session.review.change.other")}
              </Tabs.Trigger>
            </Tabs.List>
          </Tabs>
        </Show>

        <div
          classList={{
            "@container relative shrink-0 flex flex-col min-h-0 h-full bg-background-stronger flex-1 md:flex-none": true,
            "duration-[240ms] ease-[cubic-bezier(0.22,1,0.36,1)] will-change-[width] motion-reduce:transition-none":
              !size.active() && !ui.reviewSnap,
            "transition-[width]": !isV2NewSessionPage(),
          }}
          style={{
            width: sessionPanelWidth(),
          }}
        >
          {planSessionControl()}
          {workflowSessionControl()}
          <div class="flex-1 min-h-0 overflow-hidden">
            <Switch>
              <Match when={params.id && mobileChanges()}>
                <div class="relative h-full overflow-hidden">
                  {reviewContent({
                    diffStyle: "unified",
                    classes: {
                      root: "pb-8",
                      header: "px-4",
                      container: "px-4",
                    },
                    loadingClass: "px-4 py-4 text-text-weak",
                    emptyClass: "h-full pb-64 -mt-4 flex flex-col items-center justify-center text-center gap-6",
                  })}
                </div>
              </Match>
              <Match when={params.id}>
                <Show when={messagesReady()}>
                  <MessageTimeline
                    actions={actions}
                    onPlanSave={savePlanBlock}
                    onPlanContinue={usePlanBlock}
                    scroll={ui.scroll}
                    onResumeScroll={resumeScroll}
                    setScrollRef={setScrollRef}
                    onScheduleScrollState={scheduleScrollState}
                    onAutoScrollHandleScroll={autoScroll.handleScroll}
                    onMarkScrollGesture={markScrollGesture}
                    hasScrollGesture={hasScrollGesture}
                    onUserScroll={markUserScroll}
                    onHistoryScroll={historyLoader.onScrollerScroll}
                    onAutoScrollInteraction={autoScroll.handleInteraction}
                    shouldAnchorBottom={() =>
                      !location.hash && !store.messageId && !ui.pendingMessage && !autoScroll.userScrolled()
                    }
                    centered={centered()}
                    setContentRef={(el) => {
                      content = el
                      autoScroll.contentRef(el)

                      const root = scroller
                      if (root) scheduleScrollState(root)
                    }}
                    historyShift={historyLoader.shift()}
                    userMessages={historyLoader.userMessages()}
                    anchor={anchor}
                    setRevealMessage={(fn) => {
                      revealMessage = fn
                    }}
                  />
                </Show>
              </Match>
              <Match when={true}>
                <Show when={newSessionDesign()} fallback={<NewSessionView worktree={newSessionWorktree()} />}>
                  <NewSessionDesignView>{composerRegion("inline")}</NewSessionDesignView>
                </Show>
              </Match>
            </Switch>
          </div>

          <Show when={params.id || !newSessionDesign()}>{composerRegion("dock")}</Show>

          <Show when={desktopReviewOpen()}>
            <div onPointerDown={() => size.start()}>
              <ResizeHandle
                direction="horizontal"
                size={layout.session.width()}
                min={450}
                max={typeof window === "undefined" ? 1000 : window.innerWidth * 0.45}
                onResize={(width) => {
                  size.touch()
                  layout.session.resize(width)
                }}
              />
            </div>
          </Show>
        </div>

        <SessionSidePanel
          canReview={canReview}
          diffs={reviewDiffs}
          diffsReady={reviewReady}
          empty={reviewEmptyText}
          hasReview={hasReview}
          reviewCount={reviewCount}
          reviewPanel={reviewPanel}
          activeDiff={tree.activeDiff}
          focusReviewDiff={focusReviewDiff}
          reviewSnap={ui.reviewSnap}
          size={size}
        />
      </div>

      <TerminalPanel />
    </div>
  )
}
