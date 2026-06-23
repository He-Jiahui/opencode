import { Button } from "@opencode-ai/ui/button"
import { useDialog } from "@opencode-ai/ui/context/dialog"
import { Dialog } from "@opencode-ai/ui/dialog"
import { Select } from "@opencode-ai/ui/select"
import { TextField } from "@opencode-ai/ui/text-field"
import { useLanguage } from "@/context/language"
import { useModels } from "@/context/models"
import { listModelVariants } from "@/context/model-variant"
import { For, Show } from "solid-js"
import { createStore } from "solid-js/store"

type WorkflowStaffing = {
  mainPM: number
  departmentPM: number
  executor: number
  reviewer: number
  tester: number
  expert: number
}

type WorkflowModelWhitelistRole = "requester" | keyof WorkflowStaffing
type WorkflowModelWhitelistEntry = {
  providerID: string
  modelID: string
  variant?: string
  weight: number
  cacheMinutes: number
}
type WorkflowModelWhitelist = Record<WorkflowModelWhitelistRole, WorkflowModelWhitelistEntry[]>

const defaultStaffing: WorkflowStaffing = {
  mainPM: 1,
  departmentPM: 2,
  executor: 4,
  reviewer: 2,
  tester: 1,
  expert: 1,
}

const staffingFields = [
  ["mainPM", "session.workflow.staffing.mainPM"],
  ["departmentPM", "session.workflow.staffing.departmentPM"],
  ["executor", "session.workflow.staffing.executor"],
  ["reviewer", "session.workflow.staffing.reviewer"],
  ["tester", "session.workflow.staffing.tester"],
  ["expert", "session.workflow.staffing.expert"],
] as const

const modelWhitelistRoles = [
  ["requester", "session.workflow.modelWhitelist.requester"],
  ["mainPM", "session.workflow.staffing.mainPM"],
  ["departmentPM", "session.workflow.staffing.departmentPM"],
  ["executor", "session.workflow.staffing.executor"],
  ["reviewer", "session.workflow.staffing.reviewer"],
  ["tester", "session.workflow.staffing.tester"],
  ["expert", "session.workflow.staffing.expert"],
] as const

const emptyModelWhitelist = (): WorkflowModelWhitelist => ({
  requester: [],
  mainPM: [],
  departmentPM: [],
  executor: [],
  reviewer: [],
  tester: [],
  expert: [],
})

function normalizeLimit(value: string) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 1
  return Math.max(1, Math.min(12, Math.trunc(parsed)))
}

function normalizeWeight(value: string) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 50
  return Math.max(0, Math.min(100, parsed))
}

function normalizeCacheMinutes(value: string) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 240
  return Math.max(0, Math.min(43200, Math.trunc(parsed)))
}

export function DialogStartWorkflow(props: {
  initialRequest?: string
  variants?: string[]
  initialVariant?: string
  pending?: boolean
  onStart: (input: { request: string; variant?: string; staffing: WorkflowStaffing; modelWhitelist: WorkflowModelWhitelist }) => void
}) {
  const dialog = useDialog()
  const language = useLanguage()
  const models = useModels()
  const variants = () => props.variants ?? []
  const modelOptions = () =>
    models.list().map((model) => ({
      providerID: model.provider.id,
      modelID: model.id,
      name: `${model.provider.name} / ${model.name}`,
      variants: listModelVariants({
        providerID: model.provider.id,
        modelID: model.id,
        variants: model.variants,
      }),
    }))
  const [store, setStore] = createStore({
    request: props.initialRequest ?? "",
    variant: props.initialVariant,
    staffing: { ...defaultStaffing },
    modelWhitelist: emptyModelWhitelist(),
  })

  const addWhitelistModel = (role: WorkflowModelWhitelistRole) => {
    const model = modelOptions()[0]
    if (!model) return
    setStore("modelWhitelist", role, store.modelWhitelist[role].length, {
      providerID: model.providerID,
      modelID: model.modelID,
      weight: 50,
      cacheMinutes: 240,
    })
  }

  const selectWhitelistModel = (
    role: WorkflowModelWhitelistRole,
    index: number,
    model: ReturnType<typeof modelOptions>[number] | undefined,
  ) => {
    if (!model) return
    const current = store.modelWhitelist[role][index]
    if (!current) return
    setStore("modelWhitelist", role, index, {
      ...current,
      providerID: model.providerID,
      modelID: model.modelID,
      variant: undefined,
    })
  }

  const removeWhitelistModel = (role: WorkflowModelWhitelistRole, index: number) => {
    setStore(
      "modelWhitelist",
      role,
      store.modelWhitelist[role].filter((_, itemIndex) => itemIndex !== index),
    )
  }

  const submit = (event: SubmitEvent) => {
    event.preventDefault()
    if (props.pending) return
    const request = store.request.trim()
    if (!request) return
    props.onStart({
      request,
      variant: store.variant === "default" ? undefined : store.variant,
      staffing: store.staffing,
      modelWhitelist: store.modelWhitelist,
    })
  }

  return (
    <Dialog title={language.t("session.workflow.request.title")} class="w-full max-w-[720px] mx-auto">
      <form onSubmit={submit} class="flex min-h-0 flex-1 flex-col">
        <div class="min-h-0 flex-1 overflow-y-auto px-6 pb-4 pt-0">
          <div class="flex flex-col gap-4">
            <TextField
              autofocus
              multiline
              label={language.t("session.workflow.request.label")}
              placeholder={language.t("session.workflow.request.placeholder")}
              value={store.request}
              onChange={(value) => setStore("request", value)}
              spellcheck={false}
              class="min-h-32 w-full"
            />
            <Show when={variants().length > 1}>
              <div class="flex items-center justify-between gap-3">
                <div class="min-w-0">
                  <div class="text-13-medium text-text-base">{language.t("session.workflow.variant.label")}</div>
                  <div class="text-12-regular text-text-muted">{language.t("session.workflow.variant.description")}</div>
                </div>
                <Select
                  size="large"
                  variant="secondary"
                  options={variants()}
                  current={store.variant ?? "default"}
                  label={(value) => (value === "default" ? language.t("common.default") : value)}
                  onSelect={(value) => setStore("variant", value === "default" ? undefined : value)}
                  class="capitalize min-w-32"
                  valueClass="truncate text-13-regular"
                />
              </div>
            </Show>
            <details class="rounded-md border border-border-weak-base bg-surface-panel">
              <summary class="cursor-pointer px-3 py-2 text-13-medium text-text-base">
                {language.t("session.workflow.staffing.title")}
              </summary>
              <div class="grid grid-cols-2 gap-3 border-t border-border-weak-base p-3">
                {staffingFields.map(([key, label]) => (
                  <TextField
                    type="number"
                    min="1"
                    max="12"
                    step="1"
                    label={language.t(label)}
                    value={String(store.staffing[key])}
                    onChange={(value) => setStore("staffing", key, normalizeLimit(value))}
                  />
                ))}
              </div>
            </details>
            <details class="rounded-md border border-border-weak-base bg-surface-panel">
              <summary class="cursor-pointer px-3 py-2 text-13-medium text-text-base">
                {language.t("session.workflow.modelWhitelist.title")}
              </summary>
              <div class="flex flex-col gap-4 border-t border-border-weak-base p-3">
                <For each={modelWhitelistRoles}>
                  {([role, label]) => (
                    <div class="flex flex-col gap-2">
                      <div class="flex items-center justify-between gap-3">
                        <div class="min-w-0">
                          <div class="text-13-medium text-text-base">{language.t(label)}</div>
                          <div class="text-12-regular text-text-muted">
                            {language.t("session.workflow.modelWhitelist.description")}
                          </div>
                        </div>
                        <Button
                          type="button"
                          size="small"
                          variant="secondary"
                          icon="plus-small"
                          disabled={modelOptions().length === 0}
                          onClick={() => addWhitelistModel(role)}
                        >
                          {language.t("session.workflow.modelWhitelist.add")}
                        </Button>
                      </div>
                      <Show
                        when={store.modelWhitelist[role].length > 0}
                        fallback={
                          <div class="rounded-md border border-border-weak-base px-3 py-2 text-12-regular text-text-muted">
                            {language.t("session.workflow.modelWhitelist.empty")}
                          </div>
                        }
                      >
                        <div class="flex flex-col gap-2">
                          <For each={store.modelWhitelist[role]}>
                            {(entry, index) => {
                              const selectedModel = () =>
                                modelOptions().find(
                                  (model) => model.providerID === entry.providerID && model.modelID === entry.modelID,
                                )
                              const variantOptions = () => ["default", ...(selectedModel()?.variants ?? [])]
                              return (
                                <div class="grid grid-cols-1 gap-2 rounded-md border border-border-weak-base p-2 md:grid-cols-[minmax(0,1fr)_140px_96px_96px_auto] md:items-end">
                                  <Select
                                    size="small"
                                    variant="secondary"
                                    options={modelOptions()}
                                    current={selectedModel()}
                                    value={(model) => `${model.providerID}/${model.modelID}`}
                                    label={(model) => model.name}
                                    onSelect={(model) => selectWhitelistModel(role, index(), model)}
                                    valueClass="truncate text-12-regular"
                                  />
                                  <Select
                                    size="small"
                                    variant="secondary"
                                    options={variantOptions()}
                                    current={entry.variant ?? "default"}
                                    label={(value) => (value === "default" ? language.t("common.default") : value)}
                                    onSelect={(value) =>
                                      setStore("modelWhitelist", role, index(), "variant", value === "default" ? undefined : value)
                                    }
                                    valueClass="truncate text-12-regular"
                                  />
                                  <TextField
                                    type="number"
                                    min="0"
                                    max="100"
                                    step="1"
                                    label={language.t("session.workflow.modelWhitelist.weight")}
                                    value={String(entry.weight)}
                                    onChange={(value) => setStore("modelWhitelist", role, index(), "weight", normalizeWeight(value))}
                                  />
                                  <TextField
                                    type="number"
                                    min="0"
                                    max="43200"
                                    step="1"
                                    label={language.t("session.workflow.modelWhitelist.cacheMinutes")}
                                    value={String(entry.cacheMinutes)}
                                    onChange={(value) =>
                                      setStore("modelWhitelist", role, index(), "cacheMinutes", normalizeCacheMinutes(value))
                                    }
                                  />
                                  <Button
                                    type="button"
                                    size="small"
                                    variant="ghost"
                                    icon="close"
                                    onClick={() => removeWhitelistModel(role, index())}
                                  />
                                </div>
                              )
                            }}
                          </For>
                        </div>
                      </Show>
                    </div>
                  )}
                </For>
              </div>
            </details>
          </div>
        </div>
        <div class="flex shrink-0 justify-end gap-2 border-t border-border-weak-base px-6 py-4">
          <Button type="button" variant="ghost" size="large" onClick={() => dialog.close()}>
            {language.t("common.cancel")}
          </Button>
          <Button type="submit" variant="primary" size="large" disabled={props.pending || !store.request.trim()}>
            {props.pending ? language.t("common.loading") : language.t("session.workflow.start")}
          </Button>
        </div>
      </form>
    </Dialog>
  )
}
