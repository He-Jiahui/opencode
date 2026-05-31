import { Button } from "@opencode-ai/ui/button"
import { useDialog } from "@opencode-ai/ui/context/dialog"
import { Dialog } from "@opencode-ai/ui/dialog"
import { Select } from "@opencode-ai/ui/select"
import { TextField } from "@opencode-ai/ui/text-field"
import { useLanguage } from "@/context/language"
import { Show } from "solid-js"
import { createStore } from "solid-js/store"

type WorkflowStaffing = {
  mainPM: number
  departmentPM: number
  executor: number
  reviewer: number
  tester: number
  expert: number
}

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

function normalizeLimit(value: string) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 1
  return Math.max(1, Math.min(12, Math.trunc(parsed)))
}

export function DialogStartWorkflow(props: {
  initialRequest?: string
  variants?: string[]
  initialVariant?: string
  pending?: boolean
  onStart: (input: { request: string; variant?: string; staffing: WorkflowStaffing }) => void
}) {
  const dialog = useDialog()
  const language = useLanguage()
  const variants = () => props.variants ?? []
  const [store, setStore] = createStore({
    request: props.initialRequest ?? "",
    variant: props.initialVariant,
    staffing: defaultStaffing,
  })

  const submit = (event: SubmitEvent) => {
    event.preventDefault()
    if (props.pending) return
    const request = store.request.trim()
    if (!request) return
    props.onStart({ request, variant: store.variant === "default" ? undefined : store.variant, staffing: store.staffing })
  }

  return (
    <Dialog title={language.t("session.workflow.request.title")} class="w-full max-w-[520px] mx-auto">
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
