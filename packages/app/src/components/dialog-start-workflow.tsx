import { Button } from "@opencode-ai/ui/button"
import { useDialog } from "@opencode-ai/ui/context/dialog"
import { Dialog } from "@opencode-ai/ui/dialog"
import { Select } from "@opencode-ai/ui/select"
import { TextField } from "@opencode-ai/ui/text-field"
import { useLanguage } from "@/context/language"
import { Show } from "solid-js"
import { createStore } from "solid-js/store"

export function DialogStartWorkflow(props: {
  initialRequest?: string
  variants?: string[]
  initialVariant?: string
  pending?: boolean
  onStart: (input: { request: string; variant?: string }) => void
}) {
  const dialog = useDialog()
  const language = useLanguage()
  const variants = () => props.variants ?? []
  const [store, setStore] = createStore({
    request: props.initialRequest ?? "",
    variant: props.initialVariant,
  })

  const submit = (event: SubmitEvent) => {
    event.preventDefault()
    if (props.pending) return
    const request = store.request.trim()
    if (!request) return
    props.onStart({ request, variant: store.variant === "default" ? undefined : store.variant })
  }

  return (
    <Dialog title={language.t("session.workflow.request.title")} class="w-full max-w-[520px] mx-auto">
      <form onSubmit={submit} class="flex flex-col gap-4 p-6 pt-0">
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
        <div class="flex justify-end gap-2">
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
