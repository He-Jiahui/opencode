import { Markdown } from "@opencode-ai/ui/markdown"
import { Button } from "@opencode-ai/ui/button"
import { IconButton } from "@opencode-ai/ui/icon-button"
import { Tooltip } from "@opencode-ai/ui/tooltip"
import { showToast } from "@opencode-ai/ui/toast"
import { createSignal } from "solid-js"
import { useLanguage } from "@/context/language"
import { writeClipboard } from "@/utils/clipboard"
import type { OpencodePlanBlock as PlanBlock } from "@/utils/opencode-plan"

export function OpencodePlanBlock(props: {
  plan: PlanBlock
  onSave: (plan: PlanBlock) => Promise<string | undefined>
  onContinue: (plan: PlanBlock) => Promise<void> | void
}) {
  const language = useLanguage()
  const [saving, setSaving] = createSignal(false)
  const [copied, setCopied] = createSignal(false)
  const [continuing, setContinuing] = createSignal(false)

  const copy = async () => {
    if (!(await writeClipboard(props.plan.content))) {
      showToast({ variant: "error", title: language.t("common.requestFailed") })
      return
    }
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const save = async () => {
    if (saving()) return
    setSaving(true)
    const path = await props.onSave(props.plan).finally(() => setSaving(false))
    if (!path) return
    showToast({
      variant: "success",
      icon: "circle-check",
      title: language.t("session.plan.block.saved.title"),
      description: path,
    })
  }

  const continuePlan = async () => {
    if (continuing()) return
    setContinuing(true)
    await Promise.resolve(props.onContinue(props.plan)).finally(() => setContinuing(false))
  }

  return (
    <section data-component="opencode-plan-block">
      <div data-slot="opencode-plan-header">
        <div data-slot="opencode-plan-title">
          <span data-slot="opencode-plan-label">{language.t("session.plan.block.label")}</span>
          <span data-slot="opencode-plan-name">{props.plan.title}</span>
        </div>
        <div data-slot="opencode-plan-actions">
          <Tooltip placement="top" value={language.t("session.plan.block.save")}>
            <IconButton
              type="button"
              size="small"
              variant="ghost"
              icon={saving() ? "dash" : "download"}
              disabled={saving()}
              onClick={save}
              aria-label={language.t("session.plan.block.save")}
            />
          </Tooltip>
          <Tooltip placement="top" value={language.t("session.plan.block.copy")}>
            <IconButton
              type="button"
              size="small"
              variant="ghost"
              icon={copied() ? "check" : "copy"}
              onClick={copy}
              aria-label={language.t("session.plan.block.copy")}
            />
          </Tooltip>
          <Button
            type="button"
            size="small"
            variant="primary"
            icon="arrow-up"
            disabled={continuing()}
            onClick={continuePlan}
          >
            {language.t("session.plan.block.continue")}
          </Button>
        </div>
      </div>
      <div data-slot="opencode-plan-content">
        <Markdown text={props.plan.content} cacheKey={`opencode-plan-${props.plan.id}`} streaming={false} />
      </div>
    </section>
  )
}
