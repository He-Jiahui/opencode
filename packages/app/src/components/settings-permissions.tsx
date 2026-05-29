import type { PermissionActionConfig, PermissionConfig } from "@opencode-ai/sdk/v2/client"
import { Button } from "@opencode-ai/ui/button"
import { IconButton } from "@opencode-ai/ui/icon-button"
import { Select } from "@opencode-ai/ui/select"
import { TextField } from "@opencode-ai/ui/text-field"
import { showToast } from "@opencode-ai/ui/toast"
import { createEffect, createMemo, createSignal, For, Match, Show, Switch, type Component } from "solid-js"
import { createStore } from "solid-js/store"
import { useLanguage } from "@/context/language"
import { useServerSync } from "@/context/server-sync"
import { SettingsList } from "./settings-list"

type PermissionMode = "ask" | "whitelist" | "blacklist" | "allow"
type RuleDraft = {
  id: string
  permission: string
  pattern: string
}

const DEFAULT_RULE_PERMISSION = "*"
const DEFAULT_RULE_PATTERN = "*"
const RULE_SEPARATOR = "\u0000"

const TOOLS = [
  "read",
  "edit",
  "glob",
  "grep",
  "list",
  "bash",
  "task",
  "skill",
  "lsp",
  "todowrite",
  "question",
  "webfetch",
  "websearch",
  "repo_clone",
  "repo_overview",
  "external_directory",
  "doom_loop",
]
const ACTION_ONLY_TOOLS = new Set(["todowrite", "question", "webfetch", "websearch", "doom_loop"])

function action(value: unknown) {
  if (value === "ask" || value === "allow" || value === "deny") return value
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function normalizePermission(value: PermissionConfig | undefined) {
  if (!value) return { mode: "ask" as const, rules: [] as RuleDraft[] }
  if (value === "allow") return { mode: "allow" as const, rules: [] as RuleDraft[] }
  if (value === "ask") return { mode: "ask" as const, rules: [] as RuleDraft[] }
  if (value === "deny") return { mode: "whitelist" as const, rules: [] as RuleDraft[] }
  if (!isRecord(value)) return { mode: "ask" as const, rules: [] as RuleDraft[] }

  const root = action(value["*"])
  const entries = Object.entries(value).filter((entry) => entry[0] !== "*")
  if (root === "allow" && entries.length === 0) return { mode: "allow" as const, rules: [] as RuleDraft[] }

  const mode: PermissionMode = root === "deny" ? "whitelist" : root === "allow" ? "blacklist" : "ask"
  if (mode === "ask") return { mode, rules: [] }

  return {
    mode,
    rules: entries.flatMap((entry) => {
      const permission = entry[0]
      const rule = entry[1]
      if (typeof rule === "string") {
        if (rule !== (mode === "whitelist" ? "allow" : "deny")) return []
        return [
          { id: `${permission}${RULE_SEPARATOR}${DEFAULT_RULE_PATTERN}`, permission, pattern: DEFAULT_RULE_PATTERN },
        ]
      }
      if (!isRecord(rule)) return []
      return Object.entries(rule)
        .filter((entry) => entry[1] === (mode === "whitelist" ? "allow" : "deny"))
        .map(([pattern]) => ({ id: `${permission}${RULE_SEPARATOR}${pattern}`, permission, pattern }))
    }),
  }
}

function configFrom(mode: PermissionMode, rules: RuleDraft[]): PermissionConfig {
  if (mode === "allow") return "allow"
  if (mode === "ask") return "ask"

  return rules
    .flatMap((rule) => {
      const permission = rule.permission.trim() || DEFAULT_RULE_PERMISSION
      const pattern = rulePattern(permission, rule.pattern)
      return (permission === DEFAULT_RULE_PERMISSION ? TOOLS : [permission]).map((permission) => ({
        permission,
        pattern,
      }))
    })
    .reduce(
      (config, rule) => {
        const next = mode === "whitelist" ? "allow" : "deny"
        if (ACTION_ONLY_TOOLS.has(rule.permission)) {
          config[rule.permission] = next
          return config
        }

        const current = config[rule.permission]
        if (!isRecord(current)) {
          config[rule.permission] = { [rule.pattern]: next }
          return config
        }
        current[rule.pattern] = next
        return config
      },
      {
        "*": mode === "whitelist" ? "deny" : "allow",
      } as Record<string, PermissionActionConfig | Record<string, PermissionActionConfig>>,
    )
}

function canEditPattern(permission: string) {
  return permission === DEFAULT_RULE_PERMISSION || !ACTION_ONLY_TOOLS.has(permission)
}

function rulePattern(permission: string, pattern: string) {
  if (!canEditPattern(permission)) return DEFAULT_RULE_PATTERN
  return pattern.trim() || DEFAULT_RULE_PATTERN
}

function configPreview(mode: PermissionMode, rules: RuleDraft[]) {
  return JSON.stringify({
    mode,
    rules: rules.map((rule) => ({
      permission: rule.permission,
      pattern: rulePattern(rule.permission, rule.pattern),
    })),
  })
}

function resetKey(value: PermissionConfig | undefined) {
  const current = normalizePermission(value)
  return configPreview(current.mode, current.rules)
}

export const SettingsPermissions: Component = () => {
  const language = useLanguage()
  const serverSync = useServerSync()
  const normalized = normalizePermission(serverSync.data.config.permission)
  const [synced, setSynced] = createSignal(configPreview(normalized.mode, normalized.rules))

  const [state, setState] = createStore({
    saving: false,
    mode: normalized.mode as PermissionMode,
    rules: normalized.rules,
  })

  createEffect(() => {
    const current = normalizePermission(serverSync.data.config.permission)
    const currentKey = configPreview(current.mode, current.rules)
    if (state.saving) return

    const localKey = configPreview(state.mode, state.rules)
    if (localKey !== synced()) {
      if (localKey === currentKey) setSynced(currentKey)
      return
    }
    if (currentKey === synced()) return

    setState({ mode: current.mode, rules: current.rules, saving: false })
    setSynced(currentKey)
  })

  const toolOptions = createMemo(() => [
    {
      value: DEFAULT_RULE_PERMISSION,
      label: language.t("settings.permissions.rule.allTools"),
    },
    ...TOOLS.map((tool) => ({
      value: tool,
      label: language.t(`settings.permissions.tool.${tool}.title` as Parameters<typeof language.t>[0]),
    })),
  ])

  const modeOptions = createMemo(() => [
    {
      value: "ask" as const,
      label: language.t("settings.permissions.mode.ask.title"),
      description: language.t("settings.permissions.mode.ask.description"),
    },
    {
      value: "whitelist" as const,
      label: language.t("settings.permissions.mode.whitelist.title"),
      description: language.t("settings.permissions.mode.whitelist.description"),
    },
    {
      value: "blacklist" as const,
      label: language.t("settings.permissions.mode.blacklist.title"),
      description: language.t("settings.permissions.mode.blacklist.description"),
    },
    {
      value: "allow" as const,
      label: language.t("settings.permissions.mode.allow.title"),
      description: language.t("settings.permissions.mode.allow.description"),
    },
  ])

  const selectedMode = () => modeOptions().find((item) => item.value === state.mode) ?? modeOptions()[0]
  const rulesEnabled = () => state.mode === "whitelist" || state.mode === "blacklist"
  const dirty = createMemo(() => {
    return configPreview(state.mode, state.rules) !== resetKey(serverSync.data.config.permission)
  })

  const addRule = () => {
    setState("rules", (rules) => [
      ...rules,
      {
        id: `${Date.now()}:${rules.length}`,
        permission: DEFAULT_RULE_PERMISSION,
        pattern: DEFAULT_RULE_PATTERN,
      },
    ])
  }

  const save = async () => {
    setState("saving", true)
    const before = serverSync.data.config.permission
    const next = configFrom(state.mode, state.rules)
    serverSync.set("config", "permission", next)

    await serverSync
      .updateConfig({ permission: next })
      .then(() => {
        setSynced(configPreview(state.mode, state.rules))
        showToast({
          variant: "success",
          icon: "circle-check",
          title: language.t("settings.permissions.toast.updated.title"),
          description: language.t("settings.permissions.toast.updated.description"),
        })
      })
      .catch((err: unknown) => {
        serverSync.set("config", "permission", before)
        const message = err instanceof Error ? err.message : String(err)
        showToast({
          variant: "error",
          title: language.t("settings.permissions.toast.updateFailed.title"),
          description: message,
        })
      })
      .finally(() => setState("saving", false))
  }

  return (
    <div class="flex flex-col h-full overflow-y-auto no-scrollbar px-4 pb-10 sm:px-10 sm:pb-10">
      <div class="sticky top-0 z-10 bg-[linear-gradient(to_bottom,var(--surface-stronger-non-alpha)_calc(100%_-_24px),transparent)]">
        <div class="flex flex-col gap-1 pt-6 pb-8 max-w-[720px]">
          <h2 class="text-16-medium text-text-strong">{language.t("settings.permissions.title")}</h2>
          <span class="text-12-regular text-text-weak">{language.t("settings.permissions.description")}</span>
        </div>
      </div>

      <div class="flex flex-col gap-8 max-w-[720px]">
        <div class="flex flex-col gap-1">
          <h3 class="text-14-medium text-text-strong pb-2">{language.t("settings.permissions.section.mode")}</h3>
          <SettingsList>
            <div class="flex flex-wrap items-center justify-between gap-4 py-3 border-b border-border-weak-base last:border-none sm:flex-nowrap">
              <div class="flex min-w-0 flex-1 flex-col gap-0.5">
                <span class="text-14-medium text-text-strong">{selectedMode().label}</span>
                <span class="text-12-regular text-text-weak">{selectedMode().description}</span>
              </div>
              <Select
                data-action="settings-permissions-mode"
                options={modeOptions()}
                current={selectedMode()}
                value={(option) => option.value}
                label={(option) => option.label}
                onSelect={(option) => option && setState("mode", option.value)}
                variant="secondary"
                size="small"
                triggerVariant="settings"
                triggerStyle={{ "min-width": "180px" }}
              />
            </div>
          </SettingsList>
        </div>

        <Show when={rulesEnabled()}>
          <div class="flex flex-col gap-1">
            <div class="flex items-center justify-between gap-3 pb-2">
              <h3 class="text-14-medium text-text-strong">{language.t("settings.permissions.section.rules")}</h3>
              <Button size="small" variant="secondary" icon="plus-small" onClick={addRule}>
                {language.t("settings.permissions.rule.add")}
              </Button>
            </div>
            <SettingsList>
              <Show
                when={state.rules.length > 0}
                fallback={
                  <div class="py-4 text-14-regular text-text-weak">
                    <Switch>
                      <Match when={state.mode === "whitelist"}>
                        {language.t("settings.permissions.rule.empty.whitelist")}
                      </Match>
                      <Match when={state.mode === "blacklist"}>
                        {language.t("settings.permissions.rule.empty.blacklist")}
                      </Match>
                    </Switch>
                  </div>
                }
              >
                <For each={state.rules}>
                  {(rule, index) => (
                    <div class="grid grid-cols-1 gap-3 py-3 border-b border-border-weak-base last:border-none md:grid-cols-[180px_minmax(0,1fr)_auto] md:items-center">
                      <Select
                        data-action="settings-permissions-rule-tool"
                        options={toolOptions()}
                        current={toolOptions().find((option) => option.value === rule.permission) ?? toolOptions()[0]}
                        value={(option) => option.value}
                        label={(option) => option.label}
                        onSelect={(option) => {
                          if (!option) return
                          setState("rules", index(), {
                            permission: option.value,
                            pattern: rulePattern(option.value, rule.pattern),
                          })
                        }}
                        variant="secondary"
                        size="small"
                        triggerVariant="settings"
                      />
                      <TextField
                        data-action="settings-permissions-rule-pattern"
                        label={language.t("settings.permissions.rule.pattern")}
                        hideLabel
                        value={rule.pattern}
                        placeholder={DEFAULT_RULE_PATTERN}
                        onChange={(value) => setState("rules", index(), "pattern", rulePattern(rule.permission, value))}
                        disabled={!canEditPattern(rule.permission)}
                        spellcheck={false}
                        autocorrect="off"
                        autocomplete="off"
                        autocapitalize="off"
                        class="text-12-regular"
                      />
                      <IconButton
                        icon="trash"
                        variant="ghost"
                        aria-label={language.t("settings.permissions.rule.remove")}
                        onClick={() => setState("rules", (items) => items.filter((_, i) => i !== index()))}
                      />
                    </div>
                  )}
                </For>
              </Show>
            </SettingsList>
          </div>
        </Show>

        <div class="flex justify-end gap-2">
          <Button
            size="large"
            variant="secondary"
            disabled={!dirty() || state.saving}
            onClick={() => {
              const current = normalizePermission(serverSync.data.config.permission)
              setState({ mode: current.mode, rules: current.rules, saving: false })
              setSynced(configPreview(current.mode, current.rules))
            }}
          >
            {language.t("common.reset")}
          </Button>
          <Button size="large" variant="primary" disabled={!dirty() || state.saving} onClick={() => void save()}>
            {state.saving ? language.t("common.saving") : language.t("common.save")}
          </Button>
        </div>
      </div>
    </div>
  )
}
