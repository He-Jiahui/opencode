import { AppIcon } from "@opencode-ai/ui/app-icon"
import { ContextMenu } from "@opencode-ai/ui/context-menu"
import { Icon } from "@opencode-ai/ui/icon"
import { showToast } from "@opencode-ai/ui/toast"
import { getDirectory } from "@opencode-ai/core/util/path"
import { useLanguage } from "@/context/language"
import { usePlatform } from "@/context/platform"
import { useServer } from "@/context/server"
import { useSDK } from "@/context/sdk"
import type { JSX, ParentProps } from "solid-js"

export type FilePathContextMenuTarget = {
  path: string
  type?: "file" | "directory"
}

function showRequestError(language: ReturnType<typeof useLanguage>, err: unknown) {
  showToast({
    variant: "error",
    title: language.t("common.requestFailed"),
    description: err instanceof Error ? err.message : String(err),
  })
}

export function FilePathContextMenu(
  props: ParentProps<{
    target: FilePathContextMenuTarget | undefined
    class?: string
    triggerClass?: string
    contentClass?: string
    children: JSX.Element
  }>,
) {
  const language = useLanguage()
  const platform = usePlatform()
  const server = useServer()
  const sdk = useSDK()
  const canOpen = () => platform.platform === "desktop" && !!platform.openPath && server.isLocal()
  const vscodeApp = () => (platform.os === "macos" ? "Visual Studio Code" : "code")
  const targetPath = () => {
    const value = props.target?.path ?? ""
    if (!value || /^[A-Za-z]:[\\/]/.test(value) || value.startsWith("/") || value.startsWith("\\\\")) return value
    return `${sdk.directory.replace(/[\\/]+$/, "")}/${value.replace(/^[\\/]+/, "")}`
  }
  const targetDirectory = () => {
    const target = props.target
    if (!target) return ""
    return target.type === "directory" ? target.path : getDirectory(target.path)
  }
  const openPath = (value: string, app?: string) => {
    if (!value || !platform.openPath || !canOpen()) return
    platform.openPath(value, app).catch((err: unknown) => showRequestError(language, err))
  }
  const copyPath = () => {
    const value = targetPath()
    if (!value) return
    navigator.clipboard
      .writeText(value)
      .then(() =>
        showToast({
          variant: "success",
          title: language.t("session.share.copy.copied"),
        }),
      )
      .catch((err: unknown) => showRequestError(language, err))
  }

  return (
    <ContextMenu modal={false}>
      <ContextMenu.Trigger as="div" class={`${props.triggerClass ?? "contents"} ${props.class ?? ""}`}>
        {props.children}
      </ContextMenu.Trigger>
      <ContextMenu.Portal>
        <ContextMenu.Content class={props.contentClass}>
          <ContextMenu.Item disabled={!props.target || !canOpen()} onSelect={() => openPath(targetPath())}>
            <Icon name="open-file" class="size-4 mr-2" />
            <ContextMenu.ItemLabel>{language.t("file.context.openDefault")}</ContextMenu.ItemLabel>
          </ContextMenu.Item>
          <ContextMenu.Item disabled={!props.target || !canOpen()} onSelect={() => openPath(targetPath(), vscodeApp())}>
            <AppIcon id="vscode" class="size-4 mr-2" />
            <ContextMenu.ItemLabel>{language.t("session.header.open.app.vscode")}</ContextMenu.ItemLabel>
          </ContextMenu.Item>
          <ContextMenu.Item disabled={!props.target || !canOpen()} onSelect={() => openPath(targetDirectory())}>
            <Icon name="folder" class="size-4 mr-2" />
            <ContextMenu.ItemLabel>{language.t("file.context.openFolder")}</ContextMenu.ItemLabel>
          </ContextMenu.Item>
          <ContextMenu.Separator />
          <ContextMenu.Item disabled={!props.target} onSelect={copyPath}>
            <Icon name="copy" class="size-4 mr-2" />
            <ContextMenu.ItemLabel>{language.t("session.header.open.copyPath")}</ContextMenu.ItemLabel>
          </ContextMenu.Item>
        </ContextMenu.Content>
      </ContextMenu.Portal>
    </ContextMenu>
  )
}
