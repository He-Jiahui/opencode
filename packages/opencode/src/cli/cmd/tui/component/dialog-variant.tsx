import { createMemo } from "solid-js"
import { useLocal } from "@tui/context/local"
import { DEFAULT_MODEL_VARIANT } from "@tui/context/model-variant"
import { DialogSelect } from "@tui/ui/dialog-select"
import { useDialog } from "@tui/ui/dialog"

export function DialogVariant() {
  const local = useLocal()
  const dialog = useDialog()
  const configured = createMemo(() => local.model.variant.configured())

  const options = createMemo(() => {
    return [
      {
        value: DEFAULT_MODEL_VARIANT,
        title: "Default",
        description: configured() ? `config: ${configured()}` : undefined,
        onSelect: () => {
          dialog.clear()
          local.model.variant.set(undefined)
        },
      },
      ...local.model.variant.list().map((variant) => ({
        value: variant,
        title: variant,
        onSelect: () => {
          dialog.clear()
          local.model.variant.set(variant)
        },
      })),
    ]
  })

  return (
    <DialogSelect<string>
      options={options()}
      title={"Select variant"}
      current={local.model.variant.current() ?? DEFAULT_MODEL_VARIANT}
      flat={true}
    />
  )
}
