import type { Ref } from "solid-js"

export function isTabCloseTarget(target: EventTarget | null) {
  return target instanceof Element && !!target.closest('[data-slot="tab-close"]')
}

export function isTabInteractiveTarget(target: EventTarget | null) {
  return target instanceof Element && !!target.closest("[data-titlebar-tab-interactive]")
}

export function canStartTabDrag(pointerType: string) {
  return pointerType !== "touch"
}

export function stopTabControlPointer(event: Pick<Event, "stopPropagation">) {
  event.stopPropagation()
}

export function preventTabControlDefault(event: Pick<Event, "preventDefault" | "stopPropagation">) {
  event.preventDefault()
  event.stopPropagation()
}

export function forwardTabRef(ref: Ref<HTMLDivElement> | undefined, element: HTMLDivElement) {
  if (typeof ref === "function") ref(element)
}

export function canOpenTabRename(dragging: boolean | undefined, editing: boolean, committing: boolean) {
  return !dragging && !editing && !committing
}
