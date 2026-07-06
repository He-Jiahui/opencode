type SidebarNavigationEvent = Pick<
  MouseEvent,
  "altKey" | "button" | "ctrlKey" | "defaultPrevented" | "metaKey" | "preventDefault" | "shiftKey" | "stopPropagation"
>

export function shouldHandleSidebarNavigation(event: SidebarNavigationEvent) {
  if (event.defaultPrevented) return false
  if (event.button !== 0) return false
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return false
  return true
}

export function handleSidebarNavigation(
  event: SidebarNavigationEvent,
  navigate: (href: string) => void,
  href: string,
) {
  if (!shouldHandleSidebarNavigation(event)) return false
  event.preventDefault()
  event.stopPropagation()
  navigate(href)
  return true
}
