import { describe, expect, test } from "bun:test"
import { handleSidebarNavigation, shouldHandleSidebarNavigation } from "./sidebar-navigation"

const event = (input: Partial<MouseEvent> = {}) => {
  let prevented = false
  let stopped = false
  return {
    altKey: false,
    button: 0,
    ctrlKey: false,
    defaultPrevented: false,
    metaKey: false,
    shiftKey: false,
    preventDefault: () => (prevented = true),
    stopPropagation: () => (stopped = true),
    prevented: () => prevented,
    stopped: () => stopped,
    ...input,
  }
}

describe("sidebar navigation", () => {
  test("handles ordinary left clicks through client routing", () => {
    const e = event()
    expect(shouldHandleSidebarNavigation(e)).toBe(true)

    const calls: string[] = []
    expect(handleSidebarNavigation(e, (href) => calls.push(href), "/target")).toBe(true)
    expect(e.prevented()).toBe(true)
    expect(e.stopped()).toBe(true)
    expect(calls).toEqual(["/target"])
  })

  test("preserves modified clicks for native browser behavior", () => {
    expect(shouldHandleSidebarNavigation(event({ ctrlKey: true }))).toBe(false)
    expect(shouldHandleSidebarNavigation(event({ metaKey: true }))).toBe(false)
    expect(shouldHandleSidebarNavigation(event({ shiftKey: true }))).toBe(false)
    expect(shouldHandleSidebarNavigation(event({ altKey: true }))).toBe(false)
    expect(shouldHandleSidebarNavigation(event({ button: 1 }))).toBe(false)
    expect(shouldHandleSidebarNavigation(event({ defaultPrevented: true }))).toBe(false)
  })
})
