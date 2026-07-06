import { describe, expect, test } from "bun:test"
import { createRoot, onCleanup } from "solid-js"
import { createRefCountMap } from "./refcount"
import { pathKey } from "./path-key"

const flushMicrotasks = () => new Promise<void>((resolve) => queueMicrotask(resolve))

describe("createRefCountMap", () => {
  test("removes an item after its last owner is disposed", async () => {
    const removed: string[] = []
    const map = createRefCountMap(
      (key) => key,
      (key) => removed.push(key),
    )
    const first = createRoot((dispose) => {
      map("/project")
      return dispose
    })
    const second = createRoot((dispose) => {
      map("/project")
      return dispose
    })

    first()
    await flushMicrotasks()
    expect(removed).toEqual([])
    second()
    await flushMicrotasks()
    expect(removed).toEqual(["/project"])
  })

  test("keeps equivalent path consumers until the last owner is disposed", async () => {
    const removed: string[] = []
    const map = createRefCountMap(
      (key) => key,
      (key) => removed.push(key),
      pathKey,
    )
    const first = createRoot((dispose) => {
      map("C:\\repo")
      return dispose
    })
    const second = createRoot((dispose) => {
      map("C:/repo/")
      return dispose
    })

    first()
    await flushMicrotasks()
    expect(removed).toEqual([])
    second()
    await flushMicrotasks()
    expect(removed).toEqual(["C:/repo"])
  })

  test("returns cached items outside a reactive owner without registering cleanup", () => {
    const removed: string[] = []
    let created = 0
    const map = createRefCountMap(
      (key) => {
        created++
        return key
      },
      (key) => removed.push(key),
    )

    expect(map("/project")).toBe("/project")
    expect(map("/project")).toBe("/project")
    expect(created).toBe(1)
    expect(removed).toEqual([])
  })

  test("skips deferred removal when the entry is re-created before the microtask runs", async () => {
    const removed: string[] = []
    let created = 0
    const map = createRefCountMap(
      (key) => {
        created++
        return { key }
      },
      (key) => removed.push(key),
    )
    const first = createRoot((dispose) => {
      map("/project")
      return dispose
    })

    first()
    const recreated = map("/project")
    await flushMicrotasks()
    expect(removed).toEqual([])
    expect(created).toBe(2)
    expect(map("/project")).toBe(recreated)
  })

  test("owns the item's reactive scope: it survives the first consumer and disposes with the last", async () => {
    const disposed: string[] = []
    const map = createRefCountMap((key) => {
      onCleanup(() => disposed.push(key))
      return key
    })
    const first = createRoot((dispose) => {
      map("/project")
      return dispose
    })
    const second = createRoot((dispose) => {
      map("/project")
      return dispose
    })

    // Disposing the creating consumer must NOT tear down the shared item.
    first()
    await flushMicrotasks()
    expect(disposed).toEqual([])

    second()
    await flushMicrotasks()
    expect(disposed).toEqual(["/project"])
  })

  test("disposes the stale root even when the entry is re-created before the microtask runs", async () => {
    const disposed: string[] = []
    let created = 0
    const map = createRefCountMap((key) => {
      const instance = ++created
      onCleanup(() => disposed.push(`${key}:${instance}`))
      return { key, instance }
    })
    const first = createRoot((dispose) => {
      map("/project")
      return dispose
    })

    first()
    const recreated = map("/project")
    await flushMicrotasks()
    // The first instance's root is torn down; the re-created one stays live.
    expect(disposed).toEqual(["/project:1"])
    expect(recreated.instance).toBe(2)
    expect(map("/project")).toBe(recreated)
  })

  test("items created without an owner do not leak computations into the caller", async () => {
    const disposed: string[] = []
    // No reactive owner here — previously this leaked the item's scope.
    const map = createRefCountMap((key) => {
      onCleanup(() => disposed.push(key))
      return key
    })
    map("/project")

    // A later owned consumer attaches and detaches; the item is removed and
    // its root disposed rather than leaking forever.
    const consumer = createRoot((dispose) => {
      map("/project")
      return dispose
    })
    consumer()
    await flushMicrotasks()
    expect(disposed).toEqual(["/project"])
  })
})
