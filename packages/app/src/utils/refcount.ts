import { createRoot, getOwner, onCleanup } from "solid-js"

export function createRefCountMap<T>(
  create: (key: string) => T,
  remove?: (key: string) => void,
  identity: (key: string) => string = (key) => key,
) {
  const items = new Map<string, T>()
  const refCounts = new Map<string, number>()
  const roots = new Map<string, () => void>()

  return (key: string) => {
    const id = identity(key)
    const owner = getOwner()
    if (owner) {
      onCleanup(() => {
        refCounts.set(id, (refCounts.get(id) ?? 0) - 1)
        if (refCounts.get(id) === 0) {
          items.delete(id)
          refCounts.delete(id)
          const dispose = roots.get(id)
          roots.delete(id)
          // Defer the reactive teardown: disposing the item's root or running
          // the removal side effect (e.g. disableMcp toggling a signal)
          // synchronously here re-enters the graph while the cleanNode
          // cascade that fired this onCleanup is still running. The stale
          // root is always disposed; the id-keyed remove side effect is
          // skipped if the entry was re-created in the interim.
          queueMicrotask(() => {
            dispose?.()
            if (items.has(id)) return
            remove?.(id)
          })
        }
      })
    }

    const cached = items.get(id)
    if (cached) {
      if (owner) refCounts.set(id, (refCounts.get(id) ?? 0) + 1)
      return cached
    }
    // Create the item inside a dedicated root owned by this map, not by the
    // calling owner. Consumers share the cached item, so tying its
    // computations to the first caller either leaks them (no owner — e.g.
    // async callbacks) or disposes them while other consumers still hold the
    // item, leaving stale memos whose reads re-enter disposal cascades.
    const item = createRoot((dispose) => {
      roots.set(id, dispose)
      return create(key)
    })
    items.set(id, item)
    if (owner) refCounts.set(id, 1)
    return item
  }
}
