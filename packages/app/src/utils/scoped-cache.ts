type ScopedCacheOptions<T> = {
  maxEntries?: number
  ttlMs?: number
  dispose?: (value: T, key: string) => void
  now?: () => number
}

type Entry<T> = {
  value: T
  touchedAt: number
}

export function createScopedCache<T>(createValue: (key: string) => T, options: ScopedCacheOptions<T> = {}) {
  const store = new Map<string, Entry<T>>()
  const now = options.now ?? Date.now

  const dispose = (key: string, entry: Entry<T>) => {
    options.dispose?.(entry.value, key)
  }

  const expired = (entry: Entry<T>) => {
    if (options.ttlMs === undefined) return false
    return now() - entry.touchedAt >= options.ttlMs
  }

  const sweep = () => {
    if (options.ttlMs === undefined) return
    for (const [key, entry] of store) {
      if (!expired(entry)) continue
      store.delete(key)
      dispose(key, entry)
    }
  }

  const touch = (key: string, entry: Entry<T>) => {
    entry.touchedAt = now()
    store.delete(key)
    store.set(key, entry)
  }

  const prune = () => {
    if (options.maxEntries === undefined) return
    while (store.size > options.maxEntries) {
      const key = store.keys().next().value
      if (!key) return
      const entry = store.get(key)
      store.delete(key)
      if (!entry) continue
      dispose(key, entry)
    }
  }

  const remove = (key: string) => {
    const entry = store.get(key)
    if (!entry) return
    store.delete(key)
    dispose(key, entry)
    return entry.value
  }

  const peek = (key: string) => {
    sweep()
    const entry = store.get(key)
    if (!entry) return
    if (!expired(entry)) return entry.value
    store.delete(key)
    dispose(key, entry)
  }

  const get = (key: string) => {
    sweep()
    const entry = store.get(key)
    if (entry && !expired(entry)) {
      touch(key, entry)
      return entry.value
    }
    if (entry) {
      store.delete(key)
      dispose(key, entry)
    }

    const created = {
      value: createValue(key),
      touchedAt: now(),
    }
    store.set(key, created)
    prune()
    return created.value
  }

  const clear = () => {
    // Defer dispose() calls to a microtask. When clear() runs inside an
    // onCleanup during a parent remount, synchronous dispose on cached
    // createRoot entries starts a nested cleanNode cascade while the outer
    // cascade is mid-traversal, corrupting solid-js's graph walk state and
    // throwing `Cannot read properties of null` in cleanNode's owned walk.
    // Deferring lets the outer cleanup finish first.
    const pending: Array<[string, Entry<T>]> = []
    for (const entry of store) pending.push(entry)
    store.clear()
    if (pending.length && options.dispose) {
      queueMicrotask(() => {
        for (const [key, entry] of pending) dispose(key, entry)
      })
    }
  }

  return {
    get,
    peek,
    delete: remove,
    clear,
  }
}
