const OWNER_WARNING_MARKERS = [
  "cleanups created outside a `createRoot` or `render`",
  "computations created outside a `createRoot` or `render`",
]

export function installSolidOwnerWarningStack() {
  if (!import.meta.env.DEV) return
  const global = globalThis as typeof globalThis & { __opencodeSolidOwnerWarningStack?: boolean }
  if (global.__opencodeSolidOwnerWarningStack) return
  global.__opencodeSolidOwnerWarningStack = true
  const originalWarn = console.warn.bind(console)
  console.warn = (...args) => {
    originalWarn(...args)
    const message = String(args[0] ?? "")
    if (!OWNER_WARNING_MARKERS.some((marker) => message.includes(marker))) return
    originalWarn("[solid-owner-warning-stack]", new Error(message).stack)
  }
}
