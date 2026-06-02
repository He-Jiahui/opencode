import { drizzle } from "drizzle-orm/node-sqlite/driver"
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs"
import * as http from "node:http"
import { join } from "node:path"
import * as tls from "node:tls"
import * as v8 from "node:v8"

type NodeHttpWithEnvProxy = typeof http & {
  setGlobalProxyFromEnv: () => void
}

type NodeTlsWithSystemCertificates = typeof tls & {
  getCACertificates: (type: "default" | "system") => string[]
  setDefaultCACertificates: (certificates: string[]) => void
}

type StartCommand = {
  type: "start"
  hostname: string
  port: number
  password: string
  userDataPath: string
  needsMigration: boolean
}

type StopCommand = { type: "stop" }
type SidecarCommand = StartCommand | StopCommand

type SidecarMessage =
  | { type: "sqlite"; progress: { type: "InProgress"; value: number } | { type: "Done" } }
  | { type: "ready" }
  | { type: "stopped" }
  | { type: "error"; error: { message: string; stack?: string } }

type ParentPort = {
  postMessage(message: SidecarMessage): void
  on(event: "message", listener: (event: { data: unknown }) => void): void
}

type Listener = {
  stop(close?: boolean): void | Promise<void>
}

const parentPort = getParentPort()
let listener: Listener | undefined
let diagnosticsStarted = false

parentPort.on("message", (event) => {
  const command = parseCommand(event.data)
  if (!command) return
  if (command.type === "stop") {
    void stop()
    return
  }
  void start(command)
})

async function start(command: StartCommand) {
  try {
    prepareSidecarEnv(command.password, command.userDataPath)
    startSidecarDiagnostics(command.userDataPath)
    ensureLoopbackNoProxy()
    useSystemCertificates()
    useEnvProxy()
    const { Database, JsonMigration, Log, Server } = await import("virtual:opencode-server")
    await Log.init({ level: "WARN" })

    if (command.needsMigration) {
      await JsonMigration.run(drizzle({ client: Database.Client().$client }), {
        progress: (event: { current: number; total: number }) => {
          parentPort.postMessage({
            type: "sqlite",
            progress: {
              type: "InProgress",
              value: event.total === 0 ? 100 : Math.round((event.current / event.total) * 100),
            },
          })
        },
      })
      parentPort.postMessage({ type: "sqlite", progress: { type: "Done" } })
    }

    listener = await Server.listen({
      port: command.port,
      hostname: command.hostname,
      username: "opencode",
      password: command.password,
      cors: ["oc://renderer"],
    })
    parentPort.postMessage({ type: "ready" })
  } catch (error) {
    parentPort.postMessage({ type: "error", error: serializeError(error) })
    setImmediate(() => process.exit(1))
  }
}

async function stop() {
  try {
    await listener?.stop()
  } finally {
    listener = undefined
    parentPort.postMessage({ type: "stopped" })
    setImmediate(() => process.exit(0))
  }
}

function prepareSidecarEnv(password: string, userDataPath: string) {
  Object.assign(process.env, {
    OPENCODE_SERVER_USERNAME: "opencode",
    OPENCODE_SERVER_PASSWORD: password,
    XDG_STATE_HOME: process.env.XDG_STATE_HOME ?? userDataPath,
  })
}

function ensureLoopbackNoProxy() {
  const loopback = ["127.0.0.1", "localhost", "::1"]
  const upsert = (key: string) => {
    const items = (process.env[key] ?? "")
      .split(",")
      .map((value: string) => value.trim())
      .filter((value: string) => Boolean(value))

    for (const host of loopback) {
      if (items.some((value: string) => value.toLowerCase() === host)) continue
      items.push(host)
    }

    process.env[key] = items.join(",")
  }

  upsert("NO_PROXY")
  upsert("no_proxy")
}

function useSystemCertificates() {
  try {
    const nodeTls = tls as NodeTlsWithSystemCertificates
    nodeTls.setDefaultCACertificates([
      ...new Set([...nodeTls.getCACertificates("default"), ...nodeTls.getCACertificates("system")]),
    ])
  } catch (error) {
    console.warn("failed to load system certificates", error)
  }
}

function useEnvProxy() {
  try {
    ;(http as NodeHttpWithEnvProxy).setGlobalProxyFromEnv()
  } catch (error) {
    console.warn("failed to load proxy environment", error)
  }
}

function parseCommand(value: unknown): SidecarCommand | undefined {
  if (!value || typeof value !== "object") return
  const command = value as Partial<StartCommand | StopCommand>
  if (command.type === "stop") return { type: "stop" }
  if (command.type !== "start") return
  if (typeof command.hostname !== "string") return
  if (typeof command.port !== "number") return
  if (typeof command.password !== "string") return
  if (typeof command.userDataPath !== "string") return
  if (typeof command.needsMigration !== "boolean") return
  return {
    type: "start",
    hostname: command.hostname,
    port: command.port,
    password: command.password,
    userDataPath: command.userDataPath,
    needsMigration: command.needsMigration,
  }
}

function serializeError(error: unknown) {
  if (error instanceof Error) return { message: error.message, stack: error.stack }
  return { message: String(error) }
}

function getParentPort() {
  const port = process.parentPort as ParentPort | undefined
  if (!port) throw new Error("Sidecar parent port unavailable")
  return port
}

function startSidecarDiagnostics(userDataPath: string) {
  if (diagnosticsStarted || process.env.OPENCODE_SIDECAR_DIAGNOSTICS === "0") return
  diagnosticsStarted = true
  const dir = process.env.OPENCODE_SIDECAR_DIAGNOSTIC_DIR ?? join(userDataPath, "opencode", "log", "diagnostics", stamp())
  mkdirSync(dir, { recursive: true })
  process.report.directory = dir
  process.report.reportOnFatalError = true
  process.report.reportOnUncaughtException = true
  writeDiagnosticEvent(dir, "started", {
    pid: process.pid,
    execArgv: process.execArgv,
    versions: process.versions,
    diagnosticDir: dir,
  })
  let lastUsed = 0
  const reported = new Set<string | number>()
  const interval = setInterval(() => {
    const sample = sidecarMemorySample()
    appendDiagnosticLine(dir, "memory.jsonl", sample)
    writeFileSync(join(dir, "latest-memory.json"), `${JSON.stringify(sample, null, 2)}\n`)
    const grew = sample.heap.used - lastUsed
    lastUsed = sample.heap.used
    if (grew > 128 * 1024 * 1024 || sample.heap.ratio >= 0.65) {
      console.warn("[sidecar-memory]", JSON.stringify(memorySummary(sample)))
    }
    const largeObjectUsed = largeObjectSpaceUsed(sample)
    if (sample.heap.used >= 1024 * 1024 * 1024 && !reported.has("heap-1gb")) {
      reported.add("heap-1gb")
      writeDiagnosticEvent(dir, "heap-absolute-threshold", {
        threshold: "heap-1gb",
        sample,
        report: writeProcessReport(dir, "heap-1gb"),
      })
    }
    if (largeObjectUsed >= 512 * 1024 * 1024 && !reported.has("large-object-512mb")) {
      reported.add("large-object-512mb")
      writeDiagnosticEvent(dir, "heap-absolute-threshold", {
        threshold: "large-object-512mb",
        sample,
        report: writeProcessReport(dir, "large-object-512mb"),
      })
    }
    const thresholds = [0.7, 0.82, 0.92]
    thresholds.forEach((threshold) => {
      if (sample.heap.ratio < threshold || reported.has(threshold)) return
      reported.add(threshold)
      writeDiagnosticEvent(dir, "heap-threshold", { threshold, sample, report: writeProcessReport(dir, threshold) })
    })
  }, Number(process.env.OPENCODE_SIDECAR_DIAGNOSTIC_INTERVAL_MS ?? 5_000))
  interval.unref()

  process.on("warning", (warning) => writeDiagnosticEvent(dir, "warning", serializeError(warning)))
  process.on("unhandledRejection", (reason) => writeDiagnosticEvent(dir, "unhandled-rejection", serializeError(reason)))
  process.on("uncaughtException", (error) => {
    writeDiagnosticEvent(dir, "uncaught-exception", serializeError(error))
    writeProcessReport(dir, "uncaught")
  })
  process.on("beforeExit", (code) => writeDiagnosticEvent(dir, "before-exit", { code, sample: sidecarMemorySample() }))
  process.on("exit", (code) => writeDiagnosticEvent(dir, "exit", { code, sample: sidecarMemorySample() }))
}

function sidecarMemorySample() {
  const memory = process.memoryUsage()
  const heap = v8.getHeapStatistics()
  return {
    at: new Date().toISOString(),
    uptime: Math.round(process.uptime()),
    pid: process.pid,
    memory,
    heap: {
      used: heap.used_heap_size,
      total: heap.total_heap_size,
      executable: heap.total_heap_size_executable,
      limit: heap.heap_size_limit,
      available: heap.total_available_size,
      malloced: heap.malloced_memory,
      external: heap.external_memory,
      ratio: heap.heap_size_limit > 0 ? heap.used_heap_size / heap.heap_size_limit : 0,
    },
    spaces: v8.getHeapSpaceStatistics().map((space) => ({
      name: space.space_name,
      size: space.space_size,
      used: space.space_used_size,
      available: space.space_available_size,
      physical: space.physical_space_size,
    })),
    active: {
      handles: activeCount("_getActiveHandles"),
      requests: activeCount("_getActiveRequests"),
    },
  }
}

function memorySummary(sample: ReturnType<typeof sidecarMemorySample>) {
  return {
    at: sample.at,
    uptime: sample.uptime,
    heapUsedMB: mb(sample.heap.used),
    heapTotalMB: mb(sample.heap.total),
    heapLimitMB: mb(sample.heap.limit),
    heapRatio: Number(sample.heap.ratio.toFixed(3)),
    rssMB: mb(sample.memory.rss),
    externalMB: mb(sample.memory.external),
    arrayBuffersMB: mb(sample.memory.arrayBuffers),
    largeObjectMB: mb(largeObjectSpaceUsed(sample)),
    active: sample.active,
  }
}

function largeObjectSpaceUsed(sample: ReturnType<typeof sidecarMemorySample>) {
  return sample.spaces
    .filter((space) => space.name.includes("large_object_space"))
    .reduce((sum, space) => sum + space.used, 0)
}

function writeProcessReport(dir: string, label: string | number) {
  const report = process.report
  if (!report?.writeReport) return
  return report.writeReport(join(dir, `report-${stamp()}-${String(label).replace(/[^a-z0-9_.-]/gi, "_")}.json`))
}

function writeDiagnosticEvent(dir: string, type: string, data: unknown) {
  appendDiagnosticLine(dir, "events.jsonl", { at: new Date().toISOString(), type, data })
}

function appendDiagnosticLine(dir: string, file: string, data: unknown) {
  appendFileSync(join(dir, file), `${JSON.stringify(data)}\n`)
}

function activeCount(name: "_getActiveHandles" | "_getActiveRequests") {
  const fn = (process as NodeJS.Process & Record<typeof name, (() => unknown[]) | undefined>)[name]
  return typeof fn === "function" ? fn().length : undefined
}

function mb(value: number) {
  return Math.round((value / 1024 / 1024) * 10) / 10
}

function stamp() {
  return new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d+Z$/, "")
}
