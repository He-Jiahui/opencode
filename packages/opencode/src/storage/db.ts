import { type SQLiteBunDatabase } from "drizzle-orm/bun-sqlite"
import { migrate } from "drizzle-orm/bun-sqlite/migrator"
import { type SQLiteTransaction } from "drizzle-orm/sqlite-core"
export * from "drizzle-orm"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { LocalContext } from "@/util/local-context"
import { Global } from "@opencode-ai/core/global"
import * as Log from "@opencode-ai/core/util/log"
import { NamedError } from "@opencode-ai/core/util/error"
import path from "path"
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "fs"
import { Flag } from "@opencode-ai/core/flag/flag"
import { InstallationChannel } from "@opencode-ai/core/installation/version"
import { EffectBridge } from "@/effect/bridge"
import { init } from "#db"
import { Effect, Schema } from "effect"

declare const OPENCODE_MIGRATIONS: { sql: string; timestamp: number; name: string }[] | undefined

export const NotFoundError = NamedError.create("NotFoundError", {
  message: Schema.String,
})

const log = Log.create({ service: "db" })
const backupInterval = 6 * 60 * 60 * 1000
const backupKeep = 12

type DatabaseFlags = Pick<RuntimeFlags.Info, "disableChannelDb" | "skipMigrations">

const readRuntimeFlags = () =>
  Effect.runSync(RuntimeFlags.Service.useSync((flags) => flags).pipe(Effect.provide(RuntimeFlags.defaultLayer)))

export function getChannelPath(flags: Pick<DatabaseFlags, "disableChannelDb"> = readRuntimeFlags()) {
  if (["latest", "beta", "prod"].includes(InstallationChannel) || flags.disableChannelDb)
    return path.join(Global.Path.data, "opencode.db")
  const safe = InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")
  return path.join(Global.Path.data, `opencode-${safe}.db`)
}

export const getPath = (flags?: Pick<DatabaseFlags, "disableChannelDb">) => {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || path.isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return path.join(Global.Path.data, Flag.OPENCODE_DB)
  }
  return getChannelPath(flags)
}

export type Transaction = SQLiteTransaction<"sync", void>

type Client = ReturnType<typeof init>

type Journal = { sql: string; timestamp: number; name: string }[]

// Drizzle's migrate overloads trigger expensive variance checks here; narrow to the journal overload we actually use.
const migrateFromJournal = migrate as unknown as (db: SQLiteBunDatabase, entries: Journal) => void

function applyMigrations(db: SQLiteBunDatabase, entries: Journal) {
  migrateFromJournal(db, entries)
}

function time(tag: string) {
  const match = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/.exec(tag)
  if (!match) return 0
  return Date.UTC(
    Number(match[1]),
    Number(match[2]) - 1,
    Number(match[3]),
    Number(match[4]),
    Number(match[5]),
    Number(match[6]),
  )
}

function migrations(dir: string): Journal {
  const dirs = readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)

  const sql = dirs
    .map((name) => {
      const file = path.join(dir, name, "migration.sql")
      if (!existsSync(file)) return
      return {
        sql: readFileSync(file, "utf-8"),
        timestamp: time(name),
        name,
      }
    })
    .filter(Boolean) as Journal

  return sql.sort((a, b) => a.timestamp - b.timestamp)
}

function databaseBackupsEnabled() {
  const value = process.env.OPENCODE_DB_BACKUPS?.toLowerCase()
  return value !== "0" && value !== "false"
}

function backupRoot(dbPath: string) {
  return path.join(path.dirname(dbPath), "backups", path.basename(dbPath).replace(/[^a-zA-Z0-9._-]/g, "-"))
}

function backupStamp() {
  return new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d+Z$/, "")
}

function sqlString(value: string) {
  return `'${value.replaceAll("'", "''")}'`
}

function latestBackupAt(dir: string) {
  if (!existsSync(dir)) return 0
  return readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith(".db"))
    .map((entry) => statSync(path.join(dir, entry.name)).mtimeMs)
    .reduce((latest, time) => Math.max(latest, time), 0)
}

function pruneBackups(dir: string) {
  if (!existsSync(dir)) return
  readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) => ({ name: entry.name, path: path.join(dir, entry.name), time: statSync(path.join(dir, entry.name)).mtimeMs }))
    .filter((entry) => entry.name.endsWith(".db") || entry.name.endsWith(".json") || entry.name.endsWith(".db-wal") || entry.name.endsWith(".db-shm"))
    .toSorted((a, b) => b.time - a.time)
    .slice(backupKeep * 4)
    .forEach((entry) => rmSync(entry.path, { force: true }))
}

function rawBackup(dbPath: string, dir: string, stamp: string, reason: string, error: unknown) {
  const prefix = path.join(dir, `${stamp}-${reason}`)
  ;[dbPath, `${dbPath}-wal`, `${dbPath}-shm`]
    .filter((file) => existsSync(file))
    .forEach((file) => copyFileSync(file, `${prefix}${file === dbPath ? ".db" : path.extname(file)}`))
  writeFileSync(
    `${prefix}.json`,
    `${JSON.stringify(
      {
        created: new Date().toISOString(),
        reason,
        source: dbPath,
        mode: "raw-copy",
        error: error instanceof Error ? { message: error.message, stack: error.stack } : { message: String(error) },
      },
      null,
      2,
    )}\n`,
  )
}

function ensureBackup(db: Client, dbPath: string, reason: string, existing: boolean) {
  if (!databaseBackupsEnabled()) return
  if (!existing || dbPath === ":memory:") return
  const dir = backupRoot(dbPath)
  mkdirSync(dir, { recursive: true })
  if (Date.now() - latestBackupAt(dir) < backupInterval) return
  const stamp = backupStamp()
  const safeReason = reason.replace(/[^a-zA-Z0-9._-]/g, "-")
  const target = path.join(dir, `${stamp}-${safeReason}.db`)
  try {
    db.run(`VACUUM INTO ${sqlString(target)}`)
    writeFileSync(
      path.join(dir, `${stamp}-${safeReason}.json`),
      `${JSON.stringify({ created: new Date().toISOString(), reason, source: dbPath, mode: "vacuum-into" }, null, 2)}\n`,
    )
    pruneBackups(dir)
    log.info("created database backup", { path: target, reason })
  } catch (error) {
    rawBackup(dbPath, dir, stamp, safeReason, error)
    pruneBackups(dir)
    log.warn("created raw database backup after sqlite backup failed", {
      path: dir,
      reason,
      error: error instanceof Error ? error.message : String(error),
    })
  }
}

let client: Client | undefined
let loaded = false

export const Client = Object.assign(
  (flags: DatabaseFlags = readRuntimeFlags()): Client => {
    if (loaded) return client as Client

    const dbPath = getPath(flags)
    log.info("opening database", { path: dbPath })
    const existing = dbPath !== ":memory:" && existsSync(dbPath)

    const db = init(dbPath)

    db.run("PRAGMA journal_mode = WAL")
    db.run("PRAGMA synchronous = NORMAL")
    db.run("PRAGMA busy_timeout = 5000")
    db.run("PRAGMA cache_size = -64000")
    db.run("PRAGMA foreign_keys = ON")
    db.run("PRAGMA wal_checkpoint(PASSIVE)")
    ensureBackup(db, dbPath, "startup", existing)

    // Apply schema migrations
    const entries =
      typeof OPENCODE_MIGRATIONS !== "undefined"
        ? OPENCODE_MIGRATIONS
        : migrations(path.join(import.meta.dirname, "../../migration"))
    if (entries.length > 0) {
      log.info("applying migrations", {
        count: entries.length,
        mode: typeof OPENCODE_MIGRATIONS !== "undefined" ? "bundled" : "dev",
      })
      if (flags.skipMigrations) {
        for (const item of entries) {
          item.sql = "select 1;"
        }
      } else {
        ensureBackup(db, dbPath, "pre-migration", existing)
      }
      applyMigrations(db, entries)
    }

    client = db
    loaded = true
    return db
  },
  {
    reset: () => {
      loaded = false
      client = undefined
    },
    loaded: () => loaded,
  },
)

export function close() {
  if (!Client.loaded()) return
  Client().$client.close()
  Client.reset()
}

export type TxOrDb = Transaction | Client

const ctx = LocalContext.create<{
  tx: TxOrDb
  effects: (() => void | Promise<void>)[]
}>("database")

export function use<T>(callback: (trx: TxOrDb) => T): T {
  try {
    return callback(ctx.use().tx)
  } catch (err) {
    if (err instanceof LocalContext.NotFound) {
      const effects: (() => void | Promise<void>)[] = []
      const result = ctx.provide({ effects, tx: Client() }, () => callback(Client()))
      for (const effect of effects) effect()
      return result
    }
    throw err
  }
}

export function effect(fn: () => any | Promise<any>) {
  const bound = EffectBridge.bind(fn)
  try {
    ctx.use().effects.push(bound)
  } catch {
    bound()
  }
}

type NotPromise<T> = T extends Promise<any> ? never : T

export function transaction<T>(
  callback: (tx: TxOrDb) => NotPromise<T>,
  options?: {
    behavior?: "deferred" | "immediate" | "exclusive"
  },
): NotPromise<T> {
  try {
    return callback(ctx.use().tx)
  } catch (err) {
    if (err instanceof LocalContext.NotFound) {
      const effects: (() => void | Promise<void>)[] = []
      const txCallback = EffectBridge.bind((tx: TxOrDb) => ctx.provide({ tx, effects }, () => callback(tx)))
      const result = Client().transaction(txCallback, { behavior: options?.behavior })
      for (const effect of effects) effect()
      return result as NotPromise<T>
    }
    throw err
  }
}

export * as Database from "./db"
