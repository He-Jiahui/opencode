export * as Log from "./log"

type Extra = Record<string, unknown>

export type Logger = {
  debug(message?: unknown, extra?: Extra): void
  info(message?: unknown, extra?: Extra): void
  error(message?: unknown, extra?: Extra): void
  warn(message?: unknown, extra?: Extra): void
  tag(key: string, value: string): Logger
  clone(): Logger
  time(
    message: string,
    extra?: Extra,
  ): {
    stop(): void
    [Symbol.dispose](): void
  }
}

function write(level: string, tags: Extra, message?: unknown, extra?: Extra) {
  const fields = Object.entries({ ...tags, ...extra })
    .filter((entry) => entry[1] !== undefined && entry[1] !== null)
    .map((entry) => `${entry[0]}=${format(entry[1])}`)
    .join(" ")
  const text = [new Date().toISOString(), level, fields, message].filter(Boolean).join(" ")
  process.stderr.write(`${text}\n`)
}

function format(value: unknown) {
  if (value instanceof Error) return value.message
  if (typeof value === "object") return JSON.stringify(value)
  return String(value)
}

export function create(input: Extra = {}): Logger {
  const tags = { ...input }
  const logger: Logger = {
    debug(message?: unknown, extra?: Extra) {
      write("DEBUG", tags, message, extra)
    },
    info(message?: unknown, extra?: Extra) {
      write("INFO", tags, message, extra)
    },
    error(message?: unknown, extra?: Extra) {
      write("ERROR", tags, message, extra)
    },
    warn(message?: unknown, extra?: Extra) {
      write("WARN", tags, message, extra)
    },
    tag(key: string, value: string) {
      tags[key] = value
      return logger
    },
    clone() {
      return create(tags)
    },
    time(message: string, extra?: Extra) {
      const started = Date.now()
      logger.info(message, { status: "started", ...extra })
      const stop = () => logger.info(message, { status: "completed", duration: Date.now() - started, ...extra })
      return {
        stop,
        [Symbol.dispose]: stop,
      }
    },
  }
  return logger
}
