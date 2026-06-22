import path from "path"
import { Schema } from "effect"
import { Global } from "../global"
import { Glob } from "../util/glob"

const FOLDERS = new Set([
  "node_modules",
  "bower_components",
  ".pnpm-store",
  "vendor",
  ".npm",
  "dist",
  "build",
  "out",
  ".next",
  "target",
  "bin",
  "obj",
  ".git",
  ".svn",
  ".hg",
  ".vscode",
  ".idea",
  ".turbo",
  ".output",
  "desktop",
  ".sst",
  ".cache",
  ".webkit-cache",
  "__pycache__",
  ".pytest_cache",
  "mypy_cache",
  ".history",
  ".gradle",
])

const FILES = [
  "**/*.swp",
  "**/*.swo",
  "**/*.pyc",
  "**/.DS_Store",
  "**/Thumbs.db",
  "**/logs/**",
  "**/tmp/**",
  "**/temp/**",
  "**/*.log",
  "**/coverage/**",
  "**/.nyc_output/**",
]

export const DEFAULT = [...FOLDERS, ...FILES].join("\n")
export const PATTERNS = parse(DEFAULT)

export const Info = Schema.Struct({
  path: Schema.String,
  source: Schema.Literals(["project", "user", "default"]),
  content: Schema.String,
}).annotate({ identifier: "IgnoreFile" })
export type Info = typeof Info.Type

export function defaultPath() {
  return path.join(Global.Path.home, ".opencode", ".ignore")
}

export function projectPath(worktree: string) {
  return path.join(worktree, ".opencode", ".ignore")
}

export function parse(content: string) {
  return content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"))
}

export function matches(pattern: string, file: string) {
  const normalized = file.replaceAll("\\", "/").replace(/\/$/, "")
  const clean = pattern.replaceAll("\\", "/").replace(/\/$/, "").replace(/^\//, "")
  if (clean.includes("*")) {
    if (Glob.match(clean, normalized)) return true
    if (clean.startsWith("**/")) {
      if (Glob.match(clean.slice(3), normalized)) return true
      if (Glob.match(clean.replace(/^\*\*\//, "**/"), normalized)) return true
    }
    if (!clean.includes("/")) return normalized.split("/").some((part) => Glob.match(clean, part))
    return false
  }
  if (!clean.includes("/")) return normalized.split("/").includes(clean)
  if (normalized === clean) return true
  return normalized.startsWith(clean + "/")
}

export function matchWithPatterns(file: string, patterns: string[]) {
  return patterns.reduce((ignored, pattern) => {
    const negated = pattern.startsWith("!")
    const clean = negated ? pattern.slice(1) : pattern
    if (!clean) return ignored
    if (!matches(clean, file)) return ignored
    return !negated
  }, false)
}

export function match(filepath: string, opts?: { extra?: string[]; whitelist?: string[] }) {
  const whitelist = opts?.whitelist ?? []
  if (whitelist.some((pattern) => matches(pattern, filepath))) return false
  return matchWithPatterns(filepath, [...PATTERNS, ...(opts?.extra ?? [])])
}

export * as Ignore from "./ignore"
