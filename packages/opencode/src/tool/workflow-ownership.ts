import { readFile } from "fs/promises"
import path from "path"

type WorkflowManifest = {
  workflowID?: string
  ownership?: Record<string, string>
}

export async function assertWorkflowArtifactWritableByAgent(input: {
  instanceDirectory: string
  filePath: string | undefined
}) {
  if (!input.filePath) return
  const target = path.resolve(input.filePath)
  const workflowsRoot = path.resolve(input.instanceDirectory, ".opencode", "workflows")
  const relativeToWorkflows = path.relative(workflowsRoot, target)
  if (
    relativeToWorkflows === "" ||
    relativeToWorkflows.startsWith("..") ||
    path.isAbsolute(relativeToWorkflows)
  ) {
    return
  }
  const [workflowDirectory, ...segments] = relativeToWorkflows.split(path.sep)
  if (!workflowDirectory || segments.length === 0) return
  const relative = segments.join("/")
  const manifest = await readWorkflowOwnershipManifest(path.join(workflowsRoot, workflowDirectory, "manifest.json"))
  if (!manifest) return
  const owner = workflowManifestOwner(manifest, relative)
  if (!owner || owner === "agent") return
  throw new Error(
    [
      `Workflow artifact is owned by ${owner}: ${relative}`,
      "This file is generated or journaled by the workflow engine.",
      "Use the workflow tool for status changes, XML updates, resume/cancel, or milestone completion instead of editing it directly.",
    ].join(" "),
  )
}

async function readWorkflowOwnershipManifest(file: string) {
  try {
    return JSON.parse(await readFile(file, "utf8")) as WorkflowManifest
  } catch (error) {
    if (nodeErrorCode(error) === "ENOENT") return undefined
    throw new Error(
      `Workflow manifest is not readable at ${file}; run workflow doctor before editing workflow artifacts.`,
    )
  }
}

function workflowManifestOwner(manifest: WorkflowManifest, relativePath: string) {
  const ownership = manifest.ownership ?? {}
  return Object.entries(ownership)
    .filter(([pattern]) => workflowOwnershipPatternMatches(pattern, relativePath))
    .toSorted((a, b) => workflowOwnershipPatternScore(b[0]) - workflowOwnershipPatternScore(a[0]))[0]?.[1]
}

function workflowOwnershipPatternMatches(pattern: string, relativePath: string) {
  const normalized = pattern.replaceAll("\\", "/")
  if (normalized === relativePath) return true
  if (!normalized.endsWith("/**")) return false
  const prefix = normalized.slice(0, -3)
  return relativePath === prefix.slice(0, -1) || relativePath.startsWith(prefix)
}

function workflowOwnershipPatternScore(pattern: string) {
  return pattern.replaceAll("*", "").length
}

function nodeErrorCode(error: unknown) {
  return typeof error === "object" && error !== null && "code" in error ? String(error.code) : undefined
}
