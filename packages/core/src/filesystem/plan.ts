import path from "path"
import { mkdir, stat, writeFile } from "fs/promises"

const invalidFilename = /[<>:"/\\|?*\u0000-\u001f]/g
const reservedWindowsName = /^(con|prn|aux|nul|com[1-9]|lpt[1-9])$/i

export function title(input: { title?: string; content: string }) {
  const explicit = input.title?.trim()
  if (explicit) return explicit
  const heading = input.content.match(/^\s{0,3}#{1,6}\s+(.+?)(?:\s+#+\s*)?$/m)?.[1]?.trim()
  if (heading) return heading
  return input.content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find((line) => !!line)
    ?.slice(0, 80)
    .trim() || "plan"
}

export function filename(input: { title?: string; content: string }) {
  const base = title(input)
    .replace(invalidFilename, " ")
    .replace(/\s+/g, " ")
    .replace(/[. ]+$/g, "")
    .trim()
  const clipped = [...(base || "plan")].slice(0, 80).join("").replace(/[. ]+$/g, "").trim() || "plan"
  return `${reservedWindowsName.test(clipped) ? `${clipped}-plan` : clipped}.md`
}

export async function save(input: { worktree: string; title?: string; content: string }) {
  const dir = path.join(input.worktree, ".opencode", "plans")
  await mkdir(dir, { recursive: true })
  const file = await target(dir, filename(input))
  await writeFile(file, input.content)
  return {
    title: title(input),
    path: path.relative(input.worktree, file),
  }
}

async function target(dir: string, name: string) {
  const parsed = path.parse(name)
  for (let i = 0; i < 1000; i++) {
    const file = path.join(dir, i === 0 ? name : `${parsed.name}-${i}${parsed.ext}`)
    if (!(await exists(file))) return file
  }
  return path.join(dir, `${parsed.name}-${Date.now()}${parsed.ext}`)
}

async function exists(file: string) {
  return stat(file).then(
    () => true,
    () => false,
  )
}
