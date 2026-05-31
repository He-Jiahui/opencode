export type OpencodePlanBlock = {
  id: string
  title: string
  content: string
}

export type OpencodePlanSegment =
  | {
      type: "text"
      text: string
    }
  | ({
      type: "plan"
    } & OpencodePlanBlock)

const planBlock = /<opencode_plan(?:\s+[^>]*)?>([\s\S]*?)<\/opencode_plan>/gi
const planStartTag = /^<opencode_plan(?:\s+[^>]*)?>/i
const planBlockStart = /<opencode_plan\b/i
const titleAttribute = /\stitle\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i

export function parseOpencodePlanBlocks(text: string) {
  return parseOpencodePlanSegments(text).filter((segment): segment is OpencodePlanSegment & { type: "plan" } => {
    return segment.type === "plan"
  })
}

export function parseOpencodePlanSegments(text: string): OpencodePlanSegment[] {
  if (!planBlockStart.test(text)) return [{ type: "text", text }]
  const segments: OpencodePlanSegment[] = []
  let index = 0
  let count = 0

  for (const match of text.matchAll(planBlock)) {
    const start = match.index ?? 0
    const before = text.slice(index, start)
    if (before) segments.push({ type: "text", text: before })

    const content = (match[1] ?? "").trim()
    if (content) {
      segments.push({
        type: "plan",
        id: String(count++),
        title: extractTitle(content, match[0].match(planStartTag)?.[0] ?? ""),
        content,
      })
    }

    index = start + match[0].length
  }

  const after = text.slice(index)
  if (after) segments.push({ type: "text", text: after })
  return segments.length > 0 ? segments : [{ type: "text", text }]
}

function extractTitle(content: string, tag: string) {
  const attr = tag.match(titleAttribute)
  const explicit = (attr?.[1] ?? attr?.[2] ?? attr?.[3])?.trim()
  if (explicit) return cleanTitle(explicit)
  const heading = content.match(/^\s{0,3}#{1,6}\s+(.+?)(?:\s+#+\s*)?$/m)?.[1]?.trim()
  if (heading) return cleanTitle(heading)
  return cleanTitle(
    content
      .split(/\r?\n/)
      .map((line) => line.trim())
      .find((line) => !!line) ?? "Plan",
  )
}

function cleanTitle(value: string) {
  return value
    .replace(/[*_`~#[\]()]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 120)
}
