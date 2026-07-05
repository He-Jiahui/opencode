import { Parser } from "htmlparser2"

import {
  WorkflowMilestoneID,
  type WorkflowDefinition,
  type WorkflowMilestone,
  type WorkflowMilestoneReview,
  type WorkflowStep,
} from "./schema"

export type RawWorkflowElement = {
  name: string
  attributes: Record<string, string>
  children: RawWorkflowElement[]
  text: string[]
}

export type WorkflowParseOptions = {
  readPipelineItems?: (itemsPath: string) => unknown
}

type PipelineItem = {
  index: number
  slug: string
  label: string
  value: unknown
}

export type WorkflowPipelineItemsMissing = {
  type: "missing-pipeline-items"
  itemsPath: string
}

export function workflowPipelineItemsMissing(itemsPath: string): WorkflowPipelineItemsMissing {
  return { type: "missing-pipeline-items", itemsPath }
}

const containerNames = new Set(["workflow", "ordered", "parallel", "pipeline"])
const elementNames = new Set([...containerNames, "milestone"])

export class WorkflowParseError extends Error {
  override name = "WorkflowParseError"
}

export function parseWorkflowXml(input: string, options: WorkflowParseOptions = {}): WorkflowDefinition {
  return validateWorkflowElement(parseXml(input), options)
}

export function validateWorkflowElement(root: RawWorkflowElement, options: WorkflowParseOptions = {}): WorkflowDefinition {
  requireElement(root.name === "workflow", "workflow XML must have a <workflow> root")
  requireElement(root.children.length === 1, "<workflow> must contain exactly one step")
  requireElement(root.text.join("").trim().length === 0, "<workflow> cannot contain text outside steps")

  const steps = addStructuralDependencies(expandPipelineItems(readStep(root.children[0]), options)).step
  const milestones = flattenMilestones(steps)
  validateMilestones(milestones)

  return { steps, milestones }
}

function parseXml(input: string) {
  const document: RawWorkflowElement = {
    name: "#document",
    attributes: {},
    children: [],
    text: [],
  }
  const stack = [document]
  const literalCloseCounts = countLiteralCloseTags(input)
  const errors: string[] = []

  let parser: Parser
  parser = new Parser(
    {
      onopentag: (name, attributes) => {
        const current = stack.at(-1)
        if (!elementNames.has(name) && current?.name === "milestone") {
          current.text.push(serializeOpenTag(name, attributes))
          return
        }
        if (!elementNames.has(name)) {
          errors.push(withXmlLocation(input, parser.startIndex, `unsupported workflow element <${name}>`))
        }
        current?.children.push({
          name,
          attributes,
          children: [],
          text: [],
        })
        stack.push(current!.children.at(-1)!)
      },
      ontext: (text) => {
        stack.at(-1)?.text.push(text)
      },
      onclosetag: (name) => {
        if (stack.at(-1)?.name === "milestone" && !elementNames.has(name)) {
          const count = literalCloseCounts.get(name) ?? 0
          if (count > 0) {
            stack.at(-1)?.text.push(`</${name}>`)
            literalCloseCounts.set(name, count - 1)
          }
          return
        }
        if (stack.at(-1)?.name !== name) {
          errors.push(withXmlLocation(input, parser.startIndex, `workflow XML has mismatched closing tag </${name}>`))
          return
        }
        stack.pop()
      },
      onerror: (error) => {
        errors.push(error.message)
      },
    },
    { xmlMode: true, recognizeSelfClosing: true },
  )
  parser.end(input)

  requireElement(errors.length === 0, errors[0] ?? "invalid workflow XML")
  requireElement(stack.length === 1, "workflow XML has unclosed elements")
  requireElement(document.children.length === 1, "workflow XML must contain exactly one root element")
  requireElement(document.text.join("").trim().length === 0, "workflow XML cannot contain text outside <workflow>")
  return document.children[0]
}

function withXmlLocation(input: string, index: number | undefined, message: string) {
  if (index === undefined || index < 0) return message
  const before = input.slice(0, index)
  const line = before.split(/\r\n|\n|\r/).length
  const lastLineBreak = Math.max(before.lastIndexOf("\n"), before.lastIndexOf("\r"))
  return `${message} at line ${line}, column ${before.length - lastLineBreak}`
}

function countLiteralCloseTags(input: string) {
  const counts = new Map<string, number>()
  for (const match of input.matchAll(/<\/\s*([A-Za-z][\w:.-]*)\s*>/g)) {
    const name = match[1]
    if (!name || elementNames.has(name)) continue
    counts.set(name, (counts.get(name) ?? 0) + 1)
  }
  return counts
}

function serializeOpenTag(name: string, attributes: Record<string, string>) {
  const attrs = Object.entries(attributes)
    .map(([key, value]) => (value === "" ? key : `${key}="${escapeAttribute(value)}"`))
    .join(" ")
  return attrs ? `<${name} ${attrs}>` : `<${name}>`
}

function escapeAttribute(value: string) {
  return value.replace(/&/g, "&amp;").replace(/"/g, "&quot;")
}

function readStep(element: RawWorkflowElement): WorkflowStep {
  if (element.name === "milestone") {
    requireAttributes(element, ["id", "title", "department", "agent", "depends", "review"])
    requireElement(
      element.children.length === 0,
      `<milestone id="${element.attributes.id ?? ""}"> cannot contain child steps`,
    )
    requireElement(Boolean(element.attributes.id?.trim()), "<milestone> requires an id attribute")
    requireElement(Boolean(element.text.join("").trim()), `<milestone id="${element.attributes.id ?? ""}"> requires content`)
    const review = readReview(element.attributes.review)
    return {
      type: "milestone",
      id: WorkflowMilestoneID.make(element.attributes.id!.trim()),
      title: cleanAttribute(element.attributes.title),
      department: cleanAttribute(element.attributes.department ?? element.attributes.agent),
      ...(review ? { review } : {}),
      prompt: element.text.join("").trim(),
      dependsOn: readDepends(element.attributes.depends),
    }
  }

  requireElement(
    element.name === "ordered" || element.name === "parallel" || element.name === "pipeline",
    `<${element.name}> is not a workflow step`,
  )
  requireAttributes(element, element.name === "pipeline" ? ["items"] : [])
  requireElement(element.children.length > 0, `<${element.name}> must contain at least one step`)
  requireElement(element.text.join("").trim().length === 0, `<${element.name}> cannot contain text outside steps`)
  return {
    type: element.name,
    ...(cleanAttribute(element.attributes.items) ? { items: cleanAttribute(element.attributes.items) } : {}),
    children: element.children.map(readStep),
  }
}

function readDepends(input: string | undefined) {
  if (!input) return []
  return Array.from(
    new Set(
      input
        .split(/[,\s]+/)
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
        .map((item) => WorkflowMilestoneID.make(item)),
    ),
  )
}

function readReview(input: string | undefined): WorkflowMilestoneReview | undefined {
  const value = cleanAttribute(input)
  if (!value) return undefined
  if (value === "required" || value === "skip") return value
  requireElement(false, '<milestone review> must be "required" or "skip"')
  return undefined
}

function cleanAttribute(value: string | undefined) {
  const cleaned = value?.trim()
  return cleaned ? cleaned : undefined
}

function flattenMilestones(step: WorkflowStep): WorkflowMilestone[] {
  if (step.type === "milestone") return [step]
  return step.children.flatMap(flattenMilestones)
}

function expandPipelineItems(step: WorkflowStep, options: WorkflowParseOptions): WorkflowStep {
  if (step.type === "milestone") return step
  const children = step.children.map((child) => expandPipelineItems(child, options))
  if (step.type !== "pipeline" || !step.items || !options.readPipelineItems) {
    return {
      type: step.type,
      ...(step.items ? { items: step.items } : {}),
      children,
    }
  }
  const items = readPipelineItems(step.items, options)
  if (isWorkflowPipelineItemsMissing(items)) return missingPipelineItemsStep(step.items)
  const templateIDs = new Set(flattenMilestones({ type: "ordered", children }).map((milestone) => String(milestone.id)))
  return {
    type: "parallel",
    children: items.map((item) => ({
      type: "ordered",
      children: children.map((child) => clonePipelineStep(child, item, templateIDs)),
    })),
  }
}

function readPipelineItems(itemsPath: string, options: WorkflowParseOptions) {
  const raw = (() => {
    try {
      return options.readPipelineItems?.(itemsPath)
    } catch (error) {
      throw new WorkflowParseError(
        `pipeline items "${itemsPath}" could not be read: ${
          error instanceof globalThis.Error ? error.message : String(error)
        }`,
      )
    }
  })()
  if (isWorkflowPipelineItemsMissing(raw)) return raw
  requireElement(Array.isArray(raw), `<pipeline items="${itemsPath}"> must resolve to a JSON array`)
  requireElement(raw.length > 0, `<pipeline items="${itemsPath}"> must contain at least one item`)
  const items = raw.map(toPipelineItem)
  const slugs = new Set(items.map((item) => item.slug))
  requireElement(slugs.size === items.length, `<pipeline items="${itemsPath}"> contains duplicate item ids`)
  return items
}

function isWorkflowPipelineItemsMissing(value: unknown): value is WorkflowPipelineItemsMissing {
  return (
    pipelineItemObject(value) &&
    value.type === "missing-pipeline-items" &&
    typeof value.itemsPath === "string"
  )
}

function missingPipelineItemsStep(itemsPath: string): WorkflowStep {
  const id = WorkflowMilestoneID.make(`pipeline-items@${pipelineSlug(itemsPath)}`)
  return {
    type: "milestone",
    id,
    title: `Waiting for pipeline items ${itemsPath}`,
    department: "workflow",
    waitingFor: "pipeline_items",
    prompt: `Waiting for workflow pipeline items file ${itemsPath}. The workflow will expand this pipeline after the file exists and contains a JSON array.`,
    dependsOn: [],
  }
}

function toPipelineItem(value: unknown, index: number): PipelineItem {
  const label = pipelineItemText({ value, index })
  return {
    index,
    value,
    label,
    slug: pipelineSlug(label || `item-${index + 1}`),
  }
}

function clonePipelineStep(step: WorkflowStep, item: PipelineItem, templateIDs: Set<string>): WorkflowStep {
  if (step.type === "milestone") {
    return {
      ...step,
      id: pipelineMilestoneID(String(step.id), item),
      title: step.title ? substitutePipelineText(step.title, item) : undefined,
      department: step.department ? substitutePipelineText(step.department, item) : undefined,
      prompt: substitutePipelineText(step.prompt, item),
      dependsOn: step.dependsOn.map((id) => pipelineDependencyID(String(id), item, templateIDs)),
    }
  }
  return {
    type: step.type,
    ...(step.items ? { items: substitutePipelineText(step.items, item) } : {}),
    children: step.children.map((child) => clonePipelineStep(child, item, templateIDs)),
  }
}

function pipelineDependencyID(id: string, item: PipelineItem, templateIDs: Set<string>) {
  if (templateIDs.has(id)) return pipelineMilestoneID(id, item)
  if (pipelineTextHasPlaceholder(id)) return WorkflowMilestoneID.make(pipelineID(substitutePipelineText(id, item)))
  return WorkflowMilestoneID.make(id)
}

function pipelineMilestoneID(id: string, item: PipelineItem) {
  const value = pipelineTextHasPlaceholder(id) ? substitutePipelineText(id, item) : `${id}@${item.slug}`
  return WorkflowMilestoneID.make(pipelineID(value))
}

function substitutePipelineText(input: string, item: PipelineItem) {
  return input.replace(/\{(index|item(?:\.([A-Za-z0-9_-]+))?)\}/g, (_match, token: string, field: string | undefined) => {
    if (token === "index") return String(item.index + 1)
    return pipelineItemText(item, field)
  })
}

function pipelineTextHasPlaceholder(input: string) {
  return /\{(?:index|item(?:\.[A-Za-z0-9_-]+)?)\}/.test(input)
}

function pipelineItemText(item: Pick<PipelineItem, "value" | "index">, field?: string) {
  if (pipelineItemObject(item.value)) {
    const value = field
      ? item.value[field]
      : item.value.id ?? item.value.slug ?? item.value.name ?? item.value.title
    return primitiveText(value) ?? `item-${item.index + 1}`
  }
  return primitiveText(item.value) ?? `item-${item.index + 1}`
}

function pipelineItemObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function primitiveText(value: unknown) {
  if (typeof value === "string") return value.trim()
  if (typeof value === "number" || typeof value === "boolean") return String(value)
  return undefined
}

function pipelineSlug(value: string) {
  return (
    value
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 80) || "item"
  )
}

function pipelineID(value: string) {
  return (
    value
      .replace(/[^A-Za-z0-9._@-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 120) || "item"
  )
}

function addStructuralDependencies(
  step: WorkflowStep,
  incoming: WorkflowMilestoneID[] = [],
): { step: WorkflowStep; leaves: WorkflowMilestoneID[] } {
  if (step.type === "milestone") {
    return {
      step: {
        ...step,
        dependsOn: Array.from(new Set([...incoming, ...step.dependsOn])),
      },
      leaves: [step.id],
    }
  }

  if (step.type === "parallel") {
    const children = step.children.map((child) => addStructuralDependencies(child, incoming))
    return {
      step: {
        type: step.type,
        ...(step.items ? { items: step.items } : {}),
        children: children.map((child) => child.step),
      },
      leaves: Array.from(new Set(children.flatMap((child) => child.leaves))),
    }
  }

  const children = step.children.reduce<{ steps: WorkflowStep[]; leaves: WorkflowMilestoneID[] }>(
    (result, child) => {
      const next = addStructuralDependencies(child, result.leaves)
      return {
        steps: [...result.steps, next.step],
        leaves: next.leaves,
      }
    },
    { steps: [], leaves: incoming },
  )
  return {
    step: {
      type: step.type,
      ...(step.items ? { items: step.items } : {}),
      children: children.steps,
    },
    leaves: children.leaves,
  }
}

function validateMilestones(milestones: WorkflowMilestone[]) {
  requireElement(milestones.length > 0, "workflow XML must contain at least one milestone")
  const ids = new Set(milestones.map((milestone) => milestone.id))
  requireElement(ids.size === milestones.length, "workflow milestone ids must be unique")
  const missing = milestones.flatMap((milestone) => milestone.dependsOn).filter((id) => !ids.has(id))
  requireElement(missing.length === 0, `workflow milestone depends on unknown id "${missing[0]}"`)
  requireElement(!hasCycle(milestones), "workflow milestone dependencies cannot contain a cycle")
}

function hasCycle(milestones: WorkflowMilestone[]) {
  const byID = new Map(milestones.map((milestone) => [milestone.id, milestone]))
  const visiting = new Set<WorkflowMilestoneID>()
  const visited = new Set<WorkflowMilestoneID>()

  function visit(id: WorkflowMilestoneID): boolean {
    if (visited.has(id)) return false
    if (visiting.has(id)) return true
    visiting.add(id)
    const cycle = byID.get(id)?.dependsOn.some(visit) ?? false
    visiting.delete(id)
    visited.add(id)
    return cycle
  }

  return milestones.some((milestone) => visit(milestone.id))
}

function requireAttributes(element: RawWorkflowElement, allowed: string[]) {
  const allowedAttributes = new Set(allowed)
  const unknown = Object.keys(element.attributes).filter((name) => !allowedAttributes.has(name))
  requireElement(unknown.length === 0, `<${element.name}> does not support attribute "${unknown[0]}"`)
}

function requireElement(condition: boolean, message: string): asserts condition {
  if (condition) return
  throw new WorkflowParseError(message)
}
