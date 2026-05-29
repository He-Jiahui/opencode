import { Parser } from "htmlparser2"

import { WorkflowMilestoneID, type WorkflowDefinition, type WorkflowMilestone, type WorkflowStep } from "./schema"

export type RawWorkflowElement = {
  name: string
  attributes: Record<string, string>
  children: RawWorkflowElement[]
  text: string[]
}

const containerNames = new Set(["workflow", "ordered", "parallel"])
const elementNames = new Set([...containerNames, "milestone"])

export class WorkflowParseError extends Error {
  override name = "WorkflowParseError"
}

export function parseWorkflowXml(input: string): WorkflowDefinition {
  return validateWorkflowElement(parseXml(input))
}

export function validateWorkflowElement(root: RawWorkflowElement): WorkflowDefinition {
  requireElement(root.name === "workflow", "workflow XML must have a <workflow> root")
  requireElement(root.children.length === 1, "<workflow> must contain exactly one step")
  requireElement(root.text.join("").trim().length === 0, "<workflow> cannot contain text outside steps")

  const steps = addStructuralDependencies(readStep(root.children[0])).step
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
  const errors: string[] = []

  new Parser(
    {
      onopentag: (name, attributes) => {
        if (!elementNames.has(name)) {
          errors.push(`unsupported workflow element <${name}>`)
        }
        stack.at(-1)?.children.push({
          name,
          attributes,
          children: [],
          text: [],
        })
        stack.push(stack.at(-1)!.children.at(-1)!)
      },
      ontext: (text) => {
        stack.at(-1)?.text.push(text)
      },
      onclosetag: (name) => {
        if (stack.at(-1)?.name !== name) {
          errors.push(`workflow XML has mismatched closing tag </${name}>`)
          return
        }
        stack.pop()
      },
      onerror: (error) => {
        errors.push(error.message)
      },
    },
    { xmlMode: true, recognizeSelfClosing: true },
  ).end(input)

  requireElement(errors.length === 0, errors[0] ?? "invalid workflow XML")
  requireElement(stack.length === 1, "workflow XML has unclosed elements")
  requireElement(document.children.length === 1, "workflow XML must contain exactly one root element")
  requireElement(document.text.join("").trim().length === 0, "workflow XML cannot contain text outside <workflow>")
  return document.children[0]
}

function readStep(element: RawWorkflowElement): WorkflowStep {
  if (element.name === "milestone") {
    requireAttributes(element, ["id", "title", "department", "agent", "depends"])
    requireElement(
      element.children.length === 0,
      `<milestone id="${element.attributes.id ?? ""}"> cannot contain child steps`,
    )
    requireElement(Boolean(element.attributes.id?.trim()), "<milestone> requires an id attribute")
    requireElement(Boolean(element.text.join("").trim()), `<milestone id="${element.attributes.id ?? ""}"> requires content`)
    return {
      type: "milestone",
      id: WorkflowMilestoneID.make(element.attributes.id!.trim()),
      title: cleanAttribute(element.attributes.title),
      department: cleanAttribute(element.attributes.department ?? element.attributes.agent),
      prompt: element.text.join("").trim(),
      dependsOn: readDepends(element.attributes.depends),
    }
  }

  requireElement(element.name === "ordered" || element.name === "parallel", `<${element.name}> is not a workflow step`)
  requireAttributes(element, [])
  requireElement(element.children.length > 0, `<${element.name}> must contain at least one step`)
  requireElement(element.text.join("").trim().length === 0, `<${element.name}> cannot contain text outside steps`)
  return {
    type: element.name,
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

function cleanAttribute(value: string | undefined) {
  const cleaned = value?.trim()
  return cleaned ? cleaned : undefined
}

function flattenMilestones(step: WorkflowStep): WorkflowMilestone[] {
  if (step.type === "milestone") return [step]
  return step.children.flatMap(flattenMilestones)
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
