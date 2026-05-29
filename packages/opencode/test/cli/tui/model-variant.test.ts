import { describe, expect, test } from "bun:test"
import {
  DEFAULT_MODEL_VARIANT,
  cycleModelVariant,
  getConfiguredAgentVariant,
  resolveModelVariant,
} from "../../../src/cli/cmd/tui/context/model-variant"

describe("tui model variant", () => {
  test("resolves configured agent variant when model matches", () => {
    const value = getConfiguredAgentVariant({
      agent: {
        model: { providerID: "openai", modelID: "gpt-5.2" },
        variant: "xhigh",
      },
      model: {
        providerID: "openai",
        modelID: "gpt-5.2",
        variants: { low: {}, high: {}, xhigh: {} },
      },
    })

    expect(value).toBe("xhigh")
  })

  test("ignores configured variant when model does not match", () => {
    const value = getConfiguredAgentVariant({
      agent: {
        model: { providerID: "openai", modelID: "gpt-5.2" },
        variant: "xhigh",
      },
      model: {
        providerID: "anthropic",
        modelID: "claude-sonnet-4",
        variants: { low: {}, high: {}, xhigh: {} },
      },
    })

    expect(value).toBeUndefined()
  })

  test("prefers selected variant over configured variant", () => {
    expect(
      resolveModelVariant({
        variants: ["low", "high", "xhigh"],
        selected: "high",
        configured: "xhigh",
      }),
    ).toBe("high")
  })

  test("lets explicit default override configured variant", () => {
    expect(
      resolveModelVariant({
        variants: ["low", "high", "xhigh"],
        selected: DEFAULT_MODEL_VARIANT,
        configured: "xhigh",
      }),
    ).toBeUndefined()
  })

  test("cycles from configured variant to next", () => {
    expect(
      cycleModelVariant({
        variants: ["low", "high", "xhigh"],
        selected: undefined,
        configured: "high",
      }),
    ).toBe("xhigh")
  })

  test("wraps from configured last variant to first", () => {
    expect(
      cycleModelVariant({
        variants: ["low", "high", "xhigh"],
        selected: undefined,
        configured: "xhigh",
      }),
    ).toBe("low")
  })
})
