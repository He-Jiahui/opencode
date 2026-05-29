import { test, expect } from "bun:test"
import { FileIgnore } from "../../src/file/ignore"

test("match nested and non-nested", () => {
  expect(FileIgnore.match("node_modules/index.js")).toBe(true)
  expect(FileIgnore.match("node_modules")).toBe(true)
  expect(FileIgnore.match("node_modules/")).toBe(true)
  expect(FileIgnore.match("node_modules/bar")).toBe(true)
  expect(FileIgnore.match("node_modules/bar/")).toBe(true)
  expect(FileIgnore.match("packages/app/node_modules/index.js")).toBe(true)
})

test("custom patterns support negation", () => {
  expect(FileIgnore.matchWithPatterns("dist/app.js", ["dist", "!dist/app.js"])).toBe(false)
  expect(FileIgnore.matchWithPatterns("dist/chunk.js", ["dist", "!dist/app.js"])).toBe(true)
})

test("parse skips comments and blanks", () => {
  expect(FileIgnore.parse("\n# comment\nnode_modules\n\n")).toEqual(["node_modules"])
})
