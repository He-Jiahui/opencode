import { afterEach, describe, expect, test } from "bun:test"
import { Context } from "effect"
import fs from "fs/promises"
import path from "path"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { FilePaths } from "../../src/server/routes/instance/httpapi/groups/file"
import * as Log from "@opencode-ai/core/util/log"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, tmpdir } from "../fixture/fixture"

void Log.init({ print: false })

const context = Context.empty() as Context.Context<unknown>

function request(route: string, directory: string, query?: Record<string, string>, init?: RequestInit) {
  const url = new URL(`http://localhost${route}`)
  for (const [key, value] of Object.entries(query ?? {})) {
    url.searchParams.set(key, value)
  }
  const headers = new Headers(init?.headers)
  headers.set("x-opencode-directory", directory)
  if (init?.body && !headers.has("content-type")) headers.set("content-type", "application/json")
  return HttpApiApp.webHandler().handler(
    new Request(url, {
      ...init,
      headers,
    }),
    context,
  )
}

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

describe("file HttpApi", () => {
  test("serves read endpoints", async () => {
    await using tmp = await tmpdir({ git: true })
    await Bun.write(path.join(tmp.path, "hello.txt"), "hello")

    const [list, content, status] = await Promise.all([
      request(FilePaths.list, tmp.path, { path: "." }),
      request(FilePaths.content, tmp.path, { path: "hello.txt" }),
      request(FilePaths.status, tmp.path),
    ])

    expect(list.status).toBe(200)
    expect(await list.json()).toContainEqual(
      expect.objectContaining({ name: "hello.txt", path: "hello.txt", type: "file" }),
    )

    expect(content.status).toBe(200)
    expect(await content.json()).toMatchObject({ type: "text", content: "hello" })

    expect(status.status).toBe(200)
    expect(await status.json()).toContainEqual({ path: "hello.txt", added: 1, removed: 0, status: "added" })
  })

  test("file list includes direct child counts for directories", async () => {
    await using tmp = await tmpdir({ git: true })
    await fs.mkdir(path.join(tmp.path, "src"), { recursive: true })
    await Bun.write(path.join(tmp.path, "src", "index.ts"), "")
    await Bun.write(path.join(tmp.path, "src", "main.ts"), "")

    const list = await request(FilePaths.list, tmp.path, { path: "." })

    expect(list.status).toBe(200)
    expect(await list.json()).toContainEqual(expect.objectContaining({ name: "src", children: 2 }))
  })

  test("serves search endpoints", async () => {
    await using tmp = await tmpdir({ git: true })
    await Bun.write(path.join(tmp.path, "hello.txt"), "needle")

    const [text, files, symbols] = await Promise.all([
      request(FilePaths.findText, tmp.path, { pattern: "needle" }),
      request(FilePaths.findFile, tmp.path, { query: "hello", type: "file" }),
      request(FilePaths.findSymbol, tmp.path, { query: "hello" }),
    ])

    expect(text.status).toBe(200)
    expect(await text.json()).toContainEqual(expect.objectContaining({ line_number: 1 }))

    expect(files.status).toBe(200)
    expect(await files.json()).toContain("hello.txt")

    expect(symbols.status).toBe(200)
    expect(await symbols.json()).toEqual([])
  })

  test("serves ignore endpoints", async () => {
    await using tmp = await tmpdir({ git: true })

    const original = await request(FilePaths.ignore, tmp.path)
    expect(original.status).toBe(200)
    expect(await original.json()).toMatchObject({
      path: path.join(tmp.path, ".opencode", ".ignore"),
      source: "default",
    })

    const update = await request(FilePaths.ignore, tmp.path, undefined, {
      method: "PUT",
      body: JSON.stringify({ content: "*.tmp\n" }),
    })
    expect(update.status).toBe(200)
    expect(await update.json()).toMatchObject({
      path: path.join(tmp.path, ".opencode", ".ignore"),
      source: "project",
      content: "*.tmp\n",
    })
    expect(await Bun.file(path.join(tmp.path, ".opencode", ".ignore")).text()).toBe("*.tmp\n")

    const next = await request(FilePaths.ignore, tmp.path)
    expect(next.status).toBe(200)
    expect(await next.json()).toMatchObject({ source: "project", content: "*.tmp\n" })
  })

  test("search endpoints use opencode ignore instead of gitignore", async () => {
    await using tmp = await tmpdir({ git: true })
    await Bun.write(path.join(tmp.path, ".gitignore"), "*.secret\n")
    await Bun.write(path.join(tmp.path, "visible.secret"), "needle")
    await Bun.write(path.join(tmp.path, "ignored.tmp"), "needle")
    await fs.mkdir(path.join(tmp.path, ".opencode"), { recursive: true })
    await Bun.write(path.join(tmp.path, ".opencode", ".ignore"), "*.tmp\n")

    const text = await request(FilePaths.findText, tmp.path, { pattern: "needle" })
    expect(text.status).toBe(200)

    const body = JSON.stringify(await text.json())
    expect(body).toContain("visible.secret")
    expect(body).not.toContain("ignored.tmp")
  })
})
