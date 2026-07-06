import os from "os"
import path from "path"
import { fileURLToPath } from "url"

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const dir = path.resolve(__dirname, "..")

process.chdir(dir)

const modelsUrl = process.env.OPENCODE_MODELS_URL || "https://models.dev"
const snapshotFile = path.join(__dirname, ".models-dev-snapshot.json")

async function loadModelsData() {
  if (process.env.MODELS_DEV_API_JSON) return Bun.file(process.env.MODELS_DEV_API_JSON).text()
  try {
    const response = await fetch(`${modelsUrl}/api.json`)
    if (!response.ok) throw new Error(`${modelsUrl}/api.json responded ${response.status}`)
    const text = await response.text()
    await Bun.write(snapshotFile, text)
    return text
  } catch (error) {
    // Offline / blocked network: fall back to the last fetched snapshot, then
    // to the runtime cache the opencode CLI keeps, so dev builds still work.
    const cacheHome = process.env.XDG_CACHE_HOME || path.join(os.homedir(), ".cache")
    const fallbacks = [snapshotFile, path.join(cacheHome, "opencode", "models.json")]
    for (const file of fallbacks) {
      const cached = Bun.file(file)
      if (await cached.exists()) {
        console.warn(`models.dev unreachable (${error}), using cached snapshot: ${file}`)
        return cached.text()
      }
    }
    throw error
  }
}

export const modelsData = await loadModelsData()
console.log("Loaded models.dev snapshot")
