import { Schema } from "effect"
import { BusEvent } from "@/bus/bus-event"

export const Event = {
  Updated: BusEvent.define(
    "file.watcher.updated",
    Schema.Struct({
      file: Schema.String,
      event: Schema.Literals(["add", "change", "unlink"]),
    }),
  ),
}

export * as FileWatcher from "./watcher"
