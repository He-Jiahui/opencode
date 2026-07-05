import { Schema } from "effect"

import { statics } from "@opencode-ai/core/schema"

const projectIdSchema = Schema.String.pipe(Schema.brand("ProjectID"))

export type ProjectID = typeof projectIdSchema.Type

export const ProjectID = projectIdSchema.pipe(
  statics((schema: typeof projectIdSchema) => ({
    global: schema.make("global"),
  })),
)
