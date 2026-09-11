import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const path = 'src/integrations/postgres.ts'
let text = readFileSync(path, 'utf8')
if (!text.includes("from './postgres-outbox-resource.ts'")) {
  const anchor = "import { postgresOutboxLayer } from './postgres-outbox.ts'"
  assert.equal(text.split(anchor).length, 2)
  text = text.replace(anchor, "import { postgresOutboxLayer } from './postgres-outbox-resource.ts'")
  text = text.replace("} from './postgres-outbox.ts'\n", "} from './postgres-outbox.ts'\n")
}
const end = "} from './postgres-outbox.ts'"
const last = text.lastIndexOf(end)
if (last > 0 && text.slice(last - 200, last).includes('export type')) {
  text = `${text.slice(0, last)}} from './postgres-outbox.types.ts'${text.slice(last + end.length)}`
}
writeFileSync(path, text)
