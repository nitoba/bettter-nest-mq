import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const path = 'scripts/test-package.ts'
let text = readFileSync(path, 'utf8')
assert.ok(!text.includes('verifyOutboxRetryProtocol'))
text = "import { verifyOutboxRetryProtocol } from '../tests/postgres/outbox-retry.ts'\n" + text
const anchor = '  await verifyOrdinaryRecovery(recoveryDatabase)'
assert.equal(text.split(anchor).length - 1, 1)
text = text.replace(anchor, `${anchor}\n  await verifyOutboxRetryProtocol(recoveryDatabase)`)
assert.equal(text.split("      'kysely-outbox'\n").length - 1, 1)
text = text.replace("      'kysely-outbox'\n", "      'kysely-outbox',\n      'outbox-retry'\n")
writeFileSync(path, text)
