import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const test = 'tests/integration/queue-controls.test.ts'
let text = readFileSync(test, 'utf8')
if (!text.includes('controlReferenceStore')) {
  assert.equal(text.split('const store = MemoryJobStore.make()').length, 2)
  text = text.replace(
    'const store = MemoryJobStore.make()',
    'const store = controlReferenceStore()'
  )
  text = `import { controlReferenceStore } from '../fixtures/control-store.ts'\n${text}`
}
writeFileSync(test, text.replace('  MemoryJobStore,\n', ''))
const script = 'scripts/test-package.ts'
text = readFileSync(script, 'utf8')
if (!text.includes("'execution', 'controls'")) {
  assert.equal(text.split("['codec', 'postgres', 'execution']").length, 2)
  writeFileSync(
    script,
    text.replace(
      "['codec', 'postgres', 'execution']",
      "['codec', 'postgres', 'execution', 'controls']"
    )
  )
}
