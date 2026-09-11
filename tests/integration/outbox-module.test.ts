import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { MqModule, MqOutboxService, MqOutboxException } from '../../src/index.ts'
import { postgresOutbox } from '../../src/integrations/postgres.ts'

test('a contract-only application exposes outbox diagnostics without starting a publisher or a pool', async () => {
  const app = await Test.createTestingModule({ imports: [MqModule.forRoot({})] }).compile()
  try {
    await app.init()
    const outbox = app.get(MqOutboxService)
    expect(outbox.publisher()).toBeUndefined()
    await assert.rejects(outbox.counts('missing'), MqOutboxException)
    await assert.rejects(postgresOutbox(outbox, 'missing').transaction(async () => 'no'), MqOutboxException)
  } finally { await app.close() }
})
