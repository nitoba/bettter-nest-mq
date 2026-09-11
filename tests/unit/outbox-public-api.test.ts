import { expect, test } from 'bun:test'
import * as mq from '../../src/index.ts'
import * as postgres from '../../src/integrations/postgres.ts'

test('exports a Nest outbox service and a typed PostgreSQL transaction facade, not engine tokens', () => {
  expect(Object.keys(mq)).toContain('MqOutboxService')
  expect(Object.keys(mq)).toContain('MqOutboxException')
  expect(Object.keys(postgres)).toContain('postgresOutbox')
  expect(Object.keys(mq)).not.toContain('OutboxStore')
})
