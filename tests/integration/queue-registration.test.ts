import { describe, expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Inject, Injectable, Module, Scope } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'

import { ContractDefinitionException, Job, MqModule, MqRegistry, Queue, QueueService } from '../../src/index.ts'

@Injectable()
@Queue({ name: 'reports' })
class Reports extends QueueService {
  @Job({ name: 'generate', version: 1 })
  readonly generate = this.job({ payload: z.string(), result: z.string() })
}

@Injectable()
class Publisher {
  constructor(@Inject(Reports) readonly reports: Reports) {}
}

@Module({ imports: [MqModule.forFeature([Reports])], providers: [Publisher], exports: [Publisher] })
class FeatureModule {}

describe('Nest queue registration', () => {
  test('registers real injectable services and builds a root-owned registry at bootstrap', async () => {
    const app = await Test.createTestingModule({ imports: [MqModule.forRoot({ defaults: { priority: 7 } }), FeatureModule] }).compile()
    try {
      expect(app.get(MqRegistry).queues()).toEqual([])
      await app.init()
      expect(app.get(Publisher).reports).toBe(app.get(Reports))
      expect(app.get(MqRegistry).queues()).toHaveLength(1)
      const job = app.get(MqRegistry).jobs()[0]
      assert.ok(job)
      expect(job.policy.priority).toBe(7)
      expect(app.get(MqRegistry).get(job.identity.key)).toBe(job)
      expect(Object.isFrozen(app.get(MqRegistry).queues())).toBe(true)
    } finally {
      await app.close()
    }
    expect(app.get(MqRegistry).queues()).toEqual([])
  })

  test('supports ordinary provider registration and deduplicates useExisting aliases', async () => {
    const app = await Test.createTestingModule({
      imports: [MqModule.forRoot({})],
      providers: [Reports, { provide: 'REPORTS_ALIAS', useExisting: Reports }]
    }).compile()
    try {
      await app.init()
      expect(app.get(MqRegistry).jobs()).toHaveLength(1)
    } finally {
      await app.close()
    }
  })

  test('rejects duplicate identities atomically rather than keeping a partial registry', async () => {
    @Queue({ name: 'reports' })
    class Duplicate extends QueueService {
      @Job({ name: 'generate', version: 1 })
      readonly task = this.job({ payload: z.string(), result: z.string() })
    }
    const app = await Test.createTestingModule({ imports: [MqModule.forRoot({}), MqModule.forFeature([Reports, Duplicate])] }).compile()
    try {
      await assert.rejects(app.init(), ContractDefinitionException)
      expect(app.get(MqRegistry).queues()).toEqual([])
    } finally {
      await app.close()
    }
  })

  test('rejects request-scoped contracts rather than publishing an incomplete registry', async () => {
    @Injectable({ scope: Scope.REQUEST })
    @Queue({ name: 'scoped' })
    class Scoped extends QueueService {
      @Job({ name: 'task', version: 1 })
      readonly task = this.job({ payload: z.string(), result: z.string() })
    }
    const app = await Test.createTestingModule({ imports: [MqModule.forRoot({}), MqModule.forFeature([Scoped])] }).compile()
    try {
      await assert.rejects(app.init(), ContractDefinitionException)
      expect(app.get(MqRegistry).jobs()).toEqual([])
    } finally {
      await app.close()
    }
  })

  test('isolates registries and defaults between Nest application contexts', async () => {
    const first = await Test.createTestingModule({ imports: [MqModule.forRoot({ defaults: { priority: 1 } }), FeatureModule] }).compile()
    const second = await Test.createTestingModule({ imports: [MqModule.forRoot({ defaults: { priority: 2 } }), FeatureModule] }).compile()
    try {
      await first.init()
      await second.init()
      expect(first.get(MqRegistry)).not.toBe(second.get(MqRegistry))
      expect(first.get(MqRegistry).jobs()[0]?.policy.priority).toBe(1)
      expect(second.get(MqRegistry).jobs()[0]?.policy.priority).toBe(2)
      await first.close()
      expect(second.get(MqRegistry).jobs()).toHaveLength(1)
    } finally {
      await second.close()
      await first.close()
    }
  })

  test('deduplicates repeated classes in forFeature and remains inert without a root', async () => {
    const app = await Test.createTestingModule({ imports: [MqModule.forFeature([Reports, Reports])] }).compile()
    try {
      await app.init()
      expect(await app.get(Reports).generate.parsePayload('value')).toBe('value')
      expect('enqueue' in app.get(Reports).generate).toBe(false)
    } finally {
      await app.close()
    }
  })

  test('unknown identity lookups do not invent jobs', async () => {
    const app = await Test.createTestingModule({ imports: [MqModule.forRoot({})] }).compile()
    try {
      await app.init()
      expect(app.get(MqRegistry).get('missing')).toBeUndefined()
    } finally {
      await app.close()
    }
  })
})
