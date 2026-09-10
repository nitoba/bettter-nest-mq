import { expect, test } from 'bun:test'
import { Inject, Injectable, Module } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'

import { Job, MqModule, MqRegistry, Queue, QueueService } from '../../src/index.ts'

@Injectable()
@Queue({ name: 'reexported' })
class ExportedQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.string(), result: z.string() })
}

@Module({
  imports: [MqModule.forFeature([ExportedQueue])],
  exports: [MqModule]
})
class MessagingModule {}

@Injectable()
class FeatureConsumer {
  constructor(@Inject(ExportedQueue) readonly queue: ExportedQueue) {}
}

@Module({ imports: [MessagingModule], providers: [FeatureConsumer], exports: [FeatureConsumer] })
class FeatureModule {}

test('re-exports feature contracts using the public MqModule class without requiring root options in features', async () => {
  const app = await Test.createTestingModule({
    imports: [MqModule.forRoot({}), FeatureModule]
  }).compile()
  try {
    await app.init()
    expect(await app.get(FeatureConsumer).queue.task.parsePayload('value')).toBe('value')
    expect(app.get(MqRegistry).queues()).toHaveLength(1)
  } finally {
    await app.close()
  }
})
