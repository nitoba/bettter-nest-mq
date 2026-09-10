import { describe, expect, test } from 'bun:test'
import { Inject, Injectable, Module } from '@nestjs/common'
import { Test } from '@nestjs/testing'

import { MqConfiguration, MqModule, type MqOptionsFactory } from '../../src/index.ts'

const GRACE_PERIOD = Symbol('grace-period')

@Injectable()
class OptionsFactory implements MqOptionsFactory {
  createMqOptions() {
    return { shutdown: { gracePeriodMs: 250 } }
  }
}

@Module({
  providers: [OptionsFactory, { provide: GRACE_PERIOD, useValue: 450 }],
  exports: [OptionsFactory, GRACE_PERIOD]
})
class ConfigurationModule {}

@Injectable()
class FeatureService {
  constructor(@Inject(MqConfiguration) readonly configuration: MqConfiguration) {}
}

@Module({ providers: [FeatureService], exports: [FeatureService] })
class FeatureModule {}

describe('MqModule in real Nest application contexts', () => {
  test('registers synchronous configuration and closes cleanly', async () => {
    const app = await Test.createTestingModule({ imports: [MqModule.forRoot({})] }).compile()
    try {
      await app.init()
      expect(app.get(MqConfiguration).options.shutdown.gracePeriodMs).toBe(30_000)
    } finally {
      await app.close()
    }
  })

  test('resolves asynchronous factories with imports and injection', async () => {
    const app = await Test.createTestingModule({
      imports: [
        MqModule.forRootAsync({
          imports: [ConfigurationModule],
          inject: [GRACE_PERIOD],
          useFactory: async (gracePeriodMs: number) => ({ shutdown: { gracePeriodMs } })
        })
      ]
    }).compile()
    try {
      expect(app.get(MqConfiguration).options.shutdown.gracePeriodMs).toBe(450)
    } finally {
      await app.close()
    }
  })

  test('supports useClass with createMqOptions', async () => {
    const app = await Test.createTestingModule({
      imports: [MqModule.forRootAsync({ useClass: OptionsFactory })]
    }).compile()
    try {
      expect(app.get(MqConfiguration).options.shutdown.gracePeriodMs).toBe(250)
    } finally {
      await app.close()
    }
  })

  test('supports an existing options factory', async () => {
    const app = await Test.createTestingModule({
      imports: [
        MqModule.forRootAsync({ imports: [ConfigurationModule], useExisting: OptionsFactory })
      ]
    }).compile()
    try {
      expect(app.get(MqConfiguration).options.shutdown.gracePeriodMs).toBe(250)
    } finally {
      await app.close()
    }
  })

  test('keeps the module non-global by default', () => {
    expect(MqModule.forRoot({}).global).toBe(false)
  })

  test('makes configuration injectable in sibling modules when explicitly global', async () => {
    const app = await Test.createTestingModule({
      imports: [MqModule.forRoot({ isGlobal: true }), FeatureModule]
    }).compile()
    try {
      expect(app.get(FeatureService).configuration).toBe(app.get(MqConfiguration))
    } finally {
      await app.close()
    }
  })

  test('does not share configuration across application contexts', async () => {
    const first = await Test.createTestingModule({
      imports: [MqModule.forRoot({ shutdown: { gracePeriodMs: 10 } })]
    }).compile()
    try {
      const second = await Test.createTestingModule({
        imports: [MqModule.forRoot({ shutdown: { gracePeriodMs: 20 } })]
      }).compile()
      try {
        expect(first.get(MqConfiguration)).not.toBe(second.get(MqConfiguration))
        expect(first.get(MqConfiguration).options.shutdown.gracePeriodMs).toBe(10)
        expect(second.get(MqConfiguration).options.shutdown.gracePeriodMs).toBe(20)
      } finally {
        await second.close()
      }
    } finally {
      await first.close()
    }
  })

  test('propagates an options factory failure without hiding its cause', async () => {
    await expect(
      Test.createTestingModule({
        imports: [
          MqModule.forRootAsync({
            useFactory: () => {
              throw new Error('configuration is unavailable')
            }
          })
        ]
      }).compile()
    ).rejects.toThrow('configuration is unavailable')
  })
})
