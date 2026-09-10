import { MqModule, type MqResolvedOptions } from '../../src/index.ts'

MqModule.forRoot({})
MqModule.forRoot({ isGlobal: true, shutdown: { gracePeriodMs: 0 } })
MqModule.forRootAsync({ useFactory: async () => ({ shutdown: { gracePeriodMs: 500 } }) })

// @ts-expect-error Shutdown durations cannot be strings.
MqModule.forRoot({ shutdown: { gracePeriodMs: '500' } })

// @ts-expect-error Unsupported connection APIs are deliberately not exposed by this bootstrap.
MqModule.forRoot({ connections: {} })

export function checkReadonlyOptions(options: MqResolvedOptions): void {
  // @ts-expect-error Resolved configuration is immutable.
  options.shutdown.gracePeriodMs = 1
}
