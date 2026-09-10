import { MqModule, type MqResolvedOptions } from '../../src/index.ts'

MqModule.forRoot({})
MqModule.forRoot({ isGlobal: true, shutdown: { gracePeriodMs: 0 } })
MqModule.forRoot({ connections: {} })
MqModule.forRootAsync({ useFactory: async () => ({ shutdown: { gracePeriodMs: 500 } }) })

// @ts-expect-error Shutdown durations cannot be strings.
MqModule.forRoot({ shutdown: { gracePeriodMs: '500' } })

// @ts-expect-error Connection definitions must come from real integration factories.
MqModule.forRoot({ connections: { primary: { adapter: 'invented', ownership: 'owned' } } })

export function checkReadonlyOptions(options: MqResolvedOptions): void {
  // @ts-expect-error Resolved configuration is immutable.
  options.shutdown.gracePeriodMs = 1
}
