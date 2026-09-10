import { Inject, Injectable } from '@nestjs/common'

import type { MqConnectionMonitor, MqConnectionSnapshot, MqEngineState } from './connection.ts'
import { CONNECTION_MONITOR } from './tokens.ts'

/** Readiness reflects live store access, not only the existence of a Nest provider. */
@Injectable()
export class MqConnectionsService implements MqConnectionMonitor {
  constructor(@Inject(CONNECTION_MONITOR) private readonly monitor: MqConnectionMonitor) {}

  get state(): MqEngineState {
    return this.monitor.state
  }

  connections(): ReadonlyArray<MqConnectionSnapshot> {
    return this.monitor.connections()
  }

  probe(name: string): Promise<MqConnectionSnapshot> {
    return this.monitor.probe(name)
  }
}
