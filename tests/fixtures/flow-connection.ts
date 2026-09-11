import { Layer } from 'better-effect'
import { MemoryJobStore, JobStoreFailure } from 'better-effect-mq'
import { Result } from 'better-result'
import type { JobStoreV2Operation, FlowStoreV2Error, FlowStoreV2 } from 'better-effect-mq'
import { defineConnection } from '../../src/engine/connection-definition.ts'
import { flowToken } from '../../src/engine/flow-plan.ts'

async function adapt<Value>(
  operation: JobStoreV2Operation<Value>
): Promise<Result<Value, FlowStoreV2Error>> {
  const result = await operation
  return Result.isError(result)
    ? Result.err(
        new JobStoreFailure({
          operation: 'flow.fixture',
          retryable: false,
          message: result.error.message
        })
      )
    : Result.ok(result.value)
}

export function flowConnection(enabled = true) {
  const jobs = MemoryJobStore.make()
  const v2 = jobs.v2
  const flows: FlowStoreV2 = {
    descriptor: v2.flow.descriptor,
    fanOut: (request) => adapt(v2.fanOut(request)),
    recordChildResults: (request) => adapt(v2.recordChildResults(request)),
    cancel: (request) => adapt(v2.cancelFlow(request)),
    reconcile: (request) => adapt(v2.reconcile(request)),
    markCascaded: (request) => adapt(v2.markCascaded(request)),
    appendChildReport: (request) => adapt(v2.appendChildReport(request)),
    peekOutbox: (request) => adapt(v2.peekOutbox(request)),
    ackOutbox: (request) => adapt(v2.ackOutbox(request)),
    getFlow: (request) => adapt(v2.getFlow(request))
  }
  const trace = { acquired: 0, released: 0 }
  const connection = defineConnection(
    { adapter: 'memory', ownership: 'borrowed', boundary: jobs, scope: 'flows' },
    (token) => {
      const layer = Layer.scoped(
        token,
        () => {
          trace.acquired += 1
          return jobs
        },
        () => {
          trace.released += 1
        }
      )
      return enabled
        ? { layer, flows: (name: string) => Layer.succeed(flowToken(name), flows) }
        : { layer }
    }
  )
  return { connection, jobs, flows, trace }
}
