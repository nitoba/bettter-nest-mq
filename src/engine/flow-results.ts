import type { FlowResults as EngineResults } from 'better-effect-mq'
import { Result } from 'better-result'
import { JobFailureException } from '../contracts/errors.ts'
import type { JobContract, ResultOf, FailureOf } from '../contracts/job-definition.ts'
import type { FlowJobReference } from '../flows/references.ts'
import type {
  FlowResultsReader,
  FlowChildResult,
  FlowChildPage,
  FlowPageOptions
} from '../flows/types.ts'
import { MqFlowException } from '../flows/errors.ts'
import { flowFields } from '../flows/validation.ts'
import { requireInteger } from '../contracts/policies.ts'
import type { CompiledFlow, EngineFlowDefinition } from './flow-discovery.ts'
import { flowChildEntry } from './flow-discovery.ts'

export class FlowResultReader implements FlowResultsReader {
  readonly counts
  private open = true
  private readonly cursors = new Set<string>()
  private readonly active = new Set<Promise<void>>()
  constructor(
    private readonly flow: CompiledFlow,
    private readonly results: EngineResults<EngineFlowDefinition>
  ) {
    this.counts = Object.freeze({ ...results.counts })
  }
  private async admitted<Value>(operation: () => Promise<Value>): Promise<Value> {
    if (!this.open)
      throw new MqFlowException(
        'closed',
        'Flow results are only available inside their active Collect invocation'
      )
    const running = operation()
    const observed = running.then(
      () => undefined,
      () => undefined
    )
    this.active.add(observed)
    try {
      return await running
    } finally {
      this.active.delete(observed)
    }
  }
  async close(): Promise<void> {
    this.open = false
    await Promise.all(this.active)
  }
  page<Job extends JobContract>(
    reference: FlowJobReference<Job>,
    options: FlowPageOptions = {}
  ): Promise<FlowChildPage<Job>> {
    return this.admitted(() => this.readPage(reference, options))
  }
  private async readPage<Job extends JobContract>(
    reference: FlowJobReference<Job>,
    options: FlowPageOptions
  ): Promise<FlowChildPage<Job>> {
    const entry = flowChildEntry(this.flow, reference)
    flowFields(options, ['cursor', 'limit'])
    const limit = options.limit ?? 100
    requireInteger(limit, 'flow.page.limit', 1)
    if (limit > 1_000)
      throw new MqFlowException('definition', 'Flow pages are limited to 1000 manifest entries')
    if (
      options.cursor !== undefined &&
      (options.cursor.length === 0 ||
        options.cursor.length > 256 ||
        !this.cursors.has(options.cursor))
    )
      throw new MqFlowException(
        'definition',
        'Flow cursors must come from this active Collect reader'
      )
    // The pinned page Program closes over the already-resolved store. Invoke it inside the
    // supervisor-owned Collect scope, without constructing another runtime or executor.
    const result = await this.results.page({ ...options, limit })()
    if (Result.isError(result))
      throw new MqFlowException('operation', 'Unable to decode flow child outcomes', {
        cause: result.error
      })
    const items: FlowChildResult<Job>[] = []
    for (const child of result.value.items) {
      if (child.definition !== entry.compiled) continue
      const base = { key: child.childKey, job: reference }
      if (child.outcome === 'completed') {
        // SAFETY: the engine decoded the exact registered child result codec selected above.
        items.push(
          Object.freeze({ ...base, outcome: 'completed', result: child.result as ResultOf<Job> })
        )
      } else if (child.outcome === 'failed') {
        const value =
          child.failure instanceof JobFailureException ? child.failure.failure : undefined
        // SAFETY: the matching compiled failure codec validates its declared schema before wrapping it.
        items.push(
          Object.freeze({
            ...base,
            outcome: 'failed',
            failure: value as FailureOf<Job> | undefined
          })
        )
      } else items.push(Object.freeze({ ...base, outcome: 'cancelled' }))
    }
    if (result.value.nextCursor !== undefined) this.cursors.add(result.value.nextCursor)
    return Object.freeze({ items: Object.freeze(items), nextCursor: result.value.nextCursor })
  }
  all<Job extends JobContract>(
    reference: FlowJobReference<Job>,
    options: { readonly maxItems: number }
  ): Promise<readonly FlowChildResult<Job>[]> {
    return this.admitted(async () => {
      flowFields(options, ['maxItems'])
      requireInteger(options.maxItems, 'flow.all.maxItems')
      if (options.maxItems > this.flow.definition.maxChildren)
        throw new MqFlowException('definition', 'The collection bound exceeds the flow child limit')
      const items: FlowChildResult<Job>[] = []
      const seen = new Set<string>()
      let cursor: string | undefined
      do {
        const query = cursor === undefined ? { limit: 100 } : { limit: 100, cursor }
        const page = await this.readPage(reference, query)
        items.push(...page.items)
        if (items.length > options.maxItems)
          throw new MqFlowException(
            'definition',
            'Flow results exceed maxItems; use pagination rather than silently truncating'
          )
        cursor = page.nextCursor
        if (cursor !== undefined && seen.has(cursor))
          throw new MqFlowException('operation', 'The flow store returned a repeated cursor')
        if (cursor !== undefined) seen.add(cursor)
      } while (cursor !== undefined)
      return Object.freeze(items)
    })
  }
}
