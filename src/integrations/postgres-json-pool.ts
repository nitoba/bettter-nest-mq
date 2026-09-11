import type { ClientBase, CustomTypesConfig, Pool, PoolClient, QueryResultRow } from 'pg'
import type { Pool as AdapterPool, PoolClient as AdapterClient, QueryResult } from 'better-effect-mq-postgres'

type QueryValues = NonNullable<Parameters<AdapterClient['query']>[1]>
interface JsonClient extends AdapterClient {
  readonly on: PoolClient['on']
  readonly once: PoolClient['once']
  readonly removeListener: PoolClient['removeListener']
  readonly off: PoolClient['off']
}
interface JsonPool extends AdapterPool {
  readonly options: Pool['options']
  connect(): Promise<JsonClient>
  query<Row>(text: string, values?: QueryValues): Promise<QueryResult<Row>>
}

/** The pinned adapter decodes JSON text itself. Preserve wire text only for its JSON fields;
 * native application queries and all non-JSON parsing keep the actual client's configuration. */
export function adapterJsonTypes(client: Pick<ClientBase, 'getTypeParser'>): CustomTypesConfig {
  const getTypeParser: CustomTypesConfig['getTypeParser'] = (oid, format = 'text') => {
    if (format === 'binary') return client.getTypeParser(oid, 'binary')
    if (oid === 114 || oid === 3802) return (text: string) => text
    return client.getTypeParser(oid, 'text')
  }
  return { getTypeParser }
}

function jsonClient(client: PoolClient): JsonClient {
  const parsers = adapterJsonTypes(client)
  return {
    async query<Row>(text: string, values?: QueryValues): Promise<QueryResult<Row>> {
      if (values === undefined) return client.query<Row & QueryResultRow>({ text, types: parsers })
      return client.query<Row & QueryResultRow>({ text, values: [...values], types: parsers })
    },
    release(error?: Error): void { client.release(error) },
    on: client.on.bind(client),
    once: client.once.bind(client),
    removeListener: client.removeListener.bind(client),
    off: client.off.bind(client)
  }
}

// Passive views preserve the adapter's per-pool reservation identity across named stores.
// They acquire no resources, own no pool and cannot keep a native pool alive without a caller.
const views = new WeakMap<Pool, JsonPool>()

export function postgresJsonPool(pool: Pool): JsonPool {
  const previous = views.get(pool)
  if (previous !== undefined) return previous
  const view: JsonPool = {
    get options() { return pool.options },
    async connect(): Promise<JsonClient> { return jsonClient(await pool.connect()) },
    async query<Row>(text: string, values?: QueryValues): Promise<QueryResult<Row>> {
      const client = await pool.connect()
      try { return await jsonClient(client).query<Row>(text, values) }
      finally { client.release() }
    }
  }
  views.set(pool, view)
  return view
}
