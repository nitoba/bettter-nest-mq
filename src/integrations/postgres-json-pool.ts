import type { ClientBase, CustomTypesConfig, Pool, PoolClient, QueryResultRow } from 'pg'
import type {
  Pool as AdapterPool,
  PoolClient as AdapterClient,
  QueryResult
} from 'better-effect-mq-postgres'

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
function jsonTypes(client: Pick<ClientBase, 'getTypeParser'>, decoded: boolean): CustomTypesConfig {
  const getTypeParser: CustomTypesConfig['getTypeParser'] = (oid, format = 'text') => {
    if (format === 'binary') return client.getTypeParser(oid, 'binary')
    if (oid === 114 || oid === 3802) return decoded ? JSON.parse : (text: string) => text
    return client.getTypeParser(oid, 'text')
  }
  return { getTypeParser }
}

export function adapterJsonTypes(client: Pick<ClientBase, 'getTypeParser'>): CustomTypesConfig {
  return jsonTypes(client, false)
}
export function flowJsonTypes(client: Pick<ClientBase, 'getTypeParser'>): CustomTypesConfig {
  return jsonTypes(client, true)
}
export function postgresJsonClient(client: PoolClient): JsonClient {
  return jsonClient(client, false)
}
function jsonClient(client: PoolClient, decoded: boolean): JsonClient {
  const parsers = jsonTypes(client, decoded)
  return {
    async query<Row>(text: string, values?: QueryValues): Promise<QueryResult<Row>> {
      if (values === undefined) return client.query<Row & QueryResultRow>({ text, types: parsers })
      return client.query<Row & QueryResultRow>({ text, values: [...values], types: parsers })
    },
    release(error?: Error): void {
      client.release(error)
    },
    on: client.on.bind(client),
    once: client.once.bind(client),
    removeListener: client.removeListener.bind(client),
    off: client.off.bind(client)
  }
}

// Passive views preserve the adapter's per-pool reservation identity across named stores.
// They acquire no resources, own no pool and cannot keep a native pool alive without a caller.
const views = new WeakMap<Pool, JsonPool>()

const flowViews = new WeakMap<Pool, JsonPool>()
export function postgresJsonPool(pool: Pool): JsonPool {
  return jsonPool(pool, false)
}
export function postgresFlowJsonPool(pool: Pool): JsonPool {
  return jsonPool(pool, true)
}
function jsonPool(pool: Pool, decoded: boolean): JsonPool {
  const cache = decoded ? flowViews : views
  const previous = cache.get(pool)
  if (previous !== undefined) return previous
  const view: JsonPool = {
    get options() {
      return pool.options
    },
    async connect(): Promise<JsonClient> {
      return jsonClient(await pool.connect(), decoded)
    },
    async query<Row>(text: string, values?: QueryValues): Promise<QueryResult<Row>> {
      const client = await pool.connect()
      const disconnected = Promise.withResolvers<never>()
      const onError = (error: Error): void => disconnected.reject(error)
      let releaseError: Error | undefined
      client.once('error', onError)
      try {
        return await Promise.race([
          jsonClient(client, decoded).query<Row>(text, values),
          disconnected.promise
        ])
      } catch (cause) {
        // Match native pool.query disposal: an unsuccessful client is not returned as healthy.
        releaseError =
          cause instanceof Error ? cause : new Error('PostgreSQL query failed', { cause })
        throw cause
      } finally {
        client.removeListener('error', onError)
        client.release(releaseError)
      }
    }
  }
  cache.set(pool, view)
  return view
}
