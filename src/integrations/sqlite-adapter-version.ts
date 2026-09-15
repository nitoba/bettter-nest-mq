import { createRequire } from 'node:module'

interface AdapterPackageManifest {
  readonly version?: string
}

const qualifiedScheduleVersion = Object.freeze({ major: 0, minor: 1, patch: 3 })

function readAdapterVersion(): string | undefined {
  try {
    const load = createRequire(import.meta.url)
    // SAFETY: this is the exported package.json of our pinned internal dependency; the exact
    // stable semver is validated by sqliteSchedulesQualified before the value enables schedules.
    const manifest = load('better-effect-mq-sqlite/package.json') as AdapterPackageManifest
    return manifest.version
  } catch {
    return undefined
  }
}

export const sqliteAdapterVersion = readAdapterVersion()

/** Schedule payload fidelity is qualified only for the corrected 0.1.x adapter line. */
export function sqliteSchedulesQualified(version = sqliteAdapterVersion): boolean {
  if (version === undefined) return false
  const match = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.exec(version)
  if (match === null) return false
  const majorText = match[1]
  const minorText = match[2]
  const patchText = match[3]
  if (majorText === undefined || minorText === undefined || patchText === undefined) return false
  const major = Number(majorText)
  const minor = Number(minorText)
  const patch = Number(patchText)
  return (
    major === qualifiedScheduleVersion.major &&
    minor === qualifiedScheduleVersion.minor &&
    patch >= qualifiedScheduleVersion.patch
  )
}

export function assertQualifiedSqliteSchedules(): void {
  if (sqliteSchedulesQualified()) return
  throw new Error(
    `SQLite schedules require better-effect-mq-sqlite >=0.1.3 <0.2.0; installed ${sqliteAdapterVersion ?? 'unknown'}`
  )
}
