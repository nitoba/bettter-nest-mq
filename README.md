# better-nest-mq

NestJS-native foundations for a future integration with the `better-effect-mq` engine.

**Status: project bootstrap, version 0.0.0. This is not yet an operational queue library.**

The repository URL is `nitoba/bettter-nest-mq` (three `t` characters); the package name is `better-nest-mq`.

## Available now

The package exposes a real Nest 12 dynamic module with `forRoot()` and `forRootAsync()`, including `useFactory`, `useClass`, `useExisting`, imports, injection and opt-in global registration. `MqConfiguration` exposes an immutable, validated shutdown configuration local to each application context. Registration does not open connections, start workers or install process signal handlers.

The shutdown policy is stored and validated only. It will be consumed by the engine host when that integration is implemented.

**Not available yet:** queue/job/worker decorators, publication, processing, database adapters, retries, flows, schedules, transactional outbox, administration and durable events. These APIs are not exported as stubs. See [the architecture](docs/architecture.md) and [the implementation roadmap](docs/roadmap.md).

## Development

Use Bun **1.4.2**, matching the reference repository, and Node **22.12+**. CI tests Node 22 and 24. The development compiler follows the reference project (TypeScript 7); both source and the packed library are also checked with TypeScript 6.x. The public TypeScript peer floor is `>=6.0.0`.

```sh
git clone https://github.com/nitoba/bettter-nest-mq.git
cd bettter-nest-mq
bun install --frozen-lockfile
bun run hooks:install
bun run check
```

No npm release is published by CI. Development dependencies, including Zod 4 and Standard Schema, are not bundled into the library.

## Current module API

```ts
import { Injectable, Module } from '@nestjs/common'
import { MqConfiguration, MqModule } from 'better-nest-mq'

@Injectable()
class ApplicationService {
  constructor(readonly mq: MqConfiguration) {}
}

@Module({
  imports: [
    MqModule.forRoot({
      shutdown: {
        gracePeriodMs: 30_000,
        abortAfterGracePeriod: true
      }
    })
  ],
  providers: [ApplicationService]
})
export class ApplicationModule {}
```

`forRoot({})` uses those defaults. Durations must be non-negative safe integers; zero and explicit `false` are preserved. Configuration is copied, not frozen in place on the caller's object. The module is not global unless `isGlobal: true` is explicitly supplied.

```ts
MqModule.forRootAsync({
  useFactory: async () => ({
    shutdown: { gracePeriodMs: 10_000 }
  })
})
```

Async registration also supports `imports` and `inject`. For `useClass` or `useExisting`, implement `MqOptionsFactory.createMqOptions()`; `useExisting` reuses a provider exported by an imported module.

The example illustrates the local/package API, not an instruction to install an already released npm version. The package-consumer test builds and installs a real local tarball.

## Tooling

| Tool                     | Purpose                                                        |
| ------------------------ | -------------------------------------------------------------- |
| Bun                      | Package manager, lockfile, test runner and development scripts |
| TypeScript               | Strict checking, TypeScript 6 compatibility and declarations   |
| tsdown                   | ESM build, declaration bundle and source maps                  |
| Oxlint + oxlint-tsgolint | Type-aware linting with the original custom anti-slop rules    |
| Oxfmt                    | The exact formatter configuration from better-effect           |
| Lefthook                 | Local lint, formatter and typecheck hooks                      |
| publint                  | Validation of the package's public exports                     |

The `.oxlintrc.json`, `.oxfmtrc.json` and complete `tools/oxlint` plugin are copied unchanged from `nitoba/better-effect` at commit `42c28fb0af7882eb048ee5d4ab1c1db81142c9dd`. `bun run check:tooling` verifies their Git blob hashes. See [tooling provenance](docs/tooling.md) before updating those files.

The formatter uses two spaces, 100 columns, single quotes, no semicolons, no trailing commas and LF. No ESLint or Prettier configuration is added.

```sh
bun run typecheck
bun run typecheck:minimum
bun run test
bun run test:coverage
bun run lint
bun run format
bun run format:check
bun run build
bun run publint
bun run test:package
```

`bun run check` runs the quality gates. The package test installs the packed tarball outside this repository, compiles a consumer with both TypeScript majors and executes it using Node and Bun. It verifies Nest constructor injection, module lifecycle, public declarations and package contents rather than resolving imports back to `src`.

## Design boundary

Application code will use Nest modules, injectable services, decorators and `async/await`. Effect/Result/Layer/Runtime types must stay private to the future engine bridge. Reusing the engine is the chosen architecture, not rewriting leases, retries or transactions in a second implementation.

Database drivers will be optional integrations. Importing a module must never imply an HTTP server, a worker process or automatically applied database migrations. Durable delivery will remain at-least-once, with explicit idempotency and transactional outbox semantics.

## License

MIT. The vendored tooling retains the upstream license and provenance.
