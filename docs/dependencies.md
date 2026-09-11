# Dependencies and the Nest consumer boundary

The audience of better-nest-mq is a Nest application developer. Using this library does not require adopting the better-effect ecosystem, learning its syntax or manually coordinating its package versions.

## Installed automatically by this library

The following are normal dependencies of better-nest-mq, not consumer peer dependencies:

- better-effect
- better-result
- better-effect-mq
- better-effect-mq-postgres
- better-effect-mq-outbox

Their compatible versions are pinned and maintained by this package. They appear in the installed dependency tree because their code is reused; they are not copied out or removed. The consumer does not need to list or import them, create a Runtime/Layer, or return a Result from a worker. Native driver objects, such as a borrowed pg Pool, remain part of the selected integration rather than exposing engine objects.

The outbox package is currently an internal dependency required by the upstream PostgreSQL integration. It does not mean the Nest transactional-outbox API has already been implemented. Current features and remaining work are documented in roadmap.md.

## Application-facing peers

The integration uses the application's compatible Nest installation, reflect-metadata and RxJS. TypeScript consumers need a compiler supporting the declared >=6.0.0 floor. Optional integrations require only the selected schema library or native driver:

| Use | Application dependency |
| --- | --- |
| Existing Nest application | Compatible @nestjs/common, @nestjs/core, reflect-metadata and rxjs |
| PostgreSQL subpath | pg; @types/pg for TypeScript consumers |
| Zod schemas/codecs | zod when the application chooses it |
| Another Standard Schema validator | That validator, not Zod |

For example, after building and packing this unreleased repository, a Nest application can consume the tarball and the PostgreSQL driver:

```sh
bun add ./better-nest-mq-0.0.0.tgz pg
bun add -d @types/pg
```

Add Zod separately only when using Zod. There is no instruction to install internal better-effect packages. No npm release has been published; the tarball path above refers to a locally built artifact, not an existing registry release.

The package root does not load the PostgreSQL adapter or native driver merely because a QueueService is imported. Installing internal adapter code is different from loading it or opening a connection. Package managers may also resolve transitive peer dependencies; this document does not promise a minimal installed dependency count. External tests explicitly remove pg and Zod to verify root-only usage does not require either at runtime.

## Verification and version responsibility

Regression tests inspect the package manifest so none of the internal engine/adapter packages can accidentally become consumer peer dependencies. Actual tarball-consumer manifests declare only better-nest-mq, Nest/support peers and the selected pg/Zod integrations. They do not list engine packages, even during PostgreSQL worker and distributed-control tests.

Library code and public declarations are compiled with TypeScript 6 and the primary compiler. The CI Node 22 and Node 24 jobs are separate compatibility environments, not two Node versions running inside the same application. Bun remains the package manager and test runner; applications select a supported execution runtime.

Keep optional native/schema imports behind their public integration subpaths. Keep every public declaration chunk free of Effect/Result/Layer/Runtime dependencies. When upgrading the internal engine, the library maintainer must qualify the complete resource, persistence, worker, controls and package-consumption suite instead of shifting version coordination onto Nest users.
