import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: { index: 'src/index.ts', zod: 'src/integrations/zod.ts', postgres: 'src/integrations/postgres.ts' },
  format: ['esm'],
  platform: 'node',
  target: 'es2023',
  dts: true,
  sourcemap: true,
  clean: true,
  treeshake: true,
  deps: { neverBundle: [/^@nestjs\//, /^better-effect(?:$|[/-])/, 'better-result', 'reflect-metadata', 'rxjs', 'zod', 'pg'] }
})
