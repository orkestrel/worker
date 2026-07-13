import type { UserConfig } from 'vite'
import { defineConfig, mergeConfig } from 'vitest/config'
import tsconfig from './tsconfig.json' with { type: 'json' }
import { fileURLToPath, URL } from 'node:url'
import { readFileSync, writeFileSync } from 'node:fs'

export function resolveWorkspacePath(relativePath: string): string {
	return fileURLToPath(new URL(relativePath, import.meta.url))
}

/**
 * Normalize the cross-entry core-type imports in a bundled declaration entry.
 *
 * The browser and server libs mark `@src/core` external and reference the sibling
 * `dist/src/core` build instead of inlining it (see `srcBrowser` / `srcServer`).
 * api-extractor (via vite-plugin-dts `bundleTypes`) collects those imports verbatim
 * from the per-file pre-bundle emit, where the `@src/core` alias resolves to the
 * source path — so the rolled-up entry ends up importing core types from
 * `../core/index.ts` / `../../core/index.ts` (a `.ts` extension that isn't shipped,
 * at depths relative to each source file rather than the final bundle). Rewrite every
 * such specifier to the shipped ESM declaration entry `../core/index.js`, matching the
 * JS output's `paths: { '@src/core': '../core/index.js' }` rewrite so `tsc` resolves
 * the sibling `dist/src/core/index.d.ts` from the bundled entry.
 *
 * Runs as the vite-plugin-dts `afterBuild` hook — after api-extractor has written the
 * final bundled entry — so it never perturbs api-extractor's own module resolution.
 */
export function rewriteCoreEntry(outDir: string): () => void {
	return () => {
		const file = resolveWorkspacePath(`${outDir}/index.d.ts`)
		const content = readFileSync(file, 'utf8')
		const fixed = content.replace(
			/(['"])(?:@src\/core|(?:\.\.\/)+core\/index(?:\.d)?\.[cm]?ts)\1/g,
			"'../core/index.js'",
		)
		if (fixed !== content) writeFileSync(file, fixed)
	}
}

const resolve = {
	alias: Object.entries(tsconfig.compilerOptions.paths).reduce(
		(a, [k, v]) => Object.assign(a, { [k]: resolveWorkspacePath(v[0]) }),
		{},
	),
}

// Base: shared resolve + build defaults + src:core tests.
export const srcCore = (config?: UserConfig): UserConfig =>
	mergeConfig(
		{
			resolve,
			build: {
				emptyOutDir: true,
				sourcemap: true,
				minify: false,
			},
			test: {
				name: { label: 'src:core', color: 'magenta' },
				include: ['tests/src/core/**/*.test.ts'],
				setupFiles: ['./tests/setup.ts'],
				environment: 'node',
				browser: { enabled: false },
			},
		},
		config ?? {},
	)

// Extends srcCore: the guides-parity suite. Node env — it reads the real
// guides/*.md and the documented source modules off disk — but resolves like core tests.
export const guides = (config?: UserConfig): UserConfig =>
	srcCore(
		mergeConfig(
			{
				test: {
					name: { label: 'guides', color: 'green' },
					include: ['tests/guides/**/*.test.ts'],
					exclude: ['tests/src/**/*.test.ts', 'tests/setup.test.ts'],
				},
			},
			config ?? {},
		),
	)

// Extends srcCore: server-only library (`src/server`, e.g. the JSON file driver
// and, later, the SQLite driver over node:sqlite). Builds a dual ESM+CJS lib for
// Node and runs its tests in the node environment. Externalizes `node:*` (so
// node:sqlite is never bundled) AND `@src/core` → the sibling `dist/src/core`
// build (format-aware: `../core/index.js` for the ESM output, `../core/index.cjs`
// for the CJS output), exactly as core ships dual-format. Build-only — the
// test project resolves `@src/core` from source through the shared `resolve` alias.
export const srcServer = (config?: UserConfig): UserConfig =>
	srcCore(
		mergeConfig(
			{
				build: {
					lib: {
						entry: resolveWorkspacePath('src/server/index.ts'),
						formats: ['es', 'cjs'],
						fileName: (format: string) => (format === 'es' ? 'index.js' : 'index.cjs'),
					},
					outDir: 'dist/src/server',
					target: 'node24',
					rolldownOptions: {
						external: (id: string) =>
							id === '@src/core' || id.startsWith('@orkestrel/') || id.startsWith('node:'),
						output: [
							{
								format: 'es',
								entryFileNames: 'index.js',
								paths: { '@src/core': '../core/index.js' },
							},
							{
								format: 'cjs',
								entryFileNames: 'index.cjs',
								paths: { '@src/core': '../core/index.cjs' },
							},
						],
					},
				},
				test: {
					name: { label: 'src:server', color: 'red' },
					include: ['tests/src/server/**/*.test.ts'],
					exclude: ['tests/src/core/**/*.test.ts'],
					setupFiles: ['./tests/setup.ts', './tests/setupServer.ts'],
				},
			},
			config ?? {},
		),
	)

export default defineConfig({
	resolve,
	test: {
		projects: [srcCore, srcServer, guides],
	},
})
