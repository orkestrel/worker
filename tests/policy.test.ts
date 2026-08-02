import { globSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { isBrowserVuePath } from './setup.js'
import { inspectCodingLaw, inspectCodingWorkspace, inspectTSDocAliases } from './setupPolicy.js'

describe('repository coding law', () => {
	it('keeps Vue single-file components exclusively in browser environments', () => {
		const files = globSync('{app,src}/**/*.vue')

		expect(files.every(isBrowserVuePath)).toBe(true)
	})

	it('enforces source placement, exports, readonly contracts, and syntax law', () => {
		expect(inspectCodingWorkspace(process.cwd())).toEqual([])
	})

	it('rejects private aliases in TSDoc without rejecting source imports or ordinary comments', () => {
		const privateExample = `/**
 * @example
 * import { serveWorker } from '@src/server'
 */
export function inspectValue(value: unknown): unknown {
	return value
}`
		const allowedSource = `import type { Guard } from '@src/core'
// import { serveWorker } from '@src/server'
/* import { createWorker } from '@src/core' */
export function inspectValue(value: unknown): unknown {
	return value
}`

		expect(inspectTSDocAliases('src/core/helpers.ts', privateExample)).toEqual([
			'src/core/helpers.ts:1:1 forbids private @src/* imports in TSDoc examples',
		])
		expect(inspectCodingLaw('src/core/helpers.ts', privateExample)).toContain(
			'src/core/helpers.ts:1:1 forbids private @src/* imports in TSDoc examples',
		)
		expect(inspectTSDocAliases('src/core/helpers.ts', allowedSource)).toEqual([])
		expect(inspectCodingLaw('src/core/helpers.ts', allowedSource)).toEqual([])
	})
})
