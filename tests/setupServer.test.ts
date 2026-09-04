import type { NodeWorkerOptions } from '@src/server'
import { describe, expect, it } from 'vitest'
import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { Worker as ThreadWorker } from 'node:worker_threads'
import { createMemoryQueueStore } from '@orkestrel/queue'
import { isNumber, numberShape } from '@orkestrel/contract'
import { createRecorder } from '@orkestrel/test'
import {
	buildFixtureURL,
	NodeWorkerOptionsProbe,
	postRun,
	tempDatabasePath,
	ThreadReply,
} from './setupServer.js'

// tests/setupServer.ts — the node-only setup layer loaded after `setup.ts` for `src:server`
// (and any environment stacking on it). Drives real Node resources throughout: a real worker
// thread over the existing `tests/src/server/fixtures/` scripts for `postRun`/`ThreadReply`,
// a real on-disk file for `buildFixtureURL`, and a real scratch directory for
// `tempDatabasePath`. `NodeWorkerOptionsProbe`'s getter-recording/`replace` contract is
// hermetic and needs no real worker, mirroring `PoolOptionsProbe`'s proof in
// `tests/setup.test.ts`.

describe('postRun / ThreadReply', () => {
	it('posts a run envelope a real worker thread replies to, resolving a frozen copy keyed by id', async () => {
		const thread = new ThreadWorker(buildFixtureURL('double.ts'))
		try {
			const pending = new ThreadReply(thread, 'job-1').promise
			postRun(thread, 'job-1', 'job-1', 21)
			const reply = await pending
			expect(reply).toEqual({ id: 'job-1', ok: true, value: 42 })
			expect(Object.isFrozen(reply)).toBe(true)

			// Control: the assertion catches a broken envelope — a wrong id/value pair fails.
			expect(reply).not.toEqual({ id: 'job-1', ok: true, value: 43 })
		} finally {
			await thread.terminate()
		}
	})

	it('rejects when the worker thread exits before it replies to the pending id', async () => {
		const thread = new ThreadWorker(buildFixtureURL('crash.ts'))
		try {
			const pending = new ThreadReply(thread, 'job-crash').promise
			postRun(thread, 'job-crash', 'job-crash', -1)
			await expect(pending).rejects.toThrow(/exited before replying/)
		} finally {
			await thread.terminate()
		}
	})
})

describe('buildFixtureURL', () => {
	it('resolves a real worker fixture under the workspace tests directory', () => {
		const url = buildFixtureURL('double.ts')
		expect(url.href.endsWith('tests/src/server/fixtures/double.ts')).toBe(true)
		expect(existsSync(fileURLToPath(url))).toBe(true)

		// Control: the existence assertion is not vacuous — a name with no fixture behind it
		// resolves into the same directory and is absent on disk.
		expect(existsSync(fileURLToPath(buildFixtureURL('does-not-exist.ts')))).toBe(false)
	})
})

describe('tempDatabasePath', () => {
	it('allocates a real on-disk path under an owned scratch directory its scratch removes', () => {
		const { path, scratch } = tempDatabasePath()
		expect(path.endsWith('store.json')).toBe(true)
		expect(existsSync(path)).toBe(false)
		expect(existsSync(scratch.path)).toBe(true)

		scratch.destroy()
		expect(existsSync(scratch.path)).toBe(false)

		// Control: the returned scratch owns a real directory rather than a no-op disposer — a
		// second destroy over the same removed directory still settles without throwing.
		expect(() => scratch.destroy()).not.toThrow()
	})
})

describe('NodeWorkerOptionsProbe', () => {
	it('records each getter access once, in property order, and returns the configured value', () => {
		const reads = createRecorder<readonly [property: keyof NodeWorkerOptions<number, number>]>()
		const script = buildFixtureURL('double.ts')
		const on = {}
		const errors = createRecorder<readonly [unknown]>()
		const probe = new NodeWorkerOptionsProbe<number, number>(
			{
				on,
				error: errors.handler,
				script,
				input: isNumber,
				result: isNumber,
				workerData: { token: 'a' },
				concurrency: 1,
				retries: 0,
				timeout: 5_000,
				store: createMemoryQueueStore(numberShape()),
			},
			reads,
		)

		expect(probe.on).toBe(on)
		expect(probe.error).toBe(errors.handler)
		expect(probe.script).toBe(script)
		expect(probe.input).toBe(isNumber)
		expect(probe.result).toBe(isNumber)
		expect(probe.workerData).toEqual({ token: 'a' })
		expect(probe.concurrency).toBe(1)
		expect(probe.retries).toBe(0)
		expect(probe.timeout).toBe(5_000)
		expect(probe.store).toBeDefined()
		expect(reads.calls.map(([property]) => property)).toEqual([
			'on',
			'error',
			'script',
			'input',
			'result',
			'workerData',
			'concurrency',
			'retries',
			'timeout',
			'store',
		])
	})

	it('replace swaps the values every subsequent getter read returns', () => {
		const reads = createRecorder<readonly [property: keyof NodeWorkerOptions<number, number>]>()
		const errors = createRecorder<readonly [unknown]>()
		const probe = new NodeWorkerOptionsProbe<number, number>(
			{
				on: {},
				error: errors.handler,
				script: buildFixtureURL('double.ts'),
				input: isNumber,
				result: isNumber,
				workerData: undefined,
				concurrency: 1,
				retries: 0,
				timeout: 1,
				store: createMemoryQueueStore(numberShape()),
			},
			reads,
		)
		const laterScript = buildFixtureURL('echo-data.ts')
		probe.replace({
			on: {},
			error: errors.handler,
			script: laterScript,
			input: isNumber,
			result: isNumber,
			workerData: { token: 'b' },
			concurrency: 2,
			retries: 1,
			timeout: 2,
			store: createMemoryQueueStore(numberShape()),
		})

		expect(probe.script).toBe(laterScript)
		expect(probe.concurrency).toBe(2)
	})
})
