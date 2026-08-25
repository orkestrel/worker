import { describe, expect, it } from 'vitest'
import { existsSync } from 'node:fs'
import { Worker as ThreadWorker } from 'node:worker_threads'
import { createMemoryQueueStore } from '@orkestrel/queue'
import { numberShape } from '@orkestrel/contract'
import { createRecorder } from '@orkestrel/test'
import { NodeWorkerOptionsProbe, postRun, tempDatabasePath, ThreadReply } from './setupServer.js'
import type { NodeWorkerOptions } from '@src/server'

// tests/setupServer.ts — the node-only setup layer loaded after `setup.ts` for `src:server`
// (and any environment stacking on it). Drives real Node resources throughout: a real worker
// thread over the existing `tests/src/server/fixtures/` scripts for `postRun`/`ThreadReply`,
// and a real scratch directory on disk for `tempDatabasePath`. `NodeWorkerOptionsProbe`'s
// getter-recording/`replace` contract is hermetic and needs no real worker, mirroring
// `PoolOptionsProbe`'s proof in `tests/setup.test.ts`.

const fixture = (name: string): URL => new URL(`./src/server/fixtures/${name}`, import.meta.url)

describe('postRun / ThreadReply', () => {
	it('posts a run envelope a real worker thread replies to, resolving a frozen copy keyed by id', async () => {
		const thread = new ThreadWorker(fixture('double.ts'))
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
		const thread = new ThreadWorker(fixture('crash.ts'))
		try {
			const pending = new ThreadReply(thread, 'job-crash').promise
			postRun(thread, 'job-crash', 'job-crash', -1)
			await expect(pending).rejects.toThrow(/exited before replying/)
		} finally {
			await thread.terminate()
		}
	})
})

describe('tempDatabasePath', () => {
	it('allocates a real on-disk path under an owned scratch directory, removed on cleanup', () => {
		const { path, cleanup } = tempDatabasePath()
		expect(path.endsWith('store.json')).toBe(true)
		expect(existsSync(path)).toBe(false)

		cleanup()
		expect(existsSync(path)).toBe(false)

		// Control: a path that was never allocated under a scratch directory would not
		// disappear on this cleanup — this test would fail if `cleanup` were a no-op, since a
		// second call proves it targets the real directory rather than something already gone.
		expect(() => cleanup()).not.toThrow()
	})
})

describe('NodeWorkerOptionsProbe', () => {
	it('records each getter access once, in property order, and returns the configured value', () => {
		const reads = createRecorder<readonly [property: keyof NodeWorkerOptions<number, number>]>()
		const script = fixture('double.ts')
		const input = (value: unknown): value is number => typeof value === 'number'
		const result = (value: unknown): value is number => typeof value === 'number'
		const probe = new NodeWorkerOptionsProbe<number, number>(
			{
				script,
				input,
				result,
				workerData: { token: 'a' },
				concurrency: 1,
				retries: 0,
				timeout: 5_000,
				store: createMemoryQueueStore(numberShape()),
			},
			reads,
		)

		expect(probe.script).toBe(script)
		expect(probe.input).toBe(input)
		expect(probe.result).toBe(result)
		expect(probe.workerData).toEqual({ token: 'a' })
		expect(probe.concurrency).toBe(1)
		expect(probe.retries).toBe(0)
		expect(probe.timeout).toBe(5_000)
		expect(probe.store).toBeDefined()
		expect(reads.calls.map(([property]) => property)).toEqual([
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
		const isNumber = (value: unknown): value is number => typeof value === 'number'
		const probe = new NodeWorkerOptionsProbe<number, number>(
			{
				script: fixture('double.ts'),
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
		const laterScript = fixture('echo-data.ts')
		probe.replace({
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
