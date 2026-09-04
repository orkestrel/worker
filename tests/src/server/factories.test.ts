import type { NodeWorkerOptions } from '@src/server'
import { afterEach, describe, expect, it } from 'vitest'
import {
	integerShape,
	isBoolean,
	isNumber,
	isRecord,
	literalOf,
	numberShape,
	objectShape,
	recordOf,
	stringShape,
} from '@orkestrel/contract'
import { createMemoryQueueStore } from '@orkestrel/queue'
import { createJSONQueueStore, createNodeWorker, createThread } from '@src/server'
import { createRecorder, createTeardown, waitForCondition } from '@orkestrel/test'
import {
	buildFixtureURL,
	createThrowingSuccess,
	NodeWorkerOptionsProbe,
	postRun,
	tempDatabasePath,
	ThreadReply,
} from '../../setupServer.js'

// src/server/factories.ts — createJSONQueueStore over a real JSON file (node
// env, no mocks). Durability is the JSONDriver's job and the store engine is shared, so
// the proof is cross-INSTANCE: entries one store persists to a path are loaded by a
// SECOND store built over the SAME path — exactly how a queue resumes after a restart.
// `createThread` and `createNodeWorker` are driven over real worker threads.

// Track each scratch disposer so it runs in afterEach even when an assertion throws — the
// shared teardown registrar.
const teardown = createTeardown()
afterEach(() => teardown.destroy())
describe('createJSONQueueStore', () => {
	it('persists outstanding entries across store instances over the same file', async () => {
		const { path, scratch } = tempDatabasePath()
		teardown.add(() => scratch.destroy())

		const writer = createJSONQueueStore(path, stringShape())
		await writer.save({ id: 'job-1', input: 'https://example.com', attempts: 0 })
		await writer.save({ id: 'job-2', input: 'https://example.org', attempts: 2 })

		// A fresh store over the same path — the prior process's outstanding work.
		const reader = createJSONQueueStore(path, stringShape())
		const outstanding = await reader.load()

		expect(outstanding.map((entry) => entry.id)).toEqual(['job-1', 'job-2'])
		expect(outstanding[1]).toEqual({ id: 'job-2', input: 'https://example.org', attempts: 2 })
	})

	it('round-trips a nested-object input through the JSON file', async () => {
		const { path, scratch } = tempDatabasePath()
		teardown.add(() => scratch.destroy())

		const writer = createJSONQueueStore(
			path,
			objectShape({ url: stringShape(), retries: integerShape({ min: 0 }) }),
		)
		await writer.save({
			id: 'job-1',
			input: { url: 'https://example.com', retries: 3 },
			attempts: 0,
		})

		const reader = createJSONQueueStore(
			path,
			objectShape({ url: stringShape(), retries: integerShape({ min: 0 }) }),
		)
		const [entry] = await reader.load()
		expect(entry?.input).toEqual({ url: 'https://example.com', retries: 3 })
		// Typed payload survives the JSON round-trip (no `as`): nested access compiles.
		expect(entry?.input.url).toBe('https://example.com')
	})

	it('reflects a removed entry across a reopen', async () => {
		const { path, scratch } = tempDatabasePath()
		teardown.add(() => scratch.destroy())

		const writer = createJSONQueueStore(path, stringShape())
		await writer.save({ id: 'a', input: 'a', attempts: 0 })
		await writer.save({ id: 'b', input: 'b', attempts: 0 })
		await writer.remove('a')

		const reader = createJSONQueueStore(path, stringShape())
		expect((await reader.load()).map((entry) => entry.id)).toEqual(['b'])
	})
})

describe('createThread', () => {
	it('resolves a live thread and clones its `workerData` across at spawn', async () => {
		const thread = await createThread(buildFixtureURL('echo-data.ts'), { token: 'spawned' })
		try {
			expect(thread.alive).toBe(true)
			expect(thread.death).toBeUndefined()
			expect(thread.worker.threadId).toBeGreaterThan(0)

			// The second argument reaches the worker side intact — the fixture echoes back the
			// value it was handed at spawn, so a dropped argument fails this assertion.
			const reply = new ThreadReply(thread.worker, 'spawn-1').promise
			postRun(thread.worker, 'spawn-1', 'spawn-1', 0)
			expect(await reply).toEqual({ id: 'spawn-1', ok: true, value: { token: 'spawned' } })
		} finally {
			await thread.worker.terminate()
		}
	})

	it('spawns with the `workerData` argument omitted', async () => {
		const thread = await createThread(buildFixtureURL('double.ts'))
		try {
			expect(thread.alive).toBe(true)
		} finally {
			await thread.worker.terminate()
		}
	})

	it('latches the death of a script that fails to resolve', async () => {
		// A thread bootstraps and reports `online` BEFORE it fails to resolve its module, so the
		// spawn resolves a live thread that dies immediately after. The persistent listeners the
		// spawn attaches are what make that death observable: `death` latches and `alive` flips,
		// so a pool validating this thread drops it instead of leasing a corpse.
		const thread = await createThread(buildFixtureURL('does-not-exist.ts'))
		try {
			await waitForCondition('the thread latches its death', () => thread.death !== undefined, {
				budget: 5_000,
			})
			expect(thread.alive).toBe(false)
		} finally {
			await thread.worker.terminate()
		}
	})
})

describe('createNodeWorker', () => {
	it('round-trips a job over a real worker thread, then tears down', async () => {
		const worker = createNodeWorker({
			script: buildFixtureURL('double.ts'),
			input: isNumber,
			result: isNumber,
		})
		try {
			await expect(worker.enqueue(21)).resolves.toBe(42)
		} finally {
			await worker.destroy()
		}
	})

	it('wires the `on` hooks at construction and routes a listener throw to `error`', async () => {
		const settled = Promise.withResolvers<readonly [string, number]>()
		const failures = createRecorder<readonly [unknown]>()
		const worker = createNodeWorker({
			on: {
				success: createThrowingSuccess(settled.resolve),
			},
			error: failures.handler,
			script: buildFixtureURL('double.ts'),
			input: isNumber,
			result: isNumber,
		})
		try {
			await expect(worker.enqueue(21, { id: 'hooked' })).resolves.toBe(42)
			expect(await settled.promise).toEqual(['hooked', 42])
			// The initial listener ran at construction, so its throw reached the supplied handler
			// rather than being swallowed by an emitter built with neither hook.
			expect(failures.count).toBe(1)
			expect(failures.calls[0]?.[0]).toBeInstanceOf(Error)
		} finally {
			await worker.destroy()
		}
	})

	it('captures every option once and retains the snapshot across jobs', async () => {
		const reads =
			createRecorder<
				readonly [property: keyof NodeWorkerOptions<number, Readonly<Record<string, unknown>>>]
			>()
		const errors = createRecorder<readonly [unknown]>()
		const payload = { token: 'initial' }
		const probe = new NodeWorkerOptionsProbe<number, Readonly<Record<string, unknown>>>(
			{
				on: {},
				error: errors.handler,
				script: buildFixtureURL('echo-data.ts'),
				input: isNumber,
				result: isRecord,
				workerData: payload,
				concurrency: 1,
				retries: 0,
				timeout: 5_000,
				store: createMemoryQueueStore(numberShape()),
			},
			reads,
		)
		const worker = createNodeWorker(probe)
		try {
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
			probe.replace({
				on: {},
				error: errors.handler,
				script: buildFixtureURL('double.ts'),
				input: literalOf(999),
				result: recordOf({ changed: isBoolean }),
				workerData: Promise.resolve(),
				concurrency: 2,
				retries: 1,
				timeout: 1,
				store: createMemoryQueueStore(numberShape()),
			})

			await expect(worker.enqueue(1)).resolves.toEqual(payload)
			await expect(worker.enqueue(2)).resolves.toEqual(payload)
			expect(reads.count).toBe(10)
		} finally {
			await worker.destroy()
		}
	})
})
