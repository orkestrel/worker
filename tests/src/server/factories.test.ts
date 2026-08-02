import type { NodeWorkerOptions } from '@src/server'
import { describe, expect, it } from 'vitest'
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
import { createJSONQueueStore, createNodeWorker } from '@src/server'
import { createRecorder, createTeardown } from '../../setup.js'
import { NodeWorkerOptionsProbe, tempDatabasePath } from '../../setupServer.js'

// src/server/factories.ts — createJSONQueueStore over a real JSON file (node
// env, no mocks). Durability is the JSONDriver's job and the store engine is shared, so
// the proof is cross-INSTANCE: entries one store persists to a path are loaded by a
// SECOND store built over the SAME path — exactly how a queue resumes after a restart.

// Track each temp-dir `cleanup` thunk so it runs in afterEach even when an assertion throws — the
// shared §16.1 teardown registrar (the disposer just invokes the cleanup thunk).
const { track } = createTeardown((cleanup: () => void) => cleanup())
describe('createJSONQueueStore', () => {
	it('persists outstanding entries across store instances over the same file', async () => {
		const { path, cleanup } = tempDatabasePath()
		track(cleanup)

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
		const { path, cleanup } = tempDatabasePath()
		track(cleanup)

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
		const { path, cleanup } = tempDatabasePath()
		track(cleanup)

		const writer = createJSONQueueStore(path, stringShape())
		await writer.save({ id: 'a', input: 'a', attempts: 0 })
		await writer.save({ id: 'b', input: 'b', attempts: 0 })
		await writer.remove('a')

		const reader = createJSONQueueStore(path, stringShape())
		expect((await reader.load()).map((entry) => entry.id)).toEqual(['b'])
	})
})

describe('createNodeWorker', () => {
	it('round-trips a job over a real worker thread, then tears down', async () => {
		const worker = createNodeWorker({
			script: new URL('./fixtures/double.ts', import.meta.url),
			input: isNumber,
			result: isNumber,
		})
		try {
			await expect(worker.enqueue(21)).resolves.toBe(42)
		} finally {
			await worker.destroy()
		}
	})

	it('captures every option once and retains the snapshot across jobs', async () => {
		const reads =
			createRecorder<
				readonly [property: keyof NodeWorkerOptions<number, Readonly<Record<string, unknown>>>]
			>()
		const payload = { token: 'initial' }
		const probe = new NodeWorkerOptionsProbe<number, Readonly<Record<string, unknown>>>(
			{
				script: new URL('./fixtures/echo-data.ts', import.meta.url),
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
				script: new URL('./fixtures/double.ts', import.meta.url),
				input: literalOf(999),
				result: recordOf({ changed: isBoolean }),
				workerData: () => undefined,
				concurrency: 2,
				retries: 1,
				timeout: 1,
				store: createMemoryQueueStore(numberShape()),
			})

			await expect(worker.enqueue(1)).resolves.toEqual(payload)
			await expect(worker.enqueue(2)).resolves.toEqual(payload)
			expect(reads.count).toBe(8)
		} finally {
			await worker.destroy()
		}
	})
})
