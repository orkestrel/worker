import { describe, expect, it } from 'vitest'
import {
	createDatabaseQueueStore,
	createMemoryDriver,
	createMemoryQueueStore,
	createPool,
	createQueue,
	createWorker,
	stringShape,
} from '@src/core'
import { createRecorder, waitForDelay } from '../../../setup.js'

// src/core/workers/factories.ts — createQueue / createPool / createWorker each wire up
// a working, typed interface end to end (AGENTS §16).

describe('createQueue', () => {
	it('returns a working queue that runs the handler and resolves', async () => {
		const queue = createQueue<number, number>({ handler: (input) => input + 1 })
		await expect(queue.enqueue(41)).resolves.toBe(42)
	})

	it('honours concurrency, retries, and a default timeout', async () => {
		const attempts = createRecorder<[]>()
		const queue = createQueue<undefined, string>({
			concurrency: 2,
			retries: 1,
			timeout: 1_000,
			handler: () => {
				attempts.handler()
				if (attempts.count < 2) throw new Error('retry me')
				return 'done'
			},
		})
		await expect(queue.enqueue(undefined)).resolves.toBe('done')
		expect(attempts.count).toBe(2)
	})

	it('exposes the live status surface (count / active / paused / stopped)', async () => {
		const queue = createQueue<number, number>({ handler: (input) => input })
		expect(queue.count).toBe(0)
		expect(queue.active).toBe(0)
		expect(queue.paused).toBe(false)
		expect(queue.stopped).toBe(false)

		queue.pause()
		const pending = queue.enqueue(1)
		expect(queue.paused).toBe(true)
		expect(queue.count).toBe(1)

		queue.stop()
		await expect(pending).rejects.toThrow('stopped')
		expect(queue.stopped).toBe(true)
	})
})

describe('createPool', () => {
	it('returns a working pool that leases, reuses on release, and reports counts', async () => {
		let next = 0
		const pool = createPool<number>({ create: () => next++, max: 1 })
		expect(pool.size).toBe(0)

		const token = await pool.acquire()
		expect(token.value).toBe(0)
		expect(pool.active).toBe(1)

		token.release()
		expect(pool.idle).toBe(1)
		// Reuses the idle resource — no second create.
		const again = await pool.acquire()
		expect(again.value).toBe(0)
	})

	it('parks an acquire at max and serves it on release (FIFO)', async () => {
		const pool = createPool<number>({ create: () => 0, max: 1 })
		const held = await pool.acquire()
		const waiting = pool.acquire()
		await waitForDelay(10)
		expect(pool.active).toBe(1)

		held.release()
		const served = await waiting
		expect(served.value).toBe(0)
	})
})

describe('createWorker', () => {
	it('returns a working worker that runs the handler with a pooled resource', async () => {
		const worker = createWorker<number, number, string>({
			pool: { create: () => 9 },
			handler: (input, resource) => `${input}-${resource}`,
		})
		await expect(worker.enqueue(4)).resolves.toBe('4-9')
	})

	it('honours concurrency, reuses resources, and exposes the status surface', async () => {
		const created = createRecorder<[]>()
		const worker = createWorker<number, number, number>({
			concurrency: 2,
			pool: {
				create: () => {
					created.handler()
					return 0
				},
			},
			handler: (input) => input,
		})
		expect(worker.count).toBe(0)
		expect(worker.paused).toBe(false)
		expect(worker.stopped).toBe(false)

		await Promise.all([worker.enqueue(1), worker.enqueue(2), worker.enqueue(3)])
		// At most `concurrency` resources are ever created, reused across the three jobs.
		expect(created.count).toBeLessThanOrEqual(2)

		worker.stop()
		expect(worker.stopped).toBe(true)
	})
})

describe('createDatabaseQueueStore', () => {
	it('returns a working store over an injected driver that round-trips entries', async () => {
		const store = createDatabaseQueueStore(stringShape(), createMemoryDriver())
		await store.save({ id: 'job-1', input: 'task', attempts: 0 })
		const outstanding = await store.load()
		expect(outstanding).toEqual([{ id: 'job-1', input: 'task', attempts: 0 }])
	})
})

describe('createMemoryQueueStore', () => {
	it('returns a working memory-backed store that round-trips entries', async () => {
		const store = createMemoryQueueStore(stringShape())
		await store.save({ id: 'job-1', input: 'task', attempts: 0 })
		await store.save({ id: 'job-2', input: 'other', attempts: 1 })
		const outstanding = await store.load()
		expect(outstanding.map((entry) => entry.id)).toEqual(['job-1', 'job-2'])
		expect(outstanding[1]).toEqual({ id: 'job-2', input: 'other', attempts: 1 })
	})
})
