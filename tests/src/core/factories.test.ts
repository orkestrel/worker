import { describe, expect, it } from 'vitest'
import { createWorker } from '@src/core'
import { createRecorder } from '@orkestrel/test'

// src/core/factories.ts — createWorker wires up a working, typed interface end to end
// (AGENTS §16). createQueue / createPool / createDatabaseQueueStore / createMemoryQueueStore
// are @orkestrel/queue and @orkestrel/pool factories, covered by their own packages' tests.

describe('createWorker', () => {
	it('returns a working worker that runs the handler with a pooled resource', async () => {
		const worker = createWorker<number, number, string>({
			pool: { create: () => 9 },
			handler: (input, resource) => `${input}-${resource}`,
		})
		try {
			await expect(worker.enqueue(4)).resolves.toBe('4-9')
		} finally {
			await worker.destroy()
		}
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
		try {
			expect(worker.count).toBe(0)
			expect(worker.paused).toBe(false)
			expect(worker.stopped).toBe(false)

			await Promise.all([worker.enqueue(1), worker.enqueue(2), worker.enqueue(3)])
			// At most `concurrency` resources are ever created, reused across the three jobs.
			expect(created.count).toBeLessThanOrEqual(2)

			await worker.stop()
			expect(worker.stopped).toBe(true)
		} finally {
			await worker.destroy()
		}
	})
})
