import type { PoolOptions } from '@orkestrel/pool'
import type { WorkerEventMap } from '@src/core'
import { afterEach, describe, expect, it } from 'vitest'
import { stringShape } from '@orkestrel/contract'
import { isPoolError } from '@orkestrel/pool'
import { createMemoryQueueStore, isQueueError } from '@orkestrel/queue'
import { Worker } from '@src/core'
import {
	createRecorder,
	createRecorders,
	createResourceFactory,
	createTeardown,
	waitForDelay,
} from '@orkestrel/test'
import { PoolOptionsProbe, TestQueueStore } from '../../setup.js'

type DestroyableWorker = {
	readonly emitter: { readonly destroyed: boolean }
	destroy(): Promise<void>
}

const teardown = createTeardown()
afterEach(() => teardown.destroy())

function track<T extends DestroyableWorker>(worker: T): T {
	teardown.add(() => (worker.emitter.destroyed ? undefined : worker.destroy()))
	return worker
}

// src/core/workers/Worker.ts — the Queue⨉Pool facade. Real behaviour, no mocks: a
// counting `create` hook proves resources are reused and never exceed the pool max,
// gates pin jobs in flight so the cap is observable, and a throwing handler proves the
// acquired resource is released in the `finally` and reused by a later job (AGENTS §16).
// Beyond the per-feature cases, production-grade sections cover: the resource bound
// under saturation (40 jobs through 3 slots — at most 3 resources), a burst of failing
// handlers never starving the pool, the pool-max-vs-queue-concurrency mismatch (real
// parallelism = min(concurrency, pool.max), in both directions), and destroy mid-flight
// tearing down queue AND pool together (in-flight aborted, pending rejected, every
// pooled resource destroyed). The shared `createResourceFactory` (`@orkestrel/test`) hands
// out the monotonically-numbered resources plus its `created` / `destroyed` recorders.

describe('Worker — runs the handler with a pooled resource', () => {
	it('passes the input, a leased resource, and the execution to the handler', async () => {
		const seen = createRecorder<[number, number]>()
		const worker = new Worker<number, number, string>({
			pool: { create: () => 7 },
			handler: (input, resource, execution) => {
				seen.handler(input, resource)
				expect(execution.signal).toBeInstanceOf(AbortSignal)
				return `${input}:${resource}`
			},
		})
		track(worker)
		await expect(worker.enqueue(3)).resolves.toBe('3:7')
		expect(seen.calls).toEqual([[3, 7]])
	})
})

describe('Worker — queue option validation', () => {
	it('passes invalid concurrency to the queue instead of normalizing it', () => {
		const invalid = [
			0,
			-1,
			1.5,
			Number.NaN,
			Number.NEGATIVE_INFINITY,
			Number.POSITIVE_INFINITY,
			Number.MAX_SAFE_INTEGER + 1,
		]
		for (const concurrency of invalid) {
			let failure: unknown
			try {
				const normalized = new Worker<undefined, number, void>({
					concurrency,
					pool: { create: () => 0, max: 0 },
					handler: () => {},
				})
				void normalized
			} catch (error) {
				failure = error
			}
			if (!isQueueError(failure)) throw new Error('expected queue concurrency validation')
			expect(failure.code).toBe('invalid')
			expect(failure.context).toEqual({ option: 'concurrency', value: concurrency })
		}
	})

	it('preserves the queue integer timeout contract at the worker boundary', async () => {
		let worker: Worker<undefined, number, void> | undefined
		let failure: unknown
		try {
			worker = new Worker({
				timeout: 0.5,
				pool: { create: () => 0 },
				handler: () => {},
			})
		} catch (error) {
			failure = error
		}
		try {
			if (!isQueueError(failure)) throw new Error('expected queue timeout validation')
			expect(failure.code).toBe('invalid')
			expect(failure.context).toEqual({ option: 'timeout', value: 0.5 })
		} finally {
			await worker?.destroy()
		}
	})

	it('passes runtime null concurrency to the queue with its exact diagnostic context', async () => {
		const options = {
			pool: { create: () => 0 },
			handler: () => {},
		}
		Object.defineProperty(options, 'concurrency', { enumerable: true, value: null })
		let worker: Worker<undefined, number, void> | undefined
		let failure: unknown
		try {
			worker = new Worker(options)
		} catch (error) {
			failure = error
		}
		try {
			if (!isQueueError(failure)) throw new Error('expected queue concurrency validation')
			expect(failure.code).toBe('invalid')
			expect(failure.context).toEqual({ option: 'concurrency', value: null })
		} finally {
			await worker?.destroy()
		}
	})

	it('validates queue concurrency before reading a hostile pool getter', () => {
		const poolReads = createRecorder<[]>()
		const hostile = new Error('hostile pool getter')
		const options = {
			concurrency: 0,
			handler: () => {},
			get pool(): PoolOptions<number> {
				poolReads.handler()
				throw hostile
			},
		}
		let failure: unknown
		try {
			const blocked = new Worker<undefined, number, void>(options)
			void blocked
		} catch (error) {
			failure = error
		}
		if (!isQueueError(failure)) throw new Error('expected queue concurrency validation')
		expect(failure.code).toBe('invalid')
		expect(failure.context).toEqual({ option: 'concurrency', value: 0 })
		expect(poolReads.count).toBe(0)
	})
})

describe('Worker constructor option boundaries', () => {
	it('passes runtime null pool max to the pool with its exact diagnostic context', async () => {
		const pool = { create: () => 0 }
		Object.defineProperty(pool, 'max', { enumerable: true, value: null })
		let worker: Worker<undefined, number, void> | undefined
		let failure: unknown
		try {
			worker = new Worker({ pool, handler: () => {} })
		} catch (error) {
			failure = error
		}
		try {
			if (!isPoolError(failure)) throw new Error('expected pool maximum validation')
			expect(failure.code).toBe('invalid')
			expect(failure.context).toEqual({ value: null })
		} finally {
			await worker?.destroy()
		}
	})

	it('defaults explicit undefined concurrency and pool max and executes real jobs', async () => {
		const { create, created } = createResourceFactory()
		const first = Promise.withResolvers<void>()
		const second = Promise.withResolvers<void>()
		const pool = { create }
		Object.defineProperty(pool, 'max', { enumerable: true, value: undefined })
		const options = {
			pool,
			handler: async (input: number) => {
				if (input === 1) await first.promise
				else await second.promise
				return input
			},
		}
		Object.defineProperty(options, 'concurrency', { enumerable: true, value: undefined })
		const worker = new Worker<number, number, number>(options)
		track(worker)
		const work = Promise.all([worker.enqueue(1), worker.enqueue(2)])
		try {
			await waitForDelay(10)
			expect(worker.active).toBe(1)
			expect(created.count).toBe(1)
			first.resolve()
			await waitForDelay(10)
			expect(created.count).toBe(1)
			second.resolve()
			await expect(work).resolves.toEqual([1, 2])
		} finally {
			first.resolve()
			second.resolve()
			await Promise.allSettled([work])
			await worker.destroy()
		}
	})

	it('snapshots every volatile caller option once and retains the initial behavior', async () => {
		const reads = createRecorder<[string]>()
		const initialHandler = createRecorder<[number, number]>()
		const laterHandler = createRecorder<[number, number]>()
		const initialCreate = createRecorder<[]>()
		const laterCreate = createRecorder<[]>()
		const initialSuccess = createRecorder<[string, number]>()
		const laterSuccess = createRecorder<[string, number]>()
		const initialErrors = createRecorder<readonly [error: unknown, event: string]>()
		const laterErrors = createRecorder<readonly [error: unknown, event: string]>()
		const initialSaves = createRecorder<[]>()
		const laterSaves = createRecorder<[]>()
		const initialStore = new TestQueueStore<number>({ save: () => initialSaves.handler() })
		const laterStore = new TestQueueStore<number>({ save: () => laterSaves.handler() })
		const first = Promise.withResolvers<void>()
		const second = Promise.withResolvers<void>()
		const listenerFailure = new Error('volatile listener failed')
		let concurrencyReads = 0
		let handlerReads = 0
		let onReads = 0
		let errorReads = 0
		let retriesReads = 0
		let timeoutReads = 0
		let storeReads = 0
		let poolReads = 0
		let maxReads = 0
		const initialPool = {
			create: () => {
				initialCreate.handler()
				return 7
			},
			get max() {
				maxReads += 1
				reads.handler('max')
				return maxReads === 1 ? 1 : 0
			},
		}
		const laterPool = {
			create: () => {
				laterCreate.handler()
				return -1
			},
			max: 0,
		}
		const options = {
			get concurrency() {
				concurrencyReads += 1
				reads.handler('concurrency')
				return concurrencyReads === 1 ? 2 : 0
			},
			get handler() {
				handlerReads += 1
				reads.handler('handler')
				return handlerReads === 1
					? async (input: number, resource: number) => {
							initialHandler.handler(input, resource)
							if (input === 1) await first.promise
							else await second.promise
							return input + resource
						}
					: (input: number, resource: number) => {
							laterHandler.handler(input, resource)
							return -1
						}
			},
			get on() {
				onReads += 1
				reads.handler('on')
				return onReads === 1
					? { success: initialSuccess.handler }
					: { success: laterSuccess.handler }
			},
			get error() {
				errorReads += 1
				reads.handler('error')
				return errorReads === 1 ? initialErrors.handler : laterErrors.handler
			},
			get retries() {
				retriesReads += 1
				reads.handler('retries')
				return retriesReads === 1 ? 0 : -1
			},
			get timeout() {
				timeoutReads += 1
				reads.handler('timeout')
				return timeoutReads === 1 ? 0 : -1
			},
			get store() {
				storeReads += 1
				reads.handler('store')
				return storeReads === 1 ? initialStore : laterStore
			},
			get pool() {
				poolReads += 1
				reads.handler('pool')
				return poolReads === 1 ? initialPool : laterPool
			},
		}
		const worker = new Worker<number, number, number>(options)
		track(worker)
		worker.emitter.on('success', () => {
			throw listenerFailure
		})
		const work = Promise.all([
			worker.enqueue(1, { id: 'first' }),
			worker.enqueue(2, { id: 'second' }),
		])
		try {
			await waitForDelay(10)
			expect(reads.calls).toEqual([
				['concurrency'],
				['handler'],
				['on'],
				['error'],
				['retries'],
				['timeout'],
				['store'],
				['pool'],
				['max'],
			])
			expect(initialHandler.calls).toEqual([[1, 7]])
			expect(laterHandler.count).toBe(0)
			expect(initialCreate.count).toBe(1)
			expect(laterCreate.count).toBe(0)
			first.resolve()
			await waitForDelay(10)
			expect(initialHandler.calls).toEqual([
				[1, 7],
				[2, 7],
			])
			expect(initialCreate.count).toBe(1)
			second.resolve()
			await expect(work).resolves.toEqual([8, 9])
			expect(initialSuccess.calls).toEqual([
				['first', 8],
				['second', 9],
			])
			expect(laterSuccess.count).toBe(0)
			expect(initialErrors.calls).toEqual([
				[listenerFailure, 'success'],
				[listenerFailure, 'success'],
			])
			expect(laterErrors.count).toBe(0)
			expect(initialSaves.count).toBe(2)
			expect(laterSaves.count).toBe(0)
		} finally {
			first.resolve()
			second.resolve()
			await Promise.allSettled([work])
			await worker.destroy()
		}
	})

	it('snapshots every prototype-backed pool option once and preserves its behavior', async () => {
		const reads = createRecorder<readonly [property: keyof PoolOptions<number>]>()
		const created = createRecorder<[]>()
		const destroyed = createRecorder<[number]>()
		const validated = createRecorder<[number]>()
		const events = createRecorder<[string]>()
		const errors = createRecorder<readonly [error: unknown, event: string]>()
		const laterCreated = createRecorder<[]>()
		const laterDestroyed = createRecorder<[number]>()
		const laterValidated = createRecorder<[number]>()
		const laterEvents = createRecorder<[string]>()
		const laterErrors = createRecorder<readonly [error: unknown, event: string]>()
		const listenerFailure = new Error('pool listener failed')
		const initial: Required<PoolOptions<number>> = {
			max: 1,
			on: {
				create: () => {
					events.handler('create')
					throw listenerFailure
				},
				acquire: () => events.handler('acquire'),
				release: () => events.handler('release'),
				destroy: () => events.handler('destroy'),
			},
			error: errors.handler,
			create: () => {
				created.handler()
				return 7
			},
			destroy: destroyed.handler,
			validate: (value) => {
				validated.handler(value)
				return true
			},
		}
		const later: Required<PoolOptions<number>> = {
			max: 2,
			on: { create: () => laterEvents.handler('create') },
			error: laterErrors.handler,
			create: () => {
				laterCreated.handler()
				return -1
			},
			destroy: laterDestroyed.handler,
			validate: (value) => {
				laterValidated.handler(value)
				return false
			},
		}
		const pool = new PoolOptionsProbe(initial, reads)
		const worker = new Worker<number, number, number>({
			pool,
			handler: (input, resource) => input + resource,
		})
		track(worker)
		pool.replace(later)

		await expect(worker.enqueue(1)).resolves.toBe(8)
		await expect(worker.enqueue(2)).resolves.toBe(9)
		await expect(worker.destroy()).resolves.toBeUndefined()
		expect(reads.calls).toEqual([['max'], ['on'], ['error'], ['create'], ['destroy'], ['validate']])
		expect(created.count).toBe(1)
		expect(destroyed.calls).toEqual([[7]])
		expect(validated.calls).toEqual([[7]])
		expect(events.calls).toEqual([
			['create'],
			['acquire'],
			['release'],
			['acquire'],
			['release'],
			['destroy'],
		])
		expect(errors.calls).toEqual([[listenerFailure, 'create']])
		expect(laterCreated.count).toBe(0)
		expect(laterDestroyed.count).toBe(0)
		expect(laterValidated.count).toBe(0)
		expect(laterEvents.count).toBe(0)
		expect(laterErrors.count).toBe(0)
	})
})

describe('Worker — resource reuse + the pool cap', () => {
	it('reuses resources across jobs and never creates more than the pool max', async () => {
		const { create, created } = createResourceFactory()
		const first = Promise.withResolvers<void>()
		const second = Promise.withResolvers<void>()
		const third = Promise.withResolvers<void>()
		const fourth = Promise.withResolvers<void>()
		const gates = [first, second, third, fourth]
		const worker = new Worker<number, number, void>({
			concurrency: 2,
			pool: { create },
			handler: async (input) => {
				const gate = gates[input]
				if (gate === undefined) throw new Error('missing gate')
				await gate.promise
			},
		})
		track(worker)

		const all = [0, 1, 2, 3].map((input) => worker.enqueue(input))
		const settlement = Promise.allSettled(all)
		try {
			await waitForDelay(10)
			// Two in flight → at most two resources created.
			expect(created.count).toBe(2)
			expect(worker.active).toBe(2)

			// Finish the first two; the next two reuse the freed resources — still only two.
			first.resolve()
			second.resolve()
			await waitForDelay(10)
			expect(created.count).toBe(2)

			third.resolve()
			fourth.resolve()
			await Promise.all(all)
			expect(created.count).toBe(2)
			expect(worker.active).toBe(0)
		} finally {
			first.resolve()
			second.resolve()
			third.resolve()
			fourth.resolve()
			await settlement
		}
	})
})

describe('Worker — release on throw', () => {
	it('releases the resource even when the handler throws, so a later job reuses it', async () => {
		const { create, created } = createResourceFactory()
		let attempt = 0
		const worker = new Worker<undefined, number, string>({
			concurrency: 1,
			pool: { create },
			handler: () => {
				attempt += 1
				if (attempt === 1) throw new Error('boom')
				return 'recovered'
			},
		})
		track(worker)

		await expect(worker.enqueue(undefined)).rejects.toThrow('boom')
		// The second job reuses the released resource — no second create.
		await expect(worker.enqueue(undefined)).resolves.toBe('recovered')
		expect(created.count).toBe(1)
	})
})

describe('Worker — lifecycle delegation', () => {
	it('pause / resume suspend and continue dequeuing', async () => {
		const started = createRecorder<[number]>()
		const worker = new Worker<number, number, number>({
			pool: { create: () => 0 },
			handler: (input) => {
				started.handler(input)
				return input
			},
		})
		track(worker)
		worker.pause()
		const pending = worker.enqueue(1)
		await waitForDelay(10)
		expect(started.count).toBe(0)
		expect(worker.paused).toBe(true)
		expect(worker.count).toBe(1)

		worker.resume()
		await expect(pending).resolves.toBe(1)
		expect(started.count).toBe(1)
		expect(worker.paused).toBe(false)
	})

	it('abort rejects pending work and fires the in-flight handler signal', async () => {
		const gate = Promise.withResolvers<void>()
		const fired = createRecorder<[]>()
		const worker = new Worker<string, number, void>({
			concurrency: 1,
			pool: { create: () => 0 },
			handler: (_input, _resource, execution) => {
				execution.signal.addEventListener(
					'abort',
					() => {
						fired.handler()
						gate.reject(execution.signal.reason)
					},
					{ once: true },
				)
				return gate.promise
			},
		})
		track(worker)
		const running = worker.enqueue('inflight')
		const waiting = worker.enqueue('pending')
		const settlement = Promise.allSettled([running, waiting])
		try {
			await waitForDelay(10)
			expect(worker.active).toBe(1)

			const aborting = worker.abort(new Error('stop'))
			await expect(waiting).rejects.toThrow('stop')
			await expect(running).rejects.toBeDefined()
			await expect(aborting).resolves.toBeUndefined()
			expect(fired.count).toBe(1)
			expect(worker.stopped).toBe(true)
		} finally {
			await worker.abort(new Error('test cleanup'))
			await settlement
		}
	})

	it('stop ends the loops and rejects pending', async () => {
		const worker = new Worker<number, number, number>({
			pool: { create: () => 0 },
			handler: (input) => input,
		})
		track(worker)
		worker.pause()
		const pending = worker.enqueue(1)
		const stopping = worker.stop()
		await expect(pending).rejects.toThrow('stopped')
		await expect(stopping).resolves.toBeUndefined()
		expect(worker.stopped).toBe(true)
	})

	it('clear drops pending entries while the in-flight job runs on', async () => {
		const gate = Promise.withResolvers<number>()
		const worker = new Worker<number, number, number>({
			concurrency: 1,
			pool: { create: () => 0 },
			handler: (input) => (input === 0 ? gate.promise : Promise.resolve(input)),
		})
		track(worker)
		const running = worker.enqueue(0)
		const dropped = worker.enqueue(1)
		const settlement = Promise.allSettled([running, dropped])
		let clearing: Promise<void> | undefined
		try {
			await waitForDelay(10)

			clearing = worker.clear()
			await expect(dropped).rejects.toThrow('cleared')
			await expect(clearing).resolves.toBeUndefined()
			gate.resolve(99)
			await expect(running).resolves.toBe(99)
		} finally {
			gate.resolve(99)
			await settlement
			if (clearing !== undefined) await Promise.allSettled([clearing])
		}
	})
})

describe('Worker — a signal-ignoring handler keeps its resource leased', () => {
	it('frees the queue slot on timeout while the resource stays held, then reuses it once the handler settles', async () => {
		const { create, created } = createResourceFactory()
		const gate = Promise.withResolvers<void>()
		const started = createRecorder<[number]>()
		const worker = new Worker<number, number, void>({
			concurrency: 1, // pool max defaults to 1 — one resource at a time
			timeout: 20,
			pool: { create },
			// A NON-cooperative handler: it ignores `execution.signal` and only settles when
			// the gate opens, so the attempt's timeout cannot make its `finally` release early.
			handler: async (input) => {
				started.handler(input)
				await gate.promise
			},
		})
		track(worker)

		// Job A times out (it ignores the signal): the attempt rejects and the queue slot
		// frees — but A's handler is still blocked on the gate, so its `finally` has not run
		// and the leased resource (the first resource) is NOT yet released back to the pool.
		const a = worker.enqueue(0)
		const aSettlement = Promise.allSettled([a])
		let b: Promise<void> | undefined
		let bSettlement: Promise<Array<PromiseSettledResult<void>>> | undefined
		try {
			await expect(a).rejects.toThrow('attempt timed out')
			expect(worker.active).toBe(0) // the queue slot is free again
			expect(created.count).toBe(1) // the first resource was created and is still leased

			// Job B claims the freed slot and tries to acquire — but the pool is at max with
			// nothing idle (the first resource is still held by A), so B PARKS on the pool. B opts out
			// of the deadline (`timeout: 0`) so it waits for the resource rather than timing out.
			b = worker.enqueue(1, { timeout: 0 })
			bSettlement = Promise.allSettled([b])
			await waitForDelay(30) // past A's timeout window — B would have run if it could
			expect(started.calls).toEqual([[0]]) // only A ever started; B is blocked on acquire
			expect(created.count).toBe(1) // still no second resource — B did not create one

			// Release the gate: A's handler finally settles, its `finally` releases that first resource,
			// and the pool hands that same resource (FIFO) to B's parked acquire — reused, not
			// recreated.
			gate.resolve()
			await b
			expect(started.calls).toEqual([[0], [1]]) // B ran after the resource came free
			expect(created.count).toBe(1) // B reused the first resource — no new create
		} finally {
			gate.resolve()
			await aSettlement
			if (bSettlement !== undefined) await bSettlement
		}
	})
})

describe('Worker — destroy tears down the pool', () => {
	it('returns one stable barrier when the synchronous queue abort reenters destroy', async () => {
		const worker = new Worker<undefined, number, void>({
			pool: { create: () => 0 },
			handler: () => {},
		})
		track(worker)
		let reentrant: Promise<void> | undefined
		worker.emitter.on('abort', () => {
			reentrant = worker.destroy()
		})

		const ending = worker.destroy()
		const repeated = worker.destroy()
		if (reentrant === undefined) throw new Error('expected destroy reentry from abort')
		expect(reentrant).toBe(ending)
		expect(repeated).toBe(ending)
		await expect(ending).resolves.toBeUndefined()
		expect(worker.emitter.destroyed).toBe(true)
	})

	it('waits for queue cleanup before destroying the pool and worker emitter', async () => {
		const cleanup = Promise.withResolvers<void>()
		const removing = Promise.withResolvers<void>()
		const order = createRecorder<[string]>()
		const store = new TestQueueStore<undefined>({
			remove: async () => {
				removing.resolve()
				await cleanup.promise
				order.handler('queue')
			},
		})
		const worker = new Worker<undefined, number, void>({
			store,
			pool: { create: () => 0, destroy: () => order.handler('pool') },
			handler: () => {},
		})
		track(worker)
		const work = worker.enqueue(undefined)
		const workSettlement = Promise.allSettled([work])
		await removing.promise

		const ending = worker.destroy()
		const endingSettlement = Promise.allSettled([ending])
		try {
			expect(order.count).toBe(0)
			expect(worker.emitter.destroyed).toBe(false)

			cleanup.resolve()
			await workSettlement
			const [result] = await endingSettlement
			expect(result?.status).toBe('fulfilled')
			expect(order.calls).toEqual([['queue'], ['pool']])
			expect(worker.emitter.destroyed).toBe(true)
		} finally {
			cleanup.resolve()
			await workSettlement
			await endingSettlement
		}
	})

	it('preserves one queue cleanup failure by identity', async () => {
		const cleanup = Promise.withResolvers<void>()
		const removing = Promise.withResolvers<void>()
		const removals = createRecorder<[string]>()
		const failure = new Error('queue cleanup failed')
		const store = new TestQueueStore<undefined>({
			remove: async (id) => {
				removals.handler(id)
				if (removals.count > 1) return
				removing.resolve()
				await cleanup.promise
			},
		})
		const worker = new Worker<undefined, number, void>({
			store,
			pool: { create: () => 0 },
			handler: () => {},
		})
		track(worker)
		const work = worker.enqueue(undefined)
		const workSettlement = Promise.allSettled([work])
		await removing.promise

		const ending = worker.destroy()
		const endingSettlement = Promise.allSettled([ending])
		cleanup.reject(failure)
		const [workResult] = await workSettlement
		const [endingResult] = await endingSettlement
		if (workResult === undefined || workResult.status === 'fulfilled') {
			throw new Error('expected enqueue cleanup rejection')
		}
		if (endingResult === undefined || endingResult.status === 'fulfilled') {
			throw new Error('expected destroy cleanup rejection')
		}
		expect(endingResult.reason).toBe(workResult.reason)
		if (!isQueueError(workResult.reason)) throw new Error('expected queue cleanup error')
		expect(workResult.reason.code).toBe('cleanup')
		expect(workResult.reason.cause).toBe(failure)
		expect(worker.emitter.destroyed).toBe(true)
	})

	it('aggregates queue then pool cleanup failures and destroys the emitter last', async () => {
		const queueCleanup = Promise.withResolvers<void>()
		const queueRemoving = Promise.withResolvers<void>()
		const queueRemovals = createRecorder<[string]>()
		const poolCleanup = Promise.withResolvers<void>()
		const poolRemoving = Promise.withResolvers<void>()
		const gateSettlement = Promise.allSettled([queueCleanup.promise, poolCleanup.promise])
		const queueFailure = new Error('queue cleanup failed')
		const poolFailure = new Error('pool cleanup failed')
		const store = new TestQueueStore<undefined>({
			remove: async (id) => {
				queueRemovals.handler(id)
				if (queueRemovals.count > 1) return
				queueRemoving.resolve()
				await queueCleanup.promise
			},
		})
		const worker = new Worker<undefined, number, void>({
			store,
			pool: {
				create: () => 0,
				destroy: async () => {
					poolRemoving.resolve()
					await poolCleanup.promise
				},
			},
			handler: () => {},
		})
		track(worker)
		const work = worker.enqueue(undefined)
		const workSettlement = Promise.allSettled([work])
		await queueRemoving.promise

		const ending = worker.destroy()
		const endingSettlement = Promise.allSettled([ending])
		queueCleanup.reject(queueFailure)
		try {
			const [workResult] = await workSettlement
			if (workResult === undefined || workResult.status === 'fulfilled') {
				throw new Error('expected enqueue cleanup rejection')
			}
			if (!isQueueError(workResult.reason)) throw new Error('expected queue cleanup error')
			await poolRemoving.promise
			expect(worker.emitter.destroyed).toBe(false)

			poolCleanup.reject(poolFailure)
			const [endingResult] = await endingSettlement
			if (endingResult === undefined || endingResult.status === 'fulfilled') {
				throw new Error('expected aggregate destroy rejection')
			}
			if (!(endingResult.reason instanceof AggregateError)) {
				throw new Error('expected native aggregate cleanup error')
			}
			const failures: readonly unknown[] = endingResult.reason.errors
			const poolError: unknown = failures[1]
			expect(failures).toHaveLength(2)
			expect(failures[0]).toBe(workResult.reason)
			if (!isPoolError(poolError)) throw new Error('expected pool cleanup error')
			expect(poolError.code).toBe('cleanup')
			expect(poolError.cause).toBe(poolFailure)
			expect(worker.emitter.destroyed).toBe(true)
		} finally {
			queueCleanup.reject(queueFailure)
			poolCleanup.reject(poolFailure)
			await workSettlement
			await endingSettlement
			await gateSettlement
		}
	})

	it('aborts in-flight work and awaits pooled-resource destruction', async () => {
		const { create, destroy, destroyed } = createResourceFactory()
		const worker = new Worker<undefined, number, void>({
			concurrency: 1,
			pool: { create, destroy },
			// A cooperative handler that unwinds on its signal, so the `finally` releases
			// the resource into the (now-destroyed) pool, which destroys it.
			handler: (_input, _resource, execution) =>
				new Promise<void>((_resolve, reject) => {
					execution.signal.addEventListener('abort', () => reject(execution.signal.reason), {
						once: true,
					})
				}),
		})
		track(worker)
		const running = worker.enqueue(undefined)
		await waitForDelay(10)
		expect(worker.active).toBe(1)

		const ending = worker.destroy()
		await expect(running).rejects.toBeDefined()
		await expect(ending).resolves.toBeUndefined()
		expect(worker.stopped).toBe(true)
		expect(destroyed.count).toBe(1)
	})

	it('keeps its emitter alive until the pool cleanup barrier settles', async () => {
		const cleanup = Promise.withResolvers<void>()
		const entered = Promise.withResolvers<void>()
		const order = createRecorder<[string]>()
		const worker = new Worker<undefined, number, void>({
			pool: {
				create: () => 0,
				destroy: async () => {
					entered.resolve()
					await cleanup.promise
					order.handler('pool')
				},
			},
			handler: () => {},
		})
		track(worker)
		await worker.enqueue(undefined)

		const ending = worker.destroy()
		const observed = ending.then(() => order.handler('worker'))
		try {
			await entered.promise
			expect(order.count).toBe(0)
			expect(worker.emitter.destroyed).toBe(false)

			cleanup.resolve()
			await expect(ending).resolves.toBeUndefined()
			expect(order.calls).toEqual([['pool'], ['worker']])
			expect(worker.emitter.destroyed).toBe(true)
		} finally {
			cleanup.resolve()
			await Promise.allSettled([ending, observed])
		}
	})

	it('preserves the pool cleanup error and its raw hook cause', async () => {
		const failure = new Error('pool cleanup failed')
		const worker = new Worker<undefined, number, void>({
			pool: {
				create: () => 0,
				destroy: () => {
					throw failure
				},
			},
			handler: () => {},
		})
		track(worker)
		await worker.enqueue(undefined)

		const [result] = await Promise.allSettled([worker.destroy()])
		if (result === undefined || result.status === 'fulfilled') {
			throw new Error('expected pool cleanup rejection')
		}
		if (!isPoolError(result.reason)) throw new Error('expected pool cleanup error')
		expect(result.reason.code).toBe('cleanup')
		expect(result.reason.cause).toBe(failure)
		expect(worker.emitter.destroyed).toBe(true)
	})
})

describe('Worker — durability passthrough + restore', () => {
	it('persists a job through its store and re-runs it via restore (delegated to the queue)', async () => {
		const store = createMemoryQueueStore(stringShape())

		// Worker A: persist a job but never run it — paused, so its parked workers leave the
		// row in the shared store for a later worker to restore.
		const a = new Worker<string, number, string>({
			store,
			pool: { create: () => 0 },
			handler: (input) => input,
		})
		track(a)
		a.pause()
		void a.enqueue('job').catch(() => {})
		await waitForDelay(10)
		expect((await store.load()).map((entry) => entry.input)).toEqual(['job'])

		// Worker B over the SAME store: restore re-runs A's persisted job against a fresh
		// pooled resource, then the store is empty.
		const seen = createRecorder<[string, number]>()
		const b = new Worker<string, number, string>({
			store,
			pool: { create: () => 7 },
			handler: (input, resource) => {
				seen.handler(input, resource)
				return input
			},
		})
		track(b)
		await b.restore()
		b.start()
		await waitForDelay(20)

		expect(seen.calls).toEqual([['job', 7]])
		expect(await store.load()).toEqual([])
	})

	it('restore is a no-op without a store', async () => {
		const worker = new Worker<string, number, string>({
			pool: { create: () => 0 },
			handler: (input) => input,
		})
		track(worker)
		await expect(worker.restore()).resolves.toBeUndefined()
	})
})

// ── Resource bound under saturation (many jobs, few resources) ───────────────
//
// The pool's `max` (defaulting to `concurrency`) is never exceeded under sustained
// saturation, and every admitted job still completes exactly once with a resource.

describe('Worker — resource bound holds under saturation', () => {
	it('runs 40 jobs through a concurrency-3 worker reusing at most 3 resources', async () => {
		const { create, created } = createResourceFactory()
		const concurrency = 3
		let liveResources = 0
		let peakResources = 0
		const ran = createRecorder<[number]>()
		const worker = new Worker<number, number, number>({
			concurrency,
			pool: { create },
			handler: async (input) => {
				liveResources += 1
				peakResources = Math.max(peakResources, liveResources)
				await waitForDelay(1)
				ran.handler(input)
				liveResources -= 1
				return input
			},
		})
		track(worker)

		const results = await Promise.all(
			Array.from({ length: 40 }, (_unused, index) => worker.enqueue(index)),
		)
		expect(results).toEqual(Array.from({ length: 40 }, (_unused, index) => index))
		// Each job ran exactly once.
		expect(ran.count).toBe(40)
		// Never more than `concurrency` resources held at once, and at most that many created.
		expect(peakResources).toBeLessThanOrEqual(concurrency)
		expect(created.count).toBeLessThanOrEqual(concurrency)
		expect(worker.active).toBe(0)
		expect(worker.count).toBe(0)
	})
})

// ── A stream of failing jobs never starves the pool ──────────────────────────
//
// Under concurrency, each consecutively-failing handler releases its leased resource,
// so a later successful job can still acquire without starving the pool.

describe('Worker — failing jobs release resources (no pool starvation)', () => {
	it('survives a burst of throwing handlers and still serves a later success', async () => {
		const { create, created } = createResourceFactory()
		const failures = createRecorder<[number]>()
		let mode: 'fail' | 'pass' = 'fail'
		const worker = new Worker<number, number, string>({
			concurrency: 2,
			pool: { create },
			handler: (input) => {
				if (mode === 'fail') {
					failures.handler(input)
					throw new Error(`fail ${input}`)
				}
				return `ok ${input}`
			},
		})
		track(worker)

		// A burst of 10 failing jobs — each must release its resource back to the pool.
		const failed = await Promise.allSettled(
			Array.from({ length: 10 }, (_unused, index) => worker.enqueue(index)),
		)
		expect(failed.every((result) => result.status === 'rejected')).toBe(true)
		expect(failures.count).toBe(10)
		// At most `concurrency` resources were ever created despite 10 failures (reused).
		expect(created.count).toBeLessThanOrEqual(2)

		// The pool is not starved: a later success acquires + runs against a reused resource.
		mode = 'pass'
		await expect(worker.enqueue(99)).resolves.toBe('ok 99')
		expect(created.count).toBeLessThanOrEqual(2)
		expect(worker.active).toBe(0)
	})
})

// ── Pool max vs queue concurrency mismatch ───────────────────────────────────
//
// When the pool's `max` is smaller than queue `concurrency`, surplus jobs park on pool
// acquisition. When it is larger, queue concurrency remains the effective cap.

describe('Worker — pool max vs queue concurrency mismatch', () => {
	it('caps real parallelism at the smaller pool max (resource is the bottleneck)', async () => {
		const { create, created } = createResourceFactory()
		const first = Promise.withResolvers<void>()
		const second = Promise.withResolvers<void>()
		const third = Promise.withResolvers<void>()
		const fourth = Promise.withResolvers<void>()
		const gates = [first, second, third, fourth]
		let liveHandlers = 0
		let peakHandlers = 0
		// concurrency 4 but only 2 resources — at most 2 handlers can truly run at once
		// because a handler only proceeds once it has ACQUIRED a resource.
		const worker = new Worker<number, number, void>({
			concurrency: 4,
			pool: { create, max: 2 },
			handler: async (input) => {
				liveHandlers += 1
				peakHandlers = Math.max(peakHandlers, liveHandlers)
				const gate = gates[input]
				if (gate === undefined) throw new Error('missing gate')
				await gate.promise
				liveHandlers -= 1
			},
		})
		track(worker)

		const all = [0, 1, 2, 3].map((input) => worker.enqueue(input))
		const settlement = Promise.allSettled(all)
		try {
			await waitForDelay(20)
			// Only two resources exist, so only two handlers are actually running their body;
			// the other two claimed queue slots but PARK on the pool acquire.
			expect(created.count).toBe(2)
			expect(peakHandlers).toBe(2)

			// Releasing the first two lets the parked two acquire the freed resources.
			first.resolve()
			second.resolve()
			await waitForDelay(20)
			expect(created.count).toBe(2) // still only two resources — reused, never a third

			third.resolve()
			fourth.resolve()
			await Promise.all(all)
			// Throughout, no more than the pool max ran concurrently.
			expect(peakHandlers).toBe(2)
			expect(worker.active).toBe(0)
			expect(worker.count).toBe(0)
		} finally {
			first.resolve()
			second.resolve()
			third.resolve()
			fourth.resolve()
			await settlement
		}
	})

	it('uses at most `concurrency` resources when pool max exceeds concurrency', async () => {
		const { create, created } = createResourceFactory()
		const first = Promise.withResolvers<void>()
		const second = Promise.withResolvers<void>()
		const gates = [first, second]
		// concurrency 2 but pool max 10 — the queue caps parallelism at 2, so only two
		// resources are ever created; the extra pool capacity goes unused.
		const worker = new Worker<number, number, void>({
			concurrency: 2,
			pool: { create, max: 10 },
			handler: async (input) => {
				const gate = gates[input]
				if (gate === undefined) throw new Error('missing gate')
				await gate.promise
			},
		})
		track(worker)

		const all = [0, 1].map((input) => worker.enqueue(input))
		const settlement = Promise.allSettled(all)
		try {
			await waitForDelay(20)
			// Both queue slots are busy; only two resources were needed despite max 10.
			expect(created.count).toBe(2)
			expect(worker.active).toBe(2)

			first.resolve()
			second.resolve()
			await Promise.all(all)
			expect(created.count).toBe(2)
			expect(worker.active).toBe(0)
		} finally {
			first.resolve()
			second.resolve()
			await settlement
		}
	})
})

// ── destroy tears down BOTH queue and pool mid-flight ────────────────────────
//
// Destroying under load aborts in-flight work, rejects pending work, and tears down every
// pooled resource through the serial Queue ⨉ Pool cleanup.

describe('Worker — destroy mid-flight tears down queue and pool together', () => {
	it('aborts in-flight, rejects pending, and destroys every pooled resource', async () => {
		const { create, created, destroy, destroyed } = createResourceFactory()
		// Cooperative handlers that unwind on their signal so the `finally` releases the
		// resource into the destroyed pool (which then destroys it).
		const worker = new Worker<number, number, void>({
			concurrency: 2,
			pool: { create, destroy },
			handler: (_input, _resource, execution) =>
				new Promise<void>((_resolve, reject) => {
					execution.signal.addEventListener('abort', () => reject(execution.signal.reason), {
						once: true,
					})
				}),
		})
		track(worker)

		const work = [0, 1, 2, 3].map((input) => worker.enqueue(input))
		await waitForDelay(10)
		expect(worker.active).toBe(2)
		expect(created.count).toBe(2) // two resources for the two in-flight jobs

		const ending = worker.destroy()

		// In-flight jobs are aborted, pending jobs rejected — all four settle (none hangs).
		const settled = await Promise.allSettled(work)
		expect(settled.every((result) => result.status === 'rejected')).toBe(true)
		await expect(ending).resolves.toBeUndefined()
		expect(worker.stopped).toBe(true)
		expect(destroyed.count).toBe(2)
		expect(worker.count).toBe(0)
	})
})

// ── Emitter — the PUSH observation surface (AGENTS §13) ──────────────────────
//
// The Worker exposes a typed `emitter` (`WorkerEventMap<TResult>`) RE-EXPOSING the
// underlying queue's job lifecycle — `enqueue` / `start` / `retry` / `success` / `failure` /
// `abort` / `drain` — bridged from the inner queue at construction, so a consumer observes
// the worker without reaching through to internals. The bridge re-emits directly on the
// worker's own emitter, which isolates a buggy worker observer (routing its throw to the
// worker emitter's `error` handler — the `error` option) so it can NEVER corrupt the inner
// queue or pool. These pin: the facade events fire at the right moments with the right
// payloads; `on?` wires initial listeners; and the emit-safety guarantee — a throwing observer
// leaves the worker fully functional (jobs still run against pooled resources, counts
// balanced), yet the `error` handler fires.

// The WorkerEventMap event names recorded across the emitter tests — fed to the shared
// `createRecorders` (AGENTS §16.1: the per-event wiring is centralized; this file
// keeps only the names its scenarios observe).
const WORKER_EVENTS: readonly [
	'enqueue',
	'start',
	'retry',
	'success',
	'failure',
	'abort',
	'drain',
] = ['enqueue', 'start', 'retry', 'success', 'failure', 'abort', 'drain']

// `createRecorders` reaches its event map only through the generic `on` of
// `EventSourceInterface`, which yields no inference candidate, so its map type falls back to
// the constraint and rejects a `WorkerEventMap` emitter. Each call therefore names both type
// arguments, and this alias carries the recorded-name union so only the result type varies.
type WorkerEvent = (typeof WORKER_EVENTS)[number]

describe('Worker — emitter (push observation surface)', () => {
	it('re-exposes the queue lifecycle: enqueue → start → success → drain with the job result', async () => {
		const worker = new Worker<number, number, string>({
			pool: { create: () => 7 },
			handler: (input, resource) => `${input}:${resource}`,
		})
		track(worker)
		const events = createRecorders<WorkerEventMap<string>, WorkerEvent>(
			worker.emitter,
			WORKER_EVENTS,
		)
		const result = await worker.enqueue(3, { id: 'job-1' })
		expect(result).toBe('3:7')
		await waitForDelay(0)
		// The worker surfaces the underlying queue's lifecycle as its own.
		expect(events.enqueue.calls).toEqual([['job-1']])
		expect(events.start.calls).toEqual([['job-1']])
		expect(events.success.calls).toEqual([['job-1', '3:7']])
		expect(events.drain.count).toBe(1)
		expect(events.failure.count).toBe(0)
	})

	it('surfaces retry then failure when a job exhausts its retries', async () => {
		const error = new Error('always fails')
		const worker = new Worker<undefined, number, void>({
			retries: 1,
			pool: { create: () => 0 },
			handler: () => {
				throw error
			},
		})
		track(worker)
		const events = createRecorders<WorkerEventMap<void>, WorkerEvent>(worker.emitter, WORKER_EVENTS)
		await expect(worker.enqueue(undefined, { id: 'doomed' })).rejects.toThrow('always fails')
		expect(events.start.calls).toEqual([['doomed']])
		expect(events.retry.calls).toEqual([['doomed', 1]])
		expect(events.failure.calls).toEqual([['doomed', error]])
		expect(events.success.count).toBe(0)
	})

	it('surfaces abort when the worker is aborted', async () => {
		const worker = new Worker<string, number, void>({
			concurrency: 1,
			pool: { create: () => 0 },
			handler: (_input, _resource, execution) =>
				new Promise<void>((_resolve, reject) => {
					execution.signal.addEventListener('abort', () => reject(execution.signal.reason), {
						once: true,
					})
				}),
		})
		track(worker)
		const events = createRecorders<WorkerEventMap<void>, WorkerEvent>(worker.emitter, WORKER_EVENTS)
		const running = worker.enqueue('inflight', { id: 'a' })
		await waitForDelay(10)
		const reason = new Error('stop')
		const aborting = worker.abort(reason)
		await expect(running).rejects.toBeDefined()
		await expect(aborting).resolves.toBeUndefined()
		const error = events.abort.calls[0]?.[0]
		if (!isQueueError(error)) throw new Error('expected the queue coded abort error')
		expect(error.code).toBe('aborted')
		expect(error.cause).toBe(reason)
	})

	it('wires initial listeners from the `on` option at construction', async () => {
		const success = createRecorder<[id: string, result: number]>()
		const worker = new Worker<number, number, number>({
			pool: { create: () => 0 },
			handler: (input) => input + 1,
			on: { success: success.handler },
		})
		track(worker)
		await expect(worker.enqueue(41, { id: 'seed' })).resolves.toBe(42)
		expect(success.calls).toEqual([['seed', 42]])
	})

	it('EMIT SAFETY: a throwing worker observer cannot corrupt the queue or pool, and routes to the error handler', async () => {
		const thrown = new Error('worker observer blew up')
		const { create, created } = createResourceFactory()
		const ran = createRecorder<[number]>()
		const errors = createRecorder<readonly [error: unknown, event: string]>()
		const worker = new Worker<number, number, number>({
			concurrency: 2,
			pool: { create },
			error: errors.handler,
			handler: async (input) => {
				ran.handler(input)
				await waitForDelay(1)
				return input * 10
			},
		})
		track(worker)
		// A buggy `success` observer on the WORKER's emitter that throws every time. It must NOT
		// corrupt the inner queue (the bridge listener never throws, so the queue's own emit stays
		// balanced) or starve the pool — every job still runs against a pooled resource and settles.
		worker.emitter.on('success', () => {
			throw thrown
		})

		const results = await Promise.all(
			Array.from({ length: 20 }, (_unused, index) => worker.enqueue(index, { id: `j${index}` })),
		)
		await waitForDelay(0)

		// THE LOAD-BEARING ASSERTION: every job resolved correctly despite the throwing observer.
		expect(results).toEqual(Array.from({ length: 20 }, (_unused, index) => index * 10))
		expect(ran.count).toBe(20)
		// The inner queue + pool stayed balanced — no stranded slot, resources reused (≤ max).
		expect(worker.active).toBe(0)
		expect(worker.count).toBe(0)
		expect(created.count).toBeLessThanOrEqual(2)
		// EVERY throw routed to the worker emitter's OWN `error` handler — (error, event).
		expect(errors.count).toBe(20)
		expect(errors.calls.every(([, event]) => event === 'success')).toBe(true)
		// The worker still serves a fresh job after the storm.
		await expect(worker.enqueue(99, { id: 'after' })).resolves.toBe(990)
	})
})
