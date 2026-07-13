import { describe, expect, it } from 'vitest'
import { createMemoryQueueStore, stringShape, Worker } from '@src/core'
import {
	createErrorRecorder,
	createGate,
	createRecorder,
	createResourceFactory,
	recordEmitterEvents,
	waitForDelay,
} from '../../../setup.js'

// src/core/workers/Worker.ts — the Queue⨉Pool facade. Real behaviour, no mocks: a
// counting `create` hook proves resources are reused and never exceed the pool max,
// gates pin jobs in flight so the cap is observable, and a throwing handler proves the
// acquired resource is released in the `finally` and reused by a later job (AGENTS §16).
// Beyond the per-feature cases, production-grade sections cover: the resource bound
// under saturation (40 jobs through 3 slots — at most 3 resources), a burst of failing
// handlers never starving the pool, the pool-max-vs-queue-concurrency mismatch (real
// parallelism = min(concurrency, pool.max), in both directions), and destroy mid-flight
// tearing down queue AND pool together (in-flight aborted, pending rejected, every
// pooled resource destroyed). The shared `createResourceFactory` (tests/setup.ts) hands
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
		await expect(worker.enqueue(3)).resolves.toBe('3:7')
		expect(seen.calls).toEqual([[3, 7]])
	})
})

describe('Worker — resource reuse + the pool cap', () => {
	it('reuses resources across jobs and never creates more than the pool max', async () => {
		const { create, created } = createResourceFactory()
		const gates = [createGate(), createGate(), createGate(), createGate()]
		const worker = new Worker<number, number, void>({
			concurrency: 2,
			pool: { create },
			handler: async (input) => {
				await gates[input].promise
			},
		})

		const all = [0, 1, 2, 3].map((input) => worker.enqueue(input))
		await waitForDelay(10)
		// Two in flight → at most two resources created.
		expect(created.count).toBe(2)
		expect(worker.active).toBe(2)

		// Finish the first two; the next two reuse the freed resources — still only two.
		gates[0].resolve()
		gates[1].resolve()
		await waitForDelay(10)
		expect(created.count).toBe(2)

		gates[2].resolve()
		gates[3].resolve()
		await Promise.all(all)
		expect(created.count).toBe(2)
		expect(worker.active).toBe(0)
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
		const gate = createGate()
		const fired = createRecorder<[]>()
		const worker = new Worker<string, number, void>({
			concurrency: 1,
			pool: { create: () => 0 },
			handler: (_input, _resource, execution) => {
				execution.signal.addEventListener('abort', () => fired.handler(), { once: true })
				return gate.promise
			},
		})
		const running = worker.enqueue('inflight')
		const waiting = worker.enqueue('pending')
		await waitForDelay(10)
		expect(worker.active).toBe(1)

		worker.abort(new Error('stop'))
		await expect(waiting).rejects.toThrow('stop')
		await expect(running).rejects.toBeDefined()
		expect(fired.count).toBe(1)
		expect(worker.stopped).toBe(true)
	})

	it('stop ends the loops and rejects pending', async () => {
		const worker = new Worker<number, number, number>({
			pool: { create: () => 0 },
			handler: (input) => input,
		})
		worker.pause()
		const pending = worker.enqueue(1)
		worker.stop()
		await expect(pending).rejects.toThrow('stopped')
		expect(worker.stopped).toBe(true)
	})

	it('clear drops pending entries while the in-flight job runs on', async () => {
		const gate = createGate<number>()
		const worker = new Worker<number, number, number>({
			concurrency: 1,
			pool: { create: () => 0 },
			handler: (input) => (input === 0 ? gate.promise : Promise.resolve(input)),
		})
		const running = worker.enqueue(0)
		const dropped = worker.enqueue(1)
		await waitForDelay(10)

		worker.clear()
		await expect(dropped).rejects.toThrow('cleared')
		gate.resolve(99)
		await expect(running).resolves.toBe(99)
	})
})

describe('Worker — a signal-ignoring handler keeps its resource leased', () => {
	it('frees the queue slot on timeout while the resource stays held, then reuses it once the handler settles', async () => {
		const { create, created } = createResourceFactory()
		const gate = createGate()
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

		// Job A times out (it ignores the signal): the attempt rejects and the queue slot
		// frees — but A's handler is still blocked on the gate, so its `finally` has not run
		// and the leased resource (resource 0) is NOT yet released back to the pool.
		const a = worker.enqueue(0)
		await expect(a).rejects.toThrow('attempt timed out')
		expect(worker.active).toBe(0) // the queue slot is free again
		expect(created.count).toBe(1) // resource 0 was created and is still leased

		// Job B claims the freed slot and tries to acquire — but the pool is at max with
		// nothing idle (resource 0 is still held by A), so B PARKS on the pool. B opts out
		// of the deadline (`timeout: 0`) so it waits for the resource rather than timing out.
		const b = worker.enqueue(1, { timeout: 0 })
		await waitForDelay(30) // past A's timeout window — B would have run if it could
		expect(started.calls).toEqual([[0]]) // only A ever started; B is blocked on acquire
		expect(created.count).toBe(1) // still no second resource — B did not create one

		// Release the gate: A's handler finally settles, its `finally` releases resource 0,
		// and the pool hands that same resource (FIFO) to B's parked acquire — reused, not
		// recreated.
		gate.resolve()
		await b
		expect(started.calls).toEqual([[0], [1]]) // B ran after the resource came free
		expect(created.count).toBe(1) // B reused resource 0 — no new create
	})
})

describe('Worker — destroy tears down the pool', () => {
	it('aborts in-flight work, destroys pooled resources, and is idempotent', async () => {
		const { create, destroyed } = createResourceFactory()
		const worker = new Worker<undefined, number, void>({
			concurrency: 1,
			pool: { create, destroy: (value) => destroyed.handler(value) },
			// A cooperative handler that unwinds on its signal, so the `finally` releases
			// the resource into the (now-destroyed) pool, which destroys it.
			handler: (_input, _resource, execution) =>
				new Promise<void>((_resolve, reject) => {
					execution.signal.addEventListener('abort', () => reject(execution.signal.reason), {
						once: true,
					})
				}),
		})
		const running = worker.enqueue(undefined)
		await waitForDelay(10)
		expect(worker.active).toBe(1)

		worker.destroy()
		worker.destroy() // idempotent — no throw

		await expect(running).rejects.toBeDefined()
		expect(worker.stopped).toBe(true)
		// The aborted job released its resource into a destroyed pool, which destroyed it.
		await waitForDelay(10)
		expect(destroyed.count).toBe(1)
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
		await expect(worker.restore()).resolves.toBeUndefined()
	})
})

// ── Resource bound under saturation (many jobs, few resources) ───────────────
//
// PRODUCTION GAP: the reuse test runs 4 jobs through 2 slots. A real worker funnels far
// more jobs through its pool; the invariant is that the pool's `max` (defaulting to
// `concurrency`) is NEVER exceeded — resources in existence ≤ max no matter how many
// jobs queue — and every job still completes exactly once with a resource.

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
// PRODUCTION GAP: the release-on-throw test runs one failing job then one success. Under
// concurrency, a burst of consecutively-failing handlers must each release their leased
// resource so the pool is never starved — a later success still acquires promptly. (The
// resource is released in the queue handler's `finally`, even on throw.)

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
// PRODUCTION GAP: untested entirely. When the pool's `max` is SMALLER than the queue's
// `concurrency`, the resource — not the queue slot — is the bottleneck: more jobs can be
// "in flight" (claimed a queue slot) than there are resources, so the surplus jobs PARK
// on the pool's acquire. The effective parallelism is min(concurrency, pool.max), and no
// more than `pool.max` resources ever exist. When `max` is LARGER, the queue concurrency
// caps parallelism and the extra pool capacity simply goes unused.

describe('Worker — pool max vs queue concurrency mismatch', () => {
	it('caps real parallelism at the smaller pool max (resource is the bottleneck)', async () => {
		const { create, created } = createResourceFactory()
		const gates = [createGate(), createGate(), createGate(), createGate()]
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
				await gates[input].promise
				liveHandlers -= 1
			},
		})

		const all = [0, 1, 2, 3].map((input) => worker.enqueue(input))
		await waitForDelay(20)
		// Only two resources exist, so only two handlers are actually running their body;
		// the other two claimed queue slots but PARK on the pool acquire.
		expect(created.count).toBe(2)
		expect(peakHandlers).toBe(2)

		// Releasing the first two lets the parked two acquire the freed resources.
		gates[0].resolve()
		gates[1].resolve()
		await waitForDelay(20)
		expect(created.count).toBe(2) // still only two resources — reused, never a third

		gates[2].resolve()
		gates[3].resolve()
		await Promise.all(all)
		// Throughout, no more than the pool max ran concurrently.
		expect(peakHandlers).toBe(2)
		expect(worker.active).toBe(0)
		expect(worker.count).toBe(0)
	})

	it('uses at most `concurrency` resources when pool max exceeds concurrency', async () => {
		const { create, created } = createResourceFactory()
		const gates = [createGate(), createGate()]
		// concurrency 2 but pool max 10 — the queue caps parallelism at 2, so only two
		// resources are ever created; the extra pool capacity goes unused.
		const worker = new Worker<number, number, void>({
			concurrency: 2,
			pool: { create, max: 10 },
			handler: async (input) => {
				await gates[input].promise
			},
		})

		const all = [0, 1].map((input) => worker.enqueue(input))
		await waitForDelay(20)
		// Both queue slots are busy; only two resources were needed despite max 10.
		expect(created.count).toBe(2)
		expect(worker.active).toBe(2)

		gates[0].resolve()
		gates[1].resolve()
		await Promise.all(all)
		expect(created.count).toBe(2)
		expect(worker.active).toBe(0)
	})
})

// ── destroy tears down BOTH queue and pool mid-flight ────────────────────────
//
// PRODUCTION GAP: the existing destroy test uses a single cooperative job. Here destroy
// fires with multiple jobs in flight + pending against a small pool, asserting it aborts
// in-flight work, rejects pending, and destroys every pooled resource (not just the
// in-flight ones) — a complete teardown of the composed Queue ⨉ Pool.

describe('Worker — destroy mid-flight tears down queue and pool together', () => {
	it('aborts in-flight, rejects pending, and destroys every pooled resource', async () => {
		const { create, created, destroyed } = createResourceFactory()
		// Cooperative handlers that unwind on their signal so the `finally` releases the
		// resource into the destroyed pool (which then destroys it).
		const worker = new Worker<number, number, void>({
			concurrency: 2,
			pool: { create, destroy: (value) => destroyed.handler(value) },
			handler: (_input, _resource, execution) =>
				new Promise<void>((_resolve, reject) => {
					execution.signal.addEventListener('abort', () => reject(execution.signal.reason), {
						once: true,
					})
				}),
		})

		const inflight = [worker.enqueue(0), worker.enqueue(1)]
		const pending = [worker.enqueue(2), worker.enqueue(3)]
		await waitForDelay(10)
		expect(worker.active).toBe(2)
		expect(created.count).toBe(2) // two resources for the two in-flight jobs

		worker.destroy()

		// In-flight jobs are aborted, pending jobs rejected — all four settle (none hangs).
		const settled = await Promise.allSettled([...inflight, ...pending])
		expect(settled.every((result) => result.status === 'rejected')).toBe(true)
		expect(worker.stopped).toBe(true)
		// Both leased resources were released into the destroyed pool and torn down.
		await waitForDelay(10)
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
// `recordEmitterEvents` (AGENTS §16.1: the per-event wiring is centralized; this file
// keeps only the names its scenarios observe).
const WORKER_EVENTS = ['enqueue', 'start', 'retry', 'success', 'failure', 'abort', 'drain'] as const

describe('Worker — emitter (push observation surface)', () => {
	it('re-exposes the queue lifecycle: enqueue → start → success → drain with the job result', async () => {
		const worker = new Worker<number, number, string>({
			pool: { create: () => 7 },
			handler: (input, resource) => `${input}:${resource}`,
		})
		const events = recordEmitterEvents(worker.emitter, WORKER_EVENTS)
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
		const events = recordEmitterEvents(worker.emitter, WORKER_EVENTS)
		await expect(worker.enqueue(undefined, { id: 'doomed' })).rejects.toThrow('always fails')
		expect(events.start.calls).toEqual([['doomed']])
		expect(events.retry.calls).toEqual([['doomed', 1]])
		expect(events.failure.calls).toEqual([['doomed', error]])
		expect(events.success.count).toBe(0)
	})

	it('surfaces abort when the worker is aborted', async () => {
		const gate = createGate()
		const worker = new Worker<string, number, void>({
			concurrency: 1,
			pool: { create: () => 0 },
			handler: () => gate.promise,
		})
		const events = recordEmitterEvents(worker.emitter, WORKER_EVENTS)
		const running = worker.enqueue('inflight', { id: 'a' })
		await waitForDelay(10)
		const reason = new Error('stop')
		worker.abort(reason)
		await expect(running).rejects.toBeDefined()
		expect(events.abort.calls).toEqual([[reason]])
	})

	it('wires initial listeners from the `on` option at construction', async () => {
		const success = createRecorder<[id: string, result: number]>()
		const worker = new Worker<number, number, number>({
			pool: { create: () => 0 },
			handler: (input) => input + 1,
			on: { success: success.handler },
		})
		await expect(worker.enqueue(41, { id: 'seed' })).resolves.toBe(42)
		expect(success.calls).toEqual([['seed', 42]])
	})

	it('EMIT SAFETY: a throwing worker observer cannot corrupt the queue or pool, and routes to the error handler', async () => {
		const thrown = new Error('worker observer blew up')
		const { create, created } = createResourceFactory()
		const ran = createRecorder<[number]>()
		const errors = createErrorRecorder()
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
