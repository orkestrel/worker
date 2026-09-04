import { afterEach, describe, expect, it } from 'vitest'
import {
	arrayOf,
	isBoolean,
	isNumber,
	isRecord,
	isString,
	numberShape,
	unionOf,
} from '@orkestrel/contract'
import { MemoryQueueStore } from '@orkestrel/queue'
import { fileURLToPath } from 'node:url'
import { createNodeWorker, createThread, Dispatch, isReply } from '@src/server'
import { createTeardown, waitForCondition } from '@orkestrel/test'
import { buildFixtureURL } from '../../setupServer.js'

// src/server/helpers.ts — the `isReply` predicate a `Dispatch` filters inbound messages
// with, proven directly at the end of this file, plus the main-side worker-thread machinery
// (`createThread` / `Dispatch`) it serves. The following round-trip suites drive
// `createNodeWorker` over REAL worker threads (no mocking; the node `src:server`
// project), exercising `createThread` + `Dispatch` END TO END — each stands up a worker
// against a real fixture script, drives
// jobs, and tears it down in `afterEach` so no thread leaks and the process exits; the
// fixtures are raw `.ts` loaded by Node's type-stripping; the server Vitest project supplies
// the required flag on Node 22.12–22.17 and Node 23.0–23.5. Fixture paths resolve through the
// shared `buildFixtureURL` helper, anchored to the workspace root.

const isEven = (value: unknown): value is number => isNumber(value) && value % 2 === 0
const isThrowingInput = (value: unknown): value is number => {
	if (value === 1) throw new Error('input-guard-boom')
	return isNumber(value)
}
const isThrowingResult = (value: unknown): value is number => {
	if (value === 2) throw new Error('result-guard-boom')
	return isNumber(value)
}
const isWorkerPayload = (value: unknown): value is { readonly token: string } => {
	return isRecord(value) && typeof value.token === 'string'
}
const isNumberArray = arrayOf(isNumber)

// Track every worker so it is destroyed even when an assertion throws.
const teardown = createTeardown()
afterEach(() => teardown.destroy())

function track<T extends { destroy(): Promise<void> }>(worker: T): T {
	teardown.add(() => worker.destroy())
	return worker
}

describe('createNodeWorker — round-trip over a thread', () => {
	it('dispatches the input to a thread and resolves the narrowed reply', async () => {
		const worker = track(
			createNodeWorker({ script: buildFixtureURL('double.ts'), input: isNumber, result: isNumber }),
		)
		await expect(worker.enqueue(21)).resolves.toBe(42)
	})

	it('runs many jobs on a small pool and returns every result', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 3,
			}),
		)
		const inputs = [1, 2, 3, 4, 5, 6, 7, 8]
		const results = await Promise.all(inputs.map((input) => worker.enqueue(input)))
		expect(results).toEqual(inputs.map((input) => input * 2))
	})
})

describe('createNodeWorker — concurrency over parallel threads', () => {
	it('runs up to `concurrency` jobs at once and never exceeds it', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 2,
			}),
		)
		// Eight jobs through a 2-thread pool. `active` is the in-flight count and must
		// never exceed the concurrency cap; all eight still complete.
		const pending = [10, 11, 12, 13, 14, 15, 16, 17].map((input) => worker.enqueue(input))
		expect(worker.active).toBeLessThanOrEqual(2)
		const results = await Promise.all(pending)
		expect(results).toEqual([20, 22, 24, 26, 28, 30, 32, 34])
		expect(worker.active).toBe(0)
	})
})

describe('createNodeWorker — failure + retry', () => {
	it('rejects with the thread error message when the handler throws', async () => {
		const worker = track(
			createNodeWorker({ script: buildFixtureURL('fail.ts'), input: isNumber, result: isNumber }),
		)
		await expect(worker.enqueue(5)).rejects.toThrow('boom:5')
	})

	it('re-runs a failing job up to its retry budget (then rejects)', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('fail.ts'),
				input: isNumber,
				result: isNumber,
				retries: 2,
			}),
		)
		// Three attempts (1 + 2 retries), all throw — the job still rejects with the error.
		await expect(worker.enqueue(9)).rejects.toThrow('boom:9')
	})
})

describe('createNodeWorker — stable Queue job identity', () => {
	it('exposes an explicit enqueue id to the worker handler', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('execution.ts'),
				input: isNumber,
				result: isString,
			}),
		)
		await expect(worker.enqueue(1, { id: 'stable-enqueue' })).resolves.toBe('stable-enqueue')
	})

	it('keeps the job id stable while minting a fresh retry correlation id', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('identity.ts'),
				input: isNumber,
				result: (
					value: unknown,
				): value is { readonly correlation: boolean; readonly job: boolean } =>
					isRecord(value) && isBoolean(value.correlation) && isBoolean(value.job),
				retries: 1,
			}),
		)
		await expect(worker.enqueue(1, { id: 'stable-retry' })).resolves.toEqual({
			correlation: true,
			job: true,
		})
	})

	it('exposes a restored MemoryQueueStore id to the worker handler', async () => {
		const store = new MemoryQueueStore(numberShape())
		await store.save({ id: 'stable-restore', input: 1, attempts: 0 })
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('execution.ts'),
				input: isNumber,
				result: isString,
				store,
			}),
		)
		const handled = Promise.withResolvers<{ readonly id: string; readonly result: string }>()
		worker.emitter.on('success', (id, result) => handled.resolve({ id, result }))
		await worker.restore()
		await expect(handled.promise).resolves.toEqual({
			id: 'stable-restore',
			result: 'stable-restore',
		})
	})
})

describe('createNodeWorker — timeout terminates + evicts the thread', () => {
	it('rejects on the per-attempt timeout and replaces the tainted thread', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('slow.ts'),
				input: isNumber,
				result: isNumber,
				timeout: 50,
			}),
		)
		// The fixture spins for 5s ignoring its signal; the 50ms deadline fires, so the
		// attempt times out and the (uncooperative) thread is TERMINATED + evicted.
		await expect(worker.enqueue(5_000)).rejects.toThrow('attempt timed out')
	})

	it('serves a later job on a fresh thread after a timeout eviction', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('slow.ts'),
				input: isNumber,
				result: isNumber,
				timeout: 50,
			}),
		)
		// First job times out → its thread is evicted. A short job then succeeds, proving the
		// pool spun up a fresh, healthy thread (the tainted one was not reused). The recovery job
		// gets a GENEROUS per-entry timeout override: the per-attempt deadline covers the pool
		// `acquire`, and acquiring here SPAWNS a fresh thread (~25–55ms) — under the tight 50ms
		// the spawn itself can race the deadline (a flake observed under full-suite CPU load), so
		// the recovery deadline must clear the spawn cost. The eviction is still driven by the
		// first job's tight 50ms; only the recovery's headroom changes.
		await expect(worker.enqueue(5_000)).rejects.toThrow('attempt timed out')
		await expect(worker.enqueue(1, { timeout: 5_000 })).resolves.toBe(1)
	})
})

// `concurrency: 1` is deliberate in the `in-flight signal abort` eviction suite: the pool's
// `max` matches it, so when the aborted job's thread is released a SECOND, already-queued job
// is PARKED as a pool waiter (the pool is at `max` with the dead thread still leased) — the
// exact contention that drives the pool's FIFO `release`→waiter HANDOFF. That handoff
// re-validates the released resource (the core `Pool` fix), so the dead thread is destroyed
// and the parked job is served a FRESH thread instead of the terminated one — proving eviction
// through the handoff path end to end (not merely through `grow`, which `concurrency: 2` masks).
describe('createNodeWorker — in-flight signal abort terminates + evicts the thread', () => {
	it('rejects the aborted attempt and replaces the (signal-ignoring) thread', async () => {
		// `identify.ts` spins for its input and echoes its OWN `threadId`, so the thread the pool
		// leases is observable from the result — which is what turns "the thread was evicted" into
		// a determinate assertion rather than a job that merely finishes eventually.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('identify.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 1,
			}),
		)
		// Warm the pool first, so the aborted job's `acquire` returns this already-online IDLE
		// thread instead of spawning one. `active` counts a claimed queue entry, which increments
		// before the handler awaits `acquire`, so without the warm-up the abort can land while
		// the spawn is still pending and reject the acquire — never reaching the in-flight
		// listener path this spec names.
		const leased = await worker.enqueue(0)
		expect(leased).toBeGreaterThan(0)
		// The fixture spins 2s ignoring its signal. A per-entry `signal` (distinct from the
		// timeout path) aborts the attempt MID-FLIGHT: the `Dispatch`'s abort handler posts the
		// abort, terminates the uncooperative thread, and flips `alive = false` so the pool
		// evicts it.
		const controller = new AbortController()
		const aborted = worker.enqueue(2_000, { signal: controller.signal })
		// Gate on the job actually being in flight before aborting — so this exercises the
		// in-flight `addEventListener('abort')` path, not the pre-flight `signal.aborted` short
		// circuit.
		await waitForCondition('the long job is in flight', () => worker.active === 1, {
			budget: 5_000,
		})
		expect(worker.active).toBe(1)
		controller.abort()
		// The attempt rejects (the exact reason races between the signal's `reason` and the
		// dispatch's abort error, so assert only that it rejects).
		await expect(aborted).rejects.toBeDefined()
		// The eviction this spec exists for: the aborted thread was terminated, so the pool
		// cannot hand it back and the replacement reports a DIFFERENT `threadId`. An abort that
		// merely rejected the job would return the same still-spinning thread to idle and this
		// job would report the leased id again.
		const replacement = await worker.enqueue(0, { timeout: 5_000 })
		expect(replacement).toBeGreaterThan(0)
		expect(replacement).not.toBe(leased)
	})

	it('serves a queued job on a fresh thread after a signal-abort eviction (handoff)', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('slow.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 1,
			}),
		)
		// Warm the pool so the aborted job leases an already-online idle thread and the abort
		// lands on the in-flight listener rather than on a pending spawn.
		await expect(worker.enqueue(1)).resolves.toBe(1)
		const controller = new AbortController()
		const aborted = worker.enqueue(2_000, { signal: controller.signal })
		// A SECOND job is queued behind the aborted one. At `concurrency: 1` it parks on the
		// pool (the only slot is leased by the in-flight job), so when that job's thread is
		// terminated + released the queued job is the PARKED WAITER the release hands off to —
		// it must receive a fresh thread, never the dead one the re-validating handoff drops.
		const queued = worker.enqueue(1)
		await waitForCondition('the long job is in flight', () => worker.active === 1, {
			budget: 5_000,
		})
		expect(worker.active).toBe(1)
		controller.abort()
		await expect(aborted).rejects.toBeDefined()
		// The tainted thread was terminated + marked dead, so the validated handoff destroys it
		// and serves the queued job a FRESH thread (it doubles `1 → 1` under a ms) — proving the
		// abort eviction reaches even a job parked on the pool, driven by an explicit signal.
		await expect(queued).resolves.toBe(1)
	})
})

describe('createNodeWorker — a thread crash mid-flight evicts the thread', () => {
	it('rejects the in-flight job when its thread crashes, then runs a later job on a fresh thread', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('crash.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 2,
			}),
		)
		// A NEGATIVE input makes the fixture `process.exit(1)` — the thread dies WITHOUT a
		// reply, so `ThreadWorker` emits `'exit'`/`'error'` while the job is in flight and
		// the `Dispatch`'s `onExit`/`onError` rejects it (not the normal `{ ok: false }` reply that
		// `fail.ts` covers) and marks the thread dead.
		await expect(worker.enqueue(-1)).rejects.toBeDefined()
		// The dead thread is never reused; a non-negative input then doubles on a FRESH thread,
		// proving the crashed thread was replaced.
		await expect(worker.enqueue(21)).resolves.toBe(42)
	})
})

describe('createNodeWorker — result-guard violation', () => {
	it('rejects when a reply does not satisfy the result guard', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('bad-result.ts'),
				input: isNumber,
				result: isNumber,
			}),
		)
		// The fixture replies with a string; the number `result` guard rejects it.
		await expect(worker.enqueue(3)).rejects.toThrow('reply did not satisfy result guard')
	})

	it('contains a throwing result guard and keeps the worker usable', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: isNumber,
				result: isThrowingResult,
				concurrency: 1,
			}),
		)
		await expect(worker.enqueue(1)).rejects.toThrow('result-guard-boom')
		await expect(worker.enqueue(2)).resolves.toBe(4)
	})
})

describe('createNodeWorker — input-guard fail-fast', () => {
	it('rejects a bad input before it crosses the boundary', async () => {
		const worker = track(
			createNodeWorker({ script: buildFixtureURL('double.ts'), input: isEven, result: isNumber }),
		)
		await expect(worker.enqueue(3)).rejects.toThrow('input did not satisfy input guard')
		// A valid input still works on the same worker.
		await expect(worker.enqueue(4)).resolves.toBe(8)
	})

	it('contains a throwing input guard and keeps the worker usable', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: isThrowingInput,
				result: isNumber,
				concurrency: 1,
			}),
		)
		await expect(worker.enqueue(1)).rejects.toThrow('input-guard-boom')
		await expect(worker.enqueue(2)).resolves.toBe(4)
	})
})

describe('createNodeWorker — non-cloneable result', () => {
	it('rejects the failed clone and serves a later job on the same concurrency-1 worker', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('noncloneable-result.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 1,
			}),
		)
		await expect(worker.enqueue(-1)).rejects.toThrow(/clone/i)
		await expect(worker.enqueue(5)).resolves.toBe(10)
	})
})

describe('createNodeWorker — non-cloneable input', () => {
	it('rejects (and does not leak) when the input cannot be structured-cloned', async () => {
		// A guard that admits any object, so the value crosses to `postMessage` — where a
		// function property fails the structured clone. The job must reject cleanly.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: unionOf(isRecord, isNumber),
				result: isNumber,
				concurrency: 1,
			}),
		)
		await expect(worker.enqueue({ fn: () => undefined })).rejects.toThrow(/clone/i)
		// A later valid number still works — the worker is not wedged.
		await expect(worker.enqueue(5)).resolves.toBe(10)
	})
})

describe('createNodeWorker — concurrency caps the live thread count + reuses idle threads', () => {
	it('spawns at most `concurrency` threads across a surplus of jobs (distinct ids ≤ cap)', async () => {
		// `identify.ts` echoes its OWN `threadId`. With a brief busy-spin per job and four
		// threads, eight jobs distribute across the pool — the SET of distinct ids returned can
		// never exceed `concurrency`, proving the pool grows to its cap and no further (one thread
		// per in-flight slot). All eight still complete.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('identify.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 4,
			}),
		)
		const ids = await Promise.all(Array.from({ length: 8 }, () => worker.enqueue(10)))
		expect(ids).toHaveLength(8)
		for (const id of ids) expect(id).toBeGreaterThan(0)
		expect(new Set(ids).size).toBeLessThanOrEqual(4)
	})

	it('reuses the same idle thread across sequential jobs (concurrency 1)', async () => {
		// At concurrency 1 the single pooled thread is returned to idle after each job and, still
		// `alive`, is re-validated and reused for the next — so every sequential job reports the
		// SAME `threadId`. Proves idle reuse (not a fresh thread per job).
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('identify.ts'),
				input: isNumber,
				result: isNumber,
			}),
		)
		const first = await worker.enqueue(0)
		const second = await worker.enqueue(0)
		const third = await worker.enqueue(0)
		expect(first).toBeGreaterThan(0)
		expect(second).toBe(first)
		expect(third).toBe(first)
	})
})

describe('createNodeWorker — workerData reaches the worker side', () => {
	it('clones `workerData` through to the thread (echoed back intact)', async () => {
		// `echo-data.ts` replies with the `workerData` cloned to it at spawn. A nested object
		// round-trips through the structured clone; the `result` guard narrows the echoed shape.
		const payload = { token: 'abc', limits: { max: 7, names: ['a', 'b'] } }
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('echo-data.ts'),
				input: isNumber,
				result: isWorkerPayload,
				workerData: payload,
			}),
		)
		await expect(worker.enqueue(1)).resolves.toEqual(payload)
	})

	it('surfaces a clear error (no hang) when `workerData` cannot be cloned', async () => {
		// A function is not structured-cloneable, so the `ThreadWorker` constructor throws a
		// DataCloneError SYNCHRONOUSLY inside the spawn (workerData is cloned at construction,
		// before the spawn promise). The pool's `create` propagates it and the job rejects cleanly
		// — never a silent hang waiting on a thread that never spawned.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: isNumber,
				result: isNumber,
				workerData: () => undefined,
			}),
		)
		await expect(worker.enqueue(1)).rejects.toThrow(/clone/i)
	})
})

describe('createNodeWorker — large / deep payloads round-trip', () => {
	it('clones a large array input and result across the boundary', async () => {
		// A 10k-element array sums on the thread; both the large input and the numeric result
		// survive the structured clone intact (a stress on the clone path rather than a scalar).
		const size = 10_000
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('sum.ts'),
				input: isNumberArray,
				result: isNumber,
			}),
		)
		const input = Array.from({ length: size }, (_unused, index) => index)
		const expected = (size * (size - 1)) / 2
		await expect(worker.enqueue(input)).resolves.toBe(expected)
	})
})

describe('createNodeWorker — a worker script that fails to load', () => {
	it('rejects the job cleanly when the worker script throws at module load', async () => {
		// `load-throw.ts` throws while evaluating. The thread comes `'online'` (so the spawn
		// resolves a live thread) then immediately `'error'`s / `'exit'`s — the death reaches the
		// in-flight `Dispatch`, which rejects the job. No hang: the broken script settles the job.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('load-throw.ts'),
				input: isNumber,
				result: isNumber,
			}),
		)
		await expect(worker.enqueue(1)).rejects.toBeDefined()
	})

	it('rejects a job whose worker script path does not exist', async () => {
		// A non-existent module: the thread bootstraps, fails to resolve the module, and dies the
		// same way — the job rejects rather than hanging on a reply that never comes. This was the
		// suite's one residual flake (a hang under full-suite load, NOT slow bootstrap): the death
		// events could all fire before the `Dispatch` attached its listeners, losing the signal. The
		// `NodeThread.death` latch settles that ordering deterministically (the `latched-death path`
		// suite pins the latch), so the spec runs at the default timeout again.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('does-not-exist.ts'),
				input: isNumber,
				result: isNumber,
			}),
		)
		await expect(worker.enqueue(1)).rejects.toBeDefined()
	})

	it('still serves a later job after a healthy script is used (pool recovers)', async () => {
		// The broken-script worker rejects + evicts its dead threads, but the FAILURE is per-worker;
		// proving the harness recovers, a SEPARATE healthy worker enqueued right after still works —
		// the runtime is not wedged by the prior load failures.
		const broken = track(
			createNodeWorker({
				script: buildFixtureURL('load-throw.ts'),
				input: isNumber,
				result: isNumber,
			}),
		)
		await expect(broken.enqueue(1)).rejects.toBeDefined()
		const healthy = track(
			createNodeWorker({ script: buildFixtureURL('double.ts'), input: isNumber, result: isNumber }),
		)
		await expect(healthy.enqueue(21)).resolves.toBe(42)
	})

	it('keeps replacing dead threads across retries + a second job (never wedges on a broken script)', async () => {
		// A broken-script worker with retries: each attempt spawns a fresh thread that comes
		// online and immediately dies, so the job rejects only after exhausting the budget — and
		// the pool never wedges. A SECOND job on the SAME worker is dispatched onto another freshly
		// spawned thread (the dead ones are evicted, never reused) and rejects the same way. Proves
		// the eviction-and-respawn cycle is durable under repeated load failures.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('load-throw.ts'),
				input: isNumber,
				result: isNumber,
				retries: 1,
			}),
		)
		await expect(worker.enqueue(1)).rejects.toBeDefined()
		await expect(worker.enqueue(2)).rejects.toBeDefined()
	})
})

describe('Dispatch — the latched-death path (post-death dispatch settles immediately)', () => {
	it('rejects a dispatch onto a thread that already died, from the latched death', async () => {
		const thread = await createThread(buildFixtureURL('double.ts'))
		await thread.worker.terminate()
		expect(thread.alive).toBe(false)
		expect(thread.death).toBeDefined()
		const controller = new AbortController()
		const pending = new Dispatch(
			thread,
			1,
			{ id: 'post-death', signal: controller.signal },
			isNumber,
		).promise
		await expect(pending).rejects.toBe(thread.death)
	})
})

describe('Dispatch — exact terminal causes', () => {
	it('rejects with the exact caller abort reason object', async () => {
		const thread = await createThread(buildFixtureURL('slow.ts'))
		try {
			const controller = new AbortController()
			const reason = Object.freeze({ command: 'stop', source: 'caller' })
			const pending = new Dispatch(
				thread,
				5_000,
				{ id: 'exact-abort', signal: controller.signal },
				isNumber,
			).promise
			controller.abort(reason)
			await expect(pending).rejects.toBe(reason)
		} finally {
			await thread.worker.terminate()
		}
	})

	it('rejects a code-1 crash with the exact latched thread death', async () => {
		const thread = await createThread(buildFixtureURL('crash.ts'))
		try {
			const controller = new AbortController()
			const pending = new Dispatch(
				thread,
				-1,
				{ id: 'exact-crash', signal: controller.signal },
				isNumber,
			).promise
			let failure: unknown
			try {
				await pending
			} catch (error: unknown) {
				failure = error
			}
			expect(failure).toBe(thread.death)
			expect(thread.death?.message).toBe('worker thread exited (code 1)')
		} finally {
			await thread.worker.terminate()
		}
	})
})

describe('Dispatch — a consumer-supplied NodeThread owns its own liveness', () => {
	it('rejects the aborted job and terminates the supplied worker, leaving `alive` untouched', async () => {
		// A foreign `NodeThread`: a real `ThreadWorker` this package did not produce, wrapped in a
		// plain object satisfying the published interface. The documented obligation on
		// `NodeThread` is that eviction reaches `alive` only for a thread this package produced,
		// so this proves the abort still rejects the job and terminates the supplied worker while
		// the implementer's own `alive` stays exactly as the implementer reports it.
		const spawned = await createThread(buildFixtureURL('slow.ts'))
		const foreign = { worker: spawned.worker, alive: true, death: undefined }
		try {
			const controller = new AbortController()
			const reason = Object.freeze({ command: 'stop', source: 'foreign' })
			const pending = new Dispatch(
				foreign,
				5_000,
				{ id: 'foreign-abort', signal: controller.signal },
				isNumber,
			).promise
			controller.abort(reason)
			await expect(pending).rejects.toBe(reason)
			expect(foreign.alive).toBe(true)
			// Control: the abort really terminated the supplied worker, so the reading is about
			// ownership of `alive` rather than about a dispatch that did nothing.
			expect(foreign.worker.threadId).toBe(-1)
		} finally {
			await foreign.worker.terminate()
		}
	})
})

describe('Dispatch — messageerror listener lifecycle', () => {
	it('attaches one stable listener for the job and removes it on settlement', async () => {
		const thread = await createThread(buildFixtureURL('double.ts'))
		try {
			const baseline = thread.worker.listenerCount('messageerror')
			const controller = new AbortController()
			const pending = new Dispatch(
				thread,
				2,
				{ id: 'listener-lifecycle', signal: controller.signal },
				isNumber,
			).promise
			expect(thread.worker.listenerCount('messageerror')).toBe(baseline + 1)
			await expect(pending).resolves.toBe(4)
			expect(thread.worker.listenerCount('messageerror')).toBe(baseline)
		} finally {
			await thread.worker.terminate()
		}
	})
})

describe('createNodeWorker — protocol robustness (stray messages)', () => {
	it('ignores stray / foreign-id messages and still resolves the correct reply', async () => {
		// `stray.ts` posts a message with NO `id` and a well-formed reply for a DIFFERENT id BEFORE
		// its real reply. The `Dispatch`'s `isReply` filter drops both (id mismatch), so the job still
		// resolves its own correct value — a thread that chatters on the channel can't corrupt a job.
		const worker = track(
			createNodeWorker({ script: buildFixtureURL('stray.ts'), input: isNumber, result: isNumber }),
		)
		await expect(worker.enqueue(21)).resolves.toBe(42)
	})
})

describe('createNodeWorker — protocol robustness (matching-id malformed reply)', () => {
	it('rejects, evicts the tainted thread, and serves later work on a replacement', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('malformed.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 1,
				timeout: 500,
			}),
		)
		await expect(worker.enqueue(-1)).rejects.toThrow('worker reply was malformed')
		await expect(worker.enqueue(21, { timeout: 5_000 })).resolves.toBe(42)
	})
})

describe('createNodeWorker — an already-aborted enqueue signal', () => {
	it('rejects without leaving the worker wedged (pre-flight abort short-circuit)', async () => {
		// A signal already aborted when the job reaches its `Dispatch` hits the `signal.aborted` short
		// circuit: it posts the abort + evicts and rejects WITHOUT awaiting a reply. The worker is
		// not wedged — a later un-aborted job on the same worker still succeeds.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 2,
			}),
		)
		const aborted = worker.enqueue(21, { signal: AbortSignal.abort() })
		await expect(aborted).rejects.toBeDefined()
		await expect(worker.enqueue(5)).resolves.toBe(10)
	})
})

describe('createNodeWorker — destroy with multiple threads mid-job', () => {
	it('terminates every thread while jobs are in flight (suite still exits)', async () => {
		// Three uncooperative jobs spin in flight across a 3-thread pool; `destroy()` aborts the
		// queue (rejecting them) AND tears the pool down, terminating all three threads. If any
		// leaked, vitest would hang at exit — so this passing + the process exiting IS the proof.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('slow.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 3,
			}),
		)
		const inflight = [5_000, 5_000, 5_000].map((input) =>
			worker.enqueue(input).catch((error: unknown) => error),
		)
		await waitForCondition('three jobs are in flight', () => worker.active === 3, {
			budget: 5_000,
		})
		expect(worker.active).toBe(3)
		await worker.destroy()
		// Every in-flight job settles (rejected by the abort), so nothing dangles.
		const settled = await Promise.all(inflight)
		expect(settled).toHaveLength(3)
		expect(worker.stopped).toBe(true)
	})
})

describe('createNodeWorker — rapid enqueue / abort churn through the pool', () => {
	it('settles every job and leaves the counts at rest (no thread leak)', async () => {
		// Twelve jobs, every other one aborted mid-flight, through a 3-thread pool — the churn the
		// pool's terminate-and-replace handoff must survive. Each aborted job evicts + replaces its
		// thread; each surviving job runs on a fresh / reused one. After the dust settles `active`
		// and `count` return to 0 (no leaked in-flight slot, no orphaned thread), and the worker
		// still serves a final clean job. The trailing `destroy` (afterEach) terminates the pool.
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 3,
			}),
		)
		const settlements = await Promise.all(
			Array.from({ length: 12 }, (_unused, index) => {
				if (index % 2 === 0) {
					const controller = new AbortController()
					const pending = worker.enqueue(index, { signal: controller.signal })
					controller.abort()
					return pending.then(
						() => 'resolved',
						() => 'rejected',
					)
				}
				return worker.enqueue(index).then(
					(value) => (value === index * 2 ? 'resolved' : 'wrong'),
					() => 'rejected',
				)
			}),
		)
		// Every job settled one way or the other — none dangled.
		expect(settlements).toHaveLength(12)
		// The odd (un-aborted) jobs all resolved correctly.
		expect(settlements.filter((outcome) => outcome === 'wrong')).toEqual([])
		// Counts return to rest: no leaked in-flight slot.
		expect(worker.active).toBe(0)
		expect(worker.count).toBe(0)
		// The worker is not wedged — a final clean job still completes.
		await expect(worker.enqueue(50)).resolves.toBe(100)
	})
})

describe('createNodeWorker — destroy terminates every thread', () => {
	it('tears down so the process can exit (no hanging threads)', async () => {
		const worker = track(
			createNodeWorker({
				script: buildFixtureURL('double.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 2,
			}),
		)
		await worker.enqueue(2)
		// destroy() must terminate the pooled threads; if any leaked, the test runner
		// would hang at exit. A second destroy is idempotent.
		await worker.destroy()
		await worker.destroy()
		expect(worker.stopped).toBe(true)
	})
})

describe('fixture path resolves inside a thread', () => {
	it('loads a raw `.ts` worker script through the relative-to-source import', () => {
		// A guard on the fixture URL itself — the `createNodeWorker` round-trip suites are the proof
		// the relative `serveWorker` import resolves when Node loads the script in a thread.
		expect(fileURLToPath(buildFixtureURL('double.ts'))).toContain('fixtures')
	})
})

describe('isReply — the reply-envelope predicate dispatch filters on', () => {
	const id = 'job-1'

	it('accepts a well-formed success reply for the id (any value, including falsy)', () => {
		expect(isReply({ id, ok: true, value: 42 }, id)).toBe(true)
		expect(isReply({ id, ok: true, value: 0 }, id)).toBe(true)
		expect(isReply({ id, ok: true, value: undefined }, id)).toBe(true)
		expect(isReply({ id, ok: true, value: null }, id)).toBe(true)
	})

	it('accepts a well-formed failure reply for the id (string error)', () => {
		expect(isReply({ id, ok: false, error: 'boom' }, id)).toBe(true)
	})

	it('rejects a success envelope without its required value', () => {
		expect(isReply({ id, ok: true }, id)).toBe(false)
	})

	it('rejects a reply whose id does not match (a foreign job)', () => {
		expect(isReply({ id: 'other', ok: true, value: 1 }, id)).toBe(false)
		expect(isReply({ id: 'other', ok: false, error: 'x' }, id)).toBe(false)
	})

	it('rejects a failure whose error is not a string (malformed payload)', () => {
		expect(isReply({ id, ok: false, error: 7 }, id)).toBe(false)
		expect(isReply({ id, ok: false }, id)).toBe(false)
	})

	it('rejects a malformed ok discriminant (neither true nor false)', () => {
		expect(isReply({ id, ok: 'yes', value: 1 }, id)).toBe(false)
		expect(isReply({ id, value: 1 }, id)).toBe(false)
	})

	it('rejects non-records and stray messages (no id) — total, never throws', () => {
		expect(isReply(null, id)).toBe(false)
		expect(isReply(undefined, id)).toBe(false)
		expect(isReply('reply', id)).toBe(false)
		expect(isReply(42, id)).toBe(false)
		expect(isReply([id], id)).toBe(false)
		expect(isReply({ ok: true, value: 1 }, id)).toBe(false)
	})

	it('contains hostile property getters and returns false', () => {
		expect(
			isReply(
				{
					get id(): string {
						throw new Error('hostile id')
					},
				},
				id,
			),
		).toBe(false)
		expect(
			isReply(
				{
					id,
					get ok(): boolean {
						throw new Error('hostile ok')
					},
				},
				id,
			),
		).toBe(false)
		expect(
			isReply(
				{
					id,
					ok: false,
					get error(): string {
						throw new Error('hostile error')
					},
				},
				id,
			),
		).toBe(false)
	})
})
