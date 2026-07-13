import { describe, expect, it } from 'vitest'
import { fileURLToPath } from 'node:url'
import { createNodeWorker, dispatch, isReply, spawnThread } from '@src/server'
import { waitForDelay } from '../../../setup.js'
import { createTeardown } from '../../../setupServer.js'

// src/server/workers/helpers.ts — the main-side worker-thread machinery (`spawnThread` /
// `dispatch` / `isReply`) `createNodeWorker` composes over. The round-trip suites below
// drive `createNodeWorker` over REAL worker threads (no mocking; the node `src:server`
// project), exercising `spawnThread` + `dispatch` (and `dispatch`'s internal `isReply`
// filtering) END TO END — each stands up a worker against a real fixture script, drives
// jobs, and tears it down in `afterEach` so no thread leaks and the process exits; the
// fixtures are raw `.ts` loaded by Node's type-stripping (Node ≥ 23.6), paths resolved from
// this file's URL. A final focused suite unit-tests the `isReply` reply-envelope guard
// directly (the predicate `dispatch` uses to drop stray / foreign-id / malformed messages).

// A `number` guard reused as both the input and result narrower (the zero-`as` bridge).
const isNumber = (value: unknown): value is number => typeof value === 'number'

// The fixtures directory, resolved from this test's URL so the runner's cwd never matters.
const fixture = (name: string): URL => new URL(`./fixtures/${name}`, import.meta.url)

// Track every worker so it is destroyed even when an assertion throws.
const { track } = createTeardown((worker: { destroy(): void }) => worker.destroy())

describe('createNodeWorker — round-trip over a thread', () => {
	it('dispatches the input to a thread and resolves the narrowed reply', async () => {
		const worker = track(
			createNodeWorker({ script: fixture('double.ts'), input: isNumber, result: isNumber }),
		)
		await expect(worker.enqueue(21)).resolves.toBe(42)
	})

	it('runs many jobs on a small pool and returns every result', async () => {
		const worker = track(
			createNodeWorker({
				script: fixture('double.ts'),
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
				script: fixture('double.ts'),
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
			createNodeWorker({ script: fixture('fail.ts'), input: isNumber, result: isNumber }),
		)
		await expect(worker.enqueue(5)).rejects.toThrow('boom:5')
	})

	it('re-runs a failing job up to its retry budget (then rejects)', async () => {
		const worker = track(
			createNodeWorker({
				script: fixture('fail.ts'),
				input: isNumber,
				result: isNumber,
				retries: 2,
			}),
		)
		// Three attempts (1 + 2 retries), all throw — the job still rejects with the error.
		await expect(worker.enqueue(9)).rejects.toThrow('boom:9')
	})
})

describe('createNodeWorker — timeout terminates + evicts the thread', () => {
	it('rejects on the per-attempt timeout and replaces the tainted thread', async () => {
		const worker = track(
			createNodeWorker({
				script: fixture('slow.ts'),
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
				script: fixture('slow.ts'),
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

// `concurrency: 1` is deliberate in the eviction tests below: the pool's `max` matches it, so
// when the aborted job's thread is released a SECOND, already-queued job is PARKED as a pool
// waiter (the pool is at `max` with the dead thread still leased) — the exact contention that
// drives the pool's FIFO `release`→waiter HANDOFF. That handoff now re-validates the released
// resource (the core `Pool` fix), so the dead thread is destroyed and the parked job is served
// a FRESH thread instead of the terminated one — proving eviction through the handoff path
// end to end (not merely through `grow`, which a `concurrency: 2` pool would have masked).
describe('createNodeWorker — in-flight signal abort terminates + evicts the thread', () => {
	it('rejects the aborted attempt and replaces the (signal-ignoring) thread', async () => {
		const worker = track(
			createNodeWorker({
				script: fixture('slow.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 1,
			}),
		)
		// The fixture spins 2s ignoring its signal. A per-entry `signal` (distinct from the
		// timeout path) aborts the attempt MID-FLIGHT: `dispatch`'s `onAbort` posts the abort,
		// terminates the uncooperative thread, and flips `alive = false` so the pool evicts it.
		const controller = new AbortController()
		const aborted = worker.enqueue(2_000, { signal: controller.signal })
		// Gate on the job actually being in flight before aborting — so this exercises the
		// in-flight `addEventListener('abort')` path, not the pre-flight `signal.aborted` short
		// circuit (short real timer, AGENTS §16; the eviction proof below carries determinism).
		await waitForDelay(20)
		expect(worker.active).toBe(1)
		controller.abort()
		// The attempt rejects (the exact reason races between the signal's `reason` and the
		// dispatch's abort error, so assert only that it rejects — the eviction is proven next).
		await expect(aborted).rejects.toBeDefined()
	})

	it('serves a queued job on a fresh thread after a signal-abort eviction (handoff)', async () => {
		const worker = track(
			createNodeWorker({
				script: fixture('slow.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 1,
			}),
		)
		const controller = new AbortController()
		const aborted = worker.enqueue(2_000, { signal: controller.signal })
		// A SECOND job is queued behind the aborted one. At `concurrency: 1` it parks on the
		// pool (the only slot is leased by the in-flight job), so when that job's thread is
		// terminated + released the queued job is the PARKED WAITER the release hands off to —
		// it must receive a fresh thread, never the dead one the (now-validated) handoff drops.
		const queued = worker.enqueue(1)
		await waitForDelay(20)
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
				script: fixture('crash.ts'),
				input: isNumber,
				result: isNumber,
				concurrency: 2,
			}),
		)
		// A NEGATIVE input makes the fixture `process.exit(1)` — the thread dies WITHOUT a
		// reply, so `ThreadWorker` emits `'exit'`/`'error'` while the job is in flight and
		// `dispatch`'s `onExit`/`onError` rejects it (not the normal `{ ok: false }` reply that
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
			createNodeWorker({ script: fixture('bad-result.ts'), input: isNumber, result: isNumber }),
		)
		// The fixture replies with a string; the number `result` guard rejects it.
		await expect(worker.enqueue(3)).rejects.toThrow('reply did not satisfy result guard')
	})
})

describe('createNodeWorker — input-guard fail-fast', () => {
	it('rejects a bad input before it crosses the boundary', async () => {
		const onlyEven = (value: unknown): value is number =>
			typeof value === 'number' && value % 2 === 0
		const worker = track(
			createNodeWorker({ script: fixture('double.ts'), input: onlyEven, result: isNumber }),
		)
		await expect(worker.enqueue(3)).rejects.toThrow('input did not satisfy input guard')
		// A valid input still works on the same worker.
		await expect(worker.enqueue(4)).resolves.toBe(8)
	})
})

describe('createNodeWorker — non-cloneable input', () => {
	it('rejects (and does not leak) when the input cannot be structured-cloned', async () => {
		// A guard that admits any object, so the value crosses to `postMessage` — where a
		// function property fails the structured clone. The job must reject cleanly.
		const isObject = (value: unknown): value is { fn: () => void } =>
			typeof value === 'object' && value !== null
		const worker = track(
			createNodeWorker({ script: fixture('double.ts'), input: isObject, result: isNumber }),
		)
		await expect(worker.enqueue({ fn: () => undefined })).rejects.toThrow(/clone/i)
		// A later valid number still works — the worker is not wedged.
		const ok = track(
			createNodeWorker({ script: fixture('double.ts'), input: isNumber, result: isNumber }),
		)
		await expect(ok.enqueue(5)).resolves.toBe(10)
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
				script: fixture('identify.ts'),
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
			createNodeWorker({ script: fixture('identify.ts'), input: isNumber, result: isNumber }),
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
		const isPayload = (value: unknown): value is typeof payload => {
			if (typeof value !== 'object' || value === null || !('token' in value)) return false
			const token: unknown = value.token
			return typeof token === 'string'
		}
		const worker = track(
			createNodeWorker({
				script: fixture('echo-data.ts'),
				input: isNumber,
				result: isPayload,
				workerData: payload,
			}),
		)
		await expect(worker.enqueue(1)).resolves.toEqual(payload)
	})

	it('surfaces a clear error (no hang) when `workerData` cannot be cloned', async () => {
		// A function is not structured-cloneable, so the `ThreadWorker` constructor throws a
		// DataCloneError SYNCHRONOUSLY inside `spawnThread` (workerData is cloned at construction,
		// before the spawn promise). The pool's `create` propagates it and the job rejects cleanly
		// — never a silent hang waiting on a thread that never spawned.
		const worker = track(
			createNodeWorker({
				script: fixture('double.ts'),
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
		// survive the structured clone intact (a stress on the clone path, not just a scalar).
		const size = 10_000
		const isArray = (value: unknown): value is number[] => Array.isArray(value)
		const worker = track(
			createNodeWorker({ script: fixture('sum.ts'), input: isArray, result: isNumber }),
		)
		const input = Array.from({ length: size }, (_unused, index) => index)
		const expected = (size * (size - 1)) / 2
		await expect(worker.enqueue(input)).resolves.toBe(expected)
	})
})

describe('createNodeWorker — a worker script that fails to load', () => {
	it('rejects the job cleanly when the worker script throws at module load', async () => {
		// `load-throw.ts` throws while evaluating. The thread comes `'online'` (so `spawnThread`
		// resolves a live thread) then immediately `'error'`s / `'exit'`s — the death reaches the
		// in-flight `dispatch`, which rejects the job. No hang: the broken script settles the job.
		const worker = track(
			createNodeWorker({ script: fixture('load-throw.ts'), input: isNumber, result: isNumber }),
		)
		await expect(worker.enqueue(1)).rejects.toBeDefined()
	})

	it('rejects a job whose worker script path does not exist', async () => {
		// A non-existent module: the thread bootstraps, fails to resolve the module, and dies the
		// same way — the job rejects rather than hanging on a reply that never comes. This was the
		// suite's one residual flake (a hang under full-suite load, NOT slow bootstrap): the death
		// events could all fire before `dispatch` attached its listeners, losing the signal. The
		// `NodeThread.death` latch settles that ordering deterministically (the batched-drain spec
		// below pins it), so the spec runs at the default timeout again.
		const worker = track(
			createNodeWorker({ script: fixture('does-not-exist.ts'), input: isNumber, result: isNumber }),
		)
		await expect(worker.enqueue(1)).rejects.toBeDefined()
	})

	it('rejects even when the death events are batched behind a stalled event loop', async () => {
		// The deterministic pin of the full-suite flake. Under load the main thread can stall
		// long enough that the thread has ALREADY died by the time its events are processed —
		// Node then delivers `online` + `error` + `exit` in ONE synchronous exit-drain batch, so
		// every death event fires BEFORE `dispatch`'s microtask-queued listener attach (the
		// microtask chain from `spawnThread`'s `online` resolve is starved until after `exit`).
		// The stall below reproduces that interleaving on demand: enqueue, let one macrotask
		// pass (the thread is constructed within the enqueue's microtask chain), then monopolise
		// the loop until the thread is long dead. Without the `NodeThread.death` latch the
		// dispatch would await events that already fired — forever; with it, the job rejects.
		const worker = track(
			createNodeWorker({ script: fixture('does-not-exist.ts'), input: isNumber, result: isNumber }),
		)
		const pending = worker.enqueue(1)
		await waitForDelay(0)
		const until = Date.now() + 500
		while (Date.now() < until) {
			// Monopolise the event loop so the thread's whole death is batched behind the stall.
		}
		await expect(pending).rejects.toBeDefined()
	})

	it('still serves a later job once a healthy script is used (pool recovers)', async () => {
		// The broken-script worker rejects + evicts its dead threads, but the FAILURE is per-worker;
		// proving the harness recovers, a SEPARATE healthy worker enqueued right after still works —
		// the runtime is not wedged by the prior load failures.
		const broken = track(
			createNodeWorker({ script: fixture('load-throw.ts'), input: isNumber, result: isNumber }),
		)
		await expect(broken.enqueue(1)).rejects.toBeDefined()
		const healthy = track(
			createNodeWorker({ script: fixture('double.ts'), input: isNumber, result: isNumber }),
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
				script: fixture('load-throw.ts'),
				input: isNumber,
				result: isNumber,
				retries: 1,
			}),
		)
		await expect(worker.enqueue(1)).rejects.toBeDefined()
		await expect(worker.enqueue(2)).rejects.toBeDefined()
	})
})

describe('dispatch — the latched-death path (post-death dispatch settles immediately)', () => {
	it('rejects a dispatch onto a thread that already died, from the latched death', async () => {
		// `load-throw.ts` comes online (so `spawnThread` resolves) then dies at module
		// evaluation. Once the thread has died, EVERY death event has already fired — a
		// dispatch attaching now could never observe one. The wait below is on the LATCH
		// itself (not an `exit` listener, which under a batched drain could likewise attach
		// too late) — once `death` is set, the dispatch must reject immediately with it.
		const thread = await spawnThread(fixture('load-throw.ts'), undefined)
		while (thread.death === undefined) await waitForDelay(5)
		expect(thread.alive).toBe(false)
		const controller = new AbortController()
		const pending = dispatch(thread, 1, { id: 'post-death', signal: controller.signal }, isNumber)
		await expect(pending).rejects.toBe(thread.death)
	})
})

describe('createNodeWorker — protocol robustness (stray messages)', () => {
	it('ignores stray / foreign-id messages and still resolves the correct reply', async () => {
		// `stray.ts` posts a message with NO `id` and a well-formed reply for a DIFFERENT id BEFORE
		// its real reply. `dispatch`'s `isReply` guard drops both (id mismatch), so the job still
		// resolves its own correct value — a thread that chatters on the channel can't corrupt a job.
		const worker = track(
			createNodeWorker({ script: fixture('stray.ts'), input: isNumber, result: isNumber }),
		)
		await expect(worker.enqueue(21)).resolves.toBe(42)
	})
})

describe('createNodeWorker — an already-aborted enqueue signal', () => {
	it('rejects without leaving the worker wedged (pre-flight abort short-circuit)', async () => {
		// A signal already aborted when the job reaches `dispatch` hits the `signal.aborted` short
		// circuit: it posts the abort + evicts and rejects WITHOUT awaiting a reply. The worker is
		// not wedged — a later un-aborted job on the same worker still succeeds.
		const worker = track(
			createNodeWorker({
				script: fixture('double.ts'),
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
		const worker = createNodeWorker({
			script: fixture('slow.ts'),
			input: isNumber,
			result: isNumber,
			concurrency: 3,
		})
		const inflight = [5_000, 5_000, 5_000].map((input) =>
			worker.enqueue(input).catch((error: unknown) => error),
		)
		await waitForDelay(20)
		expect(worker.active).toBe(3)
		worker.destroy()
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
				script: fixture('double.ts'),
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
		const worker = createNodeWorker({
			script: fixture('double.ts'),
			input: isNumber,
			result: isNumber,
			concurrency: 2,
		})
		await worker.enqueue(2)
		// destroy() must terminate the pooled threads; if any leaked, the test runner
		// would hang at exit. A second destroy is idempotent.
		worker.destroy()
		worker.destroy()
		expect(worker.stopped).toBe(true)
	})
})

describe('fixture path resolves inside a thread', () => {
	it('loads a raw `.ts` worker script via the relative-to-source import', () => {
		// A guard on the fixture URL itself — the round-trip tests above are the real proof
		// the relative `serveWorker` import resolves when Node loads the script in a thread.
		expect(fileURLToPath(fixture('double.ts'))).toContain('fixtures')
	})
})

describe('isReply — the reply-envelope guard dispatch filters on', () => {
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
})
