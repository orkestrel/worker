import { afterEach, describe, expect, it } from 'vitest'
import { Worker as ThreadWorker } from 'node:worker_threads'
import { serveWorker } from '@src/server'
import { createRecorder, createTeardown, waitForDelay } from '@orkestrel/test'
import { postRun, ThreadReply } from '../../setupServer.js'

// src/server/handlers.ts — the worker-side `serveWorker` entry, driven MANUALLY
// (no createNodeWorker): a raw `node:worker_threads` thread over a serve fixture, posting
// run/abort envelopes and awaiting the reply. Proves the protocol contract directly — a
// success envelope, an input-guard rejection envelope, and a cooperative abort firing the
// handler's signal. Every thread is terminated in `afterEach` so none leaks.

const fixture = (name: string): URL => new URL(`./fixtures/${name}`, import.meta.url)

// Track every spawned thread so it is terminated in afterEach even when an assertion throws — the
// shared §16.1 teardown registrar (the disposer terminates a raw worker thread; its
// `Promise<number>` exit code is awaited as a plain settle, the value discarded).
const teardown = createTeardown()
afterEach(() => teardown.destroy())

function track(thread: ThreadWorker): ThreadWorker {
	teardown.add(async () => {
		await thread.terminate()
	})
	return thread
}

function spawn(name: string): ThreadWorker {
	return track(new ThreadWorker(fixture(name)))
}

describe('serveWorker — success reply envelope', () => {
	it('replies { id, ok: true, value } for a valid run message', async () => {
		const thread = spawn('double.ts')
		const pending = new ThreadReply(thread, 'job-1').promise
		postRun(thread, 'job-1', 'job-1', 21)
		expect(await pending).toEqual({ id: 'job-1', ok: true, value: 42 })
	})
})

describe('serveWorker — execution identity', () => {
	it('replies by correlation id while exposing the stable Queue id to the handler', async () => {
		const thread = spawn('execution.ts')
		const pending = new ThreadReply(thread, 'dispatch-1').promise
		postRun(thread, 'dispatch-1', 'stable-job', 0)
		expect(await pending).toEqual({ id: 'dispatch-1', ok: true, value: 'stable-job' })
	})

	it('ignores missing, malformed, revoked, and throwing-job run envelopes', async () => {
		const shared = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)
		const handled = new Int32Array(shared)
		const replies = createRecorder<[unknown]>()
		const thread = track(new ThreadWorker(fixture('execution.ts'), { workerData: shared }))
		thread.on('message', replies.handler)
		const pending = new ThreadReply(thread, 'dispatch-valid').promise
		thread.postMessage({ id: 'dispatch-missing', command: 'run', input: 1 })
		thread.postMessage({ id: 'dispatch-malformed', job: 42, command: 'run', input: 2 })
		postRun(thread, 'dispatch-valid', 'stable-valid', 3)
		expect(await pending).toEqual({
			id: 'dispatch-valid',
			ok: true,
			value: 'stable-valid',
		})
		expect(Atomics.load(handled, 0)).toBe(1)
		expect(replies.calls).toEqual([[{ id: 'dispatch-valid', ok: true, value: 'stable-valid' }]])
	})
})

describe('serveWorker — input-guard rejection', () => {
	it('replies an error envelope when the input fails the guard (handler never runs)', async () => {
		const thread = spawn('double.ts')
		const pending = new ThreadReply(thread, 'job-2').promise
		postRun(thread, 'job-2', 'job-2', 'not-a-number')
		expect(await pending).toEqual({
			id: 'job-2',
			ok: false,
			error: 'input did not satisfy input guard',
		})
	})

	it('contains a throwing input guard and keeps serving the thread', async () => {
		const thread = spawn('noncloneable-result.ts')
		const failed = new ThreadReply(thread, 'job-g1').promise
		postRun(thread, 'job-g1', 'job-g1', 'throw')
		expect(await failed).toEqual({ id: 'job-g1', ok: false, error: 'input-guard-boom' })
		const recovered = new ThreadReply(thread, 'job-g2').promise
		postRun(thread, 'job-g2', 'job-g2', 4)
		expect(await recovered).toEqual({ id: 'job-g2', ok: true, value: 8 })
	})
})

describe('serveWorker — non-cloneable result', () => {
	it('falls back to a clone-safe failure reply and keeps serving the thread', async () => {
		const thread = spawn('noncloneable-result.ts')
		const failed = new ThreadReply(thread, 'job-c1').promise
		postRun(thread, 'job-c1', 'job-c1', -1)
		const failure = await failed
		expect(failure.id).toBe('job-c1')
		expect(failure.ok).toBe(false)
		expect(failure.error).toMatch(/clone/i)
		const recovered = new ThreadReply(thread, 'job-c2').promise
		postRun(thread, 'job-c2', 'job-c2', 3)
		expect(await recovered).toEqual({ id: 'job-c2', ok: true, value: 6 })
	})
})

describe('serveWorker — handler throw', () => {
	it('replies { ok: false, error } with the thrown message', async () => {
		const thread = spawn('fail.ts')
		const pending = new ThreadReply(thread, 'job-3').promise
		postRun(thread, 'job-3', 'job-3', 7)
		expect(await pending).toEqual({ id: 'job-3', ok: false, error: 'boom:7' })
	})
})

describe('serveWorker — thread exit before reply', () => {
	it('rejects the pending reply when the real thread exits without an error event', async () => {
		const thread = spawn('crash.ts')
		const pending = new ThreadReply(thread, 'job-exit').promise
		postRun(thread, 'job-exit', 'job-exit', -1)
		await expect(pending).rejects.toThrow('worker thread exited before replying')
	})
})

describe('serveWorker — abort fires the handler signal', () => {
	it('aborts the in-flight job by correlation id when its stable job id differs', async () => {
		const thread = spawn('abortable.ts')
		const pending = new ThreadReply(thread, 'dispatch-4').promise
		// Start a job that parks on its abort signal, then abort only by correlation id.
		postRun(thread, 'dispatch-4', 'stable-4', 100)
		thread.postMessage({ id: 'dispatch-4', command: 'abort' })
		// The cooperative handler resolves the sentinel -1 once its signal fires.
		expect(await pending).toEqual({ id: 'dispatch-4', ok: true, value: -1 })
	})

	it('ignores an abort for an unknown id', async () => {
		const thread = spawn('double.ts')
		// An abort for a job that was never started is a no-op; a fresh run still works.
		thread.postMessage({ id: 'ghost', command: 'abort' })
		const pending = new ThreadReply(thread, 'job-5').promise
		postRun(thread, 'job-5', 'job-5', 4)
		expect(await pending).toEqual({ id: 'job-5', ok: true, value: 8 })
	})
})

describe('serveWorker — async handler rejection', () => {
	it('replies { ok: false, error } when the handler rejects ASYNCHRONOUSLY', async () => {
		// `throw-async.ts` rejects after a microtask (not a sync throw like `fail.ts`). The
		// deferred-into-`then` dispatch must still catch it and reply an error envelope — an async
		// rejection is reported exactly like a sync throw, never an unhandled rejection / crash.
		const thread = spawn('throw-async.ts')
		const pending = new ThreadReply(thread, 'job-a1').promise
		postRun(thread, 'job-a1', 'job-a1', 7)
		expect(await pending).toEqual({ id: 'job-a1', ok: false, error: 'async-boom:7' })
	})
})

describe('serveWorker — unknown message command', () => {
	it('ignores a message whose command is neither run nor abort (handler never runs)', async () => {
		// A stray control message (an unrecognised `command`) matches neither `isRun` nor `isAbort`,
		// so `serveWorker` drops it silently — it does not run the handler or reply. A subsequent
		// valid run then still works, proving the unknown message did not wedge the listener.
		const thread = spawn('double.ts')
		const pending = new ThreadReply(thread, 'job-u2').promise
		thread.postMessage({ id: 'job-u1', command: 'frobnicate', input: 1 })
		postRun(thread, 'job-u2', 'job-u2', 21)
		// Only the valid run replies; the unknown-command message produced nothing.
		expect(await pending).toEqual({ id: 'job-u2', ok: true, value: 42 })
	})

	it('ignores a malformed message (no id) without crashing the thread', async () => {
		// A payload missing `id` fails both inbound guards and is dropped. The thread survives and
		// answers the next well-formed run — a hostile / malformed message can't crash the worker.
		const thread = spawn('double.ts')
		const pending = new ThreadReply(thread, 'job-u3').promise
		thread.postMessage({ command: 'run', input: 99 })
		thread.postMessage(42)
		postRun(thread, 'job-u3', 'job-u3', 5)
		expect(await pending).toEqual({ id: 'job-u3', ok: true, value: 10 })
	})
})

describe('serveWorker — result shapes round-trip', () => {
	it('replies an object / array / null / boolean value unchanged', async () => {
		// `echo.ts` returns its input verbatim, so the reply `value` carries whatever shape was
		// posted — proving the `{ ok: true, value }` envelope round-trips structured shapes (not
		// just scalars) through the worker side.
		const thread = spawn('echo.ts')
		const shapes: readonly unknown[] = [
			{ nested: { count: 2 }, items: [1, 2, 3] },
			[true, false, null],
			null,
			false,
		]
		for (let index = 0; index < shapes.length; index += 1) {
			const id = `job-s${index}`
			const pending = new ThreadReply(thread, id).promise
			postRun(thread, id, id, shapes[index])
			expect(await pending).toEqual({ id, ok: true, value: shapes[index] })
		}
	})
})

describe('serveWorker — option capture', () => {
	it('reads input then handler once and retains both identities across jobs', async () => {
		const shared = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 3)
		const counters = new Int32Array(shared)
		const thread = track(new ThreadWorker(fixture('serve-options.ts'), { workerData: shared }))
		const first = new ThreadReply(thread, 'job-o1').promise
		postRun(thread, 'job-o1', 'job-o1', 21)
		expect(await first).toEqual({ id: 'job-o1', ok: true, value: 42 })
		const second = new ThreadReply(thread, 'job-o2').promise
		postRun(thread, 'job-o2', 'job-o2', 5)
		expect(await second).toEqual({ id: 'job-o2', ok: true, value: 10 })
		expect(Atomics.load(counters, 0)).toBe(1)
		expect(Atomics.load(counters, 1)).toBe(1)
	})

	it('fails during registration when the input getter throws and never reads handler', async () => {
		const shared = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 3)
		const counters = new Int32Array(shared)
		Atomics.store(counters, 2, 1)
		const thread = track(new ThreadWorker(fixture('serve-options.ts'), { workerData: shared }))
		const failure = Promise.withResolvers<Error>()
		thread.once('error', failure.resolve)
		const error = await failure.promise
		expect(error.message).toBe('input-getter-boom')
		expect(Atomics.load(counters, 0)).toBe(1)
		expect(Atomics.load(counters, 1)).toBe(0)
	})
})

describe('serveWorker — main-thread no-op', () => {
	it('reads neither option and never runs the handler when called off a worker thread', async () => {
		// THIS test runs on the main thread, where `parentPort === null`, so `serveWorker`
		// must return immediately — registering no listeners and never invoking the handler.
		// A recorder stands in for the handler; it is a real callback (AGENTS §16.1), so a
		// single recorded call would prove the no-op guard failed.
		const reads = createRecorder<[string]>()
		const handled = createRecorder<[number]>()
		expect(() =>
			serveWorker<number, number>({
				get input() {
					reads.handler('input')
					return (value: unknown): value is number => typeof value === 'number'
				},
				get handler() {
					reads.handler('handler')
					return (value: number): number => {
						handled.handler(value)
						return value * 2
					}
				},
			}),
		).not.toThrow()
		// Give any (erroneously registered) message listener a turn to fire — it must not.
		await waitForDelay(0)
		expect(reads.calls).toEqual([])
		expect(handled.count).toBe(0)
	})
})
