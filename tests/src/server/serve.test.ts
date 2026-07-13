import { describe, expect, it } from 'vitest'
import { Worker as ThreadWorker } from 'node:worker_threads'
import { serveWorker } from '@src/server'
import { createRecorder, waitForDelay } from '../../../setup.js'
import { createTeardown } from '../../../setupServer.js'

// src/server/workers/serve.ts — the worker-side `serveWorker` entry, driven MANUALLY
// (no createNodeWorker): a raw `node:worker_threads` thread over a serve fixture, posting
// run/abort envelopes and awaiting the reply. Proves the protocol contract directly — a
// success envelope, an input-guard rejection envelope, and a cooperative abort firing the
// handler's signal. Every thread is terminated in `afterEach` so none leaks.

const fixture = (name: string): URL => new URL(`./fixtures/${name}`, import.meta.url)

// Track every spawned thread so it is terminated in afterEach even when an assertion throws — the
// shared §16.1 teardown registrar (the disposer terminates a raw worker thread; its
// `Promise<number>` exit code is awaited as a plain settle, the value discarded).
const { track } = createTeardown(async (thread: ThreadWorker) => {
	await thread.terminate()
})

function spawn(name: string): ThreadWorker {
	return track(new ThreadWorker(fixture(name)))
}

// Resolve the first message whose `id` matches — the thread's reply for that job.
function reply(thread: ThreadWorker, id: string): Promise<Record<string, unknown>> {
	return new Promise((resolve, reject) => {
		const onMessage = (value: unknown): void => {
			if (typeof value !== 'object' || value === null) return
			const record: Record<string, unknown> = { ...value }
			if (record.id !== id) return
			thread.off('message', onMessage)
			thread.off('error', onError)
			resolve(record)
		}
		const onError = (error: Error): void => {
			thread.off('message', onMessage)
			reject(error)
		}
		thread.on('message', onMessage)
		thread.once('error', onError)
	})
}

describe('serveWorker — success reply envelope', () => {
	it('replies { id, ok: true, value } for a valid run message', async () => {
		const thread = spawn('double.ts')
		const pending = reply(thread, 'job-1')
		thread.postMessage({ id: 'job-1', command: 'run', input: 21 })
		expect(await pending).toEqual({ id: 'job-1', ok: true, value: 42 })
	})
})

describe('serveWorker — input-guard rejection', () => {
	it('replies an error envelope when the input fails the guard (handler never runs)', async () => {
		const thread = spawn('double.ts')
		const pending = reply(thread, 'job-2')
		thread.postMessage({ id: 'job-2', command: 'run', input: 'not-a-number' })
		expect(await pending).toEqual({
			id: 'job-2',
			ok: false,
			error: 'input did not satisfy input guard',
		})
	})
})

describe('serveWorker — handler throw', () => {
	it('replies { ok: false, error } with the thrown message', async () => {
		const thread = spawn('fail.ts')
		const pending = reply(thread, 'job-3')
		thread.postMessage({ id: 'job-3', command: 'run', input: 7 })
		expect(await pending).toEqual({ id: 'job-3', ok: false, error: 'boom:7' })
	})
})

describe('serveWorker — abort fires the handler signal', () => {
	it('aborts the in-flight job by id on an abort message', async () => {
		const thread = spawn('abortable.ts')
		const pending = reply(thread, 'job-4')
		// Start a job that parks on its abort signal, then abort it by id.
		thread.postMessage({ id: 'job-4', command: 'run', input: 100 })
		thread.postMessage({ id: 'job-4', command: 'abort' })
		// The cooperative handler resolves the sentinel -1 once its signal fires.
		expect(await pending).toEqual({ id: 'job-4', ok: true, value: -1 })
	})

	it('ignores an abort for an unknown id', async () => {
		const thread = spawn('double.ts')
		// An abort for a job that was never started is a no-op; a fresh run still works.
		thread.postMessage({ id: 'ghost', command: 'abort' })
		const pending = reply(thread, 'job-5')
		thread.postMessage({ id: 'job-5', command: 'run', input: 4 })
		expect(await pending).toEqual({ id: 'job-5', ok: true, value: 8 })
	})
})

describe('serveWorker — async handler rejection', () => {
	it('replies { ok: false, error } when the handler rejects ASYNCHRONOUSLY', async () => {
		// `throw-async.ts` rejects after a microtask (not a sync throw like `fail.ts`). The
		// deferred-into-`then` dispatch must still catch it and reply an error envelope — an async
		// rejection is reported exactly like a sync throw, never an unhandled rejection / crash.
		const thread = spawn('throw-async.ts')
		const pending = reply(thread, 'job-a1')
		thread.postMessage({ id: 'job-a1', command: 'run', input: 7 })
		expect(await pending).toEqual({ id: 'job-a1', ok: false, error: 'async-boom:7' })
	})
})

describe('serveWorker — unknown message command', () => {
	it('ignores a message whose command is neither run nor abort (handler never runs)', async () => {
		// A stray control message (an unrecognised `command`) matches neither `isRun` nor `isAbort`,
		// so `serveWorker` drops it silently — it does not run the handler or reply. A subsequent
		// valid run then still works, proving the unknown message did not wedge the listener.
		const thread = spawn('double.ts')
		thread.postMessage({ id: 'job-u1', command: 'frobnicate', input: 1 })
		thread.postMessage({ id: 'job-u2', command: 'run', input: 21 })
		// Only the valid run replies; the unknown-command message produced nothing.
		expect(await reply(thread, 'job-u2')).toEqual({ id: 'job-u2', ok: true, value: 42 })
	})

	it('ignores a malformed message (no id) without crashing the thread', async () => {
		// A payload missing `id` fails both inbound guards and is dropped. The thread survives and
		// answers the next well-formed run — a hostile / malformed message can't crash the worker.
		const thread = spawn('double.ts')
		thread.postMessage({ command: 'run', input: 99 })
		thread.postMessage(42)
		thread.postMessage({ id: 'job-u3', command: 'run', input: 5 })
		expect(await reply(thread, 'job-u3')).toEqual({ id: 'job-u3', ok: true, value: 10 })
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
			const pending = reply(thread, id)
			thread.postMessage({ id, command: 'run', input: shapes[index] })
			expect(await pending).toEqual({ id, ok: true, value: shapes[index] })
		}
	})
})

describe('serveWorker — main-thread no-op', () => {
	it('does nothing (and never runs the handler) when called off a worker thread', async () => {
		// THIS test runs on the main thread, where `parentPort === null`, so `serveWorker`
		// must return immediately — registering no listeners and never invoking the handler.
		// A recorder stands in for the handler; it is a real callback (AGENTS §16.1), so a
		// single recorded call would prove the no-op guard failed.
		const handled = createRecorder<[number]>()
		expect(() =>
			serveWorker<number, number>({
				input: (value): value is number => typeof value === 'number',
				handler: (value) => {
					handled.handler(value)
					return value * 2
				},
			}),
		).not.toThrow()
		// Give any (erroneously registered) message listener a turn to fire — it must not.
		await waitForDelay(0)
		expect(handled.count).toBe(0)
	})
})
