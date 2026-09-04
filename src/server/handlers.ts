import type { ServeWorkerOptions } from './types.js'
import { parentPort } from 'node:worker_threads'

// The worker-side request handler. SELF-CONTAINED by necessity: this module loads as RAW
// `.ts` inside a spawned thread (Node ≥ 23.6 type-stripping), so it imports ONLY
// `node:worker_threads` at runtime — no `@src/*`, no `.js`-relative value imports (the
// only non-node import is the type-only `ServeWorkerOptions`, fully erased at runtime).
// The inbound envelope is therefore narrowed inline rather than through a sibling guard in
// `helpers.ts`, which would be a runtime import this module cannot make. A worker script
// that needs the cloned `workerData` reads it directly from `node:worker_threads` (it is in
// a thread already).

/**
 * Registers a worker-thread handler — the worker-side half of {@link createNodeWorker}.
 *
 * @remarks
 * Must be the spawned thread's module entry. It listens on the parent port for the
 * run/abort protocol: a `run` message narrows its `input` through `options.input` (an
 * invalid payload replies with an error envelope, never running the handler), then runs
 * `options.handler(input, { id: job, signal })` and replies `{ id, ok: true, value }` on success or
 * `{ id, ok: false, error }` on throw. Input-guard throws use the same failure envelope. If a
 * success value cannot be cloned, the post is retried as a clone-safe failure; if that post also
 * fails, the parent port closes so the main side observes thread exit instead of waiting forever.
 * The run envelope's `id` is fresh per dispatch and keys controllers, aborts, and replies;
 * its `job` is the stable Queue idempotency key exposed as `context.id` across retries
 * and restore. That job id identifies work, not a caller, and is not authentication or
 * authorization evidence. Each attempt has its own `AbortController`, so an `abort`
 * message for the correlation id fires the handler's `signal` (cooperative — the main
 * side ALSO terminates the thread, so a handler that ignores its signal is still stopped).
 * Every inbound message is narrowed with the inlined guards — no `as`. On the main thread
 * (`parentPort === null`) it is a no-op.
 *
 * @typeParam TInput - The work payload (inferred from `options.input`)
 * @typeParam TResult - The value the handler resolves (the reply payload)
 * @param options - The `input` guard and the `handler` (see {@link ServeWorkerOptions})
 *
 * @example
 * ```ts
 * // double.ts — a worker script
 * import { serveWorker } from '@orkestrel/worker/server'
 *
 * serveWorker<number, number>({
 * 	input: (value): value is number => typeof value === 'number',
 * 	handler: (value) => value * 2,
 * })
 * ```
 */
export function serveWorker<TInput, TResult>(options: ServeWorkerOptions<TInput, TResult>): void {
	const port = parentPort
	if (port === null) return
	const input = options.input
	const handler = options.handler
	const controllers = new Map<string, AbortController>()
	port.on('message', (raw: unknown) => {
		// Read the envelope's `command`, `id`, `job`, and `input` fields once, defensively. A
		// hostile message can be a revoked
		// proxy or carry a throwing getter, so every property access sits inside this one guard:
		// a read that throws leaves the envelope unrecognised and the message is dropped without
		// a reply, exactly as a malformed envelope is.
		let command: unknown
		let correlation: unknown
		let job: unknown
		let payload: unknown
		let carried = false
		try {
			if (typeof raw === 'object' && raw !== null && !Array.isArray(raw)) {
				if ('command' in raw) command = raw.command
				if ('id' in raw) correlation = raw.id
				if ('job' in raw) job = raw.job
				if ('input' in raw) {
					payload = raw.input
					carried = true
				}
			}
		} catch {
			return
		}
		if (typeof correlation !== 'string') return
		const id = correlation
		if (command === 'abort') {
			controllers.get(id)?.abort()
			return
		}
		// A `run` envelope carries BOTH ids: `id` is the per-dispatch correlation, `job` the
		// stable Queue entry id handed to the handler. A malformed envelope
		// without a string `job`, or without an `input` at all, fails closed with no reply.
		if (command !== 'run' || typeof job !== 'string' || !carried) return
		const entry = job
		const value = payload
		const controller = new AbortController()
		controllers.set(id, controller)
		void Promise.resolve()
			.then(() => {
				if (!input(value)) {
					throw new Error('input did not satisfy input guard')
				}
				return handler(value, { id: entry, signal: controller.signal })
			})
			.then((result) => {
				controllers.delete(id)
				port.postMessage({ id, ok: true, value: result })
			})
			.catch((error: unknown) => {
				controllers.delete(id)
				let message = 'worker operation failed'
				try {
					message = error instanceof Error ? error.message : String(error)
				} catch {}
				try {
					port.postMessage({ id, ok: false, error: message })
				} catch {
					try {
						port.close()
					} catch {}
				}
			})
	})
}
