import type { ServeWorkerOptions } from './types.js'
import { attempt } from '@orkestrel/contract'
import { parentPort } from 'node:worker_threads'

// The worker-side request handler. It is self-contained by necessity: this module loads as
// raw `.ts` inside a spawned thread (Node ≥ 23.6 type-stripping), so runtime imports
// use installed packages and Node modules, never `@src/*` or `.js`-relative source paths.
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
 * side also terminates the thread, so a handler that ignores its signal is still stopped).
 * Every inbound message is narrowed with the inlined guards — no `as`. On the main thread
 * (`parentPort === null`) it is a no-op.
 * Each in-flight run adds a port listener. Posting more concurrent runs to one thread than
 * Node's per-event warning threshold can produce a possible-leak warning.
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
	port.on('message', (raw: unknown) => {
		// Read the envelope's `command`, `id`, `job`, and `input` fields once, defensively. A
		// hostile message can be a revoked
		// proxy or carry a throwing getter, so every property access sits inside this one guard:
		// a read that throws leaves the envelope unrecognised and the message is dropped without
		// a reply, exactly as a malformed envelope is.
		const envelope = attempt(() => {
			if (typeof raw === 'object' && raw !== null && !Array.isArray(raw)) {
				const command = 'command' in raw ? raw.command : undefined
				const correlation = 'id' in raw ? raw.id : undefined
				const job = 'job' in raw ? raw.job : undefined
				const carried = 'input' in raw
				const payload = carried ? raw.input : undefined
				return { command, correlation, job, carried, payload }
			}
			return undefined
		})
		if (!envelope.success || envelope.value === undefined) return
		const { command, correlation, job, carried, payload } = envelope.value
		if (typeof correlation !== 'string') return
		const id = correlation
		if (command === 'abort') return
		// A `run` envelope carries both ids: `id` is the per-dispatch correlation, `job` the
		// stable Queue entry id handed to the handler. A malformed envelope
		// without a string `job`, or without an `input` at all, fails closed with no reply.
		if (command !== 'run' || typeof job !== 'string' || !carried) return
		const entry = job
		const value = payload
		const controller = new AbortController()
		const settled = new AbortController()
		port.addEventListener(
			'message',
			(event) => {
				const notification = attempt(() => {
					const message: unknown = event.data
					if (typeof message !== 'object' || message === null || Array.isArray(message)) {
						return undefined
					}
					const action = 'command' in message ? message.command : undefined
					const target = 'id' in message ? message.id : undefined
					return { command: action, correlation: target }
				})
				if (
					notification.success &&
					notification.value?.command === 'abort' &&
					notification.value.correlation === id
				) {
					controller.abort()
				}
			},
			{ signal: settled.signal },
		)
		void Promise.resolve()
			.then(() => {
				if (!input(value)) {
					throw new Error('input did not satisfy input guard')
				}
				return handler(value, { id: entry, signal: controller.signal })
			})
			.then((result) => {
				settled.abort()
				port.postMessage({ id, ok: true, value: result })
			})
			.catch((error: unknown) => {
				settled.abort()
				const description = attempt(() => (error instanceof Error ? error.message : String(error)))
				const message = description.success ? description.value : 'worker operation failed'
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
