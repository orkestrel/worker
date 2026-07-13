import type { ServeWorkerOptions } from './types.js'
import { parentPort } from 'node:worker_threads'

// The worker-side entry. SELF-CONTAINED by necessity: this module loads as RAW `.ts`
// inside a spawned thread (Node ≥ 23.6 type-stripping), so it imports ONLY
// `node:worker_threads` at runtime — no `@src/*`, no `.js`-relative value imports (the
// only non-node import is the type-only `ServeWorkerOptions`, fully erased at runtime).
// Its guards are inlined for the same reason. A worker script that needs the cloned
// `workerData` reads it directly from `node:worker_threads` (it is in a thread already).

// Inlined record guard (do NOT import `isRecord` from `@src/core` — see above). Total:
// adversarial input returns `false`, never throws (AGENTS §14).
function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value)
}

// Narrow an inbound message to a `run` envelope (a string `id` + a `'run'` command + an
// `input` payload) — no assertion.
function isRun(value: unknown): value is { readonly id: string; readonly input: unknown } {
	return (
		isRecord(value) && typeof value.id === 'string' && value.command === 'run' && 'input' in value
	)
}

// Narrow an inbound message to an `abort` envelope (a string `id` + an `'abort'` command).
function isAbort(value: unknown): value is { readonly id: string } {
	return isRecord(value) && typeof value.id === 'string' && value.command === 'abort'
}

/**
 * Register a worker-thread handler — the worker-side half of {@link createNodeWorker}.
 *
 * @remarks
 * Must be the spawned thread's module entry. It listens on the parent port for the
 * run/abort protocol: a `run` message narrows its `input` through `options.input` (an
 * invalid payload replies with an error envelope, never running the handler), then runs
 * `options.handler(input, { signal })` and replies `{ id, ok: true, value }` on success or
 * `{ id, ok: false, error }` on throw. Each in-flight job has its own `AbortController`,
 * so an `abort` message for that id fires the handler's `signal` (cooperative — the main
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
 * import { serveWorker } from '@src/server'
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
	const controllers = new Map<string, AbortController>()
	port.on('message', (raw: unknown) => {
		if (isAbort(raw)) {
			controllers.get(raw.id)?.abort()
			return
		}
		if (!isRun(raw)) return
		const id = raw.id
		if (!options.input(raw.input)) {
			port.postMessage({ id, ok: false, error: 'input did not satisfy input guard' })
			return
		}
		const input = raw.input
		const controller = new AbortController()
		controllers.set(id, controller)
		// Defer the handler call into the `then` so a SYNCHRONOUS throw becomes a rejection
		// (not an uncaught thread exception) and is reported as an error reply.
		Promise.resolve()
			.then(() => options.handler(input, { signal: controller.signal }))
			.then(
				(value) => {
					controllers.delete(id)
					port.postMessage({ id, ok: true, value })
				},
				(error: unknown) => {
					controllers.delete(id)
					port.postMessage({
						id,
						ok: false,
						error: error instanceof Error ? error.message : String(error),
					})
				},
			)
	})
}
