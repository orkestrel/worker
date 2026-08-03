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
	try {
		return typeof value === 'object' && value !== null && !Array.isArray(value)
	} catch {
		return false
	}
}

// Narrow an inbound message to a `run` envelope: `id` is the per-dispatch correlation,
// while `job` is the stable Queue execution id exposed to the handler. Both are required;
// a legacy or malformed envelope without a string `job` fails closed without a reply.
function isRun(
	value: unknown,
): value is { readonly id: string; readonly job: string; readonly input: unknown } {
	try {
		return (
			isRecord(value) &&
			typeof value.id === 'string' &&
			typeof value.job === 'string' &&
			value.command === 'run' &&
			'input' in value
		)
	} catch {
		return false
	}
}

// Narrow an inbound message to an `abort` envelope (a string `id` + an `'abort'` command).
function isAbort(value: unknown): value is { readonly id: string } {
	try {
		return isRecord(value) && typeof value.id === 'string' && value.command === 'abort'
	} catch {
		return false
	}
}

/**
 * Register a worker-thread handler — the worker-side half of {@link createNodeWorker}.
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
 * its `job` is the stable Queue idempotency key exposed as `execution.id` across retries
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
		if (isAbort(raw)) {
			controllers.get(raw.id)?.abort()
			return
		}
		if (!isRun(raw)) return
		const id = raw.id
		const controller = new AbortController()
		controllers.set(id, controller)
		void Promise.resolve()
			.then(() => {
				if (!input(raw.input)) {
					throw new Error('input did not satisfy input guard')
				}
				const value = raw.input
				return handler(value, { id: raw.job, signal: controller.signal })
			})
			.then((value) => {
				controllers.delete(id)
				port.postMessage({ id, ok: true, value })
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
