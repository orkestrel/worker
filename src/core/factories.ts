import type { WorkerInterface, WorkerOptions } from './types.js'
import { Worker } from './Worker.js'

/**
 * Creates a resource-backed job worker — a `Queue` (`@orkestrel/queue`) composed with a
 * `Pool` (`@orkestrel/pool`), where each enqueued input runs through the handler against
 * an automatically acquired pooled resource released when the job settles.
 *
 * @remarks
 * Bounded concurrency, retries, and the per-attempt timeout and abort are the queue's.
 * Default for the pool's `max`: the `concurrency` value, so resources match the jobs in flight.
 * Resources are reused across jobs. A handler that throws still releases its resource (the
 * acquire/release pair brackets the call in a `finally`), so a later job reuses it. The
 * lifecycle (`start` / `stop` / `pause` / `resume` / `abort` / `clear` / `destroy`)
 * delegates to the queue; `destroy` also tears the pool down. It is observable (see the
 * guide's `## Observing` section): a typed `emitter` surfaces the queue lifecycle
 * (`enqueue` / `start` / `success` / `failure` / …).
 *
 * @typeParam TInput - The work input each job carries
 * @typeParam TResource - The pooled resource each job runs against
 * @typeParam TResult - The value the handler resolves for a job
 * @param options - The `handler` and `pool` plus the optional `concurrency`, `retries`,
 *   `timeout`, `store`, `on`, and `error` keys (see {@link WorkerOptions})
 * @returns A working {@link WorkerInterface}
 *
 * @example A resource-backed worker
 * ```ts
 * import { createWorker } from '@orkestrel/worker'
 *
 * // A Queue whose handler runs each job against a pooled resource (acquired before the
 * // handler, released after it — even on throw). The pool's `max` defaults to `concurrency`.
 * const worker = createWorker<Query, Connection, Rows>({
 * 	pool: { create: () => connect(), destroy: (connection) => connection.close() },
 * 	handler: (query, connection, { signal }) => connection.run(query, signal),
 * 	concurrency: 4,
 * 	retries: 1,
 * })
 *
 * const rows = await worker.enqueue(query)
 * await worker.destroy() // awaits queue cleanup, pool cleanup, then emitter teardown
 * ```
 */
export function createWorker<TInput, TResource, TResult>(
	options: WorkerOptions<TInput, TResource, TResult>,
): WorkerInterface<TInput, TResult> {
	return new Worker(options)
}
