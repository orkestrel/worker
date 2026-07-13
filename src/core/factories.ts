import type { WorkerInterface, WorkerOptions } from './types.js'
import { Worker } from './Worker.js'

/**
 * Create a resource-backed job worker — a `Queue` (`@orkestrel/queue`) marrying a `Pool`
 * (`@orkestrel/pool`). Each enqueued input runs through the handler against an
 * automatically acquired pooled resource (released when the job settles), with the
 * queue's bounded concurrency, retries, and per-attempt timeout / abort.
 *
 * @remarks
 * The pool's `max` defaults to `concurrency`, so resources match the jobs in flight and
 * are reused across jobs. A handler that throws still releases its resource (the
 * acquire/release pair brackets the call in a `finally`), so a later job reuses it. The
 * lifecycle (`start` / `stop` / `pause` / `resume` / `abort` / `clear` / `destroy`)
 * delegates to the queue; `destroy` also tears the pool down. Observable (§13): a typed
 * `emitter` surfaces the queue lifecycle (`enqueue` / `start` / `success` / `failure` / …).
 *
 * @typeParam TInput - The work input each job carries
 * @typeParam TResource - The pooled resource each job runs against
 * @typeParam TResult - The value the handler resolves for a job
 * @param options - The `handler` and `pool` plus optional `concurrency` (default `1`),
 *   `retries` (default `0`), and a default per-attempt `timeout` in milliseconds
 * @returns A working {@link WorkerInterface}
 *
 * @example
 * ```ts
 * import { createWorker } from '@src/core'
 *
 * const worker = createWorker<Query, Connection, Rows>({
 * 	pool: { create: () => connect(), destroy: (connection) => connection.close() },
 * 	handler: (query, connection, { signal }) => connection.run(query, signal),
 * 	concurrency: 4,
 * 	retries: 1,
 * })
 *
 * const rows = await worker.enqueue(query)
 * ```
 */
export function createWorker<TInput, TResource, TResult>(
	options: WorkerOptions<TInput, TResource, TResult>,
): WorkerInterface<TInput, TResult> {
	return new Worker(options)
}
