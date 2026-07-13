import type { WorkerInterface } from '@src/core'
import type { ContractShape, Infer } from '@orkestrel/contract'
import type { QueueStoreInterface } from '@orkestrel/queue'
import type { NodeThread, NodeWorkerOptions } from './types.js'
import { createWorker } from '@src/core'
import { createJSONDriver } from '@orkestrel/database/server'
import { createDatabaseQueueStore } from '@orkestrel/queue'
import { dispatch, spawnThread } from './helpers.js'

/**
 * Create a persistent JSON-file {@link QueueStoreInterface} — the core
 * `createDatabaseQueueStore` over a server {@link createJSONDriver}.
 *
 * @remarks
 * A queue's durable state is just a database table, so JSON persistence reuses the
 * existing JSON-file driver rather than a bespoke store: the entries are written to
 * (and reloaded from) the file at `path`, surviving a process restart. There is no new
 * class — the store engine ({@link createDatabaseQueueStore}) is shared, and only the
 * driver changes where the bytes live. The `input` shape must be JSON-serializable
 * (the JSON driver round-trips it as JSON). Build a second store over the SAME `path` to
 * resume the outstanding entries a prior store persisted.
 *
 * @typeParam TInput - The contract shape of each entry's `input` payload
 * @param path - The JSON file the entries are loaded from and flushed to
 * @param input - The {@link ContractShape} for the work payload (the `input` column)
 * @returns A JSON-file-backed {@link QueueStoreInterface}, typed by `input`
 *
 * @example
 * ```ts
 * import { stringShape } from '@src/core'
 * import { createJSONQueueStore } from '@src/server'
 *
 * const store = createJSONQueueStore('data/queue.json', stringShape())
 * await store.save({ id: 'job-1', input: 'https://example.com', attempts: 0 })
 * // A later process resumes the outstanding work:
 * const resumed = createJSONQueueStore('data/queue.json', stringShape())
 * const outstanding = await resumed.load()
 * ```
 */
export function createJSONQueueStore<TInput extends ContractShape>(
	path: string,
	input: TInput,
): QueueStoreInterface<Infer<TInput>> {
	return createDatabaseQueueStore(input, createJSONDriver(path))
}

/**
 * Create a CPU-parallel worker over `node:worker_threads` — a thin specialization of the
 * core `createWorker` whose pooled resource is a worker THREAD.
 *
 * @remarks
 * Composition, not reimplementation: all concurrency, retries, per-attempt timeout,
 * lifecycle, and durability are the core `Worker`'s (a `Queue` ⨉ `Pool`). This factory
 * supplies only the thread pairing — the pool `create`s a thread (via `spawnThread`),
 * `destroy`s it with `terminate()`, and `validate`s it by `alive && threadId > 0` (so an
 * evicted / crashed thread is dropped and replaced) — and an internal handler that
 * narrows the input through `options.input` (fail-fast before the structured-clone
 * boundary) then `dispatch`es the job to the leased thread, narrowing the reply through
 * `options.result`. Both generics INFER from the `input` / `result` guards, so call sites
 * need no explicit type arguments. The boundary is crossed with ZERO `as`: the guards
 * reconstruct `TInput` / `TResult` by validation (AGENTS §14). An `abort` / `timeout`
 * TERMINATES the in-flight thread (CPU-bound work can't honour a signal) and evicts it; a
 * subsequent job spawns a fresh thread. The worker script's module must call
 * `serveWorker`. Returns the plain {@link WorkerInterface} — its methods are the Worker's.
 *
 * @typeParam TInput - The work payload each job carries (inferred from `input`)
 * @typeParam TResult - The value a thread resolves for a job (inferred from `result`)
 * @param options - The `script` plus the `input` / `result` guards and optional
 *   `workerData` / `concurrency` / `retries` / `timeout` / `store`
 *   (see {@link NodeWorkerOptions})
 * @returns A working {@link WorkerInterface} backed by a thread pool
 *
 * @example
 * ```ts
 * import { createNodeWorker } from '@src/server'
 *
 * const worker = createNodeWorker({
 * 	script: new URL('./double.js', import.meta.url),
 * 	input: (value): value is number => typeof value === 'number',
 * 	result: (value): value is number => typeof value === 'number',
 * 	concurrency: 4,
 * })
 *
 * const doubled = await worker.enqueue(21) // 42, computed on a worker thread
 * worker.destroy() // terminates every thread
 * ```
 */
export function createNodeWorker<TInput, TResult>(
	options: NodeWorkerOptions<TInput, TResult>,
): WorkerInterface<TInput, TResult> {
	return createWorker<TInput, NodeThread, TResult>({
		pool: {
			create: () => spawnThread(options.script, options.workerData),
			destroy: (thread) => thread.worker.terminate().then(() => {}),
			validate: (thread) => thread.alive && thread.worker.threadId > 0,
			max: options.concurrency,
		},
		handler: (input, thread, execution) => {
			if (!options.input(input)) {
				return Promise.reject(new Error('input did not satisfy input guard'))
			}
			return dispatch(thread, input, execution, options.result)
		},
		concurrency: options.concurrency,
		retries: options.retries,
		timeout: options.timeout,
		store: options.store,
	})
}
