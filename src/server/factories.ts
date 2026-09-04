import type { WorkerInterface } from '@src/core'
import type { ContractShape, Infer } from '@orkestrel/contract'
import type { QueueStoreInterface } from '@orkestrel/queue'
import type { NodeThread, NodeWorkerOptions } from './types.js'
import { createJSONDriver } from '@orkestrel/database/server'
import { createDatabaseQueueStore } from '@orkestrel/queue'
import { NodeWorker } from './NodeWorker.js'
import { Thread } from './Thread.js'

/**
 * Creates one live worker thread and resolves it as a {@link NodeThread} after it comes
 * online.
 *
 * @remarks
 * Constructs the thread with the `script` module and the cloned `workerData`, then
 * resolves on the thread's `online` event (rejecting on an early `error` OR an `exit`
 * that arrives before `online`, so the spawn promise is total — it can never dangle on a
 * thread that died without erroring). The returned entity attaches persistent `error` /
 * `exit` listeners that flip `alive` to `false` AND latch the first terminal event on
 * {@link NodeThread.death}: a crash is observable to an in-flight {@link Dispatch} (through
 * its own listeners), to a pool's `validate` (through `alive`), and — crucially — to a
 * dispatch that attaches AFTER the death (through the latch). A `messageerror` is terminal
 * too, so a thread whose inbound payload could not be deserialized is never reused. The latch
 * closes a real race: a thread can become terminal before the readiness promise continuation
 * hands it to a {@link Dispatch}, leaving no future death event for that dispatch to observe.
 * Without the latch, that job would wait forever. {@link createNodeWorker} spawns its pooled
 * threads the same way; reach for this to drive one thread yourself.
 *
 * @param script - The worker module the thread runs (its module must call `serveWorker`)
 * @param workerData - Opaque, structured-cloneable data handed to the thread at spawn
 * @returns A promise resolving the online {@link NodeThread}
 *
 * @example
 * ```ts
 * import { createThread } from '@orkestrel/worker/server'
 *
 * const thread = await createThread(new URL('./double.js', import.meta.url))
 * await thread.worker.terminate()
 * ```
 */
export function createThread(script: string | URL, workerData?: unknown): Promise<NodeThread> {
	return new Thread(script, workerData).promise
}

/**
 * Creates a persistent JSON-file {@link QueueStoreInterface} — the core
 * `createDatabaseQueueStore` over a server {@link createJSONDriver}.
 *
 * @remarks
 * A queue's durable state is a database table, so JSON persistence reuses the
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
 * import { stringShape } from '@orkestrel/contract'
 * import { createJSONQueueStore } from '@orkestrel/worker/server'
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
 * Creates a CPU-parallel worker over `node:worker_threads` — a thin specialization of the
 * core `createWorker` whose pooled resource is a worker THREAD.
 *
 * @remarks
 * Composition, not reimplementation: all concurrency, retries, per-attempt timeout,
 * lifecycle, and durability are the core `Worker`'s (a `Queue` ⨉ `Pool`). This factory
 * supplies only the thread pairing — the pool `create`s a thread (the same spawn
 * {@link createThread} publishes), `destroy`s it with `terminate()`, and `validate`s it by
 * `alive && threadId > 0` (so an evicted / crashed thread is dropped and replaced) — and an
 * internal handler that narrows the input through `options.input` (fail-fast before the
 * structured-clone boundary) then runs a {@link Dispatch} against the leased thread,
 * narrowing the reply through
 * `options.result`. Both generics INFER from the `input` / `result` guards, so call sites
 * need no explicit type arguments. The boundary is crossed with ZERO `as`: the guards
 * reconstruct `TInput` / `TResult` by validation. An `abort` / `timeout`
 * TERMINATES the in-flight thread (CPU-bound work can't honour a signal) and evicts it; a
 * subsequent job spawns a fresh thread. The worker script's module must call
 * `serveWorker`. Returns the plain {@link WorkerInterface} — its methods are the Worker's.
 *
 * @typeParam TInput - The work payload each job carries (inferred from `input`)
 * @typeParam TResult - The value a thread resolves for a job (inferred from `result`)
 * @param options - The `script` plus the `input` / `result` guards and optional
 *   `on` / `error` / `workerData` / `concurrency` / `retries` / `timeout` / `store`
 *   (see {@link NodeWorkerOptions})
 * @returns A working {@link WorkerInterface} backed by a thread pool
 *
 * @example
 * ```ts
 * import { createNodeWorker } from '@orkestrel/worker/server'
 *
 * const worker = createNodeWorker({
 * 	script: new URL('./double.js', import.meta.url),
 * 	input: (value): value is number => typeof value === 'number',
 * 	result: (value): value is number => typeof value === 'number',
 * 	concurrency: 4,
 * })
 *
 * const doubled = await worker.enqueue(21) // 42, computed on a worker thread
 * await worker.destroy() // terminates every thread
 * ```
 */
export function createNodeWorker<TInput, TResult>(
	options: NodeWorkerOptions<TInput, TResult>,
): WorkerInterface<TInput, TResult> {
	return new NodeWorker(options).build()
}
