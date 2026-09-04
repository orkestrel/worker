import type { EmitterErrorHandler, EmitterHooks } from '@orkestrel/emitter'
import type { Guard } from '@orkestrel/contract'
import type { QueueContext, QueueStoreInterface } from '@orkestrel/queue'
import type { WorkerEventMap } from '@src/core'
import type { Worker as ThreadWorker } from 'node:worker_threads'

/**
 * Represents a thread→main reply envelope — a success carrying an opaque `value`, or a failure with a
 * message — the reply half of the wire protocol `createNodeWorker` posts and `serveWorker`
 * answers.
 *
 * @remarks
 * A reply is a discriminated union on `ok`: a
 * `true` carries any opaque `value` (narrowed at the boundary by the `result` guard,
 * with no `as`); a `false` carries a string `error`. The worker-side `serve.ts` cannot import
 * this because it loads as raw source in a spawned thread, and posts the same
 * shape structurally. The `id` ties a reply to its job: id-less / foreign-id chatter is ignored,
 * while a matching-id malformed envelope taints the thread and causes dispatch to terminate it.
 */
export type Reply =
	| { readonly id: string; readonly ok: true; readonly value: unknown }
	| { readonly id: string; readonly ok: false; readonly error: string }

/**
 * Represents a live worker thread plus its latched liveness state — the pooled resource a
 * {@link createNodeWorker} leases per job.
 *
 * @remarks
 * `alive` starts `true` and flips to `false` when the thread `error`s, reports a
 * `messageerror`, exits, or is evicted on abort; the pool's `validate` reads
 * `alive && worker.threadId > 0`, so a
 * dead thread is destroyed and replaced rather than reused. `death` LATCHES the first
 * terminal event (`error` / `messageerror`, or a synthesized error on `exit`) — the death-signal
 * record a {@link Dispatch} checks at construction, so a job dispatched AFTER the thread died (its
 * death events already fired and will never fire again) rejects immediately instead of
 * awaiting events that already happened. A thread can become terminal before the readiness
 * promise continuation attaches dispatch listeners; the latch is what makes that ordering
 * safe. `worker` is the underlying
 * `node:worker_threads` thread (its `postMessage` / `terminate` drive the protocol).
 *
 * A dispatch marks a thread dead for a `NodeThread` this package produced, through
 * {@link createThread} or a {@link createNodeWorker} pool. A foreign implementation of this
 * interface owns flipping its own `alive` when its `worker` is terminated: an abort or a
 * `messageerror` terminates the supplied `worker` and rejects the job, and leaves the
 * implementer's `alive` and `death` exactly as the implementer reports them.
 */
export interface NodeThread {
	readonly worker: ThreadWorker
	readonly alive: boolean
	readonly death: Error | undefined
}

/**
 * Configures `createNodeWorker` — a CPU-parallel worker over `node:worker_threads`.
 *
 * @remarks
 * - `script` — the worker module each pooled thread runs; its module must call
 *   `serveWorker(...)`. Raw TypeScript is unflagged on Node 22.18+ and Node 23.6+;
 *   Node 22.12–22.17 and Node 23.0–23.5 require `--experimental-strip-types`. A built
 *   `.js` / `.mjs` script is an alternative across supported Node versions.
 * - `input` — narrows the work payload BEFORE it crosses the structured-clone boundary
 *   (fail-fast) and supplies the `TInput` inference, so call sites need no type argument.
 * - `result` — narrows every reply value coming back from a thread; an invalid reply
 *   rejects the job. This is the zero-`as` type bridge — `TResult` is inferred from it.
 * - `workerData` — opaque data cloned to every thread at spawn; the key mirrors the
 *   `node:worker_threads` `Worker` constructor option of the same name, and the thread reads
 *   it back from `node:worker_threads`. It must be structured-cloneable.
 * - `concurrency` — the maximum jobs in flight at once; the thread pool's `max` matches
 *   it, so at most this many threads exist. It must be a positive safe integer, as
 *   validated by the underlying queue. Default: 1.
 * - `retries` — the default extra attempts per job on failure / timeout. Default: 0.
 * - `timeout` — the default per-attempt deadline in milliseconds. Default: no per-attempt
 *   deadline.
 * - `store` — durable backing for outstanding jobs (survives a restart; `restore()`
 *   re-runs them).
 * - `on` — the reserved {@link EmitterHooks} key: initial listeners for the worker's
 *   {@link WorkerEventMap} (the job lifecycle it surfaces from its underlying queue), wired
 *   at construction. A thread worker takes the same hooks as the core worker.
 * - `error` — the emitter's listener-error handler; a listener throw routes
 *   here, not to a domain event.
 *
 * @typeParam TInput - The work payload each job carries (inferred from `input`)
 * @typeParam TResult - The value a thread resolves for a job (inferred from `result`)
 */
export interface NodeWorkerOptions<TInput, TResult> {
	readonly on?: EmitterHooks<WorkerEventMap<TResult>>
	readonly error?: EmitterErrorHandler
	readonly script: string | URL
	readonly input: Guard<TInput>
	readonly result: Guard<TResult>
	readonly workerData?: unknown
	readonly concurrency?: number
	readonly retries?: number
	readonly timeout?: number
	readonly store?: QueueStoreInterface<TInput>
}

/**
 * Configures `serveWorker` — the worker-side entry a thread script registers.
 *
 * @remarks
 * - `input` — narrows each inbound payload inside the thread; an invalid payload replies
 *   with an error envelope rather than running the handler. Supplies the `TInput`
 *   inference for the handler.
 * - `handler` — runs one job; receives the narrowed input and the Queue's context.
 *   `context.id` is the stable Queue idempotency key across retries and crash restore;
 *   it identifies work, not a caller, and is not authentication or authorization evidence.
 *   `context.signal` is per attempt and fires when the main side aborts that attempt
 *   (cooperative). The handler may be async; its resolved value (which must be
 *   structured-cloneable) is the reply.
 *
 * @typeParam TInput - The work payload (inferred from `input`)
 * @typeParam TResult - The value the handler resolves (the reply payload)
 */
export interface ServeWorkerOptions<TInput, TResult> {
	readonly input: Guard<TInput>
	readonly handler: (input: TInput, context: QueueContext) => Promise<TResult> | TResult
}
