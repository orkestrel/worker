import type { Guard } from '@orkestrel/contract'
import type { QueueExecution, QueueStoreInterface } from '@orkestrel/queue'
import type { Worker as ThreadWorker } from 'node:worker_threads'

/**
 * A thread→main reply envelope — a success carrying an opaque `value`, or a failure with a
 * message — part of the internal wire protocol `createNodeWorker` posts and `serveWorker`
 * answers.
 *
 * @remarks
 * Internal plumbing rather than public call surface, but centralized here per AGENTS §5 (an
 * impl file holds only its class / functions). A reply is a discriminated union on `ok`: a
 * `true` carries any opaque `value` (narrowed at the boundary by the `result` guard,
 * with no `as`); a `false` carries a string `error`. The worker-side `serve.ts` cannot import
 * this (it loads as raw source in a spawned thread, AGENTS §5 exception) and posts the same
 * shape structurally. The `id` ties a reply to its job: id-less / foreign-id chatter is ignored,
 * while a matching-id malformed envelope taints the thread and causes dispatch to terminate it.
 */
export type Reply =
	| { readonly id: string; readonly ok: true; readonly value: unknown }
	| { readonly id: string; readonly ok: false; readonly error: string }

/**
 * A live worker thread plus its latched liveness state — the pooled resource a
 * {@link createNodeWorker} leases per job.
 *
 * @remarks
 * `alive` starts `true` and flips to `false` when the thread `error`s, reports a
 * `messageerror`, exits, or is evicted on abort; the pool's `validate` reads
 * `alive && worker.threadId > 0`, so a
 * dead thread is destroyed and replaced rather than reused. `death` LATCHES the first
 * terminal event (`error` / `messageerror`, or a synthesized error on `exit`) — the death-signal
 * record a `dispatch` checks at entry, so a job dispatched AFTER the thread died (its
 * death events already fired and will never fire again) rejects immediately instead of
 * awaiting events that already happened. A thread can become terminal before the readiness
 * promise continuation attaches dispatch listeners; the latch is what makes that ordering
 * safe. `worker` is the underlying
 * `node:worker_threads` thread (its `postMessage` / `terminate` drive the protocol).
 */
export interface NodeThread {
	readonly worker: ThreadWorker
	readonly alive: boolean
	readonly death: Error | undefined
}

/**
 * Options for `createNodeWorker` — a CPU-parallel worker over `node:worker_threads`.
 *
 * @remarks
 * - `script` — the worker module each pooled thread runs; its module must call
 *   `serveWorker(...)`. Raw TypeScript is unflagged on Node 22.18+ and Node 23.6+;
 *   Node 22.12–22.17 and Node 23.0–23.5 require `--experimental-strip-types`. A built
 *   `.js` / `.mjs` script remains an alternative across supported Node versions.
 * - `input` — narrows the work payload BEFORE it crosses the structured-clone boundary
 *   (fail-fast) and supplies the `TInput` inference, so call sites need no type argument.
 * - `result` — narrows every reply value coming back from a thread; an invalid reply
 *   rejects the job. This is the zero-`as` type bridge — `TResult` is inferred from it.
 * - `workerData` — opaque data cloned to every thread once at spawn (read there via
 *   `serveWorker`'s host `workerData`); must be structured-cloneable.
 * - `concurrency` — the maximum jobs in flight at once; the thread pool's `max` matches
 *   it, so at most this many threads exist. Defaults to `1` and must be a positive safe
 *   integer, as validated by the underlying queue.
 * - `retries` — the default extra attempts per job on failure / timeout; defaults to `0`.
 * - `timeout` — the default per-attempt deadline in milliseconds; defaults to none.
 * - `store` — durable backing for outstanding jobs (survives a restart; `restore()`
 *   re-runs them).
 *
 * @typeParam TInput - The work payload each job carries (inferred from `input`)
 * @typeParam TResult - The value a thread resolves for a job (inferred from `result`)
 */
export interface NodeWorkerOptions<TInput, TResult> {
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
 * Options for `serveWorker` — the worker-side entry a thread script registers.
 *
 * @remarks
 * - `input` — narrows each inbound payload inside the thread; an invalid payload replies
 *   with an error envelope rather than running the handler. Supplies the `TInput`
 *   inference for the handler.
 * - `handler` — runs one job; receives the narrowed input and the Queue's execution.
 *   `execution.id` is the stable Queue idempotency key across retries and crash restore;
 *   it identifies work, not a caller, and is not authentication or authorization evidence.
 *   `execution.signal` is per attempt and fires when the main side aborts that attempt
 *   (cooperative). The handler may be async; its resolved value (which must be
 *   structured-cloneable) is the reply.
 *
 * @typeParam TInput - The work payload (inferred from `input`)
 * @typeParam TResult - The value the handler resolves (the reply payload)
 */
export interface ServeWorkerOptions<TInput, TResult> {
	readonly input: Guard<TInput>
	readonly handler: (input: TInput, execution: QueueExecution) => Promise<TResult> | TResult
}
