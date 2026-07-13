import type { QueueStoreInterface } from '@src/core'
import type { Worker as ThreadWorker } from 'node:worker_threads'

/**
 * A runtime type predicate used to narrow a wire payload with no assertion.
 *
 * @remarks
 * Mirrors the core `Guard<T>` (a total `(value: unknown) => value is T` predicate,
 * AGENTS §14) but is re-declared here so the server workers surface is self-describing
 * and the worker-side `serve.ts` — which may not import from `@src/core` (it loads as
 * raw `.ts` inside a spawned thread) — shares the same vocabulary. A `Guard` NEVER
 * throws; adversarial input returns `false`. It is the zero-`as` bridge across the
 * structured-clone boundary: the main side narrows each reply value through the
 * `result` guard and the input through the `input` guard, so a generic `TInput` /
 * `TResult` is reconstructed by validation rather than asserted.
 *
 * @typeParam T - The type a value is narrowed to when the predicate holds
 */
export type Guard<T> = (value: unknown) => value is T

/**
 * A thread→main reply envelope — a success carrying an opaque `value`, or a failure with a
 * message — part of the internal wire protocol `createNodeWorker` posts and `serveWorker`
 * answers.
 *
 * @remarks
 * Internal plumbing rather than public call surface, but centralized here per AGENTS §5 (an
 * impl file holds only its class / functions). A reply is a discriminated union on `ok`: a
 * `true` carries any opaque `value` (narrowed at the boundary by the `result` {@link Guard},
 * with no `as`); a `false` carries a string `error`. The worker-side `serve.ts` cannot import
 * this (it loads as raw source in a spawned thread, AGENTS §5 exception) and posts the same
 * shape structurally. The `id` ties a reply to its job, so a stray / foreign-id message is
 * ignored.
 */
export type Reply =
	| { readonly id: string; readonly ok: true; readonly value: unknown }
	| { readonly id: string; readonly ok: false; readonly error: string }

/**
 * A live worker thread plus its latched liveness state — the pooled resource a
 * {@link createNodeWorker} leases per job.
 *
 * @remarks
 * `alive` starts `true` and flips to `false` the moment the thread `error`s, `exit`s, or
 * is evicted on abort; the pool's `validate` reads `alive && worker.threadId > 0`, so a
 * dead thread is destroyed and replaced rather than reused. `death` LATCHES the first
 * terminal event (`error`'s `Error`, or a synthesized one on `exit`) — the death-signal
 * record a `dispatch` checks at entry, so a job dispatched AFTER the thread died (its
 * death events already fired and will never fire again) rejects immediately instead of
 * awaiting events that already happened. Under event-loop pressure Node delivers a dead
 * thread's `online` + `error` + `exit` in ONE synchronous exit-drain batch, starving the
 * microtask chain that attaches the dispatch listeners until after every death event —
 * the latch is what makes that ordering safe. `worker` is the underlying
 * `node:worker_threads` thread (its `postMessage` / `terminate` drive the protocol).
 */
export interface NodeThread {
	readonly worker: ThreadWorker
	alive: boolean
	death: Error | undefined
}

/**
 * Options for `createNodeWorker` — a CPU-parallel worker over `node:worker_threads`.
 *
 * @remarks
 * - `script` — the worker module each pooled thread runs; its module must call
 *   `serveWorker(...)`. A `.ts` script requires Node ≥ 23.6 (native type-stripping); on
 *   older Node point this at a built `.js` / `.mjs`.
 * - `input` — narrows the work payload BEFORE it crosses the structured-clone boundary
 *   (fail-fast) and supplies the `TInput` inference, so call sites need no type argument.
 * - `result` — narrows every reply value coming back from a thread; an invalid reply
 *   rejects the job. This is the zero-`as` type bridge — `TResult` is inferred from it.
 * - `workerData` — opaque data cloned to every thread once at spawn (read there via
 *   `serveWorker`'s host `workerData`); must be structured-cloneable.
 * - `concurrency` — the maximum jobs in flight at once; the thread pool's `max` matches
 *   it, so at most this many threads exist. Defaults to `1`. Floored at `1`.
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
 * - `handler` — runs one job; receives the narrowed input and a `{ signal }` execution
 *   whose `AbortSignal` fires when the main side aborts the job (cooperative). May be
 *   async; its resolved value (which must be structured-cloneable) is the reply.
 *
 * @typeParam TInput - The work payload (inferred from `input`)
 * @typeParam TResult - The value the handler resolves (the reply payload)
 */
export interface ServeWorkerOptions<TInput, TResult> {
	readonly input: Guard<TInput>
	readonly handler: (
		input: TInput,
		execution: { readonly signal: AbortSignal },
	) => Promise<TResult> | TResult
}
