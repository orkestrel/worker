import type { EmitterErrorHandler, EmitterHooks, EmitterInterface } from '@orkestrel/emitter'
import type { PoolOptions } from '@orkestrel/pool'
import type { QueueEntryOptions, QueueExecution, QueueStoreInterface } from '@orkestrel/queue'

/**
 * The push observation surface of a {@link WorkerInterface} (AGENTS §13) — the job
 * lifecycle a fire-and-forget observer subscribes to, surfacing the underlying queue's
 * moments so a Worker consumer never reaches through to the internal `Queue`.
 *
 * @typeParam TResult - The value a job resolves (the `success` payload), mirroring the
 *   {@link WorkerInterface}'s own `TResult`.
 *
 * @remarks
 * A Worker is a `Queue`⨉`Pool` facade (both from their own `@orkestrel` packages); this
 * map RE-EXPOSES the queue lifecycle the worker surfaces (`enqueue` / `start` / `retry` /
 * `success` / `failure` / `abort` / `drain`) as the worker's OWN events — wired from the
 * underlying queue's emitter at construction, so a buggy observer is isolated exactly as
 * on the queue (a throw routes to the worker emitter's `error` handler, AGENTS §13). The
 * pool's create / acquire / release events stay the pool's internal concern (a Worker
 * manages its own resources); a consumer who wants them observes a `Pool` directly.
 * Declared as a `type` alias (§4.5).
 */
export type WorkerEventMap<TResult> = {
	/** A job was accepted — its id (delegated from the underlying queue's `enqueue`). */
	readonly enqueue: readonly [id: string]
	/** A job's attempt began running — its id. */
	readonly start: readonly [id: string]
	/** A failed job attempt is being retried — its id + the next (1-based) attempt index. */
	readonly retry: readonly [id: string, attempt: number]
	/** A job settled successfully — its id + the resolved result. */
	readonly success: readonly [id: string, result: TResult]
	/** A job settled with a terminal failure — its id + the error. */
	readonly failure: readonly [id: string, error: unknown]
	/** The worker was aborted — the cancel reason. */
	readonly abort: readonly [reason: unknown]
	/** The worker went idle — no pending jobs and none in flight. */
	readonly drain: readonly []
}

/** Runs one worker job with a leased pool resource. */
export type WorkerHandler<TInput, TResource, TResult> = (
	input: TInput,
	resource: TResource,
	execution: QueueExecution,
) => Promise<TResult> | TResult

/**
 * Options for `createWorker`.
 *
 * @remarks
 * - `handler` — runs each job against an acquired pool resource; rejecting triggers a
 *   retry while attempts remain (delegated to the underlying queue).
 * - `pool` — the {@link PoolOptions} for the resource the handler runs against; its
 *   `max` defaults to `concurrency` so resources match the jobs in flight.
 * - `concurrency` — the maximum jobs in flight at once; defaults to `1`. Floored at `1`.
 * - `retries` — the default extra attempts per job on failure; defaults to `0`.
 * - `timeout` — the default per-attempt deadline in milliseconds; defaults to none.
 * - `store` — durable backing; outstanding entries survive a restart; call
 *   `restore()` to re-run them.
 * - `on` — the reserved {@link EmitterHooks} key (§8): initial listeners for the worker's
 *   {@link WorkerEventMap} (the job lifecycle it surfaces from its underlying queue), wired
 *   at construction.
 */
export interface WorkerOptions<TInput, TResource, TResult> {
	readonly on?: EmitterHooks<WorkerEventMap<TResult>>
	/** The emitter's listener-error handler (AGENTS §13) — a listener throw routes here, not to a domain event. */
	readonly error?: EmitterErrorHandler
	readonly handler: WorkerHandler<TInput, TResource, TResult>
	readonly pool: PoolOptions<TResource>
	readonly concurrency?: number
	readonly retries?: number
	readonly timeout?: number
	readonly store?: QueueStoreInterface<TInput>
}

/**
 * A resource-backed job worker — a Queue whose handler runs against a pooled resource.
 *
 * @remarks
 * Exposes a typed {@link emitter} (AGENTS §13) carrying the job lifecycle
 * ({@link WorkerEventMap}) — the underlying queue's moments re-exposed as the worker's own,
 * so a consumer never reaches through to internals. Emitting is observation-only: a buggy
 * observer is isolated exactly as on the queue (a throw routes to the emitter's `error`
 * handler, the `error` option).
 */
export interface WorkerInterface<TInput, TResult> {
	readonly emitter: EmitterInterface<WorkerEventMap<TResult>>
	readonly count: number
	readonly active: number
	readonly paused: boolean
	readonly stopped: boolean
	enqueue(input: TInput, options?: QueueEntryOptions): Promise<TResult>
	/** Re-enqueue outstanding entries loaded from the store; no-op without a store. */
	restore(): Promise<void>
	start(): void
	stop(): void
	pause(): void
	resume(): void
	abort(reason?: unknown): void
	clear(): void
	destroy(): void
}
