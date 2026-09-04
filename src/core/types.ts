import type { EmitterErrorHandler, EmitterHooks, EmitterInterface } from '@orkestrel/emitter'
import type { PoolOptions } from '@orkestrel/pool'
import type { QueueContext, QueueEntryOptions, QueueStoreInterface } from '@orkestrel/queue'

/**
 * Represents the push observation surface of a {@link WorkerInterface} — the job
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
 * on the queue (a throw routes to the worker emitter's `error` handler). The
 * pool's create / acquire / release events stay the pool's internal concern (a Worker
 * manages its own resources); a consumer who wants them observes a `Pool` directly.
 * Declared as a `type` alias (§4.5).
 */
export type WorkerEventMap<TResult> = {
	/** Fires when a job is accepted — its id (delegated from the underlying queue's `enqueue`). */
	readonly enqueue: readonly [id: string]
	/** Fires when a job's attempt begins running — its id. */
	readonly start: readonly [id: string]
	/** Fires when a failed job attempt is being retried — its id + the next (1-based) attempt index. */
	readonly retry: readonly [id: string, attempt: number]
	/** Fires when a job settles successfully — its id + the resolved result. */
	readonly success: readonly [id: string, result: TResult]
	/** Fires when a job settles with a terminal failure — its id + the error. */
	readonly failure: readonly [id: string, error: unknown]
	/** Fires when the worker is aborted — the queue's coded abort error retaining the caller reason. */
	readonly abort: readonly [reason: unknown]
	/** Fires when the worker goes idle — no pending jobs and none in flight. */
	readonly drain: readonly []
}

/** Runs one worker job with a leased pool resource. */
export type WorkerHandler<TInput, TResource, TResult> = (
	input: TInput,
	resource: TResource,
	context: QueueContext,
) => Promise<TResult> | TResult

/**
 * Configures `createWorker`.
 *
 * @remarks
 * - `handler` — runs each job against an acquired pool resource; rejecting triggers a
 *   retry while attempts remain (delegated to the underlying queue).
 * - `pool` — the {@link PoolOptions} for the resource the handler runs against, sized so
 *   resources match the jobs in flight. Default for its `max`: the `concurrency` value.
 * - `concurrency` — the maximum jobs in flight at once; it must be a positive safe
 *   integer, as validated by the underlying queue. Default: 1.
 * - `retries` — the default extra attempts per job on failure. Default: 0.
 * - `timeout` — the default per-attempt deadline in milliseconds. Default: no per-attempt
 *   deadline.
 * - `store` — durable backing; outstanding entries survive a restart; call
 *   `restore()` to re-run them.
 * - `on` — the reserved {@link EmitterHooks} key (§8): initial listeners for the worker's
 *   {@link WorkerEventMap} (the job lifecycle it surfaces from its underlying queue), wired
 *   at construction.
 */
export interface WorkerOptions<TInput, TResource, TResult> {
	readonly on?: EmitterHooks<WorkerEventMap<TResult>>
	/** Holds the emitter's listener-error handler; a listener throw routes here, not to a domain event. */
	readonly error?: EmitterErrorHandler
	readonly handler: WorkerHandler<TInput, TResource, TResult>
	readonly pool: PoolOptions<TResource>
	readonly concurrency?: number
	readonly retries?: number
	/** Holds integer milliseconds in `0..2_147_483_647`; `0` disables the per-attempt deadline. */
	readonly timeout?: number
	readonly store?: QueueStoreInterface<TInput>
}

/**
 * Represents a resource-backed job worker — a Queue whose handler runs against a pooled resource.
 *
 * @remarks
 * Exposes a typed {@link emitter} carrying the job lifecycle
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
	/** Re-enqueues outstanding entries loaded from the store; no-op without a store. */
	restore(): Promise<void>
	start(): void
	/** Stops the queue and awaits current-loop and durable cleanup quiescence. */
	stop(): Promise<void>
	pause(): void
	resume(): void
	/**
	 * Cancels in-flight work, rejects pending work, and awaits queue-owned cleanup.
	 *
	 * @param reason - Optional cause retained by the queue's coded abort error
	 * @returns The underlying queue's stable abort barrier
	 */
	abort(reason?: unknown): Promise<void>
	/** Drops pending work and awaits its durable cleanup. */
	clear(): Promise<void>
	/**
	 * Tears down the queue, then the pool, and finally the worker emitter.
	 *
	 * @returns One stable barrier shared by every call; it rejects with the original sole
	 *   cleanup failure or an ordered `AggregateError` when both queue and pool fail
	 */
	destroy(): Promise<void>
}
