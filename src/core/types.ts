import type { EmitterErrorHandler, EmitterHooks } from '@orkestrel/emitter'
import type { PoolOptions } from '@orkestrel/pool'
import type { QueueContext, QueueEventMap, QueueStoreInterface } from '@orkestrel/queue'

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
 * - `pool` — the resource options from {@link PoolOptions}, refusing values that can carry
 *   `capacity`, forwarded to the owned pool for validation. When both
 *   `max` and `min` are absent, `max` defaults to `concurrency`. With `min`, the pool defaults
 *   `max` to `min`, requires equality and `restarts`, and starts warming at construction.
 *   The floor can exceed queue concurrency; `watch` observes each resource's loss.
 * - `concurrency` — the maximum jobs in flight at once; it must be a positive safe
 *   integer, as validated by the underlying queue. Default: 1.
 * - `retries` — the default extra attempts per job on failure. Default: 0.
 * - `timeout` — the default per-attempt deadline in milliseconds. Default: no per-attempt
 *   deadline.
 * - `store` — durable backing; outstanding entries survive a restart; call
 *   `restore()` to re-run them.
 * - `on` — the reserved {@link EmitterHooks} key: initial listeners for the worker's
 *   {@link QueueEventMap} (the job lifecycle it surfaces from its underlying queue), wired
 *   at construction.
 */
export interface WorkerOptions<TInput, TResource, TResult> {
	readonly on?: EmitterHooks<QueueEventMap<TResult>>
	/** Holds the emitter's listener-error handler; a listener throw routes here, not to a domain event. */
	readonly error?: EmitterErrorHandler
	readonly handler: WorkerHandler<TInput, TResource, TResult>
	readonly pool: Omit<PoolOptions<TResource>, 'capacity'> & { readonly capacity?: never }
	readonly concurrency?: number
	readonly retries?: number
	/** Holds integer milliseconds in `0..2_147_483_647`; `0` disables the per-attempt deadline. */
	readonly timeout?: number
	readonly store?: QueueStoreInterface<TInput>
}
