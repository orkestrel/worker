import type { EmitterErrorHandler, EmitterHooks, EmitterInterface } from '../emitters/types.js'

/**
 * The push observation surface of a {@link QueueInterface} (AGENTS §13) — the lifecycle
 * moments a fire-and-forget observer (logging, metrics, tracing) subscribes to, ALONGSIDE
 * the per-entry `enqueue` promise.
 *
 * @typeParam TResult - The value an entry resolves (the `success` payload), mirroring the
 *   {@link QueueInterface}'s own `TResult` — so the map is `QueueEventMap<TResult>`.
 *
 * @remarks
 * Listener isolation is the emitter's (AGENTS §13): every event is emitted directly and a
 * listener throw is routed to the emitter's OWN `error` handler (the `error` option), never
 * onto this domain map and never into the cooperative wake-park / settle-once engine — so a
 * buggy observer can never reorder, throw into, or corrupt the queue. Every emit sits AFTER
 * the relevant wake / park / settle transition, so observation is purely a side-channel: a
 * throwing observer cannot unbalance `active` or strand a parked worker. Subscribe via
 * `queue.emitter.on(...)`.
 *
 * Declared as a `type` alias (not `interface extends EventMap`, §4.5 — `EventMap` is a
 * `type` kind): a type-literal satisfies the `EventMap` constraint
 * (`Record<string, readonly unknown[]>`) structurally, whereas an interface lacks the
 * required index signature.
 */
export type QueueEventMap<TResult> = {
	/** An entry was accepted (and durably persisted, when a store is set) — its id. */
	readonly enqueue: readonly [id: string]
	/** An attempt began running — the entry's id (after it was dequeued, in flight). */
	readonly start: readonly [id: string]
	/** A failed attempt is being retried — the entry's id + the next (1-based) attempt index. */
	readonly retry: readonly [id: string, attempt: number]
	/** An entry settled successfully — its id + the resolved result. */
	readonly success: readonly [id: string, result: TResult]
	/** An entry settled with a terminal failure — its id + the error (always `unknown`). */
	readonly failure: readonly [id: string, error: unknown]
	/** The queue was aborted — the cancel reason. */
	readonly abort: readonly [reason: unknown]
	/** The queue went idle — no pending entries and none in flight (drained). */
	readonly drain: readonly []
}

/**
 * The push observation surface of a {@link PoolInterface} (AGENTS §13) — the resource
 * lifecycle moments a fire-and-forget observer subscribes to.
 *
 * @remarks
 * Pure signals (no `T` payload — `Pool<T>` carries no resource value on its events, so a
 * non-generic map stays lean). Listener isolation is the emitter's (AGENTS §13): every event
 * is emitted directly and a listener throw is routed to the emitter's `error` handler (the
 * `error` option), never onto this map, and sits AFTER the relevant create / acquire /
 * release / destroy transition — so a throwing observer can never corrupt the FIFO
 * handoff-eviction machinery (it cannot strand a parked waiter or unbalance the lease count).
 * Subscribe via `pool.emitter.on(...)`. Declared as a `type` alias (§4.5 — `EventMap` is a
 * `type` kind).
 */
export type PoolEventMap = {
	/** A fresh resource was created (`create` resolved) and leased. */
	readonly create: readonly []
	/** A token was handed to a lessee (a reused idle one, a fresh one, or a served waiter). */
	readonly acquire: readonly []
	/** A leased resource returned to idle (no waiter was parked). */
	readonly release: readonly []
	/** A resource was destroyed (`clear` / `destroy`, or a failed `validate`). */
	readonly destroy: readonly []
}

/**
 * The push observation surface of a {@link WorkerInterface} (AGENTS §13) — the job
 * lifecycle a fire-and-forget observer subscribes to, surfacing the underlying queue's
 * moments so a Worker consumer never reaches through to the internal `Queue`.
 *
 * @typeParam TResult - The value a job resolves (the `success` payload), mirroring the
 *   {@link WorkerInterface}'s own `TResult`.
 *
 * @remarks
 * A Worker is a {@link Queue}⨉{@link Pool} facade; this map RE-EXPOSES the queue lifecycle
 * the worker surfaces (`enqueue` / `start` / `retry` / `success` / `failure` / `abort` /
 * `drain`) as the worker's OWN events — wired from the underlying queue's emitter at
 * construction, so a buggy observer is isolated exactly as on the queue (a throw routes to
 * the worker emitter's `error` handler, AGENTS §13). The pool's create / acquire / release
 * events stay the pool's internal concern (a Worker manages its own resources); a consumer
 * who wants them observes a `Pool` directly. Declared as a `type` alias (§4.5).
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

/**
 * One queued unit of work, plus the resolvers of the promise `enqueue` returned.
 *
 * @remarks
 * `id` keys the entry in the durable store; `attempts` is the STARTING attempt index — `0`
 * for a fresh entry, the persisted count for one resumed by `restore`. Held only inside the
 * {@link QueueInterface} engine (the in-flight pending list); not part of the public call
 * surface, but centralized here per AGENTS §5 (an impl file holds only its class).
 *
 * @typeParam TInput - The work payload the entry carries
 * @typeParam TResult - The value the entry resolves
 */
export interface QueueEntry<TInput, TResult> {
	readonly id: string
	readonly input: TInput
	readonly options: QueueEntryOptions | undefined
	readonly attempts: number
	readonly resolve: (value: TResult) => void
	readonly reject: (error: unknown) => void
}

/** The per-attempt execution handle a queue handler receives. */
export interface QueueExecution {
	/**
	 * The entry's stable id — equal across every attempt and across a crash-replay
	 * (`restore` re-runs an entry under its original id). Durable persistence is
	 * at-least-once (a crash between handler-success and the store's `remove`, or a
	 * failed `remove`, re-runs the entry), so use this id to make a handler idempotent
	 * — de-dup against it over the replay window.
	 */
	readonly id: string
	/** Fires on the per-attempt timeout, a queue-level abort, or the entry's own signal. */
	readonly signal: AbortSignal
}

/** Runs one queued entry's work; may reject to trigger a retry. */
export type QueueHandler<TInput, TResult> = (
	input: TInput,
	execution: QueueExecution,
) => Promise<TResult> | TResult

/**
 * Per-entry options for `enqueue`.
 *
 * @remarks
 * - `id` — a trace label for the entry; defaults to a random UUID.
 * - `retries` — extra attempts after the first on failure (or a per-attempt
 *   timeout); overrides the queue default. A queue-level abort never retries.
 * - `timeout` — the per-attempt deadline in milliseconds; overrides the queue
 *   default. A non-positive value (or no default) means no deadline.
 * - `signal` — an entry-scoped abort; once it fires the entry rejects (its
 *   in-flight attempt's `signal` fires too) and does not retry.
 */
export interface QueueEntryOptions {
	readonly id?: string
	readonly retries?: number
	readonly timeout?: number
	readonly signal?: AbortSignal
}

/**
 * Options for `createQueue`.
 *
 * @remarks
 * - `handler` — runs each entry's work; rejecting triggers a retry while attempts
 *   remain.
 * - `concurrency` — the maximum number of entries in flight at once; defaults to
 *   `1` (ordered, one-at-a-time). Floored at `1`.
 * - `retries` — the default extra attempts per entry on failure; defaults to `0`.
 * - `timeout` — the default per-attempt deadline in milliseconds; defaults to none
 *   (a non-positive value means no deadline).
 * - `store` — durable backing; outstanding entries survive a restart; call
 *   `restore()` to re-run them.
 * - `on` — the reserved {@link EmitterHooks} key (§8): initial listeners for the queue's
 *   {@link QueueEventMap}, wired at construction (e.g. `{ drain: () => log('idle') }`).
 */
export interface QueueOptions<TInput, TResult> {
	readonly on?: EmitterHooks<QueueEventMap<TResult>>
	/** The emitter's listener-error handler (AGENTS §13) — a listener throw routes here, not to a domain event. */
	readonly error?: EmitterErrorHandler
	readonly handler: QueueHandler<TInput, TResult>
	readonly concurrency?: number
	readonly retries?: number
	readonly timeout?: number
	readonly store?: QueueStoreInterface<TInput>
}

/**
 * A concurrent, cooperative job queue.
 *
 * @remarks
 * Exposes a typed {@link emitter} (AGENTS §13) carrying its lifecycle moments
 * ({@link QueueEventMap}) for fire-and-forget observers, ALONGSIDE each `enqueue` promise.
 * Emitting is observation-only — every event fires AFTER the relevant wake / park / settle
 * transition, so a buggy observer can never reorder or corrupt the wake-park / settle-once
 * engine: the emitter isolates a listener throw and routes it to its `error` handler (the
 * `error` option), never the engine. Subscribe via `queue.emitter.on(...)`.
 */
export interface QueueInterface<TInput, TResult> {
	readonly emitter: EmitterInterface<QueueEventMap<TResult>>
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

/**
 * A leased resource from a {@link PoolInterface} — `value` is the live resource and
 * `release()` returns it to the pool for reuse (or hands it to the next waiter).
 */
export interface PoolToken<T> {
	/** The leased resource. */
	readonly value: T
	/** Return the resource to the pool; calling more than once is a no-op. */
	release(): void
}

/**
 * A parked acquirer on a {@link PoolInterface}'s FIFO waiter list — its promise resolvers
 * plus the cleanup that detaches its abort listener.
 *
 * @remarks
 * Held only inside the {@link PoolInterface} engine (a resource at `max` parks the acquirer
 * here until a `release` hands it a token); not part of the public call surface, but
 * centralized here per AGENTS §5. `resolve` hands the waiter its leased {@link PoolToken};
 * `reject` fails its `acquire` (a teardown or an aborted wait); `clear` detaches the abort
 * listener so a settled waiter leaks nothing.
 *
 * @typeParam T - The resource the pool leases
 */
export interface PoolWaiter<T> {
	readonly resolve: (token: PoolToken<T>) => void
	readonly reject: (error: unknown) => void
	clear(): void
}

/**
 * Options for `createPool` — the resource lifecycle hooks.
 *
 * @remarks
 * - `create` — make a fresh resource; called when no idle resource is reusable and
 *   the pool is below `max`. May be async.
 * - `destroy` — tear a resource down when the pool drops it (`clear` / `destroy`, or
 *   a failed `validate`); optional and awaited.
 * - `validate` — check an idle resource is still usable before leasing it; an invalid
 *   resource is destroyed and replaced. Optional (an absent validator trusts idle).
 * - `max` — the most resources that may exist at once (idle + leased); defaults to
 *   unbounded. A surplus `acquire` waits (FIFO) for a `release`.
 * - `on` — the reserved {@link EmitterHooks} key (§8): initial listeners for the pool's
 *   {@link PoolEventMap}, wired at construction (e.g. `{ create: () => count() }`).
 */
export interface PoolOptions<T> {
	readonly on?: EmitterHooks<PoolEventMap>
	/** The emitter's listener-error handler (AGENTS §13) — a listener throw routes here, not to a domain event. */
	readonly error?: EmitterErrorHandler
	readonly create: () => Promise<T> | T
	readonly destroy?: (value: T) => Promise<void> | void
	readonly validate?: (value: T) => Promise<boolean> | boolean
	readonly max?: number
}

/**
 * A bounded resource pool with idle reuse + FIFO waiting.
 *
 * @remarks
 * Exposes a typed {@link emitter} (AGENTS §13) carrying its resource lifecycle moments
 * ({@link PoolEventMap}) for fire-and-forget observers. Emitting is observation-only —
 * every event fires AFTER the relevant create / acquire / release / destroy transition, so a
 * buggy observer can never corrupt the FIFO handoff-eviction machinery: the emitter isolates
 * a listener throw and routes it to its `error` handler (the `error` option), never the pool.
 */
export interface PoolInterface<T> {
	readonly emitter: EmitterInterface<PoolEventMap>
	readonly size: number
	readonly idle: number
	readonly active: number
	acquire(signal?: AbortSignal): Promise<PoolToken<T>>
	clear(): Promise<void>
	destroy(): Promise<void>
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

/**
 * A durably persisted, still-outstanding queue entry — re-run after a restart.
 *
 * @remarks
 * The store holds only entries that have NOT yet completed, so what `load`
 * returns on startup is exactly the work to resume. `id` keys the entry (the
 * store upserts by it); `input` is the handler's work payload (it must be
 * JSON-serializable to survive a JSON / SQLite driver); `attempts` is how many
 * times the entry has been tried so far.
 */
export interface StoredEntry<TInput> {
	readonly id: string
	readonly input: TInput
	readonly attempts: number
}

/**
 * Durable backing for a Queue's outstanding entries.
 *
 * @remarks
 * The store holds ONLY work that has not yet completed: `save` upserts an entry
 * (by its `id`), `remove` drops a finished one, `load` returns everything
 * outstanding (to restore a queue after a restart), and `clear` empties it. It is
 * a minimal interface (AGENTS §21) over the `databases` layer — a queue's durable
 * state is just a table — so any `DriverInterface` backend (memory, JSON, SQLite)
 * persists it without the store knowing which.
 *
 * @typeParam TInput - The work input each {@link StoredEntry} carries
 */
export interface QueueStoreInterface<TInput> {
	save(entry: StoredEntry<TInput>): Promise<void>
	remove(id: string): Promise<void>
	load(): Promise<readonly StoredEntry<TInput>[]>
	clear(): Promise<void>
}
