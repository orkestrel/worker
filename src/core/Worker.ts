import type { EmitterInterface } from '../emitters/types.js'
import type { QueueEntryOptions, WorkerEventMap, WorkerInterface, WorkerOptions } from './types.js'
import { Emitter } from '../emitters/Emitter.js'
import { Pool } from './Pool.js'
import { Queue } from './Queue.js'

/**
 * A resource-backed job worker — a thin facade composing a {@link Queue} with a
 * {@link Pool}.
 *
 * @remarks
 * - **Composition, not reimplementation.** The Worker owns a `Pool` (built from
 *   `options.pool`) and a `Queue` whose handler ACQUIRES a pooled resource, runs the
 *   user handler against it, and RELEASES it in a `finally`. All concurrency, retries,
 *   timeout, and lifecycle are the Queue's — the Worker adds only the resource pairing.
 * - **Resource ↔ concurrency.** The pool's `max` defaults to the worker's
 *   `concurrency` (default `1`), so at most one resource exists per in-flight job and
 *   idle resources are reused across jobs.
 * - **Acquire over the attempt signal.** Each job acquires using the attempt's
 *   `execution.signal`, so an `abort` / `timeout` while waiting for a resource rejects
 *   the acquire — the Queue then handles retry / rejection, and there is no token to
 *   release (the resource was never leased).
 * - **Lifecycle (§10).** `enqueue` / `restore` / `start` / `stop` / `pause` / `resume` /
 *   `abort` / `clear` delegate to the queue; `count` / `active` / `paused` / `stopped`
 *   read it. `destroy` destroys the queue then tears the pool down, idempotently.
 * - **Durability.** An optional `store` is passed straight through to the queue, so the
 *   worker's outstanding jobs persist; `restore` re-runs them (delegated to the queue).
 * - **Observable (§13).** The owned {@link emitter} ({@link WorkerEventMap}) RE-EXPOSES the
 *   underlying queue's job lifecycle (`enqueue` / `start` / `retry` / `success` / `failure` /
 *   `abort` / `drain`) as the worker's OWN events — bridged from the inner queue's emitter at
 *   construction — so a consumer observes the worker without reaching through to internals.
 *   The bridge re-emits directly on the worker's own emitter; the worker emitter isolates a
 *   listener throw and routes it to its `error` handler (the `error` option), so a buggy
 *   worker observer can never corrupt the inner queue or pool — the bridge listener never
 *   throws, so the inner queue's own emit stays balanced. The pool's create / acquire /
 *   release events stay the pool's internal concern (a Worker manages its own resources);
 *   observe a `Pool` directly for those.
 */
export class Worker<TInput, TResource, TResult> implements WorkerInterface<TInput, TResult> {
	readonly #queue: Queue<TInput, TResult>
	readonly #pool: Pool<TResource>
	// The PUSH observation surface (§13) — the worker's OWN emitter, fed by the queue→worker
	// bridge. The emitter isolates a worker observer's throw (routing it to the `error`
	// handler), so it never escapes into queue or pool.
	readonly #emitter: Emitter<WorkerEventMap<TResult>>
	#destroyed = false

	constructor(options: WorkerOptions<TInput, TResource, TResult>) {
		const concurrency = Math.max(1, options.concurrency ?? 1)
		this.#emitter = new Emitter<WorkerEventMap<TResult>>({ on: options?.on, error: options?.error })
		this.#pool = new Pool<TResource>({
			...options.pool,
			max: options.pool.max ?? concurrency,
		})
		this.#queue = new Queue<TInput, TResult>({
			handler: async (input, execution) => {
				const token = await this.#pool.acquire(execution.signal)
				try {
					return await options.handler(input, token.value, execution)
				} finally {
					token.release()
				}
			},
			concurrency,
			retries: options.retries,
			timeout: options.timeout,
			store: options.store,
		})
		this.#bridge()
	}

	get emitter(): EmitterInterface<WorkerEventMap<TResult>> {
		return this.#emitter
	}

	get count(): number {
		return this.#queue.count
	}

	get active(): number {
		return this.#queue.active
	}

	get paused(): boolean {
		return this.#queue.paused
	}

	get stopped(): boolean {
		return this.#queue.stopped
	}

	enqueue(input: TInput, options?: QueueEntryOptions): Promise<TResult> {
		return this.#queue.enqueue(input, options)
	}

	restore(): Promise<void> {
		return this.#queue.restore()
	}

	start(): void {
		this.#queue.start()
	}

	stop(): void {
		this.#queue.stop()
	}

	pause(): void {
		this.#queue.pause()
	}

	resume(): void {
		this.#queue.resume()
	}

	abort(reason?: unknown): void {
		this.#queue.abort(reason)
	}

	clear(): void {
		this.#queue.clear()
	}

	destroy(): void {
		if (this.#destroyed) return
		this.#destroyed = true
		this.#queue.destroy()
		void this.#pool.destroy()
	}

	// Bridge the inner queue's lifecycle onto the worker's OWN emitter, once at construction.
	// Each listener re-emits the queue event directly on the worker's emitter, which isolates a
	// worker observer's throw (routing it to the worker's `error` handler). Because the bridge
	// listener itself never throws, the queue's own `#emitter.emit` — which invoked this
	// listener — sees no throw, so the inner queue's engine stays balanced regardless of what a
	// worker observer does. The events are already post-transition (they fire from the queue's
	// own post-settle / post-wake emits), so this stays observation.
	#bridge(): void {
		const queue = this.#queue.emitter
		queue.on('enqueue', (id) => this.#emitter.emit('enqueue', id))
		queue.on('start', (id) => this.#emitter.emit('start', id))
		queue.on('retry', (id, attempt) => this.#emitter.emit('retry', id, attempt))
		queue.on('success', (id, result) => this.#emitter.emit('success', id, result))
		queue.on('failure', (id, error) => this.#emitter.emit('failure', id, error))
		queue.on('abort', (reason) => this.#emitter.emit('abort', reason))
		queue.on('drain', () => this.#emitter.emit('drain'))
	}
}
