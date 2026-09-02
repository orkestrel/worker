import type { EmitterInterface } from '@orkestrel/emitter'
import type { QueueEntryOptions, QueueExecution } from '@orkestrel/queue'
import type { WorkerEventMap, WorkerHandler, WorkerInterface, WorkerOptions } from './types.js'
import { Emitter } from '@orkestrel/emitter'
import { Pool } from '@orkestrel/pool'
import { Queue } from '@orkestrel/queue'

/**
 * Represents a resource-backed job worker — a thin facade composing a `Queue`
 * (`@orkestrel/queue`) with a `Pool` (`@orkestrel/pool`).
 *
 * @remarks
 * - **Composition, not reimplementation.** The Worker owns a `Pool` (built from
 *   `options.pool`) and a `Queue` whose handler ACQUIRES a pooled resource, runs the
 *   user handler against it, and RELEASES it in a `finally`. All concurrency, retries,
 *   timeout, and lifecycle are the Queue's — the Worker adds only the resource pairing.
 * - **Resource ↔ concurrency.** The queue strictly validates `concurrency` as a positive
 *   safe integer after caller options are captured once. Only `undefined` defaults
 *   `concurrency` to `1` or pool `max` to that value; runtime `null` reaches the owning
 *   validator. The queue validates before the pool option is read; every declared pool member
 *   is then captured once by direct access, preserving inherited and non-enumerable structural
 *   options. At most one resource exists per in-flight job by default, and idle resources are
 *   reused across jobs.
 * - **Acquire over the attempt signal.** Each job acquires using the attempt's
 *   `execution.signal`, so an `abort` / `timeout` while waiting for a resource rejects
 *   the acquire — the Queue then handles retry / rejection, and there is no token to
 *   release (the resource was never leased).
 * - **Lifecycle (§10).** `enqueue` / `restore` / `start` / `stop` / `pause` / `resume` /
 *   `abort` / `clear` delegate to the queue; `count` / `active` / `paused` / `stopped`
 *   read it. `stop` / `abort` / `clear` return the queue's own cleanup barriers.
 *   `destroy` returns one stable barrier while it tears down the queue, then the pool,
 *   and destroys the worker emitter last. A sole cleanup failure is preserved by
 *   identity; failures from both layers become an ordered `AggregateError`.
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
	readonly #handler: WorkerHandler<TInput, TResource, TResult>
	#ending: PromiseWithResolvers<void> | undefined

	constructor(options: WorkerOptions<TInput, TResource, TResult>) {
		const {
			concurrency: capturedConcurrency,
			handler,
			on,
			error,
			retries,
			timeout,
			store,
		} = options
		const concurrency = capturedConcurrency === undefined ? 1 : capturedConcurrency
		this.#handler = handler
		this.#emitter = new Emitter<WorkerEventMap<TResult>>({
			...(on !== undefined ? { on } : {}),
			...(error !== undefined ? { error } : {}),
		})
		this.#queue = new Queue<TInput, TResult>({
			handler: this.#handle.bind(this),
			concurrency,
			...(retries !== undefined ? { retries } : {}),
			...(timeout !== undefined ? { timeout } : {}),
			...(store !== undefined ? { store } : {}),
		})
		const pool = options.pool
		const { max, on: poolOn, error: poolError, create, destroy, validate } = pool
		this.#pool = new Pool<TResource>({
			create,
			max: max === undefined ? concurrency : max,
			...(poolOn !== undefined ? { on: poolOn } : {}),
			...(poolError !== undefined ? { error: poolError } : {}),
			...(destroy !== undefined ? { destroy } : {}),
			...(validate !== undefined ? { validate } : {}),
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

	stop(): Promise<void> {
		return this.#queue.stop()
	}

	pause(): void {
		this.#queue.pause()
	}

	resume(): void {
		this.#queue.resume()
	}

	abort(reason?: unknown): Promise<void> {
		return this.#queue.abort(reason)
	}

	clear(): Promise<void> {
		return this.#queue.clear()
	}

	destroy(): Promise<void> {
		if (this.#ending !== undefined) return this.#ending.promise
		const ending = Promise.withResolvers<void>()
		this.#ending = ending
		void this.#teardown(ending)
		return ending.promise
	}

	async #handle(input: TInput, execution: QueueExecution): Promise<TResult> {
		const token = await this.#pool.acquire(execution.signal)
		try {
			return await this.#handler(input, token.value, execution)
		} finally {
			token.release()
		}
	}

	async #teardown(ending: PromiseWithResolvers<void>): Promise<void> {
		const failures: unknown[] = []
		try {
			await this.#queue.destroy()
		} catch (error) {
			failures.push(error)
		}
		try {
			await this.#pool.destroy()
		} catch (error) {
			failures.push(error)
		}
		this.#emitter.destroy()
		if (failures.length === 0) ending.resolve()
		else if (failures.length === 1) ending.reject(failures[0])
		else ending.reject(new AggregateError(failures, 'worker destroy cleanup failed'))
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
