import type { NodeThread } from './types.js'
import { Worker as ThreadWorker } from 'node:worker_threads'

/**
 * Internal mutable implementation of the readonly {@link NodeThread} observation contract.
 *
 * @remarks
 * Liveness and the first terminal error live behind runtime-private fields. Consumers observe
 * their current values through readonly getters, while the worker lifecycle records transitions
 * through bound instance methods without exposing writable contract properties.
 */
export class Thread implements NodeThread {
	readonly #worker: ThreadWorker
	readonly #promise: Promise<NodeThread>
	readonly #resolve: (value: NodeThread | PromiseLike<NodeThread>) => void
	readonly #reject: (reason?: unknown) => void
	readonly #recordErrorHandler: (error: Error) => void
	readonly #recordExitHandler: (code: number) => void
	readonly #onlineHandler: () => void
	readonly #spawnErrorHandler: (error: Error) => void
	readonly #spawnExitHandler: (code: number) => void
	#alive = true
	#death: Error | undefined

	constructor(script: string | URL, workerData: unknown) {
		this.#worker = new ThreadWorker(script, {
			...(workerData !== undefined ? { workerData } : {}),
		})
		const readiness = Promise.withResolvers<NodeThread>()
		this.#promise = readiness.promise
		this.#resolve = readiness.resolve
		this.#reject = readiness.reject
		this.#recordErrorHandler = this.#recordError.bind(this)
		this.#recordExitHandler = this.#recordExit.bind(this)
		this.#onlineHandler = this.#online.bind(this)
		this.#spawnErrorHandler = this.#spawnError.bind(this)
		this.#spawnExitHandler = this.#spawnExit.bind(this)

		this.#worker.on('error', this.#recordErrorHandler)
		this.#worker.on('exit', this.#recordExitHandler)
		this.#worker.once('online', this.#onlineHandler)
		this.#worker.once('error', this.#spawnErrorHandler)
		this.#worker.once('exit', this.#spawnExitHandler)
	}

	get worker(): ThreadWorker {
		return this.#worker
	}

	get alive(): boolean {
		return this.#alive
	}

	get death(): Error | undefined {
		return this.#death
	}

	get promise(): Promise<NodeThread> {
		return this.#promise
	}

	evict(): void {
		this.#alive = false
	}

	#recordError(error: Error): void {
		this.#alive = false
		if (this.#death === undefined) this.#death = error
	}

	#recordExit(code: number): void {
		this.#alive = false
		if (this.#death === undefined) {
			this.#death = new Error(`worker thread exited (code ${String(code)})`)
		}
	}

	#online(): void {
		this.#worker.off('error', this.#spawnErrorHandler)
		this.#worker.off('exit', this.#spawnExitHandler)
		this.#resolve(this)
	}

	#spawnError(error: Error): void {
		this.#worker.off('online', this.#onlineHandler)
		this.#worker.off('exit', this.#spawnExitHandler)
		this.#reject(error)
	}

	#spawnExit(code: number): void {
		this.#worker.off('online', this.#onlineHandler)
		this.#worker.off('error', this.#spawnErrorHandler)
		this.#reject(new Error(`worker thread exited before coming online (code ${String(code)})`))
	}
}
