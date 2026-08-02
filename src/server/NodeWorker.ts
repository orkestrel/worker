import type { WorkerInterface } from '@src/core'
import type { Guard } from '@orkestrel/contract'
import type { QueueExecution, QueueStoreInterface } from '@orkestrel/queue'
import type { NodeThread, NodeWorkerOptions } from './types.js'
import { createWorker } from '@src/core'
import { attempt } from '@orkestrel/contract'
import { dispatch, spawnThread } from './helpers.js'

/**
 * Internal composition entity backing {@link createNodeWorker}.
 *
 * @remarks
 * Supplies bound Pool and Queue operations without nested function assignments. The resulting
 * public entity remains the plain core {@link WorkerInterface}.
 */
export class NodeWorker<TInput, TResult> {
	readonly #script: string | URL
	readonly #input: Guard<TInput>
	readonly #result: Guard<TResult>
	readonly #workerData: unknown
	readonly #concurrency: number | undefined
	readonly #retries: number | undefined
	readonly #timeout: number | undefined
	readonly #store: QueueStoreInterface<TInput> | undefined

	constructor(options: NodeWorkerOptions<TInput, TResult>) {
		this.#script = options.script
		this.#input = options.input
		this.#result = options.result
		this.#workerData = options.workerData
		this.#concurrency = options.concurrency
		this.#retries = options.retries
		this.#timeout = options.timeout
		this.#store = options.store
	}

	build(): WorkerInterface<TInput, TResult> {
		return createWorker<TInput, NodeThread, TResult>({
			pool: {
				create: this.#create.bind(this),
				destroy: this.#destroy.bind(this),
				validate: this.#validate.bind(this),
				...(this.#concurrency !== undefined ? { max: this.#concurrency } : {}),
			},
			handler: this.#handle.bind(this),
			...(this.#concurrency !== undefined ? { concurrency: this.#concurrency } : {}),
			...(this.#retries !== undefined ? { retries: this.#retries } : {}),
			...(this.#timeout !== undefined ? { timeout: this.#timeout } : {}),
			...(this.#store !== undefined ? { store: this.#store } : {}),
		})
	}

	#create(): Promise<NodeThread> {
		return spawnThread(this.#script, this.#workerData)
	}

	async #destroy(thread: NodeThread): Promise<void> {
		await thread.worker.terminate()
	}

	#validate(thread: NodeThread): boolean {
		return thread.alive && thread.worker.threadId > 0
	}

	#handle(input: TInput, thread: NodeThread, execution: QueueExecution): Promise<TResult> {
		const outcome = attempt(() => this.#input(input))
		if (!outcome.success) return Promise.reject(outcome.error)
		if (!outcome.value) {
			return Promise.reject(new Error('input did not satisfy input guard'))
		}
		return dispatch(thread, input, execution, this.#result)
	}
}
