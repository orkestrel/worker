import type { WorkerInterface } from '@src/core'
import type { QueueExecution } from '@orkestrel/queue'
import type { NodeThread, NodeWorkerOptions } from './types.js'
import { createWorker } from '@src/core'
import { dispatch, spawnThread } from './helpers.js'

/**
 * Internal composition entity backing {@link createNodeWorker}.
 *
 * @remarks
 * Supplies bound Pool and Queue operations without nested function assignments. The resulting
 * public entity remains the plain core {@link WorkerInterface}.
 */
export class NodeWorker<TInput, TResult> {
	readonly #options: NodeWorkerOptions<TInput, TResult>

	constructor(options: NodeWorkerOptions<TInput, TResult>) {
		this.#options = options
	}

	build(): WorkerInterface<TInput, TResult> {
		return createWorker<TInput, NodeThread, TResult>({
			pool: {
				create: this.#create.bind(this),
				destroy: this.#destroy.bind(this),
				validate: this.#validate.bind(this),
				...(this.#options.concurrency !== undefined ? { max: this.#options.concurrency } : {}),
			},
			handler: this.#handle.bind(this),
			...(this.#options.concurrency !== undefined
				? { concurrency: this.#options.concurrency }
				: {}),
			...(this.#options.retries !== undefined ? { retries: this.#options.retries } : {}),
			...(this.#options.timeout !== undefined ? { timeout: this.#options.timeout } : {}),
			...(this.#options.store !== undefined ? { store: this.#options.store } : {}),
		})
	}

	#create(): Promise<NodeThread> {
		return spawnThread(this.#options.script, this.#options.workerData)
	}

	async #destroy(thread: NodeThread): Promise<void> {
		await thread.worker.terminate()
	}

	#validate(thread: NodeThread): boolean {
		return thread.alive && thread.worker.threadId > 0
	}

	#handle(input: TInput, thread: NodeThread, execution: QueueExecution): Promise<TResult> {
		if (!this.#options.input(input)) {
			return Promise.reject(new Error('input did not satisfy input guard'))
		}
		return dispatch(thread, input, execution, this.#options.result)
	}
}
