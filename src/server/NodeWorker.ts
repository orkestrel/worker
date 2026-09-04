import type { EmitterErrorHandler, EmitterHooks } from '@orkestrel/emitter'
import type { WorkerEventMap, WorkerInterface } from '@src/core'
import type { Guard } from '@orkestrel/contract'
import type { QueueContext, QueueStoreInterface } from '@orkestrel/queue'
import type { NodeThread, NodeWorkerOptions } from './types.js'
import { createWorker } from '@src/core'
import { attempt } from '@orkestrel/contract'
import { Dispatch } from './Dispatch.js'
import { Thread } from './Thread.js'

/**
 * Represents the internal composition entity backing {@link createNodeWorker}.
 *
 * @remarks
 * Supplies bound Pool and Queue operations without nested function assignments. The resulting
 * public entity is the plain core {@link WorkerInterface}.
 */
export class NodeWorker<TInput, TResult> {
	readonly #on: EmitterHooks<WorkerEventMap<TResult>> | undefined
	readonly #error: EmitterErrorHandler | undefined
	readonly #script: string | URL
	readonly #input: Guard<TInput>
	readonly #result: Guard<TResult>
	readonly #workerData: unknown
	readonly #concurrency: number | undefined
	readonly #retries: number | undefined
	readonly #timeout: number | undefined
	readonly #store: QueueStoreInterface<TInput> | undefined

	constructor(options: NodeWorkerOptions<TInput, TResult>) {
		this.#on = options.on
		this.#error = options.error
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
			...(this.#on !== undefined ? { on: this.#on } : {}),
			...(this.#error !== undefined ? { error: this.#error } : {}),
			...(this.#concurrency !== undefined ? { concurrency: this.#concurrency } : {}),
			...(this.#retries !== undefined ? { retries: this.#retries } : {}),
			...(this.#timeout !== undefined ? { timeout: this.#timeout } : {}),
			...(this.#store !== undefined ? { store: this.#store } : {}),
		})
	}

	#create(): Promise<NodeThread> {
		return new Thread(this.#script, this.#workerData).promise
	}

	async #destroy(thread: NodeThread): Promise<void> {
		await thread.worker.terminate()
	}

	#validate(thread: NodeThread): boolean {
		return thread.alive && thread.worker.threadId > 0
	}

	#handle(input: TInput, thread: NodeThread, context: QueueContext): Promise<TResult> {
		const outcome = attempt(() => this.#input(input))
		if (!outcome.success) return Promise.reject(outcome.error)
		if (!outcome.value) {
			return Promise.reject(new Error('input did not satisfy input guard'))
		}
		return new Dispatch(thread, input, context, this.#result).promise
	}
}
