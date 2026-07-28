import type { QueueExecution } from '@orkestrel/queue'
import type { Guard, NodeThread } from './types.js'
import type { Worker as ThreadWorker } from 'node:worker_threads'
import { Thread } from './Thread.js'
import { isReply } from './validators.js'

/**
 * Internal lifecycle entity for one dispatched worker-thread job.
 *
 * @remarks
 * Owns the stable listener identities, settlement guard, cleanup, result narrowing, and abort
 * eviction for one dispatch. The public {@link dispatch} helper constructs this entity and returns
 * its promise.
 */
export class Dispatch<TResult> {
	readonly #thread: NodeThread
	readonly #worker: ThreadWorker
	readonly #input: unknown
	readonly #execution: QueueExecution
	readonly #result: Guard<TResult>
	readonly #id = crypto.randomUUID()
	readonly #promise: Promise<TResult>
	readonly #fulfill: (value: TResult | PromiseLike<TResult>) => void
	readonly #reject: (reason?: unknown) => void
	readonly #messageHandler: (value: unknown) => void
	readonly #errorHandler: (error: Error) => void
	readonly #exitHandler: () => void
	readonly #abortHandler: () => void
	#settled = false

	constructor(
		thread: NodeThread,
		input: unknown,
		execution: QueueExecution,
		result: Guard<TResult>,
	) {
		this.#thread = thread
		this.#worker = thread.worker
		this.#input = input
		this.#execution = execution
		this.#result = result
		const settlement = Promise.withResolvers<TResult>()
		this.#promise = settlement.promise
		this.#fulfill = settlement.resolve
		this.#reject = settlement.reject
		this.#messageHandler = this.#message.bind(this)
		this.#errorHandler = this.#error.bind(this)
		this.#exitHandler = this.#exit.bind(this)
		this.#abortHandler = this.#abort.bind(this)
		this.#start()
	}

	get promise(): Promise<TResult> {
		return this.#promise
	}

	#start(): void {
		if (this.#thread.death !== undefined || !this.#thread.alive) {
			this.#fail(this.#thread.death ?? new Error('worker thread is dead'))
			return
		}
		this.#worker.on('message', this.#messageHandler)
		this.#worker.on('error', this.#errorHandler)
		this.#worker.on('exit', this.#exitHandler)
		if (this.#execution.signal.aborted) {
			this.#abort()
			return
		}
		this.#execution.signal.addEventListener('abort', this.#abortHandler, { once: true })
		try {
			this.#worker.postMessage({ id: this.#id, command: 'run', input: this.#input })
		} catch (error: unknown) {
			this.#fail(error instanceof Error ? error : new Error(String(error)))
		}
	}

	#message(value: unknown): void {
		if (!isReply(value, this.#id)) return
		if (value.ok) {
			const reply = value.value
			if (this.#result(reply)) this.#succeed(reply)
			else this.#fail(new Error('reply did not satisfy result guard'))
			return
		}
		this.#fail(new Error(value.error))
	}

	#error(error: Error): void {
		this.#fail(error)
	}

	#exit(): void {
		this.#fail(new Error('worker thread exited'))
	}

	#abort(): void {
		this.#worker.postMessage({ id: this.#id, command: 'abort' })
		if (this.#thread instanceof Thread) this.#thread.evict()
		void this.#worker.terminate()
		this.#fail(new Error('job aborted'))
	}

	#succeed(value: TResult): void {
		if (this.#settled) return
		this.#settled = true
		this.#detach()
		this.#fulfill(value)
	}

	#fail(error: unknown): void {
		if (this.#settled) return
		this.#settled = true
		this.#detach()
		this.#reject(error)
	}

	#detach(): void {
		this.#worker.off('message', this.#messageHandler)
		this.#worker.off('error', this.#errorHandler)
		this.#worker.off('exit', this.#exitHandler)
		this.#execution.signal.removeEventListener('abort', this.#abortHandler)
	}
}
