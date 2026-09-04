import type { QueueContext } from '@orkestrel/queue'
import type { Guard } from '@orkestrel/contract'
import type { NodeThread } from './types.js'
import type { Worker as ThreadWorker } from 'node:worker_threads'
import { attempt, isRecord } from '@orkestrel/contract'
import { isReply } from './helpers.js'
import { Thread } from './Thread.js'

/**
 * Represents one dispatched worker-thread job — the lifecycle entity behind a job posted to a
 * leased {@link NodeThread}, whose {@link promise} settles with the narrowed reply.
 *
 * @remarks
 * Mints a fresh per-dispatch correlation `id`, posts it with `job: context.id`, and settles
 * when the thread replies for that correlation id. The stable Queue job id reaches the worker
 * handler for idempotency across retries and restore; it is not caller identity or
 * authentication / authorization evidence. Per-job consumer context is explicit,
 * structured-cloneable `input`; ambient context is not worker-thread transport. A success
 * `value` is narrowed through `result` (a value that fails the guard rejects — the zero-`as`
 * type bridge); a failure rejects with the thread's error string. A thread that ALREADY died
 * rejects synchronously at construction from the latched {@link NodeThread.death} — its death
 * events fired before this dispatch existed and will never fire again, so waiting on the
 * listeners would dangle forever; the latch makes death total across every event ordering. If
 * the thread `error`s / `exit`s mid-flight the job rejects. On a `context.signal` abort it
 * contains the cooperative `abort` post, evicts the thread, and observes `terminate()`
 * settlement because CPU-bound work cannot honour the signal.
 *
 * It owns stable `message` / `messageerror` / death listener identities, settlement,
 * result-guard containment, and abort eviction for one dispatch. Deserialization failure, a
 * matching-id malformed reply, and abort each evict and terminate the thread before rejecting,
 * with termination failure preserved. Non-record, id-less, hostile-id, and foreign-id chatter
 * is ignored. Every per-job listener (`message` / `messageerror` / `error` / `exit` / `abort`)
 * is removed on settle.
 *
 * Eviction reaches `alive` for a {@link NodeThread} this package produced. Against a
 * consumer-supplied `NodeThread` an abort or a `messageerror` still terminates the supplied
 * `worker` and rejects the job, and the implementer owns flipping its own `alive`.
 *
 * @typeParam TResult - The reply type the `result` guard narrows to
 *
 * @example
 * ```ts
 * import { createThread, Dispatch } from '@orkestrel/worker/server'
 *
 * const isNumber = (value: unknown): value is number => typeof value === 'number'
 *
 * const thread = await createThread(new URL('./double.js', import.meta.url))
 * const controller = new AbortController()
 * const job = new Dispatch(thread, 21, { id: 'job-1', signal: controller.signal }, isNumber)
 * console.log(await job.promise) // 42
 * await thread.worker.terminate()
 * ```
 */
export class Dispatch<TResult> {
	readonly #thread: NodeThread
	readonly #worker: ThreadWorker
	readonly #input: unknown
	readonly #context: QueueContext
	readonly #result: Guard<TResult>
	readonly #id = crypto.randomUUID()
	readonly #promise: Promise<TResult>
	readonly #fulfill: (value: TResult | PromiseLike<TResult>) => void
	readonly #reject: (reason?: unknown) => void
	readonly #messageHandler: (value: unknown) => void
	readonly #messageErrorHandler: (error: Error) => void
	readonly #errorHandler: (error: Error) => void
	readonly #exitHandler: () => void
	readonly #abortHandler: () => void
	#settled = false

	constructor(thread: NodeThread, input: unknown, context: QueueContext, result: Guard<TResult>) {
		this.#thread = thread
		this.#worker = thread.worker
		this.#input = input
		this.#context = context
		this.#result = result
		const settlement = Promise.withResolvers<TResult>()
		this.#promise = settlement.promise
		this.#fulfill = settlement.resolve
		this.#reject = settlement.reject
		this.#messageHandler = this.#message.bind(this)
		this.#messageErrorHandler = this.#messageError.bind(this)
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
		this.#worker.on('messageerror', this.#messageErrorHandler)
		this.#worker.on('error', this.#errorHandler)
		this.#worker.on('exit', this.#exitHandler)
		if (this.#context.signal.aborted) {
			this.#abort()
			return
		}
		this.#context.signal.addEventListener('abort', this.#abortHandler, { once: true })
		try {
			this.#worker.postMessage({
				id: this.#id,
				job: this.#context.id,
				command: 'run',
				input: this.#input,
			})
		} catch (error: unknown) {
			this.#fail(error instanceof Error ? error : new Error(String(error)))
		}
	}

	#message(value: unknown): void {
		if (!isRecord(value)) return
		const id = attempt(() => value.id)
		if (!id.success || id.value !== this.#id) return
		if (!isReply(value, this.#id)) {
			this.#terminate(new Error('worker reply was malformed'))
			return
		}
		if (value.ok) {
			const reply = value.value
			try {
				if (this.#result(reply)) this.#succeed(reply)
				else this.#fail(new Error('reply did not satisfy result guard'))
			} catch (error: unknown) {
				this.#fail(error)
			}
			return
		}
		this.#fail(new Error(value.error))
	}

	#messageError(error: Error): void {
		this.#terminate(error)
	}

	#error(error: Error): void {
		this.#fail(error)
	}

	#exit(): void {
		this.#fail(this.#thread.death ?? new Error('worker thread exited'))
	}

	#abort(): void {
		const notification: unknown[] = []
		try {
			this.#worker.postMessage({ id: this.#id, command: 'abort' })
		} catch (cause: unknown) {
			notification.push(cause)
		}
		this.#terminate(this.#context.signal.reason, notification)
	}

	#terminate(error: unknown, notification: readonly unknown[] = []): void {
		if (this.#settled) return
		this.#settled = true
		this.#detach()
		if (this.#thread instanceof Thread) this.#thread.evict()
		let termination: Promise<number>
		try {
			termination = this.#worker.terminate()
		} catch (cause: unknown) {
			this.#reject(new AggregateError([error, ...notification, cause], 'worker termination failed'))
			return
		}
		void termination.then(
			() => {
				if (notification.length === 0) this.#reject(error)
				else {
					this.#reject(
						new AggregateError([error, ...notification], 'worker abort notification failed'),
					)
				}
			},
			(cause: unknown) =>
				this.#reject(
					new AggregateError([error, ...notification, cause], 'worker termination failed'),
				),
		)
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
		this.#worker.off('messageerror', this.#messageErrorHandler)
		this.#worker.off('error', this.#errorHandler)
		this.#worker.off('exit', this.#exitHandler)
		this.#context.signal.removeEventListener('abort', this.#abortHandler)
	}
}
