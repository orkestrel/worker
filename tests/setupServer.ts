// Server-test setup — node-only helpers, loaded after `setup.ts` for the node
// `src:server` project. `node:fs` / `node:os` / `node:path` imports belong here,
// never in `setup.ts` (AGENTS §16.1).

import type { NodeWorkerOptions } from '@src/server'
import type { RecorderInterface } from '@orkestrel/test'
import type { Worker as ThreadWorker } from 'node:worker_threads'
import { join } from 'node:path'
import { isRecord } from '@orkestrel/contract'
import { createScratch } from '@orkestrel/test/server'

/** Post one valid raw run envelope to a real worker thread. */
export function postRun(thread: ThreadWorker, id: string, job: string, input: unknown): void {
	thread.postMessage({ id, job, command: 'run', input })
}

// A fresh on-disk JSON-store path under an owned scratch directory, with a `cleanup`
// thunk that removes it. Used by the `createJSONQueueStore` tests, which need real
// file persistence across a store reopen. Call `cleanup` in `afterEach` so no temp
// file leaks (AGENTS §16.1).
export function tempDatabasePath(): { readonly path: string; readonly cleanup: () => void } {
	const scratch = createScratch({ prefix: 'worker-store-' })
	return {
		path: join(scratch.path, 'store.json'),
		cleanup: () => scratch.destroy(),
	}
}

/** Getter-backed Node worker options whose property reads are recorded. */
export class NodeWorkerOptionsProbe<TInput, TResult> implements NodeWorkerOptions<TInput, TResult> {
	#values: Required<NodeWorkerOptions<TInput, TResult>>
	readonly #reads: RecorderInterface<readonly [property: keyof NodeWorkerOptions<TInput, TResult>]>

	constructor(
		values: Required<NodeWorkerOptions<TInput, TResult>>,
		reads: RecorderInterface<readonly [property: keyof NodeWorkerOptions<TInput, TResult>]>,
	) {
		this.#values = values
		this.#reads = reads
	}

	get script(): string | URL {
		this.#reads.handler('script')
		return this.#values.script
	}

	get input(): NodeWorkerOptions<TInput, TResult>['input'] {
		this.#reads.handler('input')
		return this.#values.input
	}

	get result(): NodeWorkerOptions<TInput, TResult>['result'] {
		this.#reads.handler('result')
		return this.#values.result
	}

	get workerData(): Required<NodeWorkerOptions<TInput, TResult>>['workerData'] {
		this.#reads.handler('workerData')
		return this.#values.workerData
	}

	get concurrency(): Required<NodeWorkerOptions<TInput, TResult>>['concurrency'] {
		this.#reads.handler('concurrency')
		return this.#values.concurrency
	}

	get retries(): Required<NodeWorkerOptions<TInput, TResult>>['retries'] {
		this.#reads.handler('retries')
		return this.#values.retries
	}

	get timeout(): Required<NodeWorkerOptions<TInput, TResult>>['timeout'] {
		this.#reads.handler('timeout')
		return this.#values.timeout
	}

	get store(): Required<NodeWorkerOptions<TInput, TResult>>['store'] {
		this.#reads.handler('store')
		return this.#values.store
	}

	replace(values: Required<NodeWorkerOptions<TInput, TResult>>): void {
		this.#values = values
	}
}

/** A pending reply from a real worker thread with stable listener identities. */
export class ThreadReply {
	readonly #thread: ThreadWorker
	readonly #id: string
	readonly #promise: Promise<Readonly<Record<string, unknown>>>
	readonly #resolve: (value: Readonly<Record<string, unknown>>) => void
	readonly #reject: (reason?: unknown) => void
	readonly #messageHandler: (value: unknown) => void
	readonly #failHandler: (error: Error) => void
	readonly #exitHandler: (code: number) => void
	#settled = false

	constructor(thread: ThreadWorker, id: string) {
		this.#thread = thread
		this.#id = id
		const settlement = Promise.withResolvers<Readonly<Record<string, unknown>>>()
		this.#promise = settlement.promise
		this.#resolve = settlement.resolve
		this.#reject = settlement.reject
		this.#messageHandler = this.#message.bind(this)
		this.#failHandler = this.#fail.bind(this)
		this.#exitHandler = this.#exit.bind(this)
		this.#thread.on('message', this.#messageHandler)
		this.#thread.once('messageerror', this.#failHandler)
		this.#thread.once('error', this.#failHandler)
		this.#thread.once('exit', this.#exitHandler)
	}

	get promise(): Promise<Readonly<Record<string, unknown>>> {
		return this.#promise
	}

	#message(value: unknown): void {
		if (this.#settled || !isRecord(value) || value.id !== this.#id) return
		this.#settled = true
		this.#detach()
		this.#resolve(Object.freeze({ ...value }))
	}

	#exit(code: number): void {
		this.#fail(new Error(`worker thread exited before replying (code ${String(code)})`))
	}

	#fail(error: Error): void {
		if (this.#settled) return
		this.#settled = true
		this.#detach()
		this.#reject(error)
	}

	#detach(): void {
		this.#thread.off('message', this.#messageHandler)
		this.#thread.off('messageerror', this.#failHandler)
		this.#thread.off('error', this.#failHandler)
		this.#thread.off('exit', this.#exitHandler)
	}
}
