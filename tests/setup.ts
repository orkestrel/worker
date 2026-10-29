import type { QueueStoreInterface, StoredEntry } from '@orkestrel/queue'
import type { RecorderInterface, TeardownInterface } from '@orkestrel/test'
import type { WorkerOptions } from '@src/core'
import { waitForAbort } from '@orkestrel/test'

// ── Environment-agnostic base setup ───────────────────────────────────────────
//
// Loaded first by every test project (`vite.config.ts` `setupFiles[0]`). Holds ONLY
// helpers with no `node:*` / DOM dependency, so it is safe for `src:core` alike.
//
// The fleet-wide helpers live in `@orkestrel/test`. What remains here is what is
// specific to this package.

/** Configures optional protocol hooks for {@link TestQueueStore}. */
export interface TestQueueStoreHooks<TInput> {
	readonly save?: (entry: StoredEntry<TInput>) => Promise<void> | void
	readonly remove?: (id: string) => Promise<void> | void
	readonly clear?: () => Promise<void> | void
}

/**
 * Implements a protocol-faithful in-memory {@link QueueStoreInterface} with optional operation hooks.
 *
 * @remarks
 * The hooks expose external store timing and failures without reproducing Queue behavior.
 * Successful operations update one real in-memory record map using the same save / remove /
 * load / clear contract as a production store.
 *
 * @typeParam TInput - Input carried by each outstanding stored entry
 */
export class TestQueueStore<TInput> implements QueueStoreInterface<TInput> {
	readonly #hooks: TestQueueStoreHooks<TInput>
	readonly #entries = new Map<string, StoredEntry<TInput>>()

	constructor(hooks: TestQueueStoreHooks<TInput> = {}) {
		this.#hooks = hooks
	}

	async save(entry: StoredEntry<TInput>): Promise<void> {
		await this.#hooks.save?.(entry)
		this.#entries.set(entry.id, entry)
	}

	async remove(id: string): Promise<void> {
		await this.#hooks.remove?.(id)
		this.#entries.delete(id)
	}

	load(): Promise<ReadonlyArray<StoredEntry<TInput>>> {
		return Promise.resolve([...this.#entries.values()])
	}

	async clear(): Promise<void> {
		await this.#hooks.clear?.()
		this.#entries.clear()
	}
}

/** Represents the pool options accepted by a worker fixture. */
export type WorkerPoolOptions<T> = Omit<WorkerOptions<unknown, T, unknown>['pool'], 'capacity'>

/** Records each prototype property read against getter-backed pool options. */
export class PoolOptionsProbe<T> implements WorkerPoolOptions<T> {
	#values: Required<WorkerPoolOptions<T>>
	readonly #reads: RecorderInterface<readonly [property: keyof WorkerPoolOptions<T>]>

	constructor(
		values: Required<WorkerPoolOptions<T>>,
		reads: RecorderInterface<readonly [property: keyof WorkerPoolOptions<T>]>,
	) {
		this.#values = values
		this.#reads = reads
	}

	get max(): Required<WorkerPoolOptions<T>>['max'] {
		this.#reads.handler('max')
		return this.#values.max
	}

	get min(): Required<WorkerPoolOptions<T>>['min'] {
		this.#reads.handler('min')
		return this.#values.min
	}

	get restarts(): Required<WorkerPoolOptions<T>>['restarts'] {
		this.#reads.handler('restarts')
		return this.#values.restarts
	}

	get watch(): Required<WorkerPoolOptions<T>>['watch'] {
		this.#reads.handler('watch')
		return this.#values.watch
	}

	get on(): Required<WorkerPoolOptions<T>>['on'] {
		this.#reads.handler('on')
		return this.#values.on
	}

	get error(): Required<WorkerPoolOptions<T>>['error'] {
		this.#reads.handler('error')
		return this.#values.error
	}

	get create(): Required<WorkerPoolOptions<T>>['create'] {
		this.#reads.handler('create')
		return this.#values.create
	}

	get destroy(): Required<WorkerPoolOptions<T>>['destroy'] {
		this.#reads.handler('destroy')
		return this.#values.destroy
	}

	get validate(): Required<WorkerPoolOptions<T>>['validate'] {
		this.#reads.handler('validate')
		return this.#values.validate
	}

	replace(values: Required<WorkerPoolOptions<T>>): void {
		this.#values = values
	}
}
/** Records mutable fixture state without sharing it between test calls. */
export class WorkerValue<T> {
	#value: T

	constructor(value: T) {
		this.#value = value
	}

	get value(): T {
		return this.#value
	}

	set value(value: T) {
		this.#value = value
	}
}

/** Records simultaneous handler entries and their historical maximum. */
export class WorkerActivity {
	#active = 0
	#peak = 0

	get peak(): number {
		return this.#peak
	}

	enter(): void {
		this.#active += 1
		this.#peak = Math.max(this.#peak, this.#active)
	}

	leave(): void {
		this.#active -= 1
	}
}

/** Represents a worker whose disposal can be registered by a test. */
export interface DestroyableWorker {
	readonly emitter: { readonly destroyed: boolean }
	destroy(): Promise<void>
}

/** Registers worker cleanup while preserving the caller's worker type. */
export function trackWorker<T extends DestroyableWorker>(
	teardown: TeardownInterface,
	worker: T,
): T {
	teardown.add(() => (worker.emitter.destroyed ? undefined : worker.destroy()))
	return worker
}

/** Builds inert worker options for runtime option validation. */
export function buildWorkerOptions(): WorkerOptions<undefined, number, void> {
	return { pool: { create: () => 0 }, handler: () => {} }
}

/** Builds a pool getter that records and throws when validation reaches it. */
export function buildHostilePoolOptions(
	reads: RecorderInterface<[]>,
	error: Error,
): WorkerOptions<undefined, number, void> {
	return {
		concurrency: 0,
		handler: () => {},
		get pool(): WorkerPoolOptions<number> {
			reads.handler()
			throw error
		},
	}
}

/** Builds a handler that waits on the gate selected by its input. */
export function buildGatedWorkerOptions(
	pool: WorkerPoolOptions<number>,
	first: Promise<void>,
	second: Promise<void>,
): WorkerOptions<number, number, number> {
	return {
		pool,
		handler: async (input) => {
			if (input === 1) await first
			else await second
			return input
		},
	}
}

/** Builds pool options with an own enumerable changing maximum getter. */
export function buildVolatilePool(
	create: () => number,
	reads: RecorderInterface<[string]>,
): WorkerPoolOptions<number> {
	const count = new WorkerValue(0)
	return {
		create,
		get max() {
			count.value += 1
			reads.handler('max')
			return count.value === 1 ? 1 : 0
		},
	}
}

/** Records successive reads of each volatile worker option. */
export class WorkerOptionReads {
	readonly #counts = new Map<string, number>()
	readonly #reads: RecorderInterface<[string]>

	constructor(reads: RecorderInterface<[string]>) {
		this.#reads = reads
	}

	read(name: string): boolean {
		const count = (this.#counts.get(name) ?? 0) + 1
		this.#counts.set(name, count)
		this.#reads.handler(name)
		return count === 1
	}
}

/** Builds own enumerable option getters that switch after their first read. */
export function buildVolatileWorkerOptions(
	initial: Required<WorkerOptions<number, number, number>>,
	later: Required<WorkerOptions<number, number, number>>,
	reads: RecorderInterface<[string]>,
): WorkerOptions<number, number, number> {
	const access = new WorkerOptionReads(reads)
	return {
		get concurrency() {
			return access.read('concurrency') ? initial.concurrency : later.concurrency
		},
		get handler() {
			return access.read('handler') ? initial.handler : later.handler
		},
		get on() {
			return access.read('on') ? initial.on : later.on
		},
		get error() {
			return access.read('error') ? initial.error : later.error
		},
		get retries() {
			return access.read('retries') ? initial.retries : later.retries
		},
		get timeout() {
			return access.read('timeout') ? initial.timeout : later.timeout
		},
		get store() {
			return access.read('store') ? initial.store : later.store
		},
		get pool() {
			return access.read('pool') ? initial.pool : later.pool
		},
	}
}

/** Builds complete pool values for the getter identity proof. */
export function buildPoolProbeValues(): Required<WorkerPoolOptions<number>> {
	return {
		max: 3,
		min: 3,
		restarts: 2,
		watch: (_value, signal) => waitForAbort(signal),
		on: {},
		error: (): void => undefined,
		create: (): number => 7,
		destroy: (): void => undefined,
		validate: (): boolean => true,
	}
}

/** Names the lifecycle events observed by the worker facade proofs. */
export const WORKER_EVENTS: readonly [
	'enqueue',
	'start',
	'retry',
	'success',
	'failure',
	'abort',
	'drain',
] = ['enqueue', 'start', 'retry', 'success', 'failure', 'abort', 'drain']

/** Selects the lifecycle names observed by worker facade recorders. */
export type WorkerEvent = (typeof WORKER_EVENTS)[number]
