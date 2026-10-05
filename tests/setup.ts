import type { QueueStoreInterface, StoredEntry } from '@orkestrel/queue'
import type { RecorderInterface } from '@orkestrel/test'
import type { WorkerOptions } from '@src/core'

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
