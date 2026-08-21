import type { PoolOptions } from '@orkestrel/pool'
import type { QueueStoreInterface, StoredEntry } from '@orkestrel/queue'
import type { RecorderInterface } from '@orkestrel/test'

// ── Environment-agnostic base setup (AGENTS §16.1) ────────────────────────────
//
// Loaded first by every test project (`vite.config.ts` `setupFiles[0]`). Holds ONLY
// helpers with no `node:*` / DOM dependency, so it is safe for `src:core` alike.
//
// The fleet-wide helpers live in `@orkestrel/test`. What remains here is what is
// specific to this package.

/** Optional protocol hooks for {@link TestQueueStore}. */
export interface TestQueueStoreHooks<TInput> {
	readonly save?: (entry: StoredEntry<TInput>) => Promise<void> | void
	readonly remove?: (id: string) => Promise<void> | void
	readonly clear?: () => Promise<void> | void
}

/**
 * A protocol-faithful in-memory {@link QueueStoreInterface} with optional operation hooks.
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

/** Getter-backed pool options whose prototype property reads are recorded. */
export class PoolOptionsProbe<T> implements PoolOptions<T> {
	#values: Required<PoolOptions<T>>
	readonly #reads: RecorderInterface<readonly [property: keyof PoolOptions<T>]>

	constructor(
		values: Required<PoolOptions<T>>,
		reads: RecorderInterface<readonly [property: keyof PoolOptions<T>]>,
	) {
		this.#values = values
		this.#reads = reads
	}

	get max(): Required<PoolOptions<T>>['max'] {
		this.#reads.handler('max')
		return this.#values.max
	}

	get on(): Required<PoolOptions<T>>['on'] {
		this.#reads.handler('on')
		return this.#values.on
	}

	get error(): Required<PoolOptions<T>>['error'] {
		this.#reads.handler('error')
		return this.#values.error
	}

	get create(): Required<PoolOptions<T>>['create'] {
		this.#reads.handler('create')
		return this.#values.create
	}

	get destroy(): Required<PoolOptions<T>>['destroy'] {
		this.#reads.handler('destroy')
		return this.#values.destroy
	}

	get validate(): Required<PoolOptions<T>>['validate'] {
		this.#reads.handler('validate')
		return this.#values.validate
	}

	replace(values: Required<PoolOptions<T>>): void {
		this.#values = values
	}
}
