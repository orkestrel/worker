import type { EmitterInterface, EventMap } from '@orkestrel/emitter'
import type { PoolOptions } from '@orkestrel/pool'
import type { QueueStoreInterface, StoredEntry } from '@orkestrel/queue'
import type { RecorderInterface } from '@orkestrel/test'
import { createRecorder } from '@orkestrel/test'

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

/** A recorder per named event of an {@link EmitterInterface}, keyed by event name. */
export type EmitterRecorders<TMap extends EventMap, TName extends keyof TMap> = {
	readonly [K in TName]: RecorderInterface<TMap[K]>
}

/**
 * Wire one {@link createRecorder} onto `emitter` for each of the named events — the
 * one generic form of the per-entity `recordXEvents` bundles (AGENTS §16.1). Each
 * recorder subscribes via `emitter.on(name, recorder.handler)` and is returned keyed
 * by its event name, typed with that event's argument tuple — so a test asserts what
 * fired (`events.write.calls`) and with which payload, exactly as the local bundles did.
 *
 * @typeParam TMap - The emitter's {@link EventMap}
 * @typeParam TName - The subset of event names to record (inferred from `events`)
 * @param emitter - The emitter to subscribe the recorders to
 * @param events - The event names to record (each becomes a key of the result)
 * @returns A recorder per name, each subscribed and keyed by event name
 */
export function recordEmitterEvents<TMap extends EventMap, TName extends keyof TMap>(
	emitter: EmitterInterface<TMap>,
	events: readonly TName[],
): EmitterRecorders<TMap, TName> {
	// Accumulate into a `Partial` of the exact mapped shape — every value keeps its
	// precise per-event tuple type (a recorder is invariant in its argument tuple, so a
	// widened record won't hold it), all keys optional until assigned. Each recorder is
	// created against its event's tuple, so `on(name, handler)` is precisely typed as it
	// is wired. The dynamic key list is the untyped edge: once every listed name is
	// present we narrow `Partial` → total through a guard, never an assertion (§14).
	const recorders: Partial<EmitterRecorders<TMap, TName>> = {}
	for (const name of events) {
		const recorder = createRecorder<TMap[typeof name]>()
		emitter.on(name, recorder.handler)
		recorders[name] = recorder
	}
	if (!isTotal(recorders, events)) {
		throw new Error('recordEmitterEvents: a recorder was not wired for every event')
	}
	return recorders
}

/**
 * Narrow an accumulated `Partial<EmitterRecorders>` to its total mapped form once every
 * listed event has a recorder present — the §14 guard standing in for an assertion in
 * {@link recordEmitterEvents} (whose loop assigns one recorder per name, so this holds;
 * the explicit per-name presence check keeps the narrowing a sound guard, not a cast).
 *
 * @typeParam TMap - The emitter's {@link EventMap}
 * @typeParam TName - The subset of event names that must each have a recorder
 * @param recorders - The partially-accumulated recorder map to narrow
 * @param events - The event names that must all be present for the map to be total
 * @returns Whether every listed event has a recorder (narrowing `recorders` to total)
 */
export function isTotal<TMap extends EventMap, TName extends keyof TMap>(
	recorders: Partial<EmitterRecorders<TMap, TName>>,
	events: readonly TName[],
): recorders is EmitterRecorders<TMap, TName> {
	return events.every((name) => recorders[name] !== undefined)
}

// ── Pool resource factory (Pool-domain fixture) ───────────────────────────────

/**
 * A {@link createResourceFactory} fixture — a `create` hook that hands out
 * monotonically-numbered resources, plus recorders of every value created / destroyed.
 */
export interface ResourceFactoryInterface {
	/** Hands out the next monotonically-increasing resource (0, 1, 2, …). */
	readonly create: () => number
	/** Records every value `create` handed out, in order. */
	readonly created: RecorderInterface<[number]>
	/** Records every value passed to a pool's `destroy` hook wired against this factory. */
	readonly destroyed: RecorderInterface<[number]>
}

/**
 * Create a {@link ResourceFactoryInterface} — the shared `Pool<number>` resource fixture
 * (AGENTS §16.1): `create` hands out fresh monotonically-numbered resources (no mocks),
 * and `created` records exactly which values were made so a test can assert the count
 * without hand-rolling a counter closure per file. `destroyed` is exposed for symmetry —
 * a test wires it as the pool's `destroy` hook (`destroy: (value) => destroyed.handler(value)`)
 * when it needs to assert teardown; a test that ignores destruction simply never wires it.
 *
 * @returns A resource factory whose `create` is a real, recorded pool `create` hook
 */
export function createResourceFactory(): ResourceFactoryInterface {
	let next = 0
	const created = createRecorder<[number]>()
	const destroyed = createRecorder<[number]>()
	return {
		create: () => {
			const value = next
			next += 1
			created.handler(value)
			return value
		},
		created,
		destroyed,
	}
}
