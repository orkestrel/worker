// Server-test setup — node-only helpers, loaded after `setup.ts` for the node
// `src:server` project. `node:fs` / `node:os` / `node:path` imports belong here,
// never in `setup.ts` (AGENTS §16.1).

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach } from 'vitest'

// A fresh on-disk JSON-store path under the OS temp dir, with a `cleanup` thunk
// that removes its directory. Used by the `createJSONQueueStore` tests, which need
// real file persistence across a store reopen. Call `cleanup` in `afterEach` so no
// temp file leaks (AGENTS §16.1).
export function tempDatabasePath(): { readonly path: string; readonly cleanup: () => void } {
	const directory = mkdtempSync(join(tmpdir(), 'worker-store-'))
	return {
		path: join(directory, 'store.json'),
		cleanup: () => rmSync(directory, { recursive: true, force: true }),
	}
}

// ── Teardown registrar (tracked-resource cleanup) ────────────────────────────
//
// AGENTS §16.1: the duplicated `const tracked = []` + `afterEach(dispose-all)` +
// `track(item)` trio every node-resource suite hand-rolls — the started-worker
// `destroy()` form and the temp-dir `cleanup()` form — folded into one registrar.
// The caller supplies the disposer; the registrar holds the tracked list AND wires
// its OWN `afterEach` to dispose every tracked item (awaiting async disposers), so
// no thread / temp-file leak across a suite. A real cleanup wiring, not a mock.

/** A tracked-resource teardown registrar — see {@link createTeardown}. */
export interface TeardownInterface<T> {
	/** Register `item` for disposal at `afterEach`, returning it for inline use. */
	track<U extends T>(item: U): U
}

/**
 * Create a {@link TeardownInterface} that disposes every tracked item after each test —
 * the one general form of the `tracked[]` + `afterEach` + `track` pattern the server
 * suites repeat (AGENTS §16.1). Call it at a suite's top level: it registers its OWN
 * `afterEach` immediately, draining the tracked list and running `dispose` on each item
 * (awaiting a returned promise), so a spawned worker thread is `terminate()`d and a
 * temp-dir `cleanup()`ed even when an assertion throws mid-test. The disposer is the
 * caller's (`(worker) => worker.destroy()` / `(cleanup) => cleanup()`), so the
 * registrar stays agnostic to what it tears down.
 *
 * @typeParam T - The kind of item tracked (the disposer's parameter type)
 * @param dispose - How to dispose one tracked item (may be async)
 * @returns A registrar whose `track` enrolls an item and returns it
 */
export function createTeardown<T>(
	dispose: (item: T) => void | Promise<void>,
): TeardownInterface<T> {
	const tracked: T[] = []
	afterEach(async () => {
		for (const item of tracked.splice(0)) await dispose(item)
	})
	return {
		track(item) {
			tracked.push(item)
			return item
		},
	}
}
