# Queue

> A concurrent, cooperative FIFO job queue. `Queue` runs enqueued inputs through a handler
> under bounded concurrency, with retries and a per-attempt timeout / abort; each `enqueue`
> returns a promise that settles with the job's result.
>
> The word "cooperative" is load-bearing. An idle worker loop does not busy-poll or run a
> timer — it **parks** on a wake list, and `enqueue` / `resume` wake exactly one (or all)
> parked loops, so an idle queue burns zero CPU. Cancellation is built on the L1 [abort](abort.md)
>
> - [timeout](timeout.md) primitives: each attempt's `signal` fires on a queue-level abort,
>   the entry's own signal, or the per-attempt deadline, and the handler is _raced_ against it
>   — so an attempt that ignores its `signal` still fails when the clock runs out.
>
> Durability is opt-in and outstanding-only. A `QueueStoreInterface` mirrors just the jobs
> that have not yet settled — saved on accept, removed on settle — so a graceful shutdown
> empties the store and a crash leaves exactly the unfinished rows. Pass a `store` to
> `createQueue`, and after a restart a fresh queue over the same store `restore()`s precisely
> that unfinished work. `DatabaseQueueStore` is the durable engine over the
> [database](database.md) layer (a queue's durable state is just a table), driver-pluggable
> across memory / JSON / SQLite; `MemoryQueueStore` is the zero-plumbing in-process default.
>
> `Queue` is **observable**: it exposes a typed `emitter` (AGENTS §13) carrying its lifecycle
> moments for fire-and-forget observers — logging, metrics, tracing (see [Observing](#observing)).
> Observation is a pure side-channel: every event fires _after_ the relevant transition and a
> throwing listener is isolated, so a buggy observer can never reorder or corrupt the engine.
>
> It is deliberately **de-bloated** — the cuts are the design. No scheduler, no priorities,
> no delay / progress / message channels (use `concurrency: 1` for strict ordering). What
> ships is the cooperative loop and the outstanding-only store — nothing speculative.
>
> Source: [`src/core`](../../src/core).

## Surface

Create a queue over a handler, then `enqueue` inputs and await their results:

```ts
import { createQueue } from '@src/core'

const queue = createQueue<string, number>({
	handler: async (url, { signal }) => (await fetch(url, { signal })).status,
	concurrency: 4, // up to four in flight at once (default 1 = ordered)
	retries: 2, // two extra attempts on failure
	timeout: 5_000, // each attempt is bounded to 5s
})

const status = await queue.enqueue('https://example.com')
```

`enqueue` appends an entry (FIFO) and hands back a promise that settles when the entry finally completes, exhausts its retries, or is rejected by an abort — one promise per job, so the call site reads like a plain async call regardless of how many run concurrently. The `count` (pending + active), `active` (in-flight, never above `concurrency`), `paused`, and `stopped` members report live state, and the lifecycle methods (`start` / `stop` / `pause` / `resume` / `abort` / `clear` / `destroy`) follow AGENTS §10. The handler always receives a per-attempt `execution` — a stable `id` (the idempotency key, equal across every retry) and a `signal` to honour for prompt cancellation.

### Factories

| API                        | Kind     | Summary                                                                                             |
| -------------------------- | -------- | --------------------------------------------------------------------------------------------------- |
| `createQueue`              | function | Create a `QueueInterface` over a handler, with optional concurrency / retries / timeout.            |
| `createDatabaseQueueStore` | function | Create a `QueueStoreInterface` over any `DriverInterface` (memory / JSON / SQLite).                 |
| `createMemoryQueueStore`   | function | Create the zero-plumbing in-memory `QueueStoreInterface` — a `MemoryQueueStore` over a plain `Map`. |

### Entities

| API                  | Kind  | Summary                                                                                   |
| -------------------- | ----- | ----------------------------------------------------------------------------------------- |
| `Queue`              | class | The cooperative concurrent job engine — wake-park loop, retries, timeout, abort.          |
| `MemoryQueueStore`   | class | The zero-plumbing DEFAULT store for outstanding entries — a plain process-lifetime `Map`. |
| `DatabaseQueueStore` | class | The opt-in durable store for outstanding entries over one `database` table (driver-swap). |

### Types

| Type                  | Kind      | Shape                                                                                                                                              |
| --------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueueExecution`      | interface | The per-attempt handle a handler receives — a stable `id` (idempotency key) + a `signal` (timeout / abort / entry signal).                         |
| `QueueHandler`        | type      | `(input, execution) => Promise<TResult> \| TResult` — runs one entry's work; may reject to retry.                                                  |
| `QueueEntryOptions`   | interface | Per-entry `enqueue` options — `id?` / `retries?` / `timeout?` / `signal?`.                                                                         |
| `QueueOptions`        | interface | `createQueue` options — `handler` + `concurrency?` / `retries?` / `timeout?` / `store?` / `on?` / `error?`.                                        |
| `QueueInterface`      | interface | `emitter` / `count` / `active` / `paused` / `stopped` data members + the lifecycle + `enqueue` / `restore` methods.                                |
| `QueueEventMap`       | type      | The `Queue`'s observable events — `enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`.                                       |
| `QueueEntry`          | interface | One queued unit of work + the `enqueue` promise resolvers (`id` / `input` / `options` / `attempts`) — the `Queue` engine's internal pending entry. |
| `StoredEntry`         | interface | A durably persisted, outstanding entry — `id` / `input` / `attempts` (its `readonly` data members).                                                |
| `QueueStoreInterface` | interface | Durable backing for outstanding entries — `save` / `remove` / `load` / `clear` methods.                                                            |

The `emitter` / `count` / `active` / `paused` / `stopped` members of `QueueInterface` are `readonly` data members (Surface rows, above) — `emitter` is the typed push observation surface (see [Observing](#observing)); their call-signature methods are documented under [Methods](#methods).

## Methods

The public methods of `QueueInterface` and `QueueStoreInterface` — every call-signature member listed (their `readonly` data members stay Surface rows). Each class (`Queue`, and both store classes `MemoryQueueStore` / `DatabaseQueueStore`) implements its interface exactly, so this doubles as each class's instance-method surface (AGENTS §22).

#### `QueueInterface`

`enqueue` submits work; the rest are the §10 lifecycle verbs. `start` / `stop` begin / end the worker loops; `pause` / `resume` suspend / continue dequeuing; `abort` cancels in-flight work and rejects pending; `clear` drops pending only; `destroy` tears the queue down.

| Method    | Returns            | Behavior                                                                                                             |
| --------- | ------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `enqueue` | `Promise<TResult>` | Append an input (FIFO) and wake a worker; the promise settles when the entry finally settles.                        |
| `restore` | `Promise<void>`    | Re-enqueue the store's outstanding entries (at their persisted attempt count) to re-run them; no-op without a store. |
| `start`   | `void`             | (Re)spawn the worker loops up to `concurrency`; resumes processing after a `stop` (no-op once aborted).              |
| `stop`    | `void`             | End the worker loops permanently and reject pending; in-flight entries settle on their own.                          |
| `pause`   | `void`             | Suspend dequeuing — workers park until `resume`; in-flight entries keep running.                                     |
| `resume`  | `void`             | Continue a paused queue — wake the parked workers.                                                                   |
| `abort`   | `void`             | Fire the queue signal (cancelling in-flight attempt signals) and reject pending; never retried.                      |
| `clear`   | `void`             | Drop every pending entry (rejected as cleared); in-flight entries are untouched.                                     |
| `destroy` | `void`             | Tear the queue down — `abort` then `stop`, releasing resources; idempotent.                                          |

#### `QueueStoreInterface`

The durable backing for a queue's outstanding entries. `save` upserts an entry by its `id`; `remove` drops a finished one; `load` returns everything still outstanding (the work to resume after a restart); `clear` empties the store. Two classes implement it, the dual-store convention: `MemoryQueueStore` is the zero-plumbing DEFAULT over a plain `Map` (no encoding — a queue entry is already pure JSON), and `DatabaseQueueStore` is the opt-in durable backing over an injected `TableInterface`, whose reads narrow through the table's contract so `load` returns typed `StoredEntry`s with no cast (a driver-swap across memory / JSON / SQLite). Both expose exactly these four methods.

| Method   | Returns                              | Behavior                                                                     |
| -------- | ------------------------------------ | ---------------------------------------------------------------------------- |
| `save`   | `Promise<void>`                      | Upsert an entry by its `id` (a re-`save` of an existing `id` overwrites it). |
| `remove` | `Promise<void>`                      | Drop a finished entry by `id`; an absent `id` is a no-op (no throw).         |
| `load`   | `Promise<readonly StoredEntry<…>[]>` | Return every outstanding entry — the work to resume after a restart.         |
| `clear`  | `Promise<void>`                      | Empty the store — drop every outstanding entry.                              |

## Contract

These invariants hold across `src/core` ↔ `queue.md`:

1. **DOC ↔ SOURCE bijection.** Every `function` / `class` / `interface` / `type` row in the `## Surface` tables is a real export of `src/core`, and every export appears as a Surface row — exhaustive, both directions (AGENTS §22).
2. **Cooperative wake-park loop.** Up to `concurrency` worker loops run; each takes the next pending entry or — when none is ready (queue empty or paused) — PARKS by awaiting a fresh promise whose resolver is held in a wake list. `enqueue` wakes exactly one parked worker; `resume` wakes all. An idle queue therefore consumes no CPU: no busy-poll, no `setInterval`, no recursive microtask.
3. **FIFO + bounded concurrency.** Entries run in enqueue order; at most `concurrency` (default `1` — strictly ordered) are in flight at once. `count` is pending + active; `active` is the in-flight total and never exceeds `concurrency`.
4. **Per-attempt timeout + cancellation, over L1.** Each attempt builds its `signal` by combining the queue-level abort, the entry's own `signal`, and a fresh [`Timeout`](timeout.md) (when a timeout applies). The handler is RACED against that signal, so an attempt that ignores its `signal` still fails on the deadline. A queue / entry abort CLEARS the deadline (it never expires) — the L1 parent-linking contract.
5. **Retries, but abort never retries.** A failed attempt (a handler rejection or a per-attempt timeout) retries while attempts remain — `retries` + 1 total. A queue-level `abort`, or the entry's own `signal` firing, rejects the entry immediately with no further attempt. A handler rejection that is not an `Error` is coerced to one (`new Error(String(value), { cause: value })`) — so the entry always rejects with an `Error`, with the original thrown value preserved on `.cause`.
6. **Lifecycle (§10).** `pause` parks workers (resumable); `resume` wakes them; `stop` ends the loops permanently and rejects pending while in-flight settle; `abort` additionally fires the queue signal so in-flight attempts observe cancellation; `clear` drops only pending; `destroy` sets the destroyed flag, then `abort`s and `stop`s, idempotently. `stopped` reads `true` after `stop`, `abort`, or `destroy`. Enqueuing onto a stopped / aborted / destroyed queue rejects.
7. **Observable + de-bloated.** `Queue` owns a typed `Emitter` (AGENTS §13) exposed as `readonly emitter` and accepts the reserved `on?` initial-listeners hook plus the `error?` listener-error handler: `QueueEventMap<TResult>` (`enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`). **Emitting is observation-only** — the emitter isolates a listener throw (routing it to the `error` handler, never a domain event) and every event sits strictly AFTER the relevant wake / park / settle transition, so a buggy observer can NEVER reorder, throw into, or corrupt the cooperative wake-park engine — `active` stays balanced and no parked worker is stranded regardless of what a listener does. Still deliberately CUT: scheduling / delay / activation / expiration, priority ordering, an explicit `sequential` flag (use `concurrency: 1`), bail, and the progress / message channels. Durability (an optional `store`) IS wired into the queue (clause 9).
8. **DOC ↔ SOURCE method bijection.** The `## Methods` tables list exactly the public methods of `QueueInterface` / `QueueStoreInterface` — exhaustive, both directions — and `Queue` / `MemoryQueueStore` / `DatabaseQueueStore` each expose the same public methods as their interface, no more (AGENTS §22).
9. **Persistence holds outstanding-only — the dual-store convention.** A `QueueStoreInterface` durably backs a queue's still-outstanding entries: `save` upserts by `id`, `remove` drops a completed one, `load` returns everything outstanding (so a restart resumes exactly the unfinished work), `clear` empties it. TWO classes implement it: `MemoryQueueStore` is the zero-plumbing DEFAULT over a plain process-lifetime `Map` (no `database` table, no driver codec — a queue entry is already pure self-contained JSON), built by `createMemoryQueueStore`; `DatabaseQueueStore` is the opt-in durable backing — a minimal interface over the [database](database.md) layer (a queue's durable state IS a table), wrapping one `TableInterface` so any `DriverInterface` backs it. Its reads narrow through the table's contract, so its `load` returns typed `StoredEntry<TInput>`s with no cast; an entry's `input` must be JSON-serializable to survive a JSON / SQLite driver. The `Queue` consumes whichever store (clause 10); the store itself stays **event-free** (only the `Queue` engine is observable — clause 7).
10. **Durability is wired into the `Queue` (outstanding-only, restartable).** Given a `store`, the queue mirrors only OUTSTANDING work. `enqueue` `save`s the entry (at `attempts: 0`) BEFORE it becomes runnable, so accepting work is durable: a failed initial `save` PROPAGATES (rejects the `enqueue`). As an entry's attempt count climbs across retries, the queue re-`save`s the new count — best-effort (in-memory state is authoritative, so a failed per-attempt `save` is swallowed and the entry runs on). The moment an entry settles — success, terminal failure, or an abort rejection — the queue `remove`s its row first, then settles; a lifecycle drain (`clear` / `stop` / `abort` / `destroy`) likewise `remove`s each pending entry it rejects (fire-and-forget). So a graceful shutdown empties the store and a crash leaves exactly the unfinished rows. `restore()` `load`s those rows and re-enqueues each at its persisted attempt count, honouring the remaining retry budget; it is a no-op without a store or on a halted queue (it also re-checks the halt flags after the `load()` await, so a queue aborted / destroyed mid-`load` launches nothing). `restore()` is **idempotent and safe on a live queue**: the queue tracks the ids of its live (pending or in-flight) entries and `restore` SKIPS any id already live, so calling it on a non-idle queue (or calling it twice) never double-launches an entry that is still running — it re-launches only genuinely-outstanding-but-not-live rows. Restored entries use the queue-default `retries` / `timeout` and carry NO per-entry signal (per-entry options are not persisted) — the documented limitation. Persistence is **at-least-once** (a settle's `remove` can fail, or a crash can land between `save` and `remove`), so a re-run entry's handler should be **idempotent** — de-dup against `execution.id` (the entry's stable id, equal across every attempt and across a crash-replay, so it is the idempotency key for the at-least-once replay window). The no-store path is unaffected — it stays fully synchronous (an entry is queued the instant `enqueue` returns), and its observable events (clause 7) fire identically.

## Persistence

A `QueueStoreInterface` is the durable backing for a queue's **outstanding** entries — the work that has not yet completed. It is deliberately a small, four-method surface (`save` / `remove` / `load` / `clear`) over the [database](database.md) layer: a queue's durable state is just a table of `StoredEntry`s (an `id`, the handler's `input`, and the `attempts` so far), so persistence reduces to keyed CRUD. `DatabaseQueueStore` is the one engine; the backend is whichever `DriverInterface` you build it over, so the SAME store runs in memory or against a persistent driver without changing its code — the durability is the driver's job.

```ts
import { stringShape } from '@orkestrel/contract'
import { createMemoryQueueStore } from '@src/core'

// An ephemeral, memory-backed store (tests, a non-durable queue):
const store = createMemoryQueueStore(stringShape())
await store.save({ id: 'job-1', input: 'https://example.com', attempts: 0 })
await store.save({ id: 'job-1', input: 'https://example.com', attempts: 1 }) // upsert by id
const outstanding = await store.load() // readonly StoredEntry<string>[] — typed, no cast
await store.remove('job-1') // a finished entry leaves the store
await store.clear() // empty the store — drop every outstanding entry
```

`createDatabaseQueueStore(input, driver)` builds the durable form — pass any `DriverInterface` for a persistent store. The `input` shape is used directly as the entry's `input` column, so the stored payload is validated and typed by it; it must be JSON-serializable to survive a JSON / SQLite driver. The store holds only outstanding entries, so `load` on startup yields precisely the work to resume.

### Wiring a store into a queue

Pass a `store` to `createQueue` and the queue mirrors its outstanding entries automatically — `save` on accept (and as the attempt count climbs), `remove` on settle or drain. After a restart, build a fresh queue over the same store and call `restore()` to re-run the unfinished work:

```ts
import { stringShape } from '@orkestrel/contract'
import { createMemoryDriver } from '@orkestrel/database'
import { createDatabaseQueueStore, createQueue } from '@src/core'

const store = createDatabaseQueueStore(stringShape(), createMemoryDriver())
const queue = createQueue<string, number>({ store, handler: (url) => fetchStatus(url) })
await queue.enqueue('https://example.com') // durably saved before it runs

// …after a crash + restart, a fresh queue over the same store resumes the work:
const resumed = createQueue<string, number>({ store, handler: (url) => fetchStatus(url) })
await resumed.restore() // re-enqueues every still-outstanding entry, then runs it
```

Accepting work durably is NOT best-effort: a failed initial `save` rejects the `enqueue`. Per-attempt persistence IS best-effort — in-memory state is authoritative, so the entry runs on even if a climbing-count `save` fails. A graceful shutdown empties the store; a crash leaves the unfinished rows. Persistence is at-least-once (a settle's `remove`, or a crash between `save` and `remove`, can leave a stale row), so a re-run handler should be idempotent. Restored entries use the queue-default `retries` / `timeout` and carry no per-entry signal — per-entry options are not persisted. The no-store path is unchanged: a queue without a `store` stays fully synchronous and writes nothing.

## Observing

`Queue` exposes a typed `emitter` (AGENTS §13) carrying its lifecycle moments for fire-and-forget observers — logging, metrics, tracing. Subscribe via `queue.emitter.on(...)`, or wire initial listeners through the reserved `on?` option; supply an `error?` handler to receive a listener's throw. **Emitting is observation-only**: every event fires strictly AFTER the relevant wake / park / settle transition, so a listener can never change what the engine does — and a throwing listener can never corrupt it (see the safety guarantee below).

```ts
import { createQueue } from '@src/core'

const queue = createQueue<string, number>({
	handler: (url) => fetchStatus(url),
	on: { drain: () => console.log('queue idle') }, // initial listener at construction
})

queue.emitter.on('success', (id, status) => metrics.record(id, status))
queue.emitter.on('failure', (id, error) => log.warn(`job ${id} failed`, error))
```

| Event map                | Events                                                                                                                          |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `QueueEventMap<TResult>` | `enqueue(id)` · `start(id)` · `retry(id, attempt)` · `success(id, result)` · `failure(id, error)` · `abort(reason)` · `drain()` |

`enqueue` fires when an entry is accepted (after a durable `save`, when a `store` is set); `start` when an attempt begins; `retry` on each re-attempt (the 1-based attempt number); `success` / `failure` when an entry settles (after the durable row is removed); `abort` on a queue-level abort; `drain` when the queue goes idle.

**The listener-isolation safety guarantee.** A listener throw is NEVER allowed to escape into the engine: the emitter isolates it and routes it to its OWN `error` handler (the `error` option, surfaced as `(error, event)`), NOT to a domain event — so a buggy observer is isolated yet not silently lost. The `error` handler runs in its own try/catch, so even a throwing handler can't recurse or escape; with no handler, the throw is swallowed silently. Every throwing listener surfaces (not just the first). Because every emit sits after the wake / park / settle transition AND is isolated, a buggy observer **cannot corrupt the cooperative wake-park loop**: `active` stays balanced, the queue still drains, and no parked worker is ever stranded.

## Patterns

### Create a queue

```ts
import { createQueue } from '@src/core'

// Ordered (concurrency defaults to 1): each entry runs to completion before the next.
const queue = createQueue<Job, Output>({ handler: (job) => run(job) })

const output = await queue.enqueue(job)
```

### Bounded concurrency

```ts
// Up to 5 in flight at once; a sixth enqueue waits for a slot to free up.
const pool = createQueue<string, Response>({
	concurrency: 5,
	handler: (url, { signal }) => fetch(url, { signal }),
})

const responses = await Promise.all(urls.map((url) => pool.enqueue(url)))
```

### Retries

```ts
// Three extra attempts; the handler is re-run on each rejection until one succeeds.
const queue = createQueue<Job, Output>({ retries: 3, handler: (job) => flaky(job) })

// A per-entry override wins over the queue default.
await queue.enqueue(job, { retries: 0 }) // this one does not retry
```

### Per-attempt timeout

```ts
const queue = createQueue<Job, Output>({
	timeout: 2_000, // each attempt is bounded to 2s; a timeout counts as a failed attempt
	retries: 1,
	handler: (job, { signal }) => run(job, signal), // honour `signal` to stop early
})
```

A handler should observe its `execution.signal` to abandon work early; even if it ignores the signal, the queue stops waiting once the deadline fires and treats the attempt as failed.

### Abort

```ts
const queue = createQueue<Job, Output>({ handler: (job, { signal }) => run(job, signal) })
const pending = queue.enqueue(job)

queue.abort(new Error('shutting down'))
// `pending` rejects; an in-flight handler's `signal` fires; nothing is retried.
await pending.catch((error) => report(error))
```

### Lifecycle

```ts
queue.pause() // suspend dequeuing — workers park until resume
queue.resume() // wake the parked workers, continuing where it left off

queue.clear() // drop every pending entry (rejected); in-flight entries are untouched

queue.stop() // end the worker loops permanently; in-flight entries settle on their own
queue.start() // (re)spawn the worker loops, resuming after a plain stop

queue.destroy() // abort then stop, releasing resources; idempotent
```

### Practices

- **Honour `execution.signal`** — pass it to `fetch` / child aborts and bail out when it fires, so timeouts and aborts actually stop work rather than just abandoning its result.
- **`concurrency: 1` for ordering** — there is no separate `sequential` flag; a concurrency of one is the ordered, one-at-a-time mode.
- **Per-entry overrides** — `enqueue(input, { retries, timeout, signal })` overrides the queue defaults for that one entry.
- **`abort` is terminal** — a queue-level abort cancels in-flight work and stops the queue; create a new queue to start over (`start` resumes only after a plain `stop`).
- **`clear` vs `stop` vs `abort`** — `clear` drops pending and keeps running; `stop` ends the loops but lets in-flight finish; `abort` also cancels in-flight.
- **Observe, don't drive** — subscribe to `queue.emitter` for lifecycle moments (see [Observing](#observing)); emitting is a pure side-channel, so a listener never changes what the engine does (and a throwing one can't corrupt it).

## Tests

- [`tests/guides/src/parity.test.ts`](../../tests/guides/src/parity.test.ts) — the `## Surface` ↔ `src/core` bijection (value + type exports) and the `QueueInterface` / `QueueStoreInterface` ↔ `Queue` / `MemoryQueueStore` / `DatabaseQueueStore` method bijection.
- [`tests/src/core/Queue.test.ts`](../../tests/src/core/Queue.test.ts) — `enqueue` + FIFO at concurrency 1, the concurrency cap (`active` never exceeds the limit, surplus waits), retries (succeed-after-N and exhaust), the per-attempt timeout (the signal fires and the attempt fails), `abort` (rejects pending + fires in-flight signal, no retries), `pause` / `resume`, `clear` (drops pending, in-flight untouched), `stop` / `destroy`, the wake-park idle check (no handler call while idle), and durability over a real memory-backed store: persist-on-accept + remove-on-settle (success and exhausted-retries), the climbing attempt count persisted mid-flight, `restore()` re-running a prior queue's outstanding entries (a shared-store restart sim) at their persisted attempt count, a restored always-throwing entry settling without an unhandled rejection, lifecycle drains (`clear` / `abort`) removing the drained rows while an in-flight row is removed on its own settle, and the save-failure contract (a failed initial `save` rejects the `enqueue`; a failed per-attempt `save` is best-effort and the entry runs on).
- [`tests/src/core/factories.test.ts`](../../tests/src/core/factories.test.ts) — `createQueue` returns a working, typed instance end to end and honours its options + status surface, and `createDatabaseQueueStore` / `createMemoryQueueStore` round-trip a stored entry.
- [`tests/src/core/stores/MemoryQueueStore.test.ts`](../../tests/src/core/stores/MemoryQueueStore.test.ts) — the plain-`Map` DEFAULT store, driven directly over real inline `StoredEntry`s (no mocks): a `save` → `load` round-trip by value, `save` upserts by id (no duplicate), `remove` drops one (absent is a no-op), `load` returns EVERY outstanding entry (the bulk-restore semantic), `clear` empties it.
- [`tests/src/core/stores/DatabaseQueueStore.test.ts`](../../tests/src/core/stores/DatabaseQueueStore.test.ts) — over a memory-backed driver store: a `save` → `load` round-trip (value + typed `input`, including nested-object payloads), `save` upserts by id (no duplicate), `remove` drops one (absent is a no-op), `load` returns all outstanding in key order, `clear` empties it, plus scale (200 entries), upsert churn on one id, and complex / edge-value inputs (nested arrays, booleans, nullables, optionals).

## See also

- [`abort.md`](abort.md) — the cancellation primitive each attempt's `signal` is built on (a queue / entry abort).
- [`timeout.md`](timeout.md) — the deadline primitive backing the per-attempt timeout (a parent abort clears it).
- [`database.md`](database.md) — the storage layer the `QueueStoreInterface` persists over (a queue's durable state is just a table); the drivers a store can be built on.
- [`contract.md`](contract.md) — the guard / shape primitives `createDatabaseQueueStore` / `createMemoryQueueStore` are typed by.
- [`AGENTS.md`](../../AGENTS.md) — the rules; §10 lifecycle, §4.1 single-word members, §13 emitter pattern, §22 documentation-as-contracts.
- [`README.md`](../README.md) — the guides index.
