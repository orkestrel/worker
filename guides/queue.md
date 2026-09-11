# Queue

> A concurrent, cooperative FIFO job queue: a bounded-concurrency engine that runs each
> enqueued input through a handler with retries and a per-attempt timeout or abort, and
> hands back one promise per `enqueue` that settles with that job's result.

Worker loops are created only when accepted demand exists, up to the smaller of that demand
and `concurrency`. A created idle loop does not busy-poll or run a timer — it **parks** on a
wake list, and `enqueue` / `resume` wake exactly one (or all) parked loops, so an idle queue
burns zero CPU. Cancellation is built on the L1 `@orkestrel/abort` and `@orkestrel/timeout`
primitives: each attempt's `signal` fires on a queue-level abort, the entry's own signal, or
the per-attempt deadline, and the handler is _raced_ against it — so an attempt that ignores
its `signal` still fails when the clock runs out.

Durability is opt-in and outstanding-only. A `QueueStoreInterface` mirrors the jobs that have
not yet settled — saved on accept, removed on settle — so a graceful shutdown empties the
store and a crash leaves exactly the unfinished rows. Pass a `store` to `createQueue`, and
after a restart a fresh queue over the same store `restore()`s precisely that unfinished work.
`DatabaseQueueStore` is the durable engine over the `@orkestrel/database` layer (a queue's
durable state is a table), driver-pluggable across memory / JSON / SQLite; `MemoryQueueStore`
is the zero-plumbing in-process default.

`Queue` is **observable**: it exposes a typed `emitter` (`.claude/rules/patterns.md` §
Stateful emitters) carrying its lifecycle moments for fire-and-forget observers — logging,
metrics, tracing (see [Observing](#observing)). Observation is a pure side-channel: every
event fires _after_ the relevant transition and a throwing listener is isolated, so a buggy
observer can never reorder or corrupt the engine.

The queue ships no scheduler, no priorities, and no delay / progress / message channels; use
`concurrency: 1` for strict ordering. What ships is the cooperative loop and the
outstanding-only store. Source: [`src/core`](../src/core).

## Surface

Create a queue over a handler, then `enqueue` inputs and await their results:

```ts
import { createQueue } from '@orkestrel/queue'

const queue = createQueue<string, number>({
	handler: async (url, { signal }) => (await fetch(url, { signal })).status,
	concurrency: 4, // up to four in flight at once (default 1 = ordered)
	retries: 2, // two extra attempts on failure
	timeout: 5_000, // each attempt is bounded to 5s
})

const status = await queue.enqueue('https://example.com')
```

`enqueue` snapshots `id` / `retries` / `timeout` / `signal` exactly once into queue-owned options before reserving an entry (FIFO), then hands back a promise that settles when the entry finally completes, exhausts its retries, or is rejected by a lifecycle action. Inaccessible or invalid options throw a coded `QueueError` immediately instead of being clamped or reserved; `signal` must be a native `AbortSignal`, and timeout values must be integers in the native timer range (`0` through `2_147_483_647` milliseconds). A valid supplied id is reserved before any store await; a duplicate live id rejects with `QueueError` code `duplicate`. The `count` is the number of reserved live ids, `active` is the claimed in-flight total, and the handler receives a stable `context.id` plus a cooperative `signal`.

### Factories

| API                        | Kind     | Summary                                                                                                                                                                                                                     |
| -------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `createQueue`              | function | Creates a concurrent, cooperative job queue — a bounded-concurrency engine that runs each enqueued input through a handler, with retries and a per-attempt timeout / abort, all over the L1 `Abort` / `Timeout` primitives. |
| `createDatabaseQueueStore` | function | Creates a `DatabaseQueueStore` over any `DriverInterface` — the durable, driver-pluggable backing for a queue's outstanding entries, across a memory, JSON, or SQLite driver.                                               |
| `createMemoryQueueStore`   | function | Creates an in-memory `MemoryQueueStore` over a plain `Map` — the zero-plumbing `QueueStoreInterface` a queue takes when its outstanding entries need not outlive the process, and the twin of `DatabaseQueueStore`.         |

### Classes

| API                  | Kind  | Summary                                                                                                                                                                                                                           |
| -------------------- | ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Queue`              | class | Represents a concurrent, cooperative FIFO job queue — a wake-park worker loop with retries, a per-attempt timeout, abort, and optional outstanding-work persistence.                                                              |
| `QueueError`         | class | Represents a queue failure carrying a lowercase machine-readable `code`, optional structured context, and an optional cause.                                                                                                      |
| `MemoryQueueStore`   | class | Represents an in-memory store of a queue's outstanding entries — a process-lifetime `Map` owning validated, immutable JSON snapshots.                                                                                             |
| `DatabaseQueueStore` | class | Represents the opt-in durable store for a queue's outstanding entries, backed by one table of the `@orkestrel/database` layer — a queue's durable state is a table, so persistence reduces to keyed CRUD over a `TableInterface`. |

### Types

A `Shape` cell holds an interface's data members as bare names in braces, `?` marking an optional member and `plus` introducing its call-signature members, and a type alias's own type literal with a union's arms escaped as `\|`.

| Type                  | Kind      | Shape                                                                                                                  | Summary                                                                                                                                                                                             |
| --------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueueContext`        | interface | `{ id, signal }`                                                                                                       | Represents the per-attempt context a queue handler receives.                                                                                                                                        |
| `QueueHandler`        | type      | `(input: TInput, context: QueueContext) => Promise<TResult> \| TResult`                                                | Runs one queued entry's work; a rejection triggers a retry while attempts remain.                                                                                                                   |
| `QueueEntryOptions`   | interface | `{ id?, retries?, timeout?, signal? }`                                                                                 | Represents the per-entry options for `enqueue`.                                                                                                                                                     |
| `QueueOptions`        | interface | `{ on?, error?, handler, concurrency?, retries?, timeout?, store? }`                                                   | Represents the options for `createQueue`.                                                                                                                                                           |
| `QueueInterface`      | interface | `{ emitter, count, active, paused, stopped } plus enqueue, restore, start, stop, pause, resume, abort, clear, destroy` | Represents the contract a queue consumer holds — the readings of its live work, the typed observation surface, and the calls that submit, restore, and wind it down.                                |
| `QueueEventMap`       | type      | `{ enqueue, start, retry, success, failure, abort, drain }`                                                            | Represents the push observation surface of a `QueueInterface` — the lifecycle moments a fire-and-forget observer (logging, metrics, tracing) subscribes to, beside the per-entry `enqueue` promise. |
| `QueueCode`           | type      | `'invalid' \| 'duplicate' \| 'stopped' \| 'aborted' \| 'destroyed' \| 'cleared' \| 'timeout' \| 'store' \| 'cleanup'`  | Represents the machine-readable queue failure categories.                                                                                                                                           |
| `QueueOption`         | type      | `'id' \| 'concurrency' \| 'retries' \| 'timeout' \| 'signal'`                                                          | Represents the construction and per-entry option keys a queue validates.                                                                                                                            |
| `QueueErrorContext`   | interface | `{ id?, option?, operation?, value? }`                                                                                 | Represents the structured context carried by a `QueueError`.                                                                                                                                        |
| `QueueErrorOptions`   | interface | `{ code, context?, cause? }`                                                                                           | Represents the construction options for a `QueueError`.                                                                                                                                             |
| `StoredEntry`         | interface | `{ id, input, attempts }`                                                                                              | Represents a durably persisted, still-outstanding queue entry — re-run after a restart.                                                                                                             |
| `QueueStoreInterface` | interface | `{} plus save, remove, load, clear`                                                                                    | Represents the durable backing for a queue's outstanding entries — the small keyed surface a restart resumes from.                                                                                  |

### Guards

In a guard table a `Shape` cell holds the type the guard narrows to.

| API                  | Kind     | Shape                  | Summary                                                                                                                                           |
| -------------------- | -------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `isQueueError`       | function | `QueueError`           | Determines whether an unknown value is a `QueueError`, staying total for a hostile value.                                                         |
| `isQueueConcurrency` | function | `number`               | Determines whether a value is a valid queue concurrency — a positive safe integer.                                                                |
| `isQueueRetries`     | function | `number`               | Determines whether a value is a valid queue retry count — a nonnegative safe integer.                                                             |
| `isQueueTimeout`     | function | `number`               | Determines whether a value is a valid queue timeout — an integer count of milliseconds inside the native timer range.                             |
| `isQueueSignal`      | function | `AbortSignal`          | Determines whether a value is a native abort signal usable by the queue, testing the native brand rather than the shape.                          |
| `isStoredEntry`      | function | `StoredEntry<unknown>` | Determines whether a value is a valid stored queue entry — a record holding a string `id`, an `input`, and a nonnegative safe-integer `attempts`. |

Call each guard on a sample value to see what it accepts and what it refuses:

```ts
import {
	isQueueConcurrency,
	isQueueRetries,
	isQueueSignal,
	isQueueTimeout,
	isStoredEntry,
} from '@orkestrel/queue'

isQueueConcurrency(4) // true
isQueueRetries(2) // true
isQueueTimeout(Infinity) // false
isQueueSignal(new AbortController().signal) // true
isStoredEntry({ id: 'job-1', input: 'task', attempts: 0 }) // true
```

### Helpers

`readOption` and `validateOption` back the queue's option handling. `enqueue` reads every entry option through `readOption`, because a caller's `QueueEntryOptions` is foreign; `enqueue` and the constructor then check what they read through `validateOption`.

| API              | Kind     | Summary                                                                                                                                                              |
| ---------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `readOption`     | function | Reads one named option from a caller-supplied entry options object exactly once, containing a throwing getter as a coded failure.                                    |
| `validateOption` | function | Validates one already-read queue option against its guard, and throws the coded invalid failure carrying the option and the refused value when the guard refuses it. |

Read one entry option, then validate what you read — the pair `enqueue` runs for every option a caller supplies:

```ts
import { isQueueRetries, readOption, validateOption } from '@orkestrel/queue'

const raw = readOption({ retries: 2 }, 'retries', 'queue retries could not be read') // 2
const retries = validateOption(raw, isQueueRetries, 'retries', 'queue retries must be an integer') // 2
```

The `emitter`, `count`, `active`, `paused`, and `stopped` members of `QueueInterface` are `readonly` data members (Surface rows, earlier) — its call-signature methods are documented under [Methods](#methods).

## Methods

The public methods of `QueueInterface` and `QueueStoreInterface` — every call-signature member listed. Each class (`Queue`, and both store classes `MemoryQueueStore` / `DatabaseQueueStore`) implements its interface exactly, so this doubles as each class's instance-method surface (`AGENTS.md` § Documentation contract).

#### `QueueInterface`

`enqueue` and `restore` submit work; the rest carry the fixed meanings `.claude/rules/names.md` § Fixed lifecycle vocabulary gives them.

| Method    | Returns            | Summary                                                                                                                                               |
| --------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enqueue` | `Promise<TResult>` | Reserves one FIFO entry, submits it, and wakes a worker; the returned promise settles when the entry finally settles.                                 |
| `restore` | `Promise<void>`    | Re-enqueues the store's outstanding entries at their persisted attempt count so they run again; without a store it does nothing.                      |
| `start`   | `void`             | Begins or restarts worker execution after a `stop`, spawning loops for accepted demand up to the queue's concurrency; after an abort it does nothing. |
| `stop`    | `Promise<void>`    | Rejects non-active work and awaits durable removals plus current-loop quiescence, while in-flight entries settle normally.                            |
| `pause`   | `void`             | Suspends dequeuing until `resume`, parking the workers while in-flight entries keep running.                                                          |
| `resume`  | `void`             | Continues a paused queue, waking the parked workers.                                                                                                  |
| `abort`   | `Promise<void>`    | Cancels active work through its signal, rejects pending work immediately, and awaits the owned persistence cleanup.                                   |
| `clear`   | `Promise<void>`    | Rejects non-active work immediately and awaits its durable removal, leaving active entries untouched.                                                 |
| `destroy` | `Promise<void>`    | Blocks admissions, aborts, awaits the cleanup, then destroys the emitter last; idempotent.                                                            |

#### `QueueStoreInterface`

`MemoryQueueStore` and `DatabaseQueueStore` implement this interface, the dual-store convention: `MemoryQueueStore` is the zero-plumbing default over a plain `Map`, needing no encoding because a queue entry is already pure JSON, and `DatabaseQueueStore` is the opt-in durable backing over an injected `TableInterface`, whose reads narrow through the table's contract so `load` returns typed `StoredEntry`s with no cast (a driver-swap across memory / JSON / SQLite). Each exposes exactly the methods this table lists.

| Method   | Returns                              | Summary                                                                           |
| -------- | ------------------------------------ | --------------------------------------------------------------------------------- |
| `save`   | `Promise<void>`                      | Upserts one entry by its `id`, overwriting an entry already stored under that id. |
| `remove` | `Promise<void>`                      | Drops one finished entry by `id`, doing nothing when the store holds no such id.  |
| `load`   | `Promise<readonly StoredEntry<…>[]>` | Returns every outstanding entry — the work a restart resumes.                     |
| `clear`  | `Promise<void>`                      | Empties the store, dropping every outstanding entry.                              |

## Contract

These invariants hold across `src/core` ↔ `queue.md`:

1. **DOC ↔ SOURCE bijection.** Every `function` / `class` / `interface` / `type` row in the `## Surface` tables is a real export of `src/core`, and every export appears as a Surface row — exhaustive, both directions (`AGENTS.md` § Documentation contract).
2. **Constructor normalization.** Construction reads `concurrency`, `retries`, and `timeout` exactly once in fail-fast order, validates each snapshot before reading the next, then snapshots `on` and `error` once. Only `undefined` selects the `1` / `0` / `0` numeric defaults; runtime `null` is rejected with coded option context.
3. **Cooperative demand-driven wake-park loop.** Construction allocates no workers. Accepted demand creates at most `min(concurrency, active + pending)` worker loops; each takes the next pending entry or — when none is ready (queue empty or paused) — **parks** by awaiting a fresh promise whose resolver is held in a wake list. `enqueue` wakes exactly one parked worker; `resume` wakes all. An idle queue therefore consumes no CPU: no busy-poll, no `setInterval`, no recursive microtask. Even `Number.MAX_SAFE_INTEGER` concurrency creates only one loop for one accepted entry.
4. **FIFO + bounded concurrency.** Entries run in enqueue order; at most `concurrency` (default `1` — strictly ordered) are in flight at once. Durable admissions are serialized by enqueue call order, so store latency cannot reorder FIFO. Dequeue and active claim are one synchronous transition: a same-turn `enqueue(); stop()` treats the claimed entry as active and waits for it, while `enqueue(); clear()` leaves it active. Every claimed token is terminally settled and decremented exactly once even if queue orchestration throws unexpectedly; that failure remains visible to an overlapping lifecycle barrier, and turnover can serve later demand. `count` is every reserved live id (admitting, pending, active, or retained after failed cleanup); `active` is the claimed in-flight total and never exceeds `concurrency`.
5. **Per-attempt timeout + cancellation, over L1.** `enqueue` reads every option property once and owns the normalized wrapper; hostile access or a non-native `signal` fails synchronously before reservation. Each attempt builds its `signal` by combining the queue-level abort, the entry's own native `AbortSignal`, and a fresh `Timeout` from `@orkestrel/timeout` (when a timeout applies). Timeout values are integers from `0` through `2_147_483_647` milliseconds, inclusive, matching the native `setTimeout` range; zero disables the deadline. The handler is _raced_ against that signal, so an attempt that ignores its `signal` still fails on the deadline. A queue / entry abort clears the deadline (it never expires) — the L1 parent-linking contract.
6. **Retries, but abort never retries.** A failed attempt (a handler rejection or a per-attempt timeout) retries while attempts remain — `retries` + 1 total. `start` means the first execution start and fires once per entry; `retry(id, attempt)` carries the number of completed attempts before the next try. A queue-level `abort`, or the entry's own `signal` firing, rejects the entry immediately with no further attempt. Queue-owned failures are coded `QueueError`s; a non-`Error` handler rejection, including a hostile object, remains a normal retryable handler outcome and is normalized with the contract package's safe diagnostic preview to an `Error` carrying the exact original value on `.cause`.
7. **Lifecycle (`.claude/rules/names.md` § Fixed lifecycle vocabulary).** `pause` parks workers (resumable); `resume` wakes them. `stop()` rejects admitting/pending work, ends the current loop generation, and awaits durable removals plus old-loop quiescence while in-flight handlers settle normally. A same-turn `stop(); start(); enqueue()` automatically respawns workers after the old generation exits. `abort()` additionally fires the queue signal. `clear()` owns only admitting, pending, and unclaimed orphan cleanup: it neither waits for an active entry's cleanup nor inherits that active cleanup's failure, including the promise-reaction window after a failed removal has marked the token orphaned but before its claim settles. Affected execution promises reject synchronously and lifecycle promises await only their relevant persistence cleanup. `stop()` / `abort()` / `destroy()` install and reuse their exact barrier promise before cleanup, cancellation callbacks, or synchronous events, so reentrant listeners cannot recurse or replace the transition. `destroy()` blocks admissions, coordinates abort/cleanup, and destroys the emitter last. Enqueuing onto a stopped / aborted / destroyed queue rejects with a coded `QueueError`.
8. **Observable + de-bloated.** `Queue` owns a typed `Emitter` (`.claude/rules/patterns.md` § Stateful emitters) exposed as `readonly emitter` and accepts the reserved `on?` initial-listeners hook plus the `error?` listener-error handler: `QueueEventMap<TResult>` (`enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`). **Emitting is observation-only** — the emitter isolates a listener throw (routing it to the `error` handler, never a domain event) and every event sits strictly _after_ the relevant transition. The engine latches a real transition to idle before emitting terminal `success` / `failure`, then emits `drain` afterward, so a terminal listener that synchronously enqueues cannot suppress the completed transition. `drain` fires exactly once for every real non-idle → idle transition, including a pending-only lifecycle drain, and never duplicates an already-idle state. Still deliberately cut: scheduling / delay / activation / expiration, priority ordering, an explicit `sequential` flag (use `concurrency: 1`), bail, and the progress / message channels — progress belongs to the job the queue runs and reaches the consumer through that job's own contract, so the queue observes lifecycle only and never duplicates a surface the job already owns.
9. **DOC ↔ SOURCE method bijection.** The `## Methods` tables list exactly the public methods of `QueueInterface` / `QueueStoreInterface` — exhaustive, both directions — and `Queue` / `MemoryQueueStore` / `DatabaseQueueStore` each expose the same public methods as their interface, no more (`AGENTS.md` § Documentation contract).
10. **Persistence holds outstanding-only — the dual-store convention.** A `QueueStoreInterface` durably backs a queue's still-outstanding entries: `save` upserts by `id`, `remove` drops a completed one, `load` returns everything outstanding, and `clear` empties it. `MemoryQueueStore` captures `id` / `input` / `attempts` exactly once, validates those captured values, keys by the captured id, and uses `cloneJSONValue` to own deeply frozen JSON snapshots on both save and load; caller mutation, stateful accessors, hostile proxies, cloning failures, and contract failures cannot corrupt stored state and reject with operation-specific coded store context. `DatabaseQueueStore` remains the opt-in durable backing over a contract-narrowed `TableInterface`. Both stores reject invalid values through their declared boundary and stay event-free.
11. **Durability is wired into the `Queue` (outstanding-only, restartable).** A validated live id maps to one exact resolver token synchronously before any store await, so two accepted live entries can never overwrite one row. Initial admissions are serialized by enqueue call order; a failed save is rolled back and rejects only that admission, then the next admission proceeds. Every token owns at most one installed cleanup promise, shared by settle, lifecycle drain, rollback, and orphan retry; a late cleanup can release only that exact token and never a newer same-id admission. The reservation remains until durable removal succeeds. Terminal settle removes before resolving/rejecting, while `clear` / `stop` / `abort` / `destroy` reject affected pending promises immediately and their returned promises await relevant removals; `clear` explicitly excludes every claimed token from both installed and orphan cleanup at its synchronous call boundary. Active cleanup failure rejects the entry and its owning stop/abort/destroy barrier; normal workers respawn after surfacing it. Cleanup failure uses `QueueError` code `cleanup` with cause/context and retains the id reservation until a later lifecycle retry succeeds. Retry-count saves remain best-effort because in-memory execution is authoritative. `restore()` captures the lifecycle generation before loading, validates and snapshots the entire loaded iterable before reserving anything, contains hostile iteration/property access and malformed ids/attempts as a coded `store`/`load` failure, ignores a stale result after any stop/abort generation change, skips every reserved live id, and re-runs only outstanding rows at their completed-attempt count. Persistence is **at-least-once**: a crash or failed removal can replay a row, so this queue is a single-owner mechanism and handlers must be idempotent using stable `context.id`; distributed leasing and exactly-once delivery are outside its boundary.

## Persistence

A `QueueStoreInterface` is the durable backing for a queue's **outstanding** entries — the work that has not yet completed. It is deliberately a small surface (`save` / `remove` / `load` / `clear`) over the `@orkestrel/database` layer: a queue's durable state is a table of `StoredEntry`s (an `id`, the handler's `input`, and the `attempts` so far), so persistence reduces to keyed CRUD. `DatabaseQueueStore` is the one engine; the backend is whichever `DriverInterface` you build it over, so the same store runs in memory or against a persistent driver without changing its code — the durability is the driver's job.

```ts
import { stringShape } from '@orkestrel/contract'
import { createMemoryQueueStore } from '@orkestrel/queue'

// An ephemeral, memory-backed store (tests, a non-durable queue):
const store = createMemoryQueueStore(stringShape())
await store.save({ id: 'job-1', input: 'https://example.com', attempts: 0 })
await store.save({ id: 'job-1', input: 'https://example.com', attempts: 1 }) // upsert by id
const outstanding = await store.load() // readonly StoredEntry<string>[] — typed, no cast
await store.remove('job-1') // a finished entry leaves the store
await store.clear() // empty the store — drop every outstanding entry
```

Both store factories use `input` as a real runtime contract. `createMemoryQueueStore` validates and clones exact JSON into immutable owned snapshots; `createDatabaseQueueStore(input, driver)` uses the same input shape as the durable table column. Invalid or non-JSON values reject at the store boundary. The store holds only outstanding entries, so `load` yields precisely the work to resume.

### Wiring a store into a queue

Pass a `store` to `createQueue` and the queue mirrors its outstanding entries automatically — `save` on accept (and as the attempt count climbs), `remove` on settle or drain. After a restart, build a fresh queue over the same store and call `restore()` to re-run the unfinished work:

```ts
import { stringShape } from '@orkestrel/contract'
import { createMemoryDriver } from '@orkestrel/database'
import { createDatabaseQueueStore, createQueue } from '@orkestrel/queue'

const store = createDatabaseQueueStore(stringShape(), createMemoryDriver())
const queue = createQueue<string, number>({ store, handler: (url) => fetchStatus(url) })
await queue.enqueue('https://example.com') // durably saved before it runs

// …after a crash + restart, a fresh queue over the same store resumes the work:
const resumed = createQueue<string, number>({ store, handler: (url) => fetchStatus(url) })
await resumed.restore() // re-enqueues every still-outstanding entry, then runs it
```

Accepting work durably is not best-effort: admissions are serialized by call order and a failed initial `save` rejects only that enqueue after rollback. Per-attempt count persistence remains best-effort because live execution is authoritative. Every removal is awaited; a failure rejects with coded cleanup context and keeps the id reserved because the durable row may still exist. Persistence is at-least-once, not exactly-once: one queue instance must own a store at a time, and a replayable handler must de-duplicate by stable `context.id`. Restored entries use queue-default retries/timeout and carry no per-entry signal because those options are not persisted.

## Observing

`Queue` exposes a typed `emitter` (`.claude/rules/patterns.md` § Stateful emitters) carrying its lifecycle moments for fire-and-forget observers — logging, metrics, tracing. Subscribe through `queue.emitter.on(...)`, or wire initial listeners through the reserved `on?` option; supply an `error?` handler to receive a listener's throw. **Emitting is observation-only**: every event fires strictly _after_ the relevant wake / park / settle transition, so a listener can never change what the engine does — and a throwing listener can never corrupt it (see the listener-isolation safety guarantee following).

```ts
import { createQueue } from '@orkestrel/queue'

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

`enqueue` fires when an entry is accepted (after durable save); `start` fires once at the first execution start; `retry(id, attempt)` carries the number of completed attempts before the next try; `success` / `failure` fire after durable cleanup; `abort` carries the coded abort error; and `drain` follows the terminal event for each latched transition to idle, including pending-only drains and transitions whose terminal listener immediately enqueues again.

**The listener-isolation safety guarantee.** A listener throw never escapes into the engine: the emitter isolates it and routes it to its own `error` handler (the `error` option, surfaced as `(error, event)`), not to a domain event — so a buggy observer is isolated yet not silently lost. The `error` handler runs in its own try/catch, so even a throwing handler can't recurse or escape; with no handler, the throw is swallowed silently. Every throwing listener surfaces (not only the first). Because every emit sits after the wake / park / settle transition and is isolated, a buggy observer **cannot corrupt the cooperative wake-park loop**: `active` stays balanced, the queue still drains, and no parked worker is ever stranded.

## Patterns

### Create a queue

Build a queue over a handler and await each entry's result — the ordered default first, then a bounded, retried, time-boxed one:

```ts
import { createQueue } from '@orkestrel/queue'

// Ordered (concurrency defaults to 1): each entry runs to completion before the next.
const queue = createQueue<Job, Output>({ handler: (job) => run(job) })

const output = await queue.enqueue(job)

// Bounded, retried, and time-boxed: four in flight, two extra attempts, 5s per attempt.
const fetches = createQueue<string, number>({
	handler: async (url, { signal }) => (await fetch(url, { signal })).status,
	concurrency: 4,
	retries: 2,
	timeout: 5_000,
})

const status = await fetches.enqueue('https://example.com')
```

### Bounded concurrency

Set `concurrency` to cap how many entries run at once; a later `enqueue` call waits for a slot to free up:

```ts
// Up to 5 in flight at once; a sixth enqueue waits for a slot to free up.
const pool = createQueue<string, Response>({
	concurrency: 5,
	handler: (url, { signal }) => fetch(url, { signal }),
})

const responses = await Promise.all(urls.map((url) => pool.enqueue(url)))
```

### Retries

Set `retries` for the queue default, and override it on the one entry that must not be re-run:

```ts
// Three extra attempts; the handler is re-run on each rejection until one succeeds.
const queue = createQueue<Job, Output>({ retries: 3, handler: (job) => flaky(job) })

// A per-entry override wins over the queue default.
await queue.enqueue(job, { retries: 0 }) // this one does not retry
```

### Per-attempt timeout

Set `timeout` to bound each attempt; a deadline that fires counts as a failed attempt:

```ts
const queue = createQueue<Job, Output>({
	timeout: 2_000, // each attempt is bounded to 2s; a timeout counts as a failed attempt
	retries: 1,
	handler: (job, { signal }) => run(job, signal), // honour `signal` to stop early
})
```

Observe `context.signal` in the handler to abandon work early; even if it ignores the signal, the queue stops waiting after the deadline fires and treats the attempt as failed.

### Abort

Call `abort` to reject pending work and fire every in-flight handler's `signal`:

```ts
const queue = createQueue<Job, Output>({ handler: (job, { signal }) => run(job, signal) })
const pending = queue.enqueue(job)

const aborting = queue.abort(new Error('shutting down'))
// `pending` rejects; an in-flight handler's `signal` fires; nothing is retried.
await pending.catch((error) => report(error))
await aborting // persistence cleanup is complete
```

### Lifecycle

Call `pause`, `resume`, `clear`, `stop`, `start`, and `destroy` to suspend, drain, and wind down a running queue:

```ts
queue.pause() // suspend dequeuing — workers park until resume
queue.resume() // wake the parked workers, continuing where it left off

await queue.clear() // reject pending and await their durable removal; active work is untouched

await queue.stop() // reject pending, quiesce current loops; in-flight entries settle normally
queue.start() // (re)spawn the worker loops, resuming after a plain stop

await queue.destroy() // abort, clean up, then destroy the emitter last; idempotent
```

### Practices

- **Honour `context.signal`** — pass it to `fetch` / child aborts and bail out when it fires, so timeouts and aborts actually stop work rather than abandoning its result.
- **`concurrency: 1` for ordering** — there is no separate `sequential` flag; a concurrency of one is the ordered, one-at-a-time mode.
- **Per-entry overrides** — `enqueue(input, { retries, timeout, signal })` overrides the queue defaults for that one entry.
- **`abort` is terminal** — a queue-level abort cancels in-flight work and stops the queue; create a new queue to start over (`start` resumes only after a plain `stop`).
- **`clear` vs `stop` vs `abort`** — `clear` drops pending and keeps running; `stop` ends the loops but lets in-flight finish; `abort` also cancels in-flight.
- **Observe, don't drive** — subscribe to `queue.emitter` for lifecycle moments (see [Observing](#observing)); emitting is a pure side-channel, so a listener never changes what the engine does (and a throwing one can't corrupt it).

## Tests

- [`tests/guides.test.ts`](../tests/guides.test.ts) — the `## Surface` ↔ `src/core` bijection (value + type exports), the `QueueInterface` / `QueueStoreInterface` ↔ `Queue` / `MemoryQueueStore` / `DatabaseQueueStore` method bijection, and the equality gate: every `Summary` cell against its declaration's description paragraph, the titled `Create a queue` fence against the `@example` block of that title (pinned so the titled pair cannot be retired silently), and the README pitch against this guide's tagline. It also runs the value-claiming fences and asserts the values their comments claim.
- [`tests/src/core/Queue.test.ts`](../tests/src/core/Queue.test.ts) — the canonical queue suite: FIFO/concurrency/retries/native-range timeouts, hostile-safe rejection normalization, one-read constructor normalization with undefined-only defaults, runtime-null rejection, fail-fast property access, real emitter-hook capture, one-read enqueue normalization and signal branding, demand-driven workers, runtime contracts, duplicate and serialized admissions, atomic claims and restore validation, stable reentrant lifecycle barriers, stale-restore generations, exclusive cleanup ownership and orphan retry, claimed-orphan clear isolation, active cleanup propagation, terminal-listener drain ordering, lifecycle behavior, observation safety, and real-store durability.
- [`tests/src/core/factories.test.ts`](../tests/src/core/factories.test.ts) — construction identity/wiring only: each factory returns its concrete `Queue`, `DatabaseQueueStore`, or `MemoryQueueStore` entity; behavior stays in the concrete suites.
- [`tests/src/core/helpers.test.ts`](../tests/src/core/helpers.test.ts) — the shared option leaves: one-read property access, throwing-getter containment with the cause preserved, guard narrowing, and the coded invalid failure's option and refused value.
- [`tests/src/core/validators.test.ts`](../tests/src/core/validators.test.ts) — `isStoredEntry` over well-formed, malformed, non-record, hostile-accessor, and cyclic values.
- [`tests/src/core/stores/MemoryQueueStore.test.ts`](../tests/src/core/stores/MemoryQueueStore.test.ts) — the real shape-validated memory store: immutable JSON snapshots, caller/load alias isolation, one-read field capture, hostile-access containment, upsert/remove/load/clear semantics, and scale.
- [`tests/src/core/stores/DatabaseQueueStore.test.ts`](../tests/src/core/stores/DatabaseQueueStore.test.ts) — over a memory-backed driver store: a `save` → `load` round-trip (value + typed `input`, including nested-object payloads), `save` upserts by id (no duplicate), `remove` drops one (absent is a no-op), `load` returns all outstanding in key order, `clear` empties it, plus scale (200 entries), upsert churn on one id, and complex / edge-value inputs (nested arrays, booleans, nullables, optionals).

## See also

- [`@orkestrel/abort`](https://github.com/orkestrel/abort#readme) — the cancellation primitive each attempt's `signal` is built on (a queue / entry abort).
- [`@orkestrel/timeout`](https://github.com/orkestrel/timeout#readme) — the deadline primitive backing the per-attempt timeout (a parent abort clears it).
- [`@orkestrel/database`](https://github.com/orkestrel/database#readme) — the storage layer the `QueueStoreInterface` persists over (a queue's durable state is a table); the drivers a store can be built on.
- [`@orkestrel/contract`](https://github.com/orkestrel/contract#readme) — the guard / shape primitives `createDatabaseQueueStore` / `createMemoryQueueStore` are typed by.
- [`AGENTS.md`](../AGENTS.md) — the coding contract; see `.claude/rules/names.md` for the lifecycle vocabulary and one-word members, `.claude/rules/patterns.md` for the emitter pattern, and `AGENTS.md` § Documentation contract for documentation-as-contracts.
- [`README.md`](README.md) — the guides index.
