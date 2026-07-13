# Workers

> The cooperative concurrent job engine. A **`Queue`** runs enqueued inputs through a handler under bounded concurrency, with retries and a per-attempt timeout / abort; each `enqueue` returns a promise that settles with the job's result. A **`Pool`** is a bounded resource lifecycle — idle reuse + FIFO waiting — and a **`Worker`** marries the two: a Queue whose handler runs each job against a pooled resource (acquired before the job, released after). That is the whole surface — three composable primitives plus an optional durable store.
>
> The word "cooperative" is load-bearing. An idle worker loop does not busy-poll or run a timer — it **parks** on a wake list, and `enqueue` / `resume` wake exactly one (or all) parked loops, so an idle queue burns zero CPU. Cancellation is built on the L1 [aborts](aborts.md) + [timeouts](timeouts.md) primitives: each attempt's `signal` fires on a queue-level abort, the entry's own signal, or the per-attempt deadline, and the handler is _raced_ against it — so an attempt that ignores its `signal` still fails when the clock runs out.
>
> Durability is opt-in and outstanding-only. A `QueueStoreInterface` mirrors just the jobs that have not yet settled — saved on accept, removed on settle — so a graceful shutdown empties the store and a crash leaves exactly the unfinished rows. Pass a store to `createQueue` / `createWorker`, and after a restart a fresh queue over the same store `restore()`s precisely that unfinished work. `DatabaseQueueStore` is the one durable engine over the [databases](databases.md) layer (a queue's durable state is just a table), driver-pluggable across memory / JSON / SQLite; `MemoryQueueStore` is the zero-plumbing in-process default.
>
> Each engine is **observable**: `Queue` / `Pool` / `Worker` expose a typed `emitter` (AGENTS §13) carrying their lifecycle moments for fire-and-forget observers — logging, metrics, tracing (see [Observing](#observing)). Observation is a pure side-channel: every event fires _after_ the relevant transition and a throwing listener is isolated, so a buggy observer can never reorder or corrupt the engine.
>
> It is deliberately **de-bloated** — the cuts are the design. No scheduler, no priorities, no delay / progress / message channels on the queue (use `concurrency: 1` for strict ordering); no warm-floor (`min`) or eviction timers on the pool. What ships is the cooperative loop, the validated FIFO pool, and the outstanding-only store — nothing speculative.
>
> For CPU parallelism, **`createNodeWorker`** (`@src/server`) specializes `createWorker` over a pool of `node:worker_threads`, with **`serveWorker`** as the worker-side entry; the structured-clone boundary is narrowed by `input` / `result` guards with zero `as`. Source: [`src/core/workers`](../../src/core/workers) (the engines + the store) and [`src/server/workers`](../../src/server/workers) (the JSON-file store + the worker-thread `NodeWorker`). Surfaced through the `@src/core` and `@src/server` barrels.

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

A `Pool` leases resources — `acquire` hands back a `PoolToken` whose `release` returns the resource for reuse — and a `Worker` is simply a `Queue` whose handler runs each job against a pooled resource, acquired before the handler and released after it (even on a throw):

```ts
import { createWorker } from '@src/core'

const worker = createWorker<Query, Connection, Rows>({
	pool: { create: () => connect(), destroy: (connection) => connection.close() },
	handler: (query, connection, { signal }) => connection.run(query, signal),
	concurrency: 4, // up to four jobs in flight; the pool defaults its `max` to match
	retries: 1,
})

const rows = await worker.enqueue(query)
```

### Factories

| API                        | Kind     | Summary                                                                                             |
| -------------------------- | -------- | --------------------------------------------------------------------------------------------------- |
| `createQueue`              | function | Create a `QueueInterface` over a handler, with optional concurrency / retries / timeout.            |
| `createPool`               | function | Create a `PoolInterface` over a resource lifecycle — idle reuse, `max` backpressure.                |
| `createWorker`             | function | Create a `WorkerInterface` — a `Queue`⨉`Pool`; each job runs against an acquired resource.          |
| `createDatabaseQueueStore` | function | Create a `QueueStoreInterface` over any `DriverInterface` (memory / JSON / SQLite).                 |
| `createMemoryQueueStore`   | function | Create the zero-plumbing in-memory `QueueStoreInterface` — a `MemoryQueueStore` over a plain `Map`. |
| `createJSONQueueStore`     | function | Create a JSON-file `QueueStoreInterface` (`@src/server`) — durable across restarts.                 |
| `createNodeWorker`         | function | Create a `WorkerInterface` over `node:worker_threads` (`@src/server`) — CPU parallelism.            |
| `serveWorker`              | function | The worker-side entry (`@src/server`) — a thread script registers its handler with it.              |

### Threads (`@src/server`)

The lower-level `node:worker_threads` machinery `createNodeWorker` composes over — the thread-pool lifecycle hooks behind the public factory. They are exported for completeness and direct use (as `databases` exposes its driver codecs), but the factory is the intended entry point; reach for these only to drive a thread pool by hand.

| API           | Kind     | Summary                                                                                                           |
| ------------- | -------- | ----------------------------------------------------------------------------------------------------------------- |
| `spawnThread` | function | Spawn one worker thread and resolve a live `NodeThread` once it comes `online` (a death before `online` rejects). |
| `dispatch`    | function | Post a job to a leased `NodeThread` and await its reply, narrowed through a result `Guard`.                       |
| `isReply`     | function | Narrow an inbound message to a `Reply` for a job id (total guard) — `dispatch`'s filter.                          |

### Entities

| API                  | Kind  | Summary                                                                                    |
| -------------------- | ----- | ------------------------------------------------------------------------------------------ |
| `Queue`              | class | The cooperative concurrent job engine — wake-park loop, retries, timeout, abort.           |
| `Pool`               | class | A bounded resource pool — idle reuse, `max` backpressure, FIFO abort-able wait.            |
| `Worker`             | class | A resource-backed job worker — a `Queue` composed with a `Pool`.                           |
| `MemoryQueueStore`   | class | The zero-plumbing DEFAULT store for outstanding entries — a plain process-lifetime `Map`.  |
| `DatabaseQueueStore` | class | The opt-in durable store for outstanding entries over one `databases` table (driver-swap). |

### Types

| Type                  | Kind      | Shape                                                                                                                                                   |
| --------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueueExecution`      | interface | The per-attempt handle a handler receives — a stable `id` (idempotency key) + a `signal` (timeout / abort / entry signal).                              |
| `QueueHandler`        | type      | `(input, execution) => Promise<TResult> \| TResult` — runs one entry's work; may reject to retry.                                                       |
| `QueueEntryOptions`   | interface | Per-entry `enqueue` options — `id?` / `retries?` / `timeout?` / `signal?`.                                                                              |
| `QueueOptions`        | interface | `createQueue` options — `handler` + `concurrency?` / `retries?` / `timeout?` / `store?` / `on?` / `error?`.                                             |
| `QueueInterface`      | interface | `emitter` / `count` / `active` / `paused` / `stopped` data members + the lifecycle + `enqueue` / `restore` methods.                                     |
| `QueueEventMap`       | type      | The `Queue`'s observable events — `enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`.                                            |
| `QueueEntry`          | interface | One queued unit of work + the `enqueue` promise resolvers (`id` / `input` / `options` / `attempts`) — the `Queue` engine's internal pending entry.      |
| `AttemptOutcome`      | type      | The settled outcome of one attempt — `{ ok: true, value }` or `{ ok: false, error }`; threaded from a `Queue` attempt to its settle.                    |
| `PoolToken`           | interface | A leased resource — `value` + an idempotent `release()`.                                                                                                |
| `PoolWaiter`          | interface | A parked acquirer on the `Pool`'s FIFO waiter list — its `resolve` / `reject` resolvers + a `clear()` abort-listener cleanup.                           |
| `PoolOptions`         | interface | `createPool` options — `create` + `destroy?` / `validate?` / `max?` / `on?` / `error?`.                                                                 |
| `PoolInterface`       | interface | `emitter` / `size` / `idle` / `active` data members + `acquire` / `clear` / `destroy` methods.                                                          |
| `PoolEventMap`        | type      | The `Pool`'s observable events — `create` / `acquire` / `release` / `destroy`.                                                                          |
| `WorkerHandler`       | type      | `(input, resource, execution) => Promise<TResult> \| TResult` — runs one job against a resource.                                                        |
| `WorkerOptions`       | interface | `createWorker` options — `handler` + `pool` + `concurrency?` / `retries?` / `timeout?` / `store?` / `on?` / `error?`.                                   |
| `WorkerInterface`     | interface | `emitter` / `count` / `active` / `paused` / `stopped` data members + the lifecycle + `enqueue` / `restore` methods.                                     |
| `WorkerEventMap`      | type      | The `Worker`'s observable events — the queue lifecycle it surfaces (`enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`).         |
| `StoredEntry`         | interface | A durably persisted, outstanding entry — `id` / `input` / `attempts` (its `readonly` data members).                                                     |
| `QueueStoreInterface` | interface | Durable backing for outstanding entries — `save` / `remove` / `load` / `clear` methods.                                                                 |
| `Guard`               | type      | `(value: unknown) => value is T` — the total predicate narrowing a wire payload with no assertion.                                                      |
| `NodeWorkerOptions`   | interface | `createNodeWorker` options — `script` + `input` / `result` guards + `workerData?` / `concurrency?` / ….                                                 |
| `ServeWorkerOptions`  | interface | `serveWorker` options — the `input` guard + the `handler` (receives the narrowed input + `{ signal }`).                                                 |
| `NodeThread`          | interface | A leased worker thread + its `alive` flag + the latched `death` (the first terminal `error` / `exit`) — the pooled resource `createNodeWorker` runs on. |
| `Reply`               | type      | A thread→main reply envelope — `{ id, ok: true, value }` or `{ id, ok: false, error }`; the internal wire protocol.                                     |

The `emitter` / `count` / `active` / `paused` / `stopped` members of `QueueInterface` / `WorkerInterface`, and the `emitter` / `size` / `idle` / `active` members of `PoolInterface`, are `readonly` data members (Surface rows, above) — `emitter` is the typed push observation surface (see [Observing](#observing)); their call-signature methods are documented under [Methods](#methods).

## Methods

The public methods of `QueueInterface`, `PoolInterface`, `WorkerInterface`, and `QueueStoreInterface` — every call-signature member listed (their `readonly` data members stay Surface rows). Each class (`Queue` / `Pool` / `Worker`, and BOTH store classes `MemoryQueueStore` / `DatabaseQueueStore`) implements its interface exactly, so this doubles as each class's instance-method surface (AGENTS §22).

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

#### `PoolInterface`

`acquire` leases a resource (returning a `PoolToken` whose `release` returns it); `clear` / `destroy` are the §10 teardown verbs. The `size` / `idle` / `active` counts are Surface rows.

| Method    | Returns                 | Behavior                                                                                                                    |
| --------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `acquire` | `Promise<PoolToken<T>>` | Lease a resource — reuse a validated idle one, grow up to `max`, or park (FIFO) until a `release`; an aborted wait rejects. |
| `clear`   | `Promise<void>`         | Destroy every idle resource; leased ones keep running.                                                                      |
| `destroy` | `Promise<void>`         | Destroy all resources and reject parked waiters; idempotent.                                                                |

#### `WorkerInterface`

The same lifecycle as `QueueInterface`, delegated to the underlying queue; `enqueue` runs the handler against an acquired pooled resource, and `destroy` also tears the pool down.

| Method    | Returns            | Behavior                                                                                           |
| --------- | ------------------ | -------------------------------------------------------------------------------------------------- |
| `enqueue` | `Promise<TResult>` | Submit a job (FIFO); the handler runs against an acquired resource, released when the job settles. |
| `restore` | `Promise<void>`    | Re-run the store's outstanding jobs (delegated to the queue); no-op without a store.               |
| `start`   | `void`             | (Re)start the underlying queue's worker loops.                                                     |
| `stop`    | `void`             | Stop the queue — reject pending; in-flight jobs settle on their own.                               |
| `pause`   | `void`             | Suspend dequeuing (delegated to the queue).                                                        |
| `resume`  | `void`             | Continue a paused worker (delegated to the queue).                                                 |
| `abort`   | `void`             | Cancel in-flight jobs and reject pending (delegated to the queue); never retried.                  |
| `clear`   | `void`             | Drop every pending job; in-flight jobs are untouched.                                              |
| `destroy` | `void`             | Tear the worker down — destroy the queue then the pool (releasing resources); idempotent.          |

#### `QueueStoreInterface`

The durable backing for a queue's outstanding entries. `save` upserts an entry by its `id`; `remove` drops a finished one; `load` returns everything still outstanding (the work to resume after a restart); `clear` empties the store. Two classes implement it, the dual-store convention: `MemoryQueueStore` is the zero-plumbing DEFAULT over a plain `Map` (no encoding — a queue entry is already pure JSON), and `DatabaseQueueStore` is the opt-in durable backing over an injected `TableInterface`, whose reads narrow through the table's contract so `load` returns typed `StoredEntry`s with no cast (a driver-swap across memory / JSON / SQLite). Both expose exactly these four methods.

| Method   | Returns                              | Behavior                                                                     |
| -------- | ------------------------------------ | ---------------------------------------------------------------------------- |
| `save`   | `Promise<void>`                      | Upsert an entry by its `id` (a re-`save` of an existing `id` overwrites it). |
| `remove` | `Promise<void>`                      | Drop a finished entry by `id`; an absent `id` is a no-op (no throw).         |
| `load`   | `Promise<readonly StoredEntry<…>[]>` | Return every outstanding entry — the work to resume after a restart.         |
| `clear`  | `Promise<void>`                      | Empty the store — drop every outstanding entry.                              |

## Contract

These invariants hold across `src/core/workers` ↔ `workers.md`:

1. **DOC ↔ SOURCE bijection.** Every `function` / `class` / `interface` / `type` row in the `## Surface` tables is a real export of the workers module, and every export appears as a Surface row — exhaustive, both directions (AGENTS §22).
2. **Cooperative wake-park loop.** Up to `concurrency` worker loops run; each takes the next pending entry or — when none is ready (queue empty or paused) — PARKS by awaiting a fresh promise whose resolver is held in a wake list. `enqueue` wakes exactly one parked worker; `resume` wakes all. An idle queue therefore consumes no CPU: no busy-poll, no `setInterval`, no recursive microtask. (Proven by a test asserting the handler is never called while the queue sits idle.)
3. **FIFO + bounded concurrency.** Entries run in enqueue order; at most `concurrency` (default `1` — strictly ordered) are in flight at once. `count` is pending + active; `active` is the in-flight total and never exceeds `concurrency`.
4. **Per-attempt timeout + cancellation, over L1.** Each attempt builds its `signal` by combining the queue-level abort, the entry's own `signal`, and a fresh [`Timeout`](timeouts.md) (when a timeout applies). The handler is RACED against that signal, so an attempt that ignores its `signal` still fails on the deadline. A queue / entry abort CLEARS the deadline (it never expires) — the L1 parent-linking contract.
5. **Retries, but abort never retries.** A failed attempt (a handler rejection or a per-attempt timeout) retries while attempts remain — `retries` + 1 total. A queue-level `abort`, or the entry's own `signal` firing, rejects the entry immediately with no further attempt. A handler rejection that is not an `Error` is coerced to one (`new Error(String(value), { cause: value })`) — so the entry always rejects with an `Error`, with the original thrown value preserved on `.cause`.
6. **Lifecycle (§10).** `pause` parks workers (resumable); `resume` wakes them; `stop` ends the loops permanently and rejects pending while in-flight settle; `abort` additionally fires the queue signal so in-flight attempts observe cancellation; `clear` drops only pending; `destroy` sets the destroyed flag, then `abort`s and `stop`s, idempotently. `stopped` reads `true` after `stop`, `abort`, or `destroy`. Enqueuing onto a stopped / aborted / destroyed queue rejects.
7. **Pool — idle reuse + `max` backpressure + FIFO abort-able wait.** `acquire` reuses a validated idle resource (an invalid one — by a `validate` hook — is destroyed and the next idle / a fresh one tried), else `create`s one while below `max`, else PARKS on a FIFO waiter list. `release` (idempotent — a second call is a no-op) hands the resource to the oldest waiter (it stays leased) or returns it to idle. **The FIFO handoff is validated:** when a waiter is parked, the released resource is re-validated (the same `validate` hook the idle path uses) BEFORE handoff — a resource that went invalid WHILE leased (e.g. a worker thread terminated on abort) is destroyed and the waiter is served a fresh/valid resource instead, so a dead resource is never handed to the next lessee. The lease slot transfers straight from the dead resource to its replacement, so `active` is steady across the swap (no overshoot of `max`); if the replacement `create` throws, the waiter is rejected with that error and the slot is freed. **`validate` is total:** a `validate` hook that THROWS is treated exactly like one returning `false` (the resource is "not usable", so it is destroyed and replaced), on BOTH the idle and the handoff paths — so a throwing validator can never escape as an unhandled rejection that strands a parked waiter. The no-waiter path stays synchronous (decrement + idle). A parked `acquire` given an `AbortSignal` rejects + de-queues itself when the signal fires — no leaked waiter, so a later `release` still serves the next live waiter. `size` = idle + leased; `idle` = available; `active` = leased. `clear` destroys idle (leased untouched); `destroy` destroys all + rejects waiters (both await `destroy`).
8. **Worker = Queue ⨉ Pool.** A `Worker` does not reimplement concurrency / retry / lifecycle — it composes a `Queue` (the engine) with a `Pool` (the resources). Its queue handler `acquire`s a resource over the attempt's `execution.signal`, runs the user handler against it, and `release`s it in a `finally` (so a throwing or aborted handler still frees the resource). Because the `finally` runs only when the handler actually settles, a handler that IGNORES its `execution.signal` keeps its leased resource until it returns — so on a timeout / abort the resource can outlive the freed queue slot; a cooperative handler that honours the signal releases promptly. The pool's `max` defaults to `concurrency`, so resources match the jobs in flight. The lifecycle delegates to the queue; `destroy` destroys the queue then the pool.
9. **Observable + de-bloated.** Each engine owns a typed `Emitter` (AGENTS §13) exposed as `readonly emitter` and accepts the reserved `on?` initial-listeners hook plus the `error?` listener-error handler: `Queue` ⇒ `QueueEventMap<TResult>` (`enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`), `Pool` ⇒ `PoolEventMap` (`create` / `acquire` / `release` / `destroy`), `Worker` ⇒ `WorkerEventMap<TResult>` (the queue lifecycle it surfaces). **Emitting is observation-only** — the emitter isolates a listener throw (routing it to the `error` handler, never a domain event) and every event sits strictly AFTER the relevant wake / park / handoff / settle transition, so a buggy observer can NEVER reorder, throw into, or corrupt the cooperative wake-park / validated FIFO handoff-eviction engine — `active` / lease counts stay balanced and no parked worker / waiter is stranded regardless of what a listener does. Still deliberately CUT: on the `Queue` — scheduling / delay / activation / expiration, priority ordering, an explicit `sequential` flag (use `concurrency: 1`), bail, and the progress / message channels; on the `Pool` — a warm-floor (`min`), eviction timers, and acquire-timeout. Durability (an optional `store`) IS wired into the `Queue` (clause 12).
10. **DOC ↔ SOURCE method bijection.** The `## Methods` tables list exactly the public methods of `QueueInterface` / `PoolInterface` / `WorkerInterface` / `QueueStoreInterface` — exhaustive, both directions — and `Queue` / `Pool` / `Worker` / `MemoryQueueStore` / `DatabaseQueueStore` each expose the same public methods as their interface, no more (AGENTS §22).
11. **Persistence holds outstanding-only — the dual-store convention.** A `QueueStoreInterface` durably backs a queue's still-outstanding entries: `save` upserts by `id`, `remove` drops a completed one, `load` returns everything outstanding (so a restart resumes exactly the unfinished work), `clear` empties it. TWO classes implement it (mirroring the `MemorySessionStore` / `MemoryWorkflowStore` dual-store split): `MemoryQueueStore` is the zero-plumbing DEFAULT over a plain process-lifetime `Map` (no `databases` table, no driver codec — a queue entry is already pure self-contained JSON, AGENTS §21), built by `createMemoryQueueStore`; `DatabaseQueueStore` is the opt-in durable backing — a minimal interface (§21) over the [databases](databases.md) layer (a queue's durable state IS a table), wrapping one `TableInterface` so any `DriverInterface` backs it (a JSON file via `createJSONQueueStore` (`@src/server`), or SQLite via `createDatabaseQueueStore`). The `DatabaseQueueStore`'s reads narrow through the table's contract, so its `load` returns typed `StoredEntry<TInput>`s with no cast; an entry's `input` must be JSON-serializable to survive the JSON / SQLite drivers. The `Queue` consumes whichever store (clause 12); the store itself stays **event-free** (only the `Queue` / `Pool` / `Worker` engines are observable — clause 9).

12. **Durability is wired into the `Queue` (outstanding-only, restartable).** Given a `store`, the queue mirrors only OUTSTANDING work. `enqueue` `save`s the entry (at `attempts: 0`) BEFORE it becomes runnable, so accepting work is durable: a failed initial `save` PROPAGATES (rejects the `enqueue`). As an entry's attempt count climbs across retries, the queue re-`save`s the new count — best-effort (in-memory state is authoritative, so a failed per-attempt `save` is swallowed and the entry runs on). The moment an entry settles — success, terminal failure, or an abort rejection — the queue `remove`s its row first, then settles; a lifecycle drain (`clear` / `stop` / `abort` / `destroy`) likewise `remove`s each pending entry it rejects (fire-and-forget). So a graceful shutdown empties the store and a crash leaves exactly the unfinished rows. `restore()` `load`s those rows and re-enqueues each at its persisted attempt count, honouring the remaining retry budget; it is a no-op without a store or on a halted queue (it also re-checks the halt flags after the `load()` await, so a queue aborted / destroyed mid-`load` launches nothing). `restore()` is **idempotent and safe on a live queue**: the queue tracks the ids of its live (pending or in-flight) entries and `restore` SKIPS any id already live, so calling it on a non-idle queue (or calling it twice) never double-launches an entry that is still running — it re-launches only genuinely-outstanding-but-not-live rows. Restored entries use the queue-default `retries` / `timeout` and carry NO per-entry signal (per-entry options are not persisted) — the documented limitation. Persistence is **at-least-once** (a settle's `remove` can fail, or a crash can land between `save` and `remove`), so a re-run entry's handler should be **idempotent** — de-dup against `execution.id` (the entry's stable id, equal across every attempt and across a crash-replay, so it is the idempotency key for the at-least-once replay window). The no-store path is unaffected — it stays fully synchronous (an entry is queued the instant `enqueue` returns), and its observable events (clause 9) fire identically.

13. **`createNodeWorker` is a thread specialization of `createWorker` (`@src/server`).** It does NOT reimplement concurrency / retry / timeout / lifecycle — it calls `createWorker` with a `Pool` whose resource is a `node:worker_threads` thread (`create` = `spawnThread`, `destroy` = `terminate()`, `validate` = `alive && threadId > 0`) and an internal handler that narrows the input through `options.input` then `dispatch`es the job to the leased thread. Both generics INFER from the `input` / `result` guards, so a call site needs no type argument. The structured-clone boundary is crossed with ZERO `as`: `dispatch` narrows each reply value through `options.result` (a value that fails it rejects with `'reply did not satisfy result guard'`), and the worker side narrows each payload through `options.input` (a bad input replies `'input did not satisfy input guard'`) — `TInput` / `TResult` are reconstructed by validation, never asserted. The run/abort/reply protocol is internal: main → thread posts `{ id, command: 'run', input }` / `{ id, command: 'abort' }`; thread → main posts `{ id, ok: true, value }` / `{ id, ok: false, error }`. **Abort TERMINATES + evicts the thread:** because CPU-bound work cannot honour an `AbortSignal`, an `abort` / `timeout` posts the cooperative `abort` envelope AND calls `terminate()` + flips `alive = false`, so the freed pool slot spawns a fresh thread on the next job (the tainted thread is never reused). **A thread death settles its job under EVERY event ordering — the `death` latch:** `spawnThread` attaches persistent `error` / `exit` listeners that flip `alive = false` AND latch the first terminal event on `NodeThread.death`; `dispatch` checks that latch synchronously at entry, so a job dispatched onto a thread that already died rejects immediately (under event-loop pressure Node delivers a dead thread's `online` + `error` + `exit` in one synchronous exit-drain batch — before the dispatch listeners can attach — so without the latch the job would await events that already fired, forever). A death mid-flight still rejects via the dispatch's own `error` / `exit` listeners, and a death before `online` rejects the spawn itself (`spawnThread`'s promise also rejects on an exit-before-online, so it can never dangle). `TInput` / `TResult` / `workerData` must be structured-cloneable (no functions / Promises / `AbortSignal`). A `.ts` `script` requires Node ≥ 23.6 (type-stripping); on older Node point `script` at a built `.js` / `.mjs`. The worker script's module MUST call `serveWorker` — the worker-side entry that runs the handler and answers the protocol; it is self-contained (only `node:worker_threads` at runtime) so it loads as a raw module in a thread. It returns the plain `WorkerInterface`, so it inherits the Worker's `emitter` (`WorkerEventMap` — clause 9) unchanged.

## NodeWorker

`createNodeWorker` (`@src/server`) runs jobs on a pool of `node:worker_threads` threads — true CPU parallelism for work that would otherwise block the event loop. It is a thin specialization of [`createWorker`](#a-resource-backed-worker): the pooled resource is a worker THREAD, and it returns the plain `WorkerInterface`, so its methods, lifecycle, concurrency, retries, timeout, and durability are exactly the Worker's (see [Methods → `WorkerInterface`](#workerinterface)). The only additions are the thread pairing and the zero-`as` wire bridge.

Two guards define the boundary and the inference: `input` narrows each payload (and fail-fasts a bad one before it crosses to a thread), and `result` narrows each reply value. Both generics infer from these, so call sites pass no type arguments:

```ts
import { createNodeWorker } from '@src/server'

const isNumber = (value: unknown): value is number => typeof value === 'number'

const worker = createNodeWorker({
	script: new URL('./double.js', import.meta.url), // a .ts script needs Node ≥ 23.6
	input: isNumber, // narrows + infers TInput; fail-fasts a bad input before posting
	result: isNumber, // narrows each reply — the zero-`as` type bridge; infers TResult
	concurrency: 4, // up to four threads run jobs in parallel
	retries: 1,
})

const doubled = await worker.enqueue(21) // 42, computed on a worker thread
worker.destroy() // terminates every thread
```

The worker script registers its handler with `serveWorker`, which must be the thread module's entry. It receives the narrowed input and a `{ signal }` execution (the signal fires on a cooperative abort); its resolved value is the reply:

```ts
// double.ts — the worker script
import { serveWorker } from '@src/server'

serveWorker<number, number>({
	input: (value): value is number => typeof value === 'number',
	handler: (value, { signal }) => value * 2,
})
```

Because CPU-bound work cannot honour its signal, an `abort` or a per-attempt `timeout` **terminates** the in-flight thread and evicts it from the pool — the next job spawns a fresh one. A thread that dies (a crash, a script that fails to load or even to resolve) always settles its job: the death is latched on the `NodeThread`, so even a job dispatched after the death — its events already fired — rejects immediately rather than waiting on a reply that never comes. `workerData`, the input, and the result must all be structured-cloneable (no functions, Promises, or `AbortSignal`s cross the boundary).

## Persistence

A `QueueStoreInterface` is the durable backing for a queue's **outstanding** entries — the work that has not yet completed. It is deliberately a small, four-method surface (`save` / `remove` / `load` / `clear`) over the [databases](databases.md) layer: a queue's durable state is just a table of `StoredEntry`s (an `id`, the handler's `input`, and the `attempts` so far), so persistence reduces to keyed CRUD. `DatabaseQueueStore` is the one engine; the backend is whichever `DriverInterface` you build it over, so the SAME store runs in memory, against a JSON file, or against SQLite without changing its code — the durability is the driver's job.

```ts
import { createMemoryQueueStore, stringShape } from '@src/core'

// An ephemeral, memory-backed store (tests, a non-durable queue):
const store = createMemoryQueueStore(stringShape())
await store.save({ id: 'job-1', input: 'https://example.com', attempts: 0 })
await store.save({ id: 'job-1', input: 'https://example.com', attempts: 1 }) // upsert by id
const outstanding = await store.load() // readonly StoredEntry<string>[] — typed, no cast
await store.remove('job-1') // a finished entry leaves the store
```

For durability across restarts, build the store over a server driver. `createJSONQueueStore` (`@src/server`) persists to a JSON file, so a fresh store over the same path resumes the prior process's outstanding work:

```ts
import { stringShape } from '@src/core'
import { createJSONQueueStore } from '@src/server'

const store = createJSONQueueStore('data/queue.json', stringShape())
await store.save({ id: 'job-1', input: 'https://example.com', attempts: 0 })

// A later process — the entries persisted to the file are still outstanding:
const resumed = createJSONQueueStore('data/queue.json', stringShape())
const work = await resumed.load() // [{ id: 'job-1', input: 'https://example.com', attempts: 0 }]
```

`createDatabaseQueueStore(input, driver)` is the general form — pass any `DriverInterface` (e.g. a `createSQLiteDriver(path)` from `@src/server`) for a SQLite-backed store. The `input` shape is used directly as the entry's `input` column, so the stored payload is validated and typed by it; it must be JSON-serializable to survive the JSON / SQLite drivers. The store holds only outstanding entries, so `load` on startup yields precisely the work to resume.

### Wiring a store into a queue

Pass a `store` to `createQueue` / `createWorker` and the queue mirrors its outstanding entries automatically — `save` on accept (and as the attempt count climbs), `remove` on settle or drain. After a restart, build a fresh queue over the same store and call `restore()` to re-run the unfinished work:

```ts
import { createQueue, stringShape } from '@src/core'
import { createJSONQueueStore } from '@src/server'

const store = createJSONQueueStore('data/queue.json', stringShape())
const queue = createQueue<string, number>({ store, handler: (url) => fetchStatus(url) })
await queue.enqueue('https://example.com') // durably saved before it runs

// …after a crash + restart, a fresh queue over the same store resumes the work:
const resumed = createQueue<string, number>({ store, handler: (url) => fetchStatus(url) })
await resumed.restore() // re-enqueues every still-outstanding entry, then runs it
```

Accepting work durably is NOT best-effort: a failed initial `save` rejects the `enqueue`. Per-attempt persistence IS best-effort — in-memory state is authoritative, so the entry runs on even if a climbing-count `save` fails. A graceful shutdown empties the store; a crash leaves the unfinished rows. Persistence is at-least-once (a settle's `remove`, or a crash between `save` and `remove`, can leave a stale row), so a re-run handler should be idempotent. Restored entries use the queue-default `retries` / `timeout` and carry no per-entry signal — per-entry options are not persisted. The no-store path is unchanged: a queue without a `store` stays fully synchronous and writes nothing.

## Observing

Each engine exposes a typed `emitter` (AGENTS §13) carrying its lifecycle moments for fire-and-forget observers — logging, metrics, tracing. Subscribe via `entity.emitter.on(...)`, or wire initial listeners through the reserved `on?` option; supply an `error?` handler to receive a listener's throw. **Emitting is observation-only**: every event fires strictly AFTER the relevant wake / park / handoff / settle transition, so a listener can never change what the engine does — and a throwing listener can never corrupt it (see the safety guarantee below).

```ts
import { createQueue } from '@src/core'

const queue = createQueue<string, number>({
	handler: (url) => fetchStatus(url),
	on: { drain: () => console.log('queue idle') }, // initial listener at construction
})

queue.emitter.on('success', (id, status) => metrics.record(id, status))
queue.emitter.on('failure', (id, error) => log.warn(`job ${id} failed`, error))
```

The event vocabulary, per engine:

| Engine   | Event map                 | Events                                                                                                                          |
| -------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `Queue`  | `QueueEventMap<TResult>`  | `enqueue(id)` · `start(id)` · `retry(id, attempt)` · `success(id, result)` · `failure(id, error)` · `abort(reason)` · `drain()` |
| `Pool`   | `PoolEventMap`            | `create()` · `acquire()` · `release()` · `destroy()`                                                                            |
| `Worker` | `WorkerEventMap<TResult>` | `enqueue(id)` · `start(id)` · `retry(id, attempt)` · `success(id, result)` · `failure(id, error)` · `abort(reason)` · `drain()` |

`enqueue` fires when an entry is accepted (after a durable `save`, when a `store` is set); `start` when an attempt begins; `retry` on each re-attempt (the 1-based attempt number); `success` / `failure` when an entry settles (after the durable row is removed); `abort` on a queue-level abort; `drain` when the queue goes idle. The `Pool` fires `create` when a fresh resource is made, `acquire` when one is leased (a reused idle one, a fresh one, or a served waiter), `release` when one returns to idle, and `destroy` when one is torn down. A `Worker` RE-EXPOSES its underlying queue's job lifecycle as its own events (so a consumer never reaches through to the internal `Queue`); the pool's resource events stay the pool's concern — observe a `Pool` directly for those.

**The listener-isolation safety guarantee.** A listener throw is NEVER allowed to escape into the engine: the emitter isolates it and routes it to its OWN `error` handler (the `error` option, surfaced as `(error, event)`), NOT to a domain event — so a buggy observer is isolated yet not silently lost. The `error` handler runs in its own try/catch, so even a throwing handler can't recurse or escape; with no handler, the throw is swallowed silently. Every throwing listener surfaces (not just the first). Because every emit sits after the wake / park / handoff / settle transition AND is isolated, a buggy observer **cannot corrupt the cooperative wake-park loop or the validated FIFO handoff-eviction**: `active` / lease counts stay balanced, the queue still drains, the pool still hands off, and no parked worker / waiter is ever stranded — proven by the per-engine emit-safety tests.

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

### A resource pool

```ts
import { createPool } from '@src/core'

const pool = createPool<Connection>({
	create: () => connect(), // make a resource (called lazily, up to `max`)
	destroy: (connection) => connection.close(), // tear one down on clear / destroy / invalid
	validate: (connection) => connection.alive, // check an idle one before reuse (optional)
	max: 8, // at most 8 live at once; a 9th acquire waits for a release
})

const token = await pool.acquire()
try {
	await token.value.query('select 1')
} finally {
	token.release() // returns it to the next waiter, or to idle
}
```

`acquire` accepts an optional `AbortSignal`; if it fires while the acquire is parked (the pool is at `max`), the acquire rejects and its waiter is removed — no leak.

### A resource-backed worker

```ts
import { createWorker } from '@src/core'

// A Queue whose handler runs each job against a pooled resource (acquired before the
// handler, released after it — even on throw). The pool's `max` defaults to `concurrency`.
const worker = createWorker<Query, Connection, Rows>({
	pool: { create: () => connect(), destroy: (connection) => connection.close() },
	handler: (query, connection, { signal }) => connection.run(query, signal),
	concurrency: 4,
	retries: 1,
})

const rows = await worker.enqueue(query)
worker.destroy() // tears down the queue, then the pool
```

### Practices

- **Honour `execution.signal`** — pass it to `fetch` / child aborts and bail out when it fires, so timeouts and aborts actually stop work rather than just abandoning its result.
- **`concurrency: 1` for ordering** — there is no separate `sequential` flag; a concurrency of one is the ordered, one-at-a-time mode.
- **Per-entry overrides** — `enqueue(input, { retries, timeout, signal })` overrides the queue defaults for that one entry.
- **`abort` is terminal** — a queue-level abort cancels in-flight work and stops the queue; create a new queue to start over (`start` resumes only after a plain `stop`).
- **`clear` vs `stop` vs `abort`** — `clear` drops pending and keeps running; `stop` ends the loops but lets in-flight finish; `abort` also cancels in-flight.
- **Observe, don't drive** — subscribe to `queue.emitter` / `pool.emitter` / `worker.emitter` for lifecycle moments (see [Observing](#observing)); emitting is a pure side-channel, so a listener never changes what the engine does (and a throwing one can't corrupt it).

## Tests

- [`tests/guides/parity.test.ts`](../../tests/guides/src/parity.test.ts) — the `## Surface` ↔ `src/core/workers` bijection (value + type exports) and the `QueueInterface` / `PoolInterface` / `WorkerInterface` ↔ `Queue` / `Pool` / `Worker` method bijection.
- [`tests/src/core/workers/Queue.test.ts`](../../tests/src/core/workers/Queue.test.ts) — `enqueue` + FIFO at concurrency 1, the concurrency cap (`active` never exceeds the limit, surplus waits), retries (succeed-after-N and exhaust), the per-attempt timeout (the signal fires and the attempt fails), `abort` (rejects pending + fires in-flight signal, no retries), `pause` / `resume`, `clear` (drops pending, in-flight untouched), `stop` / `destroy`, the wake-park idle check (no handler call while idle), and durability over a real memory store: persist-on-accept + remove-on-settle (success and exhausted-retries), the climbing attempt count persisted mid-flight, `restore()` re-running a prior queue's outstanding entries (a shared-store restart sim) at their persisted attempt count, a restored always-throwing entry settling without an unhandled rejection, lifecycle drains (`clear` / `abort`) removing the drained rows while an in-flight row is removed on its own settle, and the save-failure contract (a failed initial `save` rejects the `enqueue`; a failed per-attempt `save` is best-effort and the entry runs on).
- [`tests/src/core/workers/Pool.test.ts`](../../tests/src/core/workers/Pool.test.ts) — create up to `max`, idle reuse (release → same resource), `max` backpressure (a parked acquire served FIFO on release), an aborted waiting-acquire (rejects + no leaked waiter), `validate` (destroys + replaces an invalid idle one), `clear` (destroys idle, leased untouched), `destroy` (destroys all + rejects waiters), the double-release no-op, and accurate `size` / `idle` / `active`.
- [`tests/src/core/workers/Worker.test.ts`](../../tests/src/core/workers/Worker.test.ts) — the handler runs against a pooled resource; resources are reused across jobs and never exceed the pool max; the resource is released even when the handler throws (a later job reuses it); the lifecycle (`pause` / `resume` / `abort` / `stop` / `clear` / `destroy`) delegates to the queue; `destroy` tears the pool down; and durability passthrough — a `store` persists a job and `restore()` re-runs it against a fresh resource (delegated to the queue), a no-op without a store.
- [`tests/src/core/workers/factories.test.ts`](../../tests/src/core/workers/factories.test.ts) — `createQueue` / `createPool` / `createWorker` each return a working, typed instance end to end and honour their options + status surface, and `createDatabaseQueueStore` / `createMemoryQueueStore` round-trip a stored entry.
- [`tests/src/core/workers/stores/MemoryQueueStore.test.ts`](../../tests/src/core/workers/stores/MemoryQueueStore.test.ts) — the plain-`Map` DEFAULT store, driven directly over real inline `StoredEntry`s (no mocks): a `save` → `load` round-trip by value, `save` upserts by id (no duplicate), `remove` drops one (absent is a no-op), `load` returns EVERY outstanding entry (the bulk-restore semantic), `clear` empties it, `load` hands back a fresh snapshot array each call, plus scale (200 entries), upsert churn on one id, interleaved-id isolation, and a structured-object input round-tripping by (reference-equal) value.
- [`tests/src/core/workers/stores/DatabaseQueueStore.test.ts`](../../tests/src/core/workers/stores/DatabaseQueueStore.test.ts) — over a memory-backed driver store: a `save` → `load` round-trip (value + typed `input`, including a nested-object payload), `save` upserts by id (no duplicate), `remove` drops one (absent is a no-op), `load` returns all outstanding in key order, and `clear` empties it.
- [`tests/src/server/workers/factories.test.ts`](../../tests/src/server/workers/factories.test.ts) — `createJSONQueueStore` over a real temp file: entries persist ACROSS store instances on the same path (a second store `load`s the first's work), a nested-object input survives the JSON round-trip, and a `remove` is reflected across a reopen; plus a `createNodeWorker` round-trip smoke (a job over a real thread, then teardown).
- [`tests/src/server/workers/helpers.test.ts`](../../tests/src/server/workers/helpers.test.ts) — the main-side worker-thread machinery (`spawnThread` / `dispatch` / `isReply`), driven through `createNodeWorker` over REAL worker threads (no mocking): a `21 → 42` round-trip and a batch over a small pool; the concurrency cap (`active` never exceeds the limit) AND the live-thread cap + idle reuse (distinct echoed `threadId`s never exceed `concurrency`; sequential jobs reuse the same idle thread); a throwing handler rejecting with its error, and re-running under `retries`; a per-attempt `timeout` rejecting AND terminating the uncooperative thread, with a later job served on a fresh thread (the tainted one not reused); an in-flight signal abort + a mid-flight thread crash each evicting and replacing the thread; `workerData` cloned through to the worker side (and a non-cloneable `workerData` surfacing a clear `DataCloneError` from the constructor, never a hang); a large array input + result round-trip; a broken worker script (a module-load throw, and a non-existent path) rejecting the job cleanly with the pool recovering across retries + a fresh worker; a stray / foreign-id message ignored while the correct reply still resolves; an already-aborted enqueue signal short-circuiting then a later job still succeeding; a reply that fails the `result` guard rejecting; a bad input fail-fasting before the boundary; `destroy` with multiple threads mid-job terminating all (suite exits); rapid enqueue/abort churn settling every job with the counts returning to rest (no thread leak); and `destroy` terminating every thread so the process exits.
- [`tests/src/server/workers/serve.test.ts`](../../tests/src/server/workers/serve.test.ts) — `serveWorker` driven MANUALLY over a raw `node:worker_threads` thread (post a run/abort envelope, await the reply): the success envelope, an input-guard rejection envelope, a handler-throw error envelope (SYNC and ASYNC rejections both reported as `{ ok: false }`), a `{ command: 'abort' }` firing the handler's signal (a cooperative fixture), an abort for an unknown id being a no-op, an unknown message `command` and a malformed (no-`id`) message both ignored without crashing the thread, and object / array / null / boolean result shapes round-tripping through the `{ ok: true, value }` envelope.
- The worker fixtures under [`tests/src/server/workers/fixtures`](../../tests/src/server/workers/fixtures) (`double` / `fail` / `slow` / `bad-result` / `abortable` / `crash` / `identify` / `echo-data` / `sum` / `stray` / `throw-async` / `load-throw` / `echo`) are real `.ts` worker scripts loaded by Node's type-stripping; each imports `serveWorker` by relative-to-source path (`load-throw` deliberately throws before it can) and is exempt from the test mirror (not a `*.test.ts`, not under `src/`).

## See also

- [`aborts.md`](aborts.md) — the cancellation primitive each attempt's `signal` is built on (a queue / entry abort).
- [`timeouts.md`](timeouts.md) — the deadline primitive backing the per-attempt timeout (a parent abort clears it).
- [`databases.md`](databases.md) — the storage layer the `QueueStoreInterface` persists over (a queue's durable state is just a table); the memory / JSON / SQLite drivers a store can be built on.
- [`AGENTS.md`](../../AGENTS.md) — the rules; §10 lifecycle, §4.1 single-word members, §21 minimal-interface, §22 documentation-as-contracts.
- [`README.md`](README.md) — the guides index.
