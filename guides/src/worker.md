# Worker

> A resource-backed job worker — a thin facade composing a [`Queue`](queue.md) (from
> `@orkestrel/queue`) with a [`Pool`](pool.md) (from `@orkestrel/pool`). A `Worker` is a
> `Queue` whose handler ACQUIRES a pooled resource, runs the caller's handler against it,
> and RELEASES it in a `finally` — so all concurrency, retries, per-attempt timeout, and
> lifecycle are the Queue's, and all resource lifecycle (idle reuse, `max` backpressure,
> FIFO waiting) is the Pool's. The Worker adds only the resource pairing: it does not
> reimplement either primitive.
>
> The pool's `max` defaults to the worker's `concurrency`, so at most one resource exists
> per in-flight job and idle resources are reused across jobs. Each job acquires over the
> attempt's `execution.signal`, so an abort / timeout while waiting for a resource rejects
> the acquire cleanly (no token to release). The worker is **observable** (AGENTS §13): its
> `emitter` RE-EXPOSES the underlying queue's job lifecycle (`enqueue` / `start` / `retry` /
> `success` / `failure` / `abort` / `drain`) as its own events, bridged at construction, so a
> consumer never reaches through to the internal `Queue`.
>
> For CPU parallelism, `createNodeWorker` (`@src/server`) specializes `createWorker` over a
> pool of `node:worker_threads`, with `serveWorker` as the worker-side entry; the
> structured-clone boundary is narrowed by `input` / `result` guards with zero `as`.
> Source: [`src/core`](../../src/core) (the `Worker` facade) and
> [`src/server`](../../src/server) (the thread pool + the worker-side entry). Surfaced
> through the `@src/core` and `@src/server` barrels.

## Surface

Create a worker over a resource lifecycle and a handler, then `enqueue` inputs and await
their results:

```ts
import { createWorker } from '@src/core'

const worker = createWorker<Query, Connection, Rows>({
	pool: { create: () => connect(), destroy: (connection) => connection.close() },
	handler: (query, connection, { signal }) => connection.run(query, signal),
	concurrency: 4, // up to four jobs in flight; the pool defaults its `max` to match
	retries: 1,
})

const rows = await worker.enqueue(query)
worker.destroy() // tears down the queue, then the pool
```

### Factories

| API                    | Kind     | Summary                                                                                      |
| ---------------------- | -------- | -------------------------------------------------------------------------------------------- |
| `createWorker`         | function | Create a `WorkerInterface` — a `Queue` ⨉ `Pool`; each job runs against an acquired resource. |
| `createJSONQueueStore` | function | Create a JSON-file `QueueStoreInterface` (`@src/server`) — durable across restarts.          |
| `createNodeWorker`     | function | Create a `WorkerInterface` over `node:worker_threads` (`@src/server`) — CPU parallelism.     |
| `serveWorker`          | function | The worker-side entry (`@src/server`) — a thread script registers its handler with it.       |

### Threads

The lower-level `node:worker_threads` machinery `createNodeWorker` composes over
(`@src/server`) — the thread-pool lifecycle hooks behind the public factory. Exported
for completeness and direct use; the factory is the intended entry point. Driving a
thread by hand:

```ts
import { dispatch, isReply, spawnThread } from '@src/server'

const isNumber = (value: unknown): value is number => typeof value === 'number'

const thread = await spawnThread(new URL('./double.ts', import.meta.url), undefined)
const controller = new AbortController()
const result = await dispatch(thread, 21, { id: 'job-1', signal: controller.signal }, isNumber)
// `isReply` is the total guard `dispatch` uses internally to filter replies by job id.
```

| API           | Kind     | Summary                                                                                                           |
| ------------- | -------- | ----------------------------------------------------------------------------------------------------------------- |
| `spawnThread` | function | Spawn one worker thread and resolve a live `NodeThread` once it comes `online` (a death before `online` rejects). |
| `dispatch`    | function | Post a job to a leased `NodeThread` and await its reply, narrowed through a result `Guard`.                       |
| `isReply`     | function | Narrow an inbound message to a `Reply` for a job id (total guard) — `dispatch`'s filter.                          |

### Entities

| API      | Kind  | Summary                                                          |
| -------- | ----- | ---------------------------------------------------------------- |
| `Worker` | class | A resource-backed job worker — a `Queue` composed with a `Pool`. |

### Types

| Type                 | Kind      | Shape                                                                                                                                                   |
| -------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WorkerHandler`      | type      | `(input, resource, execution) => Promise<TResult> \| TResult` — runs one job against a leased pool resource.                                            |
| `WorkerOptions`      | interface | `createWorker` options — `handler` + `pool` + `concurrency?` / `retries?` / `timeout?` / `store?` / `on?` / `error?`.                                   |
| `WorkerInterface`    | interface | `emitter` / `count` / `active` / `paused` / `stopped` data members + the lifecycle + `enqueue` / `restore` methods.                                     |
| `WorkerEventMap`     | type      | The `Worker`'s observable events — the queue lifecycle it surfaces (`enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`).         |
| `Guard`              | type      | `(value: unknown) => value is T` — the total predicate narrowing a wire payload with no assertion.                                                      |
| `NodeWorkerOptions`  | interface | `createNodeWorker` options — `script` + `input` / `result` guards + `workerData?` / `concurrency?` / `retries?` / `timeout?` / `store?`.                |
| `ServeWorkerOptions` | interface | `serveWorker` options — the `input` guard + the `handler` (receives the narrowed input + `{ signal }`).                                                 |
| `NodeThread`         | interface | A leased worker thread + its `alive` flag + the latched `death` (the first terminal `error` / `exit`) — the pooled resource `createNodeWorker` runs on. |
| `Reply`              | type      | A thread→main reply envelope — `{ id, ok: true, value }` or `{ id, ok: false, error }`; the internal wire protocol.                                     |

The `emitter` / `count` / `active` / `paused` / `stopped` members of `WorkerInterface` are
`readonly` data members (Surface rows, above) — `emitter` is the typed push observation
surface (see [Observing](#observing)); the call-signature methods are documented under
[Methods](#methods). `Queue` / `Pool` themselves — and their own options, event maps, and
stores — are documented in their own packages: [queue.md](queue.md) / [pool.md](pool.md).

## Methods

The public methods of `WorkerInterface` — every call-signature member listed (its
`readonly` data members stay Surface rows). `Worker` implements `WorkerInterface`
exactly, so this doubles as the class's instance-method surface (AGENTS §22).

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

## Contract

These invariants hold across `src/core` ↔ `worker.md`:

1. **DOC ↔ SOURCE bijection.** Every `function` / `class` / `interface` / `type` row in
   the `## Surface` tables is a real export of the worker module, and every export
   appears as a Surface row — exhaustive, both directions (AGENTS §22).
2. **Composition, not reimplementation.** A `Worker` does not reimplement concurrency /
   retry / lifecycle — it composes a `Queue` (`@orkestrel/queue`) with a `Pool`
   (`@orkestrel/pool`). Its queue handler `acquire`s a resource over the attempt's
   `execution.signal`, runs the caller's handler against it, and `release`s it in a
   `finally` (so a throwing or aborted handler still frees the resource). Because the
   `finally` runs only when the handler actually settles, a handler that IGNORES its
   `execution.signal` keeps its leased resource until it returns — so on a timeout /
   abort the resource can outlive the freed queue slot; a cooperative handler that
   honours the signal releases promptly. The pool's `max` defaults to `concurrency`, so
   resources match the jobs in flight. The lifecycle delegates to the queue; `destroy`
   destroys the queue then the pool.
3. **Observable — the queue lifecycle re-exposed.** The `Worker` owns a typed `Emitter`
   (AGENTS §13) exposed as `readonly emitter` carrying `WorkerEventMap<TResult>`
   (`enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`), bridged
   from the underlying queue's own emitter at construction — each bridge listener
   re-emits directly on the worker's emitter and never throws, so the inner queue's own
   emit stays balanced regardless of what a worker observer does. **Emitting is
   observation-only** — the emitter isolates a listener throw (routing it to the `error`
   handler, never a domain event) and every event sits strictly AFTER the relevant
   queue transition, so a buggy observer can NEVER corrupt the queue or pool. The pool's
   own `create` / `acquire` / `release` / `destroy` events stay the pool's internal
   concern — observe a `Pool` directly for those.
4. **DOC ↔ SOURCE method bijection.** The `## Methods` table lists exactly the public
   methods of `WorkerInterface` — exhaustive, both directions — and `Worker` exposes
   the same public methods as its interface, no more (AGENTS §22).
5. **`createNodeWorker` is a thread specialization of `createWorker` (`@src/server`).**
   It does NOT reimplement concurrency / retry / timeout / lifecycle — it calls
   `createWorker` with a `Pool` whose resource is a `node:worker_threads` thread
   (`create` = `spawnThread`, `destroy` = `terminate()`, `validate` = `alive &&
threadId > 0`) and an internal handler that narrows the input through
   `options.input` then `dispatch`es the job to the leased thread. Both generics INFER
   from the `input` / `result` guards, so a call site needs no type argument. The
   structured-clone boundary is crossed with ZERO `as`: `dispatch` narrows each reply
   value through `options.result` (a value that fails it rejects with `'reply did not
satisfy result guard'`), and the worker side narrows each payload through
   `options.input` (a bad input replies `'input did not satisfy input guard'`) —
   `TInput` / `TResult` are reconstructed by validation, never asserted. The run/abort/
   reply protocol is internal: main → thread posts `{ id, command: 'run', input }` /
   `{ id, command: 'abort' }`; thread → main posts `{ id, ok: true, value }` /
   `{ id, ok: false, error }`.
6. **Abort TERMINATES + evicts the thread.** Because CPU-bound work cannot honour an
   `AbortSignal`, an `abort` / `timeout` posts the cooperative `abort` envelope AND calls
   `terminate()` + flips `alive = false`, so the freed pool slot spawns a fresh thread on
   the next job (the tainted thread is never reused).
7. **A thread death settles its job under EVERY event ordering — the `death` latch.**
   `spawnThread` attaches persistent `error` / `exit` listeners that flip `alive = false`
   AND latch the first terminal event on `NodeThread.death`; `dispatch` checks that latch
   synchronously at entry, so a job dispatched onto a thread that already died rejects
   immediately (under event-loop pressure Node delivers a dead thread's `online` +
   `error` + `exit` in one synchronous exit-drain batch — before the dispatch listeners
   can attach — so without the latch the job would await events that already fired,
   forever). A death mid-flight still rejects via the dispatch's own `error` / `exit`
   listeners, and a death before `online` rejects the spawn itself.
8. **Structured-clone constraints.** `TInput` / `TResult` / `workerData` must be
   structured-cloneable (no functions / Promises / `AbortSignal`). A `.ts` `script`
   requires Node ≥ 23.6 (type-stripping); on older Node point `script` at a built `.js` /
   `.mjs`. The worker script's module MUST call `serveWorker` — the worker-side entry
   that runs the handler and answers the protocol; it is self-contained (only
   `node:worker_threads` at runtime, per the AGENTS §5 runtime-self-contained exception)
   so it loads as a raw module in a spawned thread. `createNodeWorker` returns the plain
   `WorkerInterface`, so it inherits the Worker's `emitter` unchanged (clause 3).
9. **`createJSONQueueStore` is `@orkestrel/queue`'s `createDatabaseQueueStore` over a
   server JSON driver.** A queue's durable state is just a `@orkestrel/database` table,
   so JSON persistence reuses the existing driver rather than a bespoke store — see
   [queue.md](queue.md) for the `QueueStoreInterface` contract itself.

## NodeWorker

`createNodeWorker` (`@src/server`) runs jobs on a pool of `node:worker_threads` threads —
true CPU parallelism for work that would otherwise block the event loop. It is a thin
specialization of [`createWorker`](#surface): the pooled resource is a worker THREAD, and
it returns the plain `WorkerInterface`, so its methods, lifecycle, concurrency, retries,
timeout, and durability are exactly the Worker's (see [Methods](#methods)). The only
additions are the thread pairing and the zero-`as` wire bridge.

Two guards define the boundary and the inference: `input` narrows each payload (and
fail-fasts a bad one before it crosses to a thread), and `result` narrows each reply
value. Both generics infer from these, so call sites pass no type arguments:

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

The worker script registers its handler with `serveWorker`, which must be the thread
module's entry. It receives the narrowed input and a `{ signal }` execution (the signal
fires on a cooperative abort); its resolved value is the reply:

```ts
// double.ts — the worker script
import { serveWorker } from '@src/server'

serveWorker<number, number>({
	input: (value): value is number => typeof value === 'number',
	handler: (value, { signal }) => value * 2,
})
```

Because CPU-bound work cannot honour its signal, an `abort` or a per-attempt `timeout`
**terminates** the in-flight thread and evicts it from the pool — the next job spawns a
fresh one. A thread that dies (a crash, a script that fails to load or even to resolve)
always settles its job: the death is latched on the `NodeThread`, so even a job dispatched
after the death — its events already fired — rejects immediately rather than waiting on a
reply that never comes. `workerData`, the input, and the result must all be
structured-cloneable (no functions, Promises, or `AbortSignal`s cross the boundary).

## Persistence

`createJSONQueueStore` (`@src/server`) builds a `QueueStoreInterface` over a JSON file —
`@orkestrel/queue`'s `createDatabaseQueueStore` composed with `@orkestrel/database`'s
`createJSONDriver` — so a fresh store over the same path resumes a prior process's
outstanding work:

```ts
import { stringShape } from '@orkestrel/contract'
import { createJSONQueueStore } from '@src/server'

const store = createJSONQueueStore('data/worker.json', stringShape())
await store.save({ id: 'job-1', input: 'https://example.com', attempts: 0 })

// A later process — the entries persisted to the file are still outstanding:
const resumed = createJSONQueueStore('data/worker.json', stringShape())
const work = await resumed.load() // [{ id: 'job-1', input: 'https://example.com', attempts: 0 }]
```

Pass the resulting `store` to `createWorker`'s `store` option to wire it into a worker's
underlying queue — see [queue.md](queue.md) for the `QueueStoreInterface` contract, the
`save` / `remove` / `load` / `clear` semantics, and the durability guarantees.

## Observing

The `Worker` exposes a typed `emitter` (AGENTS §13) carrying the job lifecycle it
re-exposes from its underlying queue — logging, metrics, tracing. Subscribe via
`worker.emitter.on(...)`, or wire initial listeners through the reserved `on?` option;
supply an `error?` handler to receive a listener's throw.

```ts
import { createWorker } from '@src/core'

const worker = createWorker({
	pool: { create: () => connect() },
	handler: (job, connection) => run(job, connection),
	on: { drain: () => console.log('worker idle') }, // initial listener at construction
})

worker.emitter.on('success', (id, result) => metrics.record(id, result))
worker.emitter.on('failure', (id, error) => log.warn(`job ${id} failed`, error))
```

| Event map                 | Events                                                                                                                          |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `WorkerEventMap<TResult>` | `enqueue(id)` · `start(id)` · `retry(id, attempt)` · `success(id, result)` · `failure(id, error)` · `abort(reason)` · `drain()` |

See [queue.md](queue.md) / [pool.md](pool.md) for the underlying
`Queue` / `Pool` event vocabulary and the listener-isolation safety guarantee (the same
guarantee applies here — the Worker's bridge never throws, so a buggy worker observer can
never corrupt the inner queue or pool).

## Patterns

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

### CPU-parallel jobs over threads

```ts
import { createNodeWorker } from '@src/server'

const isNumber = (value: unknown): value is number => typeof value === 'number'

const worker = createNodeWorker({
	script: new URL('./double.ts', import.meta.url),
	input: isNumber,
	result: isNumber,
	concurrency: 4,
})

const doubled = await worker.enqueue(21) // 42
worker.destroy()
```

### Durable jobs across restarts

```ts
import { stringShape } from '@orkestrel/contract'
import { createWorker } from '@src/core'
import { createJSONQueueStore } from '@src/server'

const store = createJSONQueueStore('data/worker.json', stringShape())
const worker = createWorker({ store, pool: { create: () => connect() }, handler: run })

await worker.enqueue('https://example.com') // durably saved before it runs

// …after a crash + restart, a fresh worker over the same store resumes the work:
const resumed = createWorker({ store, pool: { create: () => connect() }, handler: run })
await resumed.restore() // re-enqueues every still-outstanding entry, then runs it
```

### Practices

- **Honour `execution.signal`** — pass it through to the resource's operation and bail
  out when it fires, so timeouts and aborts actually stop work rather than just
  abandoning its result.
- **Size the pool via `concurrency`** — the pool's `max` defaults to it; override
  `pool.max` explicitly only when the resource cap should diverge from the job cap.
- **`abort` is terminal** — a worker-level abort cancels in-flight work and stops the
  underlying queue; create a new worker to start over.
- **Observe, don't drive** — subscribe to `worker.emitter` for lifecycle moments (see
  [Observing](#observing)); emitting is a pure side-channel.
- **CPU-parallel work needs `createNodeWorker`** — the core `createWorker` runs its
  handler on the SAME event loop (a resource pool, not a thread pool); reach for
  `createNodeWorker` only when the work is genuinely CPU-bound and would otherwise
  block.

## Tests

- [`tests/guides/src/parity.test.ts`](../../tests/guides/src/parity.test.ts) — the
  `## Surface` ↔ `src/core` / `src/server` bijection (value + type exports) and the
  `WorkerInterface` ↔ `Worker` method bijection.
- [`tests/src/core/Worker.test.ts`](../../tests/src/core/Worker.test.ts) — the handler
  runs against a pooled resource; resources are reused across jobs and never exceed the
  pool max; the resource is released even when the handler throws (a later job reuses
  it); the lifecycle (`pause` / `resume` / `abort` / `stop` / `clear` / `destroy`)
  delegates to the queue; `destroy` tears the pool down; and durability passthrough — a
  `store` persists a job and `restore()` re-runs it against a fresh resource (delegated
  to the queue), a no-op without a store.
- [`tests/src/core/factories.test.ts`](../../tests/src/core/factories.test.ts) —
  `createWorker` returns a working, typed instance end to end and honours its options +
  status surface.
- [`tests/src/server/factories.test.ts`](../../tests/src/server/factories.test.ts) —
  `createJSONQueueStore` over a real temp file: entries persist ACROSS store instances on
  the same path (a second store `load`s the first's work), a nested-object input survives
  the JSON round-trip, and a `remove` is reflected across a reopen; plus a
  `createNodeWorker` round-trip smoke (a job over a real thread, then teardown).
- [`tests/src/server/helpers.test.ts`](../../tests/src/server/helpers.test.ts) — the
  main-side worker-thread machinery (`spawnThread` / `dispatch` / `isReply`), driven
  through `createNodeWorker` over REAL worker threads (no mocking): a round-trip and a
  batch over a small pool; the concurrency cap AND the live-thread cap + idle reuse; a
  throwing handler rejecting with its error, and re-running under `retries`; a
  per-attempt `timeout` rejecting AND terminating the uncooperative thread, with a later
  job served on a fresh thread; an in-flight signal abort + a mid-flight thread crash
  each evicting and replacing the thread; `workerData` cloned through to the worker side
  (and a non-cloneable `workerData` surfacing a clear error, never a hang); a large array
  input + result round-trip; a broken worker script rejecting the job cleanly with the
  pool recovering across retries + a fresh worker; a stray / foreign-id message ignored
  while the correct reply still resolves; an already-aborted enqueue signal
  short-circuiting; a reply that fails the `result` guard rejecting; a bad input
  fail-fasting before the boundary; `destroy` with multiple threads mid-job terminating
  all; rapid enqueue/abort churn settling every job with no thread leak; and `destroy`
  terminating every thread so the process exits.
- [`tests/src/server/serve.test.ts`](../../tests/src/server/serve.test.ts) —
  `serveWorker` driven MANUALLY over a raw `node:worker_threads` thread (post a run/abort
  envelope, await the reply): the success envelope, an input-guard rejection envelope, a
  handler-throw error envelope (SYNC and ASYNC rejections both reported as `{ ok: false
}`), a `{ command: 'abort' }` firing the handler's signal, an abort for an unknown id
  being a no-op, an unknown message `command` and a malformed (no-`id`) message both
  ignored without crashing the thread, and object / array / null / boolean result shapes
  round-tripping through the `{ ok: true, value }` envelope.
- The worker fixtures under
  [`tests/src/server/fixtures`](../../tests/src/server/fixtures) (`double` / `fail` /
  `slow` / `bad-result` / `abortable` / `crash` / `identify` / `echo-data` / `sum` /
  `stray` / `throw-async` / `load-throw` / `echo`) are real `.ts` worker scripts loaded
  by Node's type-stripping; each imports `serveWorker` by relative-to-source path and is
  exempt from the test mirror (not a `*.test.ts`, not under `src/`).

## See also

- [`queue.md`](queue.md) — the `Queue` engine a `Worker` composes (cooperative wake-park
  loop, retries, timeout, durability) — the `QueueStoreInterface` contract for
  `createJSONQueueStore` / the `store` option.
- [`pool.md`](pool.md) — the `Pool` engine a `Worker` composes (idle reuse, `max`
  backpressure, FIFO abort-able wait).
- [`contract.md`](contract.md) — the `Guard<T>` / shape vocabulary threaded through the
  structured-clone boundary (`input` / `result` on `createNodeWorker`).
- [`database.md`](database.md) — the storage layer `createJSONQueueStore` builds on.
- [`AGENTS.md`](../../AGENTS.md) — the rules; §10 lifecycle, §4.1 single-word members,
  §13 emitter pattern, §22 documentation-as-contracts.
- [`README.md`](../README.md) — the guides index.
