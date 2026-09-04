# @orkestrel/worker

A typed, resource-backed **job worker** for the `@orkestrel` line: a `Worker`
is a `Queue` (`@orkestrel/queue`) whose handler runs against an automatically
acquired resource leased from a `Pool` (`@orkestrel/pool`) — released when the
job settles, even on throw. Composition, not reimplementation: all
concurrency, retries, per-attempt timeout, abort, and durability are the
Queue's; all idle reuse and `max` backpressure are the Pool's. The worker is
observable (a typed `emitter` re-exposes the underlying queue's job lifecycle
— `enqueue` / `start` / `retry` / `success` / `failure` / `abort` / `drain`).
For CPU-parallel work, the server surface's `createNodeWorker` specializes the
core `createWorker` over a pool of `node:worker_threads`, crossing the
structured-clone boundary with zero `as` through `input` / `result` guards. Each
thread handler receives `{ id, signal }`: `id` is the Queue's stable idempotency
key across retries and crash restore, while `signal` is per attempt. The wire
protocol separately mints a fresh correlation id for each dispatch so a stale
reply cannot settle a later retry. The stable id identifies work, not a caller,
and is not authentication or authorization evidence; per-job consumer context
is explicit, structured-cloneable input rather than ambient thread state.
Part of the `@orkestrel` line.

## Install

```sh
npm install @orkestrel/worker
```

## Requirements

The package runs under these conditions:

- Node.js >= 22.12.0
- ESM and CommonJS builds ship for both the core and server entry points

## Usage

```ts
import { createWorker } from '@orkestrel/worker'

const worker = createWorker<Query, Connection, Rows>({
	pool: { create: () => connect(), destroy: (connection) => connection.close() },
	handler: (query, connection, { signal }) => connection.run(query, signal),
	concurrency: 4, // up to four jobs in flight; the pool defaults its `max` to match
	retries: 1,
})

const rows = await worker.enqueue(query)
await worker.destroy() // awaits queue cleanup, then pool cleanup, then emitter teardown
```

CPU-parallel jobs over `node:worker_threads`:

```ts
import { createNodeWorker } from '@orkestrel/worker/server'

const isNumber = (value: unknown): value is number => typeof value === 'number'

const worker = createNodeWorker({
	script: new URL('./double.js', import.meta.url),
	input: isNumber,
	result: isNumber,
	concurrency: 4,
})

const doubled = await worker.enqueue(21) // 42, computed on a worker thread
await worker.destroy()
```

## Guide

For the full surface — the `Worker` facade, `createNodeWorker` / `serveWorker`,
the durable `createJSONQueueStore`, the observable `emitter`, and usage
patterns — see [`guides/worker.md`](guides/worker.md).

## Package

Published with the entry points the `exports` field in `package.json` names:
the environment-agnostic core (`.`) and the Node-only server surface
(`./server`).

## License

MIT © [Orkestrel](https://github.com/orkestrel) — see [LICENSE](./LICENSE).
