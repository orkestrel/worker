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
structured-clone boundary with zero `as` via `input` / `result` guards. Part
of the `@orkestrel` line.

## Install

```sh
npm install @orkestrel/worker
```

## Requirements

- Node.js >= 24
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
worker.destroy() // tears down the queue, then the pool
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
```

## Guide

For the full surface — the `Worker` facade, `createNodeWorker` / `serveWorker`,
the durable `createJSONQueueStore`, the observable `emitter`, and usage
patterns — see [`guides/src/worker.md`](guides/src/worker.md).

## Package

Published with two entry points per the `exports` field in `package.json`:
the environment-agnostic core (`.`) and the Node-only server surface
(`./server`).

## License

MIT © [Orkestrel](https://github.com/orkestrel) — see [LICENSE](./LICENSE).
