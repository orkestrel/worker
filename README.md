# @orkestrel/worker

> A resource-backed job worker: a thin facade composing a `Queue` (`@orkestrel/queue`) with
> a `Pool` (`@orkestrel/pool`), where each job's handler runs against an automatically
> acquired pooled resource released when the job settles.

Create a worker with the `createWorker` function, give it the pool's `create` and
`destroy` plus the handler each job runs, then `enqueue` inputs and await their
results. Subscribe to the typed `emitter` for the job lifecycle the worker
re-exposes from its underlying queue. Reach for the server surface's
`createNodeWorker` where the work is CPU-bound: it specializes `createWorker`
over a pool of `node:worker_threads` and crosses the structured-clone boundary
through `input` and `result` guards with no `as`. A thread handler receives
`{ id, signal }`: `id` is the Queue's stable idempotency key across retries and
crash restore, and `signal` is per attempt. That id identifies work, not a
caller, and is not authentication or authorization evidence.
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
