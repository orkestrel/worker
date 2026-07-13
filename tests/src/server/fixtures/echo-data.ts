// @ts-nocheck — a real worker-thread script (see double.ts).
import { workerData } from 'node:worker_threads'
import { serveWorker } from '../../../../../src/server/workers/serve.ts'

// Ignores the per-job input and replies with the `workerData` cloned to the thread once at
// spawn (read directly from `node:worker_threads` — the worker is already in a thread). Proves
// `createNodeWorker`'s `workerData` option reaches the worker side intact across the structured
// clone. The `result` guard on the main side narrows whatever shape `workerData` carries.
serveWorker({
	input: (value) => typeof value === 'number',
	handler: () => workerData,
})
