// A runnable worker entry that exposes handler aborts through shared counters.
import { workerData } from 'node:worker_threads'
import { serveWorker } from '../../../../src/server/handlers.ts'

if (!(workerData instanceof SharedArrayBuffer)) throw new Error('shared counters are required')
const counters = new Int32Array(workerData)

serveWorker({
	input: (value: unknown): value is number => typeof value === 'number',
	handler: (value, { signal }) =>
		new Promise<number>((resolve) => {
			signal.addEventListener(
				'abort',
				() => {
					Atomics.add(counters, value, 1)
					resolve(-1)
				},
				{ once: true },
			)
			if (value !== 1 && value !== 2) resolve(value)
		}),
})
