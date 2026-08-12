// A real worker-thread script exposing the Queue execution id received by `serveWorker`.
import { getEventListeners } from 'node:events'
import { parentPort, workerData } from 'node:worker_threads'
import { serveWorker } from '../../../../src/server/handlers.ts'

class ThrowingRun {
	readonly id = 'hostile-job'
	readonly command = 'run'
	readonly input = 1

	get job(): string {
		throw new Error('hostile job getter')
	}
}

const port = parentPort
if (port === null) throw new Error('worker parent port is unavailable')

if (workerData !== undefined && !(workerData instanceof SharedArrayBuffer)) {
	throw new Error('execution fixture requires shared counters when worker data is supplied')
}

const handled = workerData === undefined ? undefined : new Int32Array(workerData)
const existing = new Set(getEventListeners(port, 'message'))

serveWorker({
	input: (value: unknown): value is number => typeof value === 'number',
	handler: (_value, execution) => {
		if (handled !== undefined) Atomics.add(handled, 0, 1)
		return execution.id
	},
})

if (handled !== undefined) {
	const revoked = Proxy.revocable({}, {})
	revoked.revoke()
	for (const listener of getEventListeners(port, 'message')) {
		if (existing.has(listener)) continue
		Reflect.apply(listener, port, [revoked.proxy])
		Reflect.apply(listener, port, [new ThrowingRun()])
	}
}
