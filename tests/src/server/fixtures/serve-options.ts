import { workerData } from 'node:worker_threads'
import { serveWorker } from '../../../../src/server/handlers.ts'

if (!(workerData instanceof SharedArrayBuffer)) {
	throw new Error('serve option fixture requires shared counters')
}

const counters = new Int32Array(workerData)

serveWorker<number, number>({
	get input() {
		const reads = Atomics.add(counters, 0, 1) + 1
		if (Atomics.load(counters, 2) === 1) throw new Error('input-getter-boom')
		if (reads === 1) return (value: unknown): value is number => typeof value === 'number'
		return (_value: unknown): _value is number => false
	},
	get handler() {
		const reads = Atomics.add(counters, 1, 1) + 1
		if (reads === 1) return (value: number): number => value * 2
		return (value: number): number => -value
	},
})
