// A protocol-faithful real worker fixture that observes both internal run-envelope ids.
// It speaks the minimal raw protocol necessary because `serveWorker` intentionally exposes
// only the stable Queue execution id, never the per-dispatch correlation id.
import { isRecord } from '@orkestrel/contract'
import { parentPort } from 'node:worker_threads'

const port = parentPort
if (port === null) throw new Error('worker parent port is unavailable')

let correlation: string | undefined
let job: string | undefined

port.on('message', (raw: unknown) => {
	if (
		!isRecord(raw) ||
		typeof raw.id !== 'string' ||
		typeof raw.job !== 'string' ||
		raw.command !== 'run' ||
		typeof raw.input !== 'number'
	) {
		return
	}
	if (correlation === undefined || job === undefined) {
		correlation = raw.id
		job = raw.job
		port.postMessage({ id: raw.id, ok: false, error: 'retry identity probe' })
		return
	}
	port.postMessage({
		id: raw.id,
		ok: true,
		value: { correlation: raw.id !== correlation, job: raw.job === job },
	})
})
