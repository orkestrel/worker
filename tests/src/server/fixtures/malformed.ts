// A real worker-thread protocol fixture. It intentionally answers a negative input with
// one matching-id malformed reply and stays alive, so only main-side eviction can prevent
// that tainted thread from handling later work. A replacement thread handles positive input.
import { isRecord } from '@orkestrel/contract'
import { parentPort } from 'node:worker_threads'

const port = parentPort
if (port === null) throw new Error('worker parent port is unavailable')

let tainted = false
port.on('message', (message: unknown) => {
	if (
		!isRecord(message) ||
		typeof message.id !== 'string' ||
		message.command !== 'run' ||
		typeof message.input !== 'number'
	) {
		return
	}
	if (message.input < 0) {
		tainted = true
		port.postMessage({ id: message.id, ok: 'malformed' })
		return
	}
	if (tainted) {
		port.postMessage({ id: message.id, ok: false, error: 'tainted thread was reused' })
		return
	}
	port.postMessage({ id: message.id, ok: true, value: message.input * 2 })
})
