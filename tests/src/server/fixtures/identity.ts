// A protocol-faithful real worker fixture that observes both internal run-envelope ids.
// It speaks the minimal raw protocol necessary because `serveWorker` intentionally exposes
// only the stable Queue execution id, never the per-dispatch correlation id.
import type { MessagePort } from 'node:worker_threads'
import { isRecord } from '@orkestrel/contract'
import { parentPort } from 'node:worker_threads'

class IdentityThread {
	readonly #port: MessagePort
	#correlation: string | undefined
	#job: string | undefined

	constructor(port: MessagePort) {
		this.#port = port
		port.on('message', this.#receive.bind(this))
	}

	#receive(raw: unknown): void {
		if (
			!isRecord(raw) ||
			typeof raw.id !== 'string' ||
			typeof raw.job !== 'string' ||
			raw.command !== 'run' ||
			typeof raw.input !== 'number'
		) {
			return
		}
		if (this.#correlation === undefined || this.#job === undefined) {
			this.#correlation = raw.id
			this.#job = raw.job
			this.#port.postMessage({ id: raw.id, ok: false, error: 'retry identity probe' })
			return
		}
		this.#port.postMessage({
			id: raw.id,
			ok: true,
			value: { correlation: raw.id !== this.#correlation, job: raw.job === this.#job },
		})
	}
}

const port = parentPort
if (port === null) throw new Error('worker parent port is unavailable')
const thread = new IdentityThread(port)
void thread
