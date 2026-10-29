// A real worker-thread protocol fixture. It intentionally answers a negative input with
// one matching-id malformed reply and stays alive, so only main-side eviction can prevent
// that tainted thread from handling later work. A replacement thread handles positive input.
import type { MessagePort } from 'node:worker_threads'
import { isRecord } from '@orkestrel/contract'
import { parentPort } from 'node:worker_threads'

class MalformedThread {
	readonly #port: MessagePort
	#tainted = false

	constructor(port: MessagePort) {
		this.#port = port
		port.on('message', this.#receive.bind(this))
	}

	#receive(message: unknown): void {
		if (
			!isRecord(message) ||
			typeof message.id !== 'string' ||
			message.command !== 'run' ||
			typeof message.input !== 'number'
		) {
			return
		}
		if (message.input < 0) {
			this.#tainted = true
			this.#port.postMessage({ id: message.id, ok: 'malformed' })
			return
		}
		if (this.#tainted) {
			this.#port.postMessage({ id: message.id, ok: false, error: 'tainted thread was reused' })
			return
		}
		this.#port.postMessage({ id: message.id, ok: true, value: message.input * 2 })
	}
}

const port = parentPort
if (port === null) throw new Error('worker parent port is unavailable')
const thread = new MalformedThread(port)
void thread
