// A real worker-thread script whose negative-input result cannot be structured-cloned.
// A later non-negative input returns a clone-safe number on the same thread.
import { serveWorker } from '../../../../src/server/handlers.ts'

serveWorker({
	input: (value: unknown): value is number => {
		if (value === 'throw') throw new Error('input-guard-boom')
		return typeof value === 'number'
	},
	handler: (value) => (value < 0 ? { execute: () => undefined } : value * 2),
})
