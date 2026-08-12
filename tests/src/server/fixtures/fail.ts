// A real worker-thread script.
import { serveWorker } from '../../../../src/server/handlers.ts'

// Always throws — proves a handler rejection surfaces as an error reply (and retries).
serveWorker({
	input: (value: unknown): value is number => typeof value === 'number',
	handler: (value) => {
		throw new Error(`boom:${value}`)
	},
})
