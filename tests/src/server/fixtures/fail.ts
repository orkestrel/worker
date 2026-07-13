// @ts-nocheck — a real worker-thread script (see double.ts).
import { serveWorker } from '../../../../src/server/serve.ts'

// Always throws — proves a handler rejection surfaces as an error reply (and retries).
serveWorker({
	input: (value) => typeof value === 'number',
	handler: (value) => {
		throw new Error(`boom:${value}`)
	},
})
