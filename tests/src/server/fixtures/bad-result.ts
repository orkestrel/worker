// @ts-nocheck — a real worker-thread script (see double.ts).
import { serveWorker } from '../../../../../src/server/workers/serve.ts'

// Returns a STRING although the main side's `result` guard expects a number — proves a
// reply that fails the result guard rejects the job (the zero-`as` boundary in action).
serveWorker({
	input: (value) => typeof value === 'number',
	handler: (value) => `not-a-number:${value}`,
})
