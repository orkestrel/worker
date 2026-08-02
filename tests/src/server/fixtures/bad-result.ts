// A real worker-thread script.
import { serveWorker } from '../../../../src/server/serve.ts'

// Returns a STRING although the main side's `result` guard expects a number — proves a
// reply that fails the result guard rejects the job (the zero-`as` boundary in action).
serveWorker({
	input: (value: unknown): value is number => typeof value === 'number',
	handler: (value) => `not-a-number:${value}`,
})
