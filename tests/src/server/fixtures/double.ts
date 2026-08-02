// A real worker-thread script loaded natively by Node's type-stripping. Imports
// `serveWorker` by relative-to-source path because aliases do not resolve in a raw thread.
import { serveWorker } from '../../../../src/server/serve.ts'

// Doubles a number. The hand-written guard narrows the inbound payload with no `as`.
serveWorker({
	input: (value: unknown): value is number => typeof value === 'number',
	handler: (value) => value * 2,
})
