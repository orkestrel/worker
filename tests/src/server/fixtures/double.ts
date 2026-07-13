// @ts-nocheck — a real worker-thread script loaded natively by Node's type-stripping
// (not part of the typechecked library). Imports `serveWorker` by relative-to-SOURCE
// path (no `@src` alias inside a raw thread) — depth verified to resolve at runtime.
import { serveWorker } from '../../../../src/server/serve.ts'

// Doubles a number. The hand-written guard narrows the inbound payload with no `as`.
serveWorker({
	input: (value) => typeof value === 'number',
	handler: (value) => value * 2,
})
