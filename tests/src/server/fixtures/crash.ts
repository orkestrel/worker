// @ts-nocheck — a real worker-thread script (see double.ts).
import { serveWorker } from '../../../../../src/server/workers/serve.ts'

// CRASHES the thread mid-flight (not a handler throw): a NEGATIVE input calls
// `process.exit(1)`, killing the thread WITHOUT a reply — so the parent's `ThreadWorker`
// emits `'exit'` while the job is in flight and hits `dispatch`'s `onExit` (the thread is
// marked dead + evicted), distinct from `fail.ts`'s normal `{ ok: false }` error reply. A
// non-negative input doubles as usual, so a SUBSEQUENT job proves the pool spun up a fresh
// thread after the crashed one was evicted.
serveWorker({
	input: (value) => typeof value === 'number',
	handler: (value) => {
		if (value < 0) process.exit(1)
		return value * 2
	},
})
