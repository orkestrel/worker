// @ts-nocheck — a real worker-thread script (see double.ts).
import { serveWorker } from '../../../../src/server/serve.ts'

// A busy CPU loop that does NOT honour its abort signal — it spins for `value`
// milliseconds of wall-clock work. Proves the timeout/abort path TERMINATES the thread
// (the only way to stop uncooperative CPU-bound work) rather than waiting it out.
serveWorker({
	input: (value) => typeof value === 'number',
	handler: (value) => {
		const deadline = Date.now() + value
		// Intentionally ignores the signal — a tight spin loop.
		while (Date.now() < deadline) {
			// burn CPU
		}
		return value
	},
})
