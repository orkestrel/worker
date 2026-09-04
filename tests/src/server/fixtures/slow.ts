// A real worker-thread script.
import { serveWorker } from '../../../../src/server/handlers.ts'

// A busy CPU loop that does NOT honour its abort signal — it spins for `value`
// milliseconds of wall-clock work. Proves the timeout/abort path TERMINATES the thread
// (the only way to stop uncooperative CPU-bound work) rather than waiting it out.
serveWorker({
	input: (value: unknown): value is number => typeof value === 'number',
	handler: (value) => {
		const deadline = performance.now() + value
		// Intentionally ignores the signal — a tight spin loop.
		while (performance.now() < deadline) {
			// burn CPU
		}
		return value
	},
})
