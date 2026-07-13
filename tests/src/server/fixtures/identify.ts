// @ts-nocheck — a real worker-thread script (see double.ts).
import { threadId } from 'node:worker_threads'
import { serveWorker } from '../../../../src/server/serve.ts'

// Echoes the thread's OWN `threadId` (read straight from `node:worker_threads`, since the
// handler runs inside the thread). A test counts the distinct ids across many jobs to prove
// the concurrency cap (at most `concurrency` threads ever spawn) and idle reuse (the same
// thread serves multiple sequential jobs once it is returned to idle). The input gates the
// job by a small busy-spin so several jobs genuinely overlap in flight at concurrency > 1.
serveWorker({
	input: (value) => typeof value === 'number',
	handler: (value) => {
		const deadline = Date.now() + value
		while (Date.now() < deadline) {
			// brief CPU spin so concurrent jobs co-reside on distinct threads
		}
		return threadId
	},
})
