// @ts-nocheck — a real worker-thread script (see double.ts). Cooperative: it AWAITS its
// abort signal and resolves the sentinel `-1` once it fires, so a manually-driven test
// (serve.test.ts) can post `{ command: 'abort' }` and observe the handler's signal react.
import { serveWorker } from '../../../../../src/server/workers/serve.ts'

serveWorker({
	input: (value) => typeof value === 'number',
	handler: (value, { signal }) =>
		new Promise((resolve) => {
			if (signal.aborted) {
				resolve(-1)
				return
			}
			signal.addEventListener('abort', () => resolve(-1), { once: true })
		}),
})
