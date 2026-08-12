// A real worker-thread script. Cooperative: it awaits its abort signal and resolves the
// sentinel `-1` once it fires, so a manually-driven test can observe the handler react.
import { serveWorker } from '../../../../src/server/handlers.ts'

serveWorker({
	input: (value: unknown): value is number => typeof value === 'number',
	handler: (_value, { signal }) =>
		new Promise<number>((resolve) => {
			if (signal.aborted) {
				resolve(-1)
				return
			}
			signal.addEventListener('abort', () => resolve(-1), { once: true })
		}),
})
