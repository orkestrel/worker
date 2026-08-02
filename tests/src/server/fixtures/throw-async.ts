// A real worker-thread script. The handler rejects
// ASYNCHRONOUSLY (a rejected promise after a microtask), distinct from `fail.ts`'s
// SYNCHRONOUS throw — proving `serveWorker` reports a rejected async handler as an
// `{ ok: false, error }` reply (not an unhandled rejection / thread crash) just as it does a
// sync throw.
import { serveWorker } from '../../../../src/server/serve.ts'

serveWorker({
	input: (value: unknown): value is number => typeof value === 'number',
	handler: async (value) => {
		await Promise.resolve()
		throw new Error(`async-boom:${value}`)
	},
})
