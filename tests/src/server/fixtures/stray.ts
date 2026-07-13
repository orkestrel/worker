// @ts-nocheck — a real worker-thread script (see double.ts). Uses `parentPort` directly to
// post a STRAY message BEFORE the normal reply, proving the main side's `isReply` guard
// ignores foreign / malformed payloads (no `id`, then a non-matching `id`) and still resolves
// the correct reply for the job. The stray posts run inside the handler so they precede the
// real reply on the same message channel.
import { parentPort } from 'node:worker_threads'
import { serveWorker } from '../../../../../src/server/workers/serve.ts'

serveWorker({
	input: (value) => typeof value === 'number',
	handler: (value) => {
		// A message with NO `id` — `isReply` rejects it (id mismatch) and the listener ignores it.
		parentPort.postMessage({ noise: true })
		// A well-formed-looking reply for a DIFFERENT id — also ignored (wrong id).
		parentPort.postMessage({ id: 'someone-elses-job', ok: true, value: -999 })
		// The real result follows; `serveWorker` posts it under the correct id, so it resolves.
		return value * 2
	},
})
