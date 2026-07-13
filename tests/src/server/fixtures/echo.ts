// @ts-nocheck — a real worker-thread script (see double.ts). Accepts ANY input (a
// permissive guard) and replies with it unchanged — so a manually-driven serve test can
// post various result SHAPES (object, array, null, boolean) and assert each round-trips
// through the `{ ok: true, value }` reply envelope intact.
import { serveWorker } from '../../../../../src/server/workers/serve.ts'

serveWorker({
	input: () => true,
	handler: (value) => value,
})
