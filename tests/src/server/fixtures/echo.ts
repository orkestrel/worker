// A real worker-thread script. Accepts any input through a permissive guard and replies
// with it unchanged, so a manually-driven serve test can
// post various result SHAPES (object, array, null, boolean) and assert each round-trips
// through the `{ ok: true, value }` reply envelope intact.
import { serveWorker } from '../../../../src/server/serve.ts'

serveWorker({
	input: (_value: unknown): _value is unknown => true,
	handler: (value) => value,
})
