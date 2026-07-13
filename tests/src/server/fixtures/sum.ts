// @ts-nocheck — a real worker-thread script (see double.ts). Sums a numeric array, so a
// LARGE / deep input and its numeric result both cross the structured-clone boundary —
// stressing the clone path beyond a single scalar.
import { serveWorker } from '../../../../src/server/serve.ts'

serveWorker({
	input: (value) => Array.isArray(value) && value.every((entry) => typeof entry === 'number'),
	handler: (value) => value.reduce((total, entry) => total + entry, 0),
})
