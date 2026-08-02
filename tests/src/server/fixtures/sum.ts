// A real worker-thread script. Sums a numeric array, so a
// LARGE / deep input and its numeric result both cross the structured-clone boundary —
// stressing the clone path beyond a single scalar.
import { serveWorker } from '../../../../src/server/serve.ts'

serveWorker({
	input: (value: unknown): value is readonly number[] =>
		Array.isArray(value) && value.every((entry) => typeof entry === 'number'),
	handler: (value) => value.reduce((total, entry) => total + entry, 0),
})
