import type { Reply } from './types.js'
import { attempt, isRecord } from '@orkestrel/contract'

// === The wire protocol (main ↔ thread)
//
// The reply half of the run/abort/reply protocol `serveWorker` answers — the leaf predicate
// a `Dispatch` filters inbound messages with. The envelope types ({@link Reply},
// `NodeThread`) live in `./types.js`; the public bridge across the
// structured-clone boundary is the `input` / `result` `Guard`s, which narrow the envelopes'
// opaque `unknown` payloads with no assertion. This file imports no
// implementation class, so it stays the bottom of the module's graph.

/**
 * Narrows an inbound `message` to a {@link Reply} for a given job `id` — no assertion.
 *
 * @remarks
 * A total predicate: a record whose `id` matches and whose `ok` discriminant is well-formed.
 * Anything else is rejected so a dispatch listener can ignore foreign or malformed messages.
 * It correlates against the `id` argument rather than narrowing one value alone, so it is a
 * correlated predicate rather than a `Guard<Reply>` and is not accepted where a `Guard` is.
 *
 * @param value - The inbound message to narrow
 * @param id - The job id a matching reply must carry
 * @returns True if the value is this job's well-formed reply; false otherwise
 */
export function isReply(value: unknown, id: string): value is Reply {
	const outcome = attempt(() => {
		if (!isRecord(value)) return false
		if (value.id !== id) return false
		if (value.ok === true) return 'value' in value
		return value.ok === false && typeof value.error === 'string'
	})
	return outcome.success && outcome.value
}
