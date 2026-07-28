import type { Reply } from './types.js'
import { attempt, isRecord } from '@orkestrel/contract'

/**
 * Narrow an inbound `message` to a {@link Reply} for a given job `id` — no assertion.
 *
 * @remarks
 * A total predicate: a record whose `id` matches and whose `ok` discriminant is well-formed.
 * Anything else is rejected so a dispatch listener can ignore foreign or malformed messages.
 *
 * @param value - The inbound message to narrow
 * @param id - The job id a matching reply must carry
 * @returns `true` when the value is this job's well-formed reply
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
