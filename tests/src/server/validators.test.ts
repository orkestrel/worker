import { describe, expect, it } from 'vitest'
import { isReply } from '@src/server'

describe('isReply — the reply-envelope guard dispatch filters on', () => {
	const id = 'job-1'

	it('accepts a well-formed success reply for the id (any value, including falsy)', () => {
		expect(isReply({ id, ok: true, value: 42 }, id)).toBe(true)
		expect(isReply({ id, ok: true, value: 0 }, id)).toBe(true)
		expect(isReply({ id, ok: true, value: undefined }, id)).toBe(true)
		expect(isReply({ id, ok: true, value: null }, id)).toBe(true)
	})

	it('accepts a well-formed failure reply for the id (string error)', () => {
		expect(isReply({ id, ok: false, error: 'boom' }, id)).toBe(true)
	})

	it('rejects a success envelope without its required value', () => {
		expect(isReply({ id, ok: true }, id)).toBe(false)
	})

	it('rejects a reply whose id does not match (a foreign job)', () => {
		expect(isReply({ id: 'other', ok: true, value: 1 }, id)).toBe(false)
		expect(isReply({ id: 'other', ok: false, error: 'x' }, id)).toBe(false)
	})

	it('rejects a failure whose error is not a string (malformed payload)', () => {
		expect(isReply({ id, ok: false, error: 7 }, id)).toBe(false)
		expect(isReply({ id, ok: false }, id)).toBe(false)
	})

	it('rejects a malformed ok discriminant (neither true nor false)', () => {
		expect(isReply({ id, ok: 'yes', value: 1 }, id)).toBe(false)
		expect(isReply({ id, value: 1 }, id)).toBe(false)
	})

	it('rejects non-records and stray messages (no id) — total, never throws', () => {
		expect(isReply(null, id)).toBe(false)
		expect(isReply(undefined, id)).toBe(false)
		expect(isReply('reply', id)).toBe(false)
		expect(isReply(42, id)).toBe(false)
		expect(isReply([id], id)).toBe(false)
		expect(isReply({ ok: true, value: 1 }, id)).toBe(false)
	})

	it('contains hostile property getters and returns false', () => {
		expect(
			isReply(
				{
					get id(): string {
						throw new Error('hostile id')
					},
				},
				id,
			),
		).toBe(false)
		expect(
			isReply(
				{
					id,
					get ok(): boolean {
						throw new Error('hostile ok')
					},
				},
				id,
			),
		).toBe(false)
		expect(
			isReply(
				{
					id,
					ok: false,
					get error(): string {
						throw new Error('hostile error')
					},
				},
				id,
			),
		).toBe(false)
	})
})
