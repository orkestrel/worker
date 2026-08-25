import type { PoolOptions } from '@orkestrel/pool'
import { describe, expect, it } from 'vitest'
import { createRecorder } from '@orkestrel/test'
import { PoolOptionsProbe, TestQueueStore } from './setup.js'

// tests/setup.ts — the environment-agnostic base setup. Proves the two exports the
// `src:core` (and every environment layered on top of it) suites rely on: `TestQueueStore`'s
// protocol-faithful hook/state contract, and `PoolOptionsProbe`'s getter-recording/`replace`
// contract. Behavior only this workspace can assert; production Queue/Pool behavior is proven
// by their own packages, never re-proven here.

describe('TestQueueStore', () => {
	it('records a saved entry, invokes its hook once, and defaults to no hooks', async () => {
		const calls: string[] = []
		const store = new TestQueueStore<string>({
			save: (entry) => {
				calls.push(`hook:${entry.id}`)
			},
		})
		await store.save({ id: 'a', input: 'x', attempts: 0 })
		expect(calls).toEqual(['hook:a'])
		expect(await store.load()).toEqual([{ id: 'a', input: 'x', attempts: 0 }])

		// Control: a store built with no hooks object at all still saves without throwing —
		// each hook call is optional-chained, not required.
		const hookless = new TestQueueStore<number>()
		await hookless.save({ id: 'b', input: 1, attempts: 0 })
		expect(await hookless.load()).toEqual([{ id: 'b', input: 1, attempts: 0 }])
	})

	it('invokes remove/clear hooks and mutates the in-memory record to match', async () => {
		const calls: string[] = []
		const store = new TestQueueStore<string>({
			remove: (id) => {
				calls.push(`remove:${id}`)
			},
			clear: () => {
				calls.push('clear')
			},
		})
		await store.save({ id: 'a', input: 'x', attempts: 0 })
		await store.save({ id: 'b', input: 'y', attempts: 0 })

		await store.remove('a')
		expect(await store.load()).toEqual([{ id: 'b', input: 'y', attempts: 0 }])

		await store.clear()
		expect(await store.load()).toEqual([])
		expect(calls).toEqual(['remove:a', 'clear'])
	})
})

describe('PoolOptionsProbe', () => {
	it('records each getter access once, in property order, and returns the configured value', () => {
		const reads = createRecorder<readonly [property: keyof PoolOptions<number>]>()
		const create = (): number => 7
		const destroy = (): void => undefined
		const validate = (): boolean => true
		const on = {}
		const error = (): void => undefined
		const probe = new PoolOptionsProbe<number>(
			{ max: 3, on, error, create, destroy, validate },
			reads,
		)

		expect(probe.max).toBe(3)
		expect(probe.on).toBe(on)
		expect(probe.error).toBe(error)
		expect(probe.create).toBe(create)
		expect(probe.destroy).toBe(destroy)
		expect(probe.validate).toBe(validate)
		expect(reads.calls).toEqual([['max'], ['on'], ['error'], ['create'], ['destroy'], ['validate']])
	})

	it('replace swaps the values every subsequent getter read returns', () => {
		const reads = createRecorder<readonly [property: keyof PoolOptions<number>]>()
		const probe = new PoolOptionsProbe<number>(
			{
				max: 1,
				on: {},
				error: (): void => undefined,
				create: (): number => 0,
				destroy: (): void => undefined,
				validate: (): boolean => true,
			},
			reads,
		)
		probe.replace({
			max: 9,
			on: {},
			error: (): void => undefined,
			create: (): number => 0,
			destroy: (): void => undefined,
			validate: (): boolean => false,
		})

		expect(probe.max).toBe(9)
		expect(probe.validate(0)).toBe(false)
	})
})
