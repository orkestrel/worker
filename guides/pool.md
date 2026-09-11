# Pool

> A typed resource pool with optional bounded capacity, unique ownership, FIFO settlement,
> validated reuse, caller-owned cancellation, explicit cleanup failures, and a stable
> event-driven teardown barrier.

The pool has no warm floor, eviction timer, acquire timeout, or polling loop. Every wait parks
on a promise or a signal listener and wakes when a settlement reaches it, and the lifecycle
hooks are the caller's, so the engine itself performs no I/O.

## Surface

`createPool` constructs the interface-oriented form; `Pool` exposes the same contract as a
class. Each created value receives an opaque ownership record, so duplicate primitives,
`undefined`, `NaN`, and repeated references are independent resources.

### Create a pool

Construct a pool from create, destroy, and validate hooks, then acquire and release one token:

```ts
import { createPool } from '@orkestrel/pool'

const pool = createPool<Connection>({
	create: () => connect(),
	destroy: (connection) => connection.close(),
	validate: (connection) => connection.alive,
	max: 8,
})

const token = await pool.acquire()
try {
	await token.value.query('select 1')
} finally {
	token.release()
}
```

### Factories

| API          | Kind     | Summary                                                                                                                                  |
| ------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `createPool` | function | Creates a distinct `PoolInterface` from resource lifecycle hooks, with optional bounded capacity, unique ownership, and FIFO settlement. |

### Classes

| API         | Kind  | Summary                                                                                                                                                                                  |
| ----------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Pool`      | class | Represents a capacity-aware resource pool whose opaque ownership records preserve FIFO settlement, cancellation, exact lease release, and deterministic teardown under concurrent hooks. |
| `PoolError` | class | Represents a stable, machine-readable pool failure that retains the original thrown value as its cause without unsafe coercion, alongside structured context.                            |

### Guards

In a guard table a `Shape` cell holds the type the guard narrows to.

| API            | Kind     | Shape         | Summary                                                                                                          |
| -------------- | -------- | ------------- | ---------------------------------------------------------------------------------------------------------------- |
| `isPoolError`  | function | `PoolError`   | Tests whether an unknown value is a `PoolError`, returning `false` for hostile proxies.                          |
| `isPoolMax`    | function | `number`      | Tests whether a value is a positive safe integer, the only valid explicit pool maximum.                          |
| `isPoolSignal` | function | `AbortSignal` | Tests whether a value is a native `AbortSignal` for the acquire boundary, returning `false` for hostile proxies. |

### Types

A `Shape` cell holds an interface's data members as bare names in braces, `?` marking an optional member and `plus` introducing its call-signature members, and a type alias's own type literal with a union's arms escaped as `\|`.

| API                | Kind      | Shape                                                          | Summary                                                                                                                                                     |
| ------------------ | --------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PoolCode`         | type      | `'invalid' \| 'destroyed' \| 'create' \| 'cleanup'`            | Names the machine-readable failure codes produced by `PoolError`.                                                                                           |
| `PoolContext`      | interface | `{ value?, failures? }`                                        | Represents the structured context attached to a `PoolError`: the rejected input, or the distinct destroy-hook failures an aggregate cleanup collected.      |
| `PoolErrorOptions` | interface | `{ code, cause?, context? }`                                   | Represents the construction options for `PoolError`: the stable code, an optional cause, and optional structured context.                                   |
| `PoolEventMap`     | type      | `{ create, acquire, release, destroy }`                        | Represents the observable resource lifecycle events emitted by a `PoolInterface`.                                                                           |
| `PoolToken`        | interface | `{ value } plus release`                                       | Represents a unique lease over one pool-owned resource record, exposing that record as a readonly `value` and returning it through an idempotent `release`. |
| `PoolOptions`      | interface | `{ on?, error?, create, destroy?, validate?, max? }`           | Represents the resource lifecycle options for `Pool` and `createPool`: creation, destruction, validation, capacity, and observation.                        |
| `PoolInterface`    | interface | `{ emitter, size, idle, active } plus acquire, clear, destroy` | Represents a FIFO resource pool with optional bounded capacity and deterministic teardown, exposing its record counts and a typed lifecycle emitter.        |

`size` counts every owned record, including records being validated or destroyed. `idle`
counts only immediately available records. `active` counts only leased records. An in-flight
create reservation claims capacity but is not yet an owned record and therefore is not part
of `size`.

## Methods

The public call-signature members of `PoolInterface` and `PoolToken`; `Pool` implements the
`PoolInterface` list exactly.

#### `PoolInterface`

| Method    | Returns                 | Summary                                                                                                                   |
| --------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `acquire` | `Promise<PoolToken<T>>` | Queues the caller in FIFO order, validates an idle record or creates one, and settles queued acquires in that same order. |
| `clear`   | `Promise<void>`         | Destroys the records that are idle at this call's synchronous snapshot.                                                   |
| `destroy` | `Promise<void>`         | Tears down the pool permanently and returns its stable completion barrier.                                                |

#### `PoolToken`

The lease returned by `acquire`, with the operation that returns its record.

| Method    | Returns | Summary                                                                                                             |
| --------- | ------- | ------------------------------------------------------------------------------------------------------------------- |
| `release` | `void`  | Gives this exact record back to the pool once; a repeat call, and a call after teardown took ownership, are no-ops. |

## Contract

### Capacity and FIFO

Every `acquire` receives its queue position before a create or validation hook starts. The
reentrancy-safe pump may assign several hook operations concurrently, but a head commit
barrier settles successes and failures in request order. A later fast create or validation
cannot overtake an earlier slow one. Capacity obeys:

```text
owned records + create reservations <= max
```

`max` must be a positive safe integer. Omit it for an unbounded pool; `Infinity`, fractions,
zero, negative values, and unsafe integers are invalid. Construction snapshots `max` once and
validates it before retention, then snapshots `on` and `error` once each.

Record phases are disjoint:

```text
create reservation -> ready -> leased -> available -> validating -> ready
                                      \-> destroying -> removed
```

Invalid validation, whether `false` or a thrown value, claims and cleans the record before a
replacement capacity slot becomes available. Cleanup failure rejects that acquire with
`PoolError` code `cleanup`; successful cleanup lets the same FIFO waiter seek a replacement.
The bound outcome and replacement eligibility are established before synchronous `destroy`
observers can reenter acquisition. A create failure rejects its bound acquire with code
`create` and never strands later waiters.

### Cancellation

`acquire` validates a native `AbortSignal` before queueing. An invalid signal throws a
code-`invalid` `PoolError` synchronously instead of returning a rejected promise. A
pre-aborted signal and every later abort preserve the caller's exact `signal.reason`. The
listener is attached, recorded, and followed by an aborted-state recheck, then detached on
every settlement. Aborting while create or validation work is assigned removes the waiter
exactly once; any late resource is returned to the live pump or cleaned during teardown. A
ready result waiting behind a slower head can likewise be aborted without leaking its record.
If cancelled validation later proves the record invalid and its cleanup fails, the acquire
still preserves the caller's abort reason while the cleanup failure is retained for the
eventual `destroy()` barrier.

### Release and cleanup

A token captures its exact opaque record, so `release()` is correct even when multiple
records contain the same value. Release is idempotent and removes the lease synchronously.
With a waiter, the record is validated before handoff. Without an assignable waiter, it
becomes idle and emits `release`. Release after teardown ownership transferred is a no-op.

`clear()` synchronously snapshots idle records and installs one cleanup promise per record
before invoking the hook. Concurrent clears therefore own disjoint snapshots, and a lease
released after a snapshot is taken is not part of it. Every claimed record stays in `size` until its
hook attempt completes. Distinct failures are aggregated in a code-`cleanup` `PoolError`
whose `context.failures` retains the original thrown values.
Each claimed record's cleanup settlement independently wakes queued acquires after the
destroy ledger transition, whether its cleanup hook succeeded or failed; a failed `clear()`
still rejects its own aggregate cleanup barrier.

### Destruction

`destroy()` is deliberately non-`async`: it installs and returns its exact promise before it
rejects waiters, emits events, or invokes cleanup. Reentrant and repeated calls return that
same object. Teardown invalidates idle, leased, and ready records, waits for create,
validation, existing clear cleanup, and new cleanup activity, and disposes every late
resource. Unresolved hooks keep the barrier pending without polling.

Cleanup already owned by an overlapping `clear()` is shared; its failure is reported to both
the clear call and the destroy aggregate. Create failures that produced no resource do not
fail destruction. The emitter is destroyed last, after every resource `destroy` event and
hook attempt, then the stable barrier resolves or rejects.

### Errors

`PoolError.code` is stable and lowercase:

| Code        | Owner                                                                    |
| ----------- | ------------------------------------------------------------------------ |
| `invalid`   | Invalid explicit `max`, invalid acquire signal, or an internal boundary. |
| `destroyed` | Acquire or clear attempted after terminal teardown began.                |
| `create`    | The create hook failed for its bound acquire.                            |
| `cleanup`   | Invalid-handoff, clear, or destroy cleanup failed.                       |

The original thrown value is retained as `cause`; aggregate cleanup values are also in
`context.failures`. Message construction and `isPoolError` avoid unsafe string coercion and
return safely for hostile proxies.

## Observing

The composed `Emitter` isolates listener throws through the optional `error` handler and
invokes listeners synchronously at each emission point. The `destroy` emission is deliberately
one microtask after cleanup settlement so bound outcomes precede observer reentry. Events
follow their ledger transitions:

| Event     | Emission point                                                                                                                                                      |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `create`  | After a fresh resource is inserted into the ownership ledger.                                                                                                       |
| `acquire` | After the waiter promise receives its token and the record is leased.                                                                                               |
| `release` | Only when the released or orphaned-ready record remains idle.                                                                                                       |
| `destroy` | After the hook attempt and resource-ledger removal, one microtask after private cleanup settlement, while destroying ownership remains installed through the event. |

Because listeners may synchronously reenter or destroy the pool, the engine checks terminal
and waiter state after every awaited hook and every emit.

```ts
import { createPool } from '@orkestrel/pool'

const pool = createPool({
	create: () => connect(),
	on: {
		create: () => metrics.increment('pool.create'),
		destroy: () => metrics.increment('pool.destroy'),
	},
	error: (error, event) => report(error, event),
})
```

## Patterns

### Validate public boundaries

Check a candidate value or error against the public boundary guards before acting on it:

```ts
import { PoolError, isPoolError, isPoolMax, isPoolSignal } from '@orkestrel/pool'

isPoolMax(4) // true
isPoolMax(Infinity) // false: omit max for unbounded capacity
isPoolSignal(new AbortController().signal) // true

const failure = new PoolError({ code: 'destroyed' })
if (isPoolError(failure)) console.error(failure.code)
```

### Always release and explicitly tear down

Release every acquired token and call `destroy()` explicitly after work finishes:

```ts
import { Pool } from '@orkestrel/pool'

const pool = new Pool({ create: () => connect(), max: 4 })
const controller = new AbortController()
const token = await pool.acquire(controller.signal)
try {
	await use(token.value)
} finally {
	token.release()
}
await pool.clear()
await pool.destroy()
```

## Tests

- [`tests/src/core/Pool.test.ts`](../tests/src/core/Pool.test.ts) — canonical behavior:
  validation, hostile errors, duplicate ownership, transitional counts, overlapping FIFO
  hooks, create continuation, abort boundaries, abort-listener detachment, exclusive
  invalid-cleanup waiter ownership, bounded and unbounded replacement, destroy-observer
  reentry ordering, concurrent clear, stable reentrant destruction, late resources,
  aggregate failures, emitter ordering, and high contention.
- [`tests/src/core/validators.test.ts`](../tests/src/core/validators.test.ts) — the public
  boundary guards alone: accepted and rejected maxima, and native versus hostile signals.
- [`tests/src/core/factories.test.ts`](../tests/src/core/factories.test.ts) — factory
  construction and instance identity only.
- [`tests/guides.test.ts`](../tests/guides.test.ts) — the `## Surface` ↔ `src/core` bijection
  over value and type exports, the `PoolInterface` ↔ `Pool` and `PoolToken` method bijections,
  fence-import and relative-link resolution, and the equality gate: every `Summary` cell against
  its declaration's description paragraph, the titled `Create a pool` fence against the
  `@example` block of that title (pinned so the titled pair cannot be retired silently), and the
  README pitch against this guide's tagline. It also runs the boundary-guard fence and asserts
  the values its comments claim.

## See also

- [`emitter.md`](emitter.md) — the installed observation primitive.
- [`AGENTS.md`](../AGENTS.md) — repository coding and lifecycle rules.
- [`README.md`](README.md) — guide manifest.
