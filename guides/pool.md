# Pool

> A typed resource pool with optional bounded capacity, a warm floor, bounded loss recovery,
> unique ownership, FIFO settlement, validated reuse, caller-owned cancellation, and explicit cleanup.

The pool supports lazy creation or a warm floor, with no eviction timer, acquire timeout,
or polling loop. Waits park on promises or signal listeners. An acquire rejects with
`cleanup` when a floor retains a record, owns `min` records, has nothing idle, and has no
refill or disposal pending, even with live leases. The lifecycle hooks are the caller's,
so the engine itself performs no I/O.

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

| API         | Kind  | Summary                                                                                                                                                                                                                    |
| ----------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Pool`      | class | Represents a resource pool with optional bounded capacity and a warm floor, whose opaque ownership records preserve FIFO settlement, cancellation, exact lease release, and deterministic teardown under concurrent hooks. |
| `PoolError` | class | Represents a stable, machine-readable pool failure that retains the original thrown value as its cause without unsafe coercion, alongside structured context.                                                              |

### Guards

In a guard table a `Shape` cell holds the type the guard narrows to.

| API            | Kind     | Shape         | Summary                                                                                                          |
| -------------- | -------- | ------------- | ---------------------------------------------------------------------------------------------------------------- |
| `isPoolError`  | function | `PoolError`   | Tests whether an unknown value is a `PoolError`, returning `false` for hostile proxies.                          |
| `isPoolMax`    | function | `number`      | Tests whether a value is a positive safe integer, the only valid explicit pool maximum.                          |
| `isPoolSignal` | function | `AbortSignal` | Tests whether a value is a native `AbortSignal` for the acquire boundary, returning `false` for hostile proxies. |

### Types

A `Shape` cell holds an interface's data members as bare names in braces, `?` marking an optional member and `plus` introducing its call-signature members, and a type alias's own type literal with a union's arms escaped as `\|`.

| API                | Kind      | Shape                                                                         | Summary                                                                                                                                                                            |
| ------------------ | --------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PoolCode`         | type      | `'invalid' \| 'destroyed' \| 'create' \| 'cleanup'`                           | Names the machine-readable failure codes produced by `PoolError`.                                                                                                                  |
| `PoolContext`      | interface | `{ value?, failures? }`                                                       | Represents the structured context attached to a `PoolError`: the rejected input, or the distinct destroy-hook failures an aggregate cleanup collected.                             |
| `PoolErrorOptions` | interface | `{ code, cause?, context? }`                                                  | Represents the construction options for `PoolError`: the stable code, an optional cause, and optional structured context.                                                          |
| `PoolEventMap`     | type      | `{ create, acquire, release, destroy }`                                       | Represents the observable resource lifecycle events emitted by a `PoolInterface`.                                                                                                  |
| `PoolToken`        | interface | `{ value } plus release, destroy`                                             | Represents a unique lease over one pool-owned resource record, exposing that record as a readonly `value` and ending it through an idempotent `release` or `destroy`.              |
| `PoolOptions`      | interface | `{ on?, error?, create, destroy?, validate?, watch?, max?, min?, restarts? }` | Represents the resource lifecycle options for `Pool` and `createPool`: creation, destruction, validation, capacity, a bounded warm floor, and loss observation.                    |
| `PoolInterface`    | interface | `{ emitter, size, idle, active } plus start, acquire, clear, destroy`         | Represents a FIFO resource pool with optional bounded capacity, a warm floor, loss recovery, and deterministic teardown, exposing its record counts and a typed lifecycle emitter. |

`size` counts every owned record, including records being validated, destroyed, or retained
after failed cleanup under `min`. `idle` counts only immediately available records.
`active` counts only leased records. An in-flight create reservation claims capacity but is
not yet an owned record and therefore is not part of `size`.

## Methods

The public call-signature members of `PoolInterface` and `PoolToken`; `Pool` implements the
`PoolInterface` list exactly.

#### `PoolInterface`

| Method    | Returns                 | Summary                                                                                                                            |
| --------- | ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `start`   | `Promise<void>`         | Fills the warm floor and resets the strikes of a spent floor; without a floor, resolves immediately.                               |
| `acquire` | `Promise<PoolToken<T>>` | Queues the caller in FIFO order, validates an idle record or waits for a floor refill, and creates on demand only without a floor. |
| `clear`   | `Promise<void>`         | Destroys the records that are idle at this call's synchronous snapshot and restores a started floor.                               |
| `destroy` | `Promise<void>`         | Tears down the pool permanently and returns its stable completion barrier.                                                         |

#### `PoolToken`

The lease returned by `acquire`, with operations that return or dispose its exact record.

| Method    | Returns         | Summary                                                                                                                     |
| --------- | --------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `release` | `void`          | Gives this exact record back to the pool once; a repeat call, and a call after loss or teardown took ownership, are no-ops. |
| `destroy` | `Promise<void>` | Destroys this exact record instead of returning it; a repeat call, and a call after release, are no-ops.                    |

## Contract

### Warm floor and loss

Construction creates nothing. Set `min` to a positive safe integer and `restarts` to a
non-negative safe integer, then call `start()`. `max` defaults to `min`; explicit values
must equal each other. A missing restart bound, a bound without `min`, and a non-callable
`watch` are invalid. The restart bound has no default.

`start()` resolves when the pool owns `min` live records. Concurrent calls share the pending
promise. Without `min`, it resolves immediately. Under `min`, queued acquires never cause
creation, including before `start()` and during failed refills. The pool refills missing
capacity after startup and cleanup, one create attempt at a time.
Bound your `create` and `destroy` hooks. A create hook that never settles blocks every later
refill, including the attempt owed to a lost leased record. A destroy hook that never settles
keeps its record counted against `max` and blocks replacement of that record.

`watch(value, signal)` runs once for each created record. A fulfillment, rejection, or
synchronous throw declares loss while the record is live. The pool removes the record from
service, disposes it, and refills if the bound permits. Rejections and synchronous throws
reach `error(error, 'watch')` only while the record is live; a throwing error handler is
isolated. Disposal aborts the watch signal before the destroy hook runs, including disposal
through validation, clear, pool teardown, and token destruction.
The watch must release its listeners when that signal aborts. Any settlement after the signal
aborts is ignored, including rejection. A late or repeated loss cannot
dispose a record twice. A lost record cannot be handed out after validation or from a ready
result waiting behind an earlier caller. The pool disposes a lost leased record while its
holder still holds the token. The holder learns of that loss from its own `watch` or `destroy`
hook. After disposal, the token's `release()` and `destroy()` calls do nothing. During disposal,
the first `destroy()` call waits for that cleanup attempt and rejects with `cleanup` if it fails.

A failed refill adds a strike. Losing a record that has never been leased also adds a strike,
including failed validation. Granting a lease resets the strikes; successful creation does not.
When strikes exceed `restarts`, the floor is spent. With `restarts: 1`, the first failed
create permits another attempt, and the second refuses the next attempt. `start()` rejects
with `create` and the last cause; another `start()` resets the strikes. While the floor is
spent, an acquire without an idle record rejects with `create` after pending refills and
disposal settle. If the floor retains a record and owns `min` records, that acquire rejects
with `cleanup` instead, even with live leases. A spent floor and a rejected `start()` still
serve their live records. A loss without a thrown cause leaves that cause undefined.

A record lost while leased earns one refill attempt even after the bound is spent.
The credit becomes spendable only when successful disposal removes the record; failed disposal
grants no credit. That attempt remains owed during a concurrent refill and never resets the strikes.
Its failed create adds a strike; its success alone does not reset the strikes. A record that was leased
and later released adds no strike when lost. `token.destroy()` disposes the exact leased
record, waits for an existing cleanup attempt, and rejects with `cleanup` if disposal fails.
A token ends once: after `release()` or `destroy()`, the other call does nothing. Repeating
either call does nothing. A repeat `token.destroy()` after a `cleanup` rejection resolves
while the record stays retained.

Under `min`, a failed destroy hook leaves its record counted against `max` and excluded from
idle and active counts. The pool never replaces that retained record or retries its destroy
hook. The pool can run short; `start()` rejects with `cleanup` if retained records prevent
filling. An acquire rejects with `cleanup` when the floor retains a record, owns `min` records,
has nothing idle, and has no refill or disposal pending, even with live leases. The cause is
the retained record's own cleanup failure. The terminal `destroy()` barrier
reports the original cleanup failures. Without `min`, cleanup retains its lazy behavior and
frees capacity after either hook outcome.

This example warms an inert resource, explicitly loses its lease, and tears down the pool:

```ts
import { createPool } from '@orkestrel/pool'

const pool = createPool({
	create: () => new EventTarget(),
	min: 1,
	restarts: 1,
	watch: (resource, signal) =>
		new Promise<void>((resolve) => {
			resource.addEventListener('loss', () => resolve(), { once: true, signal })
			signal.addEventListener('abort', () => resolve(), { once: true })
		}),
})
await pool.start()
const token = await pool.acquire()
await token.destroy()
await pool.destroy()
```

### Capacity and FIFO

Every `acquire` receives its queue position before a create or validation hook starts. The
reentrancy-safe pump may assign several hook operations concurrently, but a head commit
barrier settles successes and failures in request order. A later fast create or validation
cannot overtake an earlier slow one. Capacity obeys:

```text
owned records + create reservations <= max
```

`max` must be a positive safe integer. Omit both capacity options for an unbounded pool.
`Infinity`, fractions, zero, negative values, and unsafe integers are invalid.
Construction snapshots `max` once and validates it before retention, then snapshots `on`
and `error` once each.

Record phases are disjoint:

```text
create reservation -> ready -> leased -> available -> validating -> ready
                                      \-> destroying -> removed
                                                     \-> retained (failed cleanup with min)
floor refill --------------------------> available
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
becomes idle and emits `release`. Release after loss or teardown took ownership does nothing.
The pool disposes a lost lease while its holder still holds the token; the holder learns of
the loss through its own `watch` or `destroy` hook. After disposal, both token methods do nothing.

`clear()` synchronously snapshots idle records and installs one cleanup promise per record
before invoking the hook. Concurrent clears therefore own disjoint snapshots, and a lease
released after a snapshot is taken is not part of it. Every claimed record stays in `size` during
its hook attempt; failed cleanup under `min` retains it afterward. Distinct failures are
aggregated in a code-`cleanup` `PoolError` whose `context.failures` retains the original thrown
values.
Each claimed record's cleanup settlement wakes queued acquires after the destroy ledger
transition. A failed `clear()` rejects its own aggregate cleanup barrier; under `min`,
the pool keeps a record whose cleanup fails counted against `max`, and successful cleanup
restores a started floor.

### Destruction

`destroy()` is deliberately non-`async`: it installs and returns its exact promise before it
rejects waiters, emits events, or invokes cleanup. Reentrant and repeated calls return that
same object. Teardown invalidates idle, leased, and ready records, waits for create,
validation, existing clear cleanup, and new cleanup activity, and disposes every late
resource. A pending `start()` rejects with `destroyed`. Unresolved create, validation, and
destroy hooks keep the barrier pending without polling; aborted watches do not delay it.

Cleanup already owned by an overlapping `clear()` is shared; its failure is reported to both
the clear call and the destroy aggregate. Create failures that produced no resource do not
fail destruction. The emitter is destroyed last, after every resource `destroy` event and
hook attempt, then the stable barrier resolves or rejects.

### Errors

`PoolError.code` is stable and lowercase:

| Code        | Owner                                                                               |
| ----------- | ----------------------------------------------------------------------------------- |
| `invalid`   | Invalid capacity, restart bound, watch hook, acquire signal, or internal boundary.  |
| `destroyed` | Start, acquire, or clear attempted during terminal teardown.                        |
| `create`    | A lazy create failed, or a floor refill bound was spent.                            |
| `cleanup`   | Record disposal failed, including retained records blocking startup or acquisition. |

The original thrown value is retained as `cause`; aggregate cleanup values are also in
`context.failures`. Message construction and `isPoolError` avoid unsafe string coercion and
return safely for hostile proxies.

## Observing

The composed `Emitter` isolates listener throws through the optional `error` handler and
invokes listeners synchronously at each emission point. The `destroy` emission is deliberately
one microtask after cleanup settlement so bound outcomes precede observer reentry. Events
follow their ledger transitions:

| Event     | Emission point                                                                                                                                                                   |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `create`  | After a fresh resource is inserted into the ownership ledger.                                                                                                                    |
| `acquire` | After the waiter promise receives its token and the record is leased.                                                                                                            |
| `release` | Only when a released, orphaned, or refilled record remains idle.                                                                                                                 |
| `destroy` | After the hook attempt and resource-ledger removal or retention, one microtask after private cleanup settlement, while destroying ownership remains installed through the event. |

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
  aggregate failures, emitter ordering, high contention, warm floors, exact refill bounds,
  retained cleanup failures, watch cancellation, lost-record handoff races, and owed lease refills.
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
  the values its comments claim. The floor fence is checked line for line in execution order
  and runs with assertions for warming, token cleanup, and watch-listener removal.

## See also

- [`emitter.md`](emitter.md) — the installed observation primitive.
- [`AGENTS.md`](../AGENTS.md) — repository coding and lifecycle rules.
- [`README.md`](README.md) — guide manifest.
