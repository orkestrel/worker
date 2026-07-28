# Pool

> A bounded, typed resource pool: idle reuse + FIFO waiting. `acquire` leases a resource — reusing a validated idle one, growing up to `max`, or parking on a FIFO waiter list until a `release` frees one — and the returned `PoolToken`'s `release()` returns it for reuse (or hands it straight to the next waiter). A parked `acquire` given an `AbortSignal` rejects and de-queues itself when the signal fires — no leaked waiter. It is deliberately **de-bloated**: no warm-floor (`min`), no eviction timers, no acquire-timeout — what ships is the validated FIFO pool, nothing speculative.
>
> The FIFO handoff is **validated**: when a waiter is parked, the released resource is re-validated (the same `validate` hook the idle path uses) before handoff — a resource that went invalid WHILE leased is destroyed and the waiter is served a fresh/valid one instead, so a dead resource is never handed to the next lessee. `validate` is total: a hook that THROWS is treated exactly like one returning `false`, on both the idle and the handoff paths, so a throwing validator can never strand a parked waiter.
>
> The pool is **observable** (AGENTS §13): the owned `emitter` (`PoolEventMap`) carries the resource lifecycle — `create` / `acquire` / `release` / `destroy` — for fire-and-forget observers (logging, metrics, tracing). Every event is emitted directly, strictly AFTER the relevant transition; the emitter isolates a listener throw and routes it to its `error` handler, so a buggy observer can never corrupt the validated FIFO handoff-eviction machinery. Source: [`src/core`](../../src/core). Surfaced through the `@src/core` barrel.

## Surface

Create a pool over a resource lifecycle, then `acquire` / `release` around the work:

```ts
import { createPool } from '@orkestrel/pool'

const pool = createPool<Connection>({
	create: () => connect(), // make a resource (called lazily, up to `max`)
	destroy: (connection) => connection.close(), // tear one down on clear / destroy / invalid
	validate: (connection) => connection.alive, // check an idle one before reuse (optional)
	max: 8, // at most 8 live at once; a 9th acquire waits for a release
})

const token = await pool.acquire()
try {
	await token.value.query('select 1')
} finally {
	token.release() // returns it to the next waiter, or to idle
}
```

`acquire` accepts an optional `AbortSignal`; if it fires while the acquire is parked (the pool is at `max`), the acquire rejects and its waiter is removed — no leak. The `size` (idle + leased), `idle` (available now), and `active` (leased out) members report live state.

### Factories

| API          | Kind     | Summary                                                                              |
| ------------ | -------- | ------------------------------------------------------------------------------------ |
| `createPool` | function | Create a `PoolInterface` over a resource lifecycle — idle reuse, `max` backpressure. |

### Entities

| API    | Kind  | Summary                                                                         |
| ------ | ----- | ------------------------------------------------------------------------------- |
| `Pool` | class | A bounded resource pool — idle reuse, `max` backpressure, FIFO abort-able wait. |

### Types

| Type            | Kind      | Shape                                                                                                                         |
| --------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `PoolToken`     | interface | A leased resource — `value` + an idempotent `release()`.                                                                      |
| `PoolWaiter`    | interface | A parked acquirer on the `Pool`'s FIFO waiter list — its `resolve` / `reject` resolvers + a `clear()` abort-listener cleanup. |
| `PoolOptions`   | interface | `createPool` options — `create` + `destroy?` / `validate?` / `max?` / `on?` / `error?`.                                       |
| `PoolInterface` | interface | `emitter` / `size` / `idle` / `active` data members + `acquire` / `clear` / `destroy` methods.                                |
| `PoolEventMap`  | type      | The `Pool`'s observable events — `create` / `acquire` / `release` / `destroy`.                                                |

The `emitter` / `size` / `idle` / `active` members of `PoolInterface` are `readonly` data members (Surface row, above) — `emitter` is the typed push observation surface (see [Observing](#observing)); its call-signature methods are documented under [Methods](#methods).

## Methods

The public methods of `PoolInterface` — every call-signature member listed (its `readonly` data members stay a Surface row). `Pool` implements `PoolInterface` exactly, so this doubles as its instance-method surface (AGENTS §22).

| Method    | Returns                 | Behavior                                                                                                                    |
| --------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `acquire` | `Promise<PoolToken<T>>` | Lease a resource — reuse a validated idle one, grow up to `max`, or park (FIFO) until a `release`; an aborted wait rejects. |
| `clear`   | `Promise<void>`         | Destroy every idle resource; leased ones keep running.                                                                      |
| `destroy` | `Promise<void>`         | Destroy all resources and reject parked waiters; idempotent.                                                                |

## Contract

These invariants hold across `src/core` ↔ `pool.md`:

1. **DOC ↔ SOURCE bijection.** Every `function` / `class` / `interface` / `type` row in the `## Surface` tables is a real export of this package, and every export appears as a Surface row — exhaustive, both directions (AGENTS §22).
2. **Idle reuse + `max` backpressure + FIFO abort-able wait.** `acquire` reuses a validated idle resource (an invalid one — by a `validate` hook — is destroyed and the next idle / a fresh one tried), else `create`s one while below `max`, else PARKS on a FIFO waiter list. `release` (idempotent — a second call is a no-op) hands the resource to the oldest waiter (it stays leased) or returns it to idle. **The FIFO handoff is validated:** when a waiter is parked, the released resource is re-validated (the same `validate` hook the idle path uses) BEFORE handoff — a resource that went invalid WHILE leased is destroyed and the waiter is served a fresh/valid resource instead, so a dead resource is never handed to the next lessee. The lease slot transfers straight from the dead resource to its replacement, so `active` is steady across the swap (no overshoot of `max`); if the replacement `create` throws, the waiter is rejected with that error and the slot is freed. **`validate` is total:** a `validate` hook that THROWS is treated exactly like one returning `false` (the resource is "not usable", so it is destroyed and replaced), on BOTH the idle and the handoff paths — so a throwing validator can never escape as an unhandled rejection that strands a parked waiter. The no-waiter path stays synchronous (decrement + idle). A parked `acquire` given an `AbortSignal` rejects + de-queues itself when the signal fires — no leaked waiter, so a later `release` still serves the next live waiter. `size` = idle + leased; `idle` = available; `active` = leased. `clear` destroys idle (leased untouched); `destroy` destroys all + rejects waiters (both await `destroy`).
3. **Observable + de-bloated.** The pool owns a typed `Emitter` (AGENTS §13) exposed as `readonly emitter` and accepts the reserved `on?` initial-listeners hook plus the `error?` listener-error handler — `PoolEventMap` (`create` / `acquire` / `release` / `destroy`). **Emitting is observation-only** — the emitter isolates a listener throw (routing it to the `error` handler, never a domain event) and every event sits strictly AFTER the relevant create / acquire / release / destroy transition, so a buggy observer can NEVER reorder, throw into, or corrupt the validated FIFO handoff-eviction engine — lease counts stay balanced and no parked waiter is ever stranded regardless of what a listener does. Still deliberately CUT: a warm-floor (`min`), eviction timers, and acquire-timeout.
4. **DOC ↔ SOURCE method bijection.** The `## Methods` table lists exactly the public methods of `PoolInterface` — exhaustive, both directions — and `Pool` exposes the same public methods as its interface, no more (AGENTS §22).

## Observing

The pool exposes a typed `emitter` (AGENTS §13) carrying its resource lifecycle for fire-and-forget observers — logging, metrics, tracing. Subscribe via `pool.emitter.on(...)`, or wire initial listeners through the reserved `on?` option; supply an `error?` handler to receive a listener's throw. **Emitting is observation-only**: every event fires strictly AFTER the relevant create / acquire / release / destroy transition, so a listener can never change what the pool does — and a throwing listener can never corrupt it (see the safety guarantee below).

```ts
import { createPool } from '@orkestrel/pool'

const pool = createPool<Connection>({
	create: () => connect(),
	on: { create: () => metrics.increment('pool.created') }, // initial listener at construction
})

pool.emitter.on('acquire', () => metrics.increment('pool.leased'))
pool.emitter.on('destroy', () => metrics.increment('pool.destroyed'))
```

| Event map      | Events                                               |
| -------------- | ---------------------------------------------------- |
| `PoolEventMap` | `create()` · `acquire()` · `release()` · `destroy()` |

`create` fires when a fresh resource is made, `acquire` when one is leased (a reused idle one, a fresh one, or a served waiter), `release` when one returns to idle, and `destroy` when one is torn down.

**The listener-isolation safety guarantee.** A listener throw is NEVER allowed to escape into the pool: the emitter isolates it and routes it to its OWN `error` handler (the `error` option, surfaced as `(error, event)`), NOT to a domain event — so a buggy observer is isolated yet not silently lost. The `error` handler runs in its own try/catch, so even a throwing handler can't recurse or escape; with no handler, the throw is swallowed silently. Every throwing listener surfaces (not just the first). Because every emit sits after the create / acquire / release / destroy transition AND is isolated, a buggy observer **cannot corrupt the validated FIFO handoff-eviction**: lease counts stay balanced, the pool still hands off, and no parked waiter is ever stranded — proven by the emit-safety tests.

## Patterns

### A resource pool

```ts
import { createPool } from '@orkestrel/pool'

const pool = createPool<Connection>({
	create: () => connect(), // make a resource (called lazily, up to `max`)
	destroy: (connection) => connection.close(), // tear one down on clear / destroy / invalid
	validate: (connection) => connection.alive, // check an idle one before reuse (optional)
	max: 8, // at most 8 live at once; a 9th acquire waits for a release
})

const token = await pool.acquire()
try {
	await token.value.query('select 1')
} finally {
	token.release() // returns it to the next waiter, or to idle
}
```

`acquire` accepts an optional `AbortSignal`; if it fires while the acquire is parked (the pool is at `max`), the acquire rejects and its waiter is removed — no leak.

### Practices

- **`finally` the release** — bracket `acquire` / `release` in a `finally` so a throwing consumer still frees the resource for the next lessee.
- **`validate` for connection health** — a pool over network connections should validate before reuse (a socket may have died while idle); a throwing validator is treated as invalid, never propagated.
- **Observe, don't drive** — subscribe to `pool.emitter` for lifecycle moments (see [Observing](#observing)); emitting is a pure side-channel, so a listener never changes what the pool does (and a throwing one can't corrupt it).
- **`clear` vs `destroy`** — `clear` drops idle resources and keeps leased ones running (a warm restart); `destroy` tears everything down and rejects parked waiters (a full shutdown).

## Tests

- [`tests/guides/parity.test.ts`](../../tests/guides/src/parity.test.ts) — the `## Surface` ↔ `src/core` bijection (value + type exports) and the `PoolInterface` ↔ `Pool` method bijection.
- [`tests/src/core/Pool.test.ts`](../../tests/src/core/Pool.test.ts) — create up to `max`, idle reuse (release → same resource), `max` backpressure (a parked acquire served FIFO on release), an aborted waiting-acquire (rejects + no leaked waiter), `validate` (destroys + replaces an invalid idle one, and the same on FIFO handoff), `clear` (destroys idle, leased untouched), `destroy` (destroys all + rejects waiters), the double-release no-op, `destroy` mid-`create`, high contention (many waiters over few slots — FIFO fairness, never overshoots `max`), handoff validation under stress, `create` / `validate` / `destroy` hooks throwing, rapid acquire/release churn, teardown with active leases + parked waiters + an in-flight create at once, accurate `size` / `idle` / `active`, and the emitter (push observation surface, including emit-safety under a throwing listener).
- [`tests/src/core/factories.test.ts`](../../tests/src/core/factories.test.ts) — `createPool` returns a working, typed instance end to end: leases, reuses on release, reports counts, and parks + serves FIFO at `max`.

## See also

- [`emitter.md`](emitter.md) — the observable primitive `Pool` composes as its `emitter`.
- [`AGENTS.md`](../../AGENTS.md) — the rules; §10 lifecycle, §4.1 single-word members, §13 emitter pattern, §22 documentation-as-contracts.
- [`README.md`](../README.md) — the guides index.
