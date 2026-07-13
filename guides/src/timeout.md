# Timeout

> A controllable `setTimeout` wrapper that exposes an `AbortSignal` which fires
> on expiry, for racing work against a deadline. A `Timeout` carries a trace
> `id`, a deadline `ms`, and `start()` / `clear()` controls — arm the deadline,
> then race its `signal` against work to bound how long that work may run. The
> time-bound half of the substrate's time-and-cancellation pair. Deliberately
> thin: it is not a scheduler, not a debounce/throttle, not a retry policy —
> just one `setTimeout` made re-armable, clearable, and parent-linkable.
> Event-free for now (no Emitter wiring; the observability pass owns that) — a
> pure functional primitive with a start / clear lifecycle: `start()` arms the
> deadline, `clear()` cancels it without firing, and re-`start()`ing after an
> expiry swaps in a fresh signal and resets `expired`, so a handle is reusable
> across deadlines without re-construction. Source: [`src/core`](../../src/core).
> Surfaced through the `@src/core` barrel.

## Surface

Create a deadline handle, arm it, and hand its `signal` to deadline-aware
work — `clear()` the deadline if the work finishes first:

```ts
import { createTimeout } from '@src/core'

const timeout = createTimeout({ ms: 5_000 })
timeout.start()

// `signal` aborts on expiry — pass it anywhere a native AbortSignal is accepted:
const response = await fetch(url, { signal: timeout.signal })

timeout.clear() // work finished first — cancel the deadline
```

`ms` is passed straight to the host `setTimeout` — a non-negative finite number
is the intended input, but a negative or `NaN` value is clamped to roughly `0`
(firing on the next macrotask) rather than throwing. An optional parent
`signal` CLEARS the timeout (rather than expiring it) if it aborts before the
deadline. An optional `id` labels the handle for tracing and defaults to a
random UUID.

### Factories

| API             | Kind     | Summary                                                            |
| --------------- | -------- | ------------------------------------------------------------------ |
| `createTimeout` | function | Create a `TimeoutInterface` deadline handle from `TimeoutOptions`. |

### Entities

| API       | Kind  | Summary                                                                       |
| --------- | ----- | ----------------------------------------------------------------------------- |
| `Timeout` | class | The controllable `setTimeout` wrapper; implements `TimeoutInterface` exactly. |

### Types

| Type               | Kind      | Shape                                                                                                |
| ------------------ | --------- | ---------------------------------------------------------------------------------------------------- |
| `TimeoutOptions`   | interface | `{ id?: string; ms: number; signal?: AbortSignal }` — options for `createTimeout` / the constructor. |
| `TimeoutInterface` | interface | `id` / `ms` / `signal` / `expired` data members + `start` / `clear` methods.                         |

The `id`, `ms`, `signal`, and `expired` members are `readonly` data members of
`TimeoutInterface` (Surface rows, above) — its call-signature methods are
documented under [Methods](#methods).

## Methods

The public methods of `TimeoutInterface` — every call-signature member listed
(its `readonly` data members `id` / `ms` / `signal` / `expired` stay Surface
rows). `Timeout` implements the interface exactly, so this doubles as the
class's instance-method surface (AGENTS §22).

#### `TimeoutInterface`

`start` arms (or re-arms) the deadline; `clear` cancels a pending expiry
without firing.

| Method  | Returns | Behavior                                                                                                 |
| ------- | ------- | -------------------------------------------------------------------------------------------------------- |
| `start` | `void`  | Arm the deadline for `ms`. Re-arming resets `expired` and swaps a fresh `signal` if the prior one fired. |
| `clear` | `void`  | Cancel a pending expiry without firing `signal`; resets `expired` back to `false` (the §10 reset).       |

## Contract

These invariants hold across `src/core` ↔ `timeout.md`:

1. **DOC ↔ SOURCE bijection.** Every `function` / `class` / `interface` /
   `type` row in the `## Surface` tables is a real export of the timeout
   source, and every export appears as a Surface row — exhaustive, both
   directions (AGENTS §22).
2. **`ms` clamps, never throws.** `ms` is passed straight to the host
   `setTimeout`, which clamps a negative or `NaN` value to roughly `0` (firing
   on the next macrotask) rather than throwing. An `ms` above the host's
   32-bit signed integer ceiling (`2_147_483_647`) is also clamped, firing
   near-immediately rather than after the intended delay.
3. **Deadline signal.** The exposed `signal` fires (aborts) on expiry — race it
   against work to bound how long that work may run.
4. **Signal identity swaps only on a real expiry.** A cleared-but-never-fired
   timeout keeps its original `signal` (not aborted); the identity is only
   swapped for a fresh, non-aborted controller after the CURRENT controller has
   actually fired — whether that swap happens inside `clear()` (to keep the
   post-clear signal non-aborted) or at the next `start()`.
5. **Parent linking clears, never expires.** A parent `options.signal` abort
   CLEARS the timeout — it does not flip `expired` or fire the timeout's own
   `signal`. The parent listener is attached only while a timer is armed
   (added on `start()`, removed on expiry or `clear()`); once the parent has
   aborted, a later `start()` is a no-op.
6. **Non-string `id` falls back to a generated UUID.** Construction is the
   defensive JS boundary: `options.id` is checked with `isString` (from
   `@orkestrel/contract`), and any non-string value falls back to
   `crypto.randomUUID()` rather than being coerced or throwing.
7. **Event-free.** `Timeout` is a pure functional primitive — no `Emitter`, no
   observable event map; consumers observe expiry only through `signal`.

## Patterns

### Race work against a deadline

```ts
import { createTimeout } from '@src/core'

async function fetchWithDeadline(url: string, ms: number): Promise<Response> {
	const timeout = createTimeout({ ms })
	timeout.start()

	try {
		return await fetch(url, { signal: timeout.signal })
	} finally {
		timeout.clear() // no-op if the fetch already won the race
	}
}
```

### Link a parent signal

A parent `AbortSignal` clears the deadline instead of letting it expire — so
an outer cancellation (a request abort, a shutdown signal) short-circuits the
timer cleanly:

```ts
import { createTimeout } from '@src/core'

function withDeadline(parent: AbortSignal, ms: number) {
	const timeout = createTimeout({ id: 'request-deadline', ms, signal: parent })
	timeout.start()

	timeout.signal.addEventListener(
		'abort',
		() => {
			if (timeout.expired) giveUp() // real expiry, not a parent-triggered clear
		},
		{ once: true },
	)

	return timeout
}
```

### Reuse a handle across deadlines

```ts
import { createTimeout } from '@src/core'

const timeout = createTimeout({ ms: 100 })

timeout.start()
timeout.clear() // cancels before firing — expired stays false

timeout.start() // re-armed; a fresh deadline window begins
```

### Practices

- **Race, don't poll** — attach a listener to `signal` (or pass it straight to
  an API that accepts an `AbortSignal`, e.g. `fetch`) rather than polling
  `expired`.
- **`clear()` is always safe to call** — clearing an idle, already-cleared, or
  already-expired handle is a no-op; call it unconditionally in a `finally`.
- **Distinguish a real expiry from a parent clear** — check `expired` inside a
  `signal` abort listener when the handle also links a parent `signal`, since
  a parent abort fires no event of its own but does clear the timeout.
- **Reuse, don't reconstruct** — call `start()` again on the same handle for a
  new deadline window instead of constructing a fresh `Timeout`.

## Tests

- [`tests/src/core/Timeout.test.ts`](../../tests/src/core/Timeout.test.ts) —
  `id` / `ms` exposure, a fresh handle's initial state, `start()` → expiry
  (`expired` flips, `signal` fires), `clear()` before expiry (never expires,
  `signal` never fires), re-`start()` after expiry (fresh `signal`, reset
  `expired`), parent-`signal` clearing (clears rather than expiring, a
  pre-aborted parent makes `start()` a no-op), and non-string `id` falling
  back to a generated UUID.
- [`tests/src/core/factories.test.ts`](../../tests/src/core/factories.test.ts) —
  `createTimeout` returns a working `TimeoutInterface` that arms and expires.

## See also

- [`AGENTS.md`](../../AGENTS.md) — the rules; §10 lifecycle (`start` / `clear`),
  §8 options design, §22 documentation-as-contracts.
- [`contract.md`](contract.md) — the mirrored guide for `@orkestrel/contract`,
  the source of the `isString` guard used at the construction boundary.
- [`guide.md`](guide.md) — the mirrored guide for `@orkestrel/guide`, the
  devDependency powering this repo's guides-parity test suite.
- [`README.md`](../README.md) — the guides index.
