# Guides

A dual-axis index into this repository's guides — by concept, and by directory (AGENTS §22).

## By concept

| Concept | Spec                             | Source                                                   | Tests                                                                            |
| ------- | -------------------------------- | -------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Worker  | [`src/worker.md`](src/worker.md) | [`src/core`](../src/core), [`src/server`](../src/server) | [`tests/src/core`](../tests/src/core), [`tests/src/server`](../tests/src/server) |

## By directory

| Directory    | Guide                            |
| ------------ | -------------------------------- |
| `src/core`   | [`src/worker.md`](src/worker.md) |
| `src/server` | [`src/worker.md`](src/worker.md) |

## Dependency reference

[`src/contract.md`](src/contract.md) is a byte-identical mirror of the guide for
`@orkestrel/contract` — a runtime dependency, the `Guard<T>` / shape vocabulary
threaded through the structured-clone boundary (`input` / `result` on
`createNodeWorker`) and the `isRecord` guard the server helpers use. It
documents **that package's** surface, not anything sourced in this repo; it is
kept here so a reader of this package can see the primitive it is built from
without leaving this guide set.

[`src/database.md`](src/database.md) is a byte-identical mirror of the guide
for `@orkestrel/database` — a runtime dependency, the storage layer
`createJSONQueueStore` persists over. It documents **that package's** surface
(the database, tables, and driver layer), not anything sourced in this repo;
it is kept here so a reader of this guide can see the persistence layer
without leaving this guide set.

[`src/emitter.md`](src/emitter.md) is a byte-identical mirror of the guide for
`@orkestrel/emitter` — a runtime dependency, the typed push-observation
surface `Worker` exposes as `emitter` (bridged from its underlying `Queue`).
It documents **that package's** surface, not anything sourced in this repo;
it is kept here for the same reason.

[`src/pool.md`](src/pool.md) is a byte-identical mirror of the guide for
`@orkestrel/pool` — a runtime dependency, the bounded resource pool a `Worker`
composes with a `Queue`. It documents **that package's** surface (idle reuse,
`max` backpressure, FIFO abort-able wait), not anything sourced in this repo;
it is kept here so a reader of this guide can see the primitive it is built
from without leaving this guide set.

[`src/queue.md`](src/queue.md) is a byte-identical mirror of the guide for
`@orkestrel/queue` — a runtime dependency, the cooperative concurrent job
engine a `Worker` composes and the `QueueStoreInterface` / durability contract
`createJSONQueueStore` and `store` build on. It documents **that package's**
surface, not anything sourced in this repo; it is kept here so a reader of
this guide can see the primitive it is built from without leaving this guide
set.

[`src/guide.md`](src/guide.md) is a byte-identical mirror of the guide for
`@orkestrel/guide` — the devDependency powering this repo's guides-parity test
suite (`tests/guides/src/parity.test.ts`). It documents **that package's**
surface (`Guide` / `Source`, the manifest and comparison helpers), not
anything sourced in this repo; it is kept here so a reader of the parity suite
can see the primitives it is built from without leaving this guide set.

## See also

- [`AGENTS.md`](../AGENTS.md) — the rules; §22 documentation-as-contracts.
