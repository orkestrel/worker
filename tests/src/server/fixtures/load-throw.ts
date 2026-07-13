// @ts-nocheck — a real worker-thread script (see double.ts). THROWS AT MODULE LOAD, before
// it ever registers a handler with `serveWorker`. A worker thread fires `'online'` as its
// bootstrap completes and only THEN surfaces a module-evaluation throw as an `'error'` +
// `'exit'` — so `spawnThread` resolves a live thread that immediately dies. The death reaches
// an in-flight `dispatch` (its `onError` / `onExit` rejects the job) and flips `alive = false`
// so the pool evicts the thread and a later job spawns a fresh one. Proves a broken worker
// script rejects the job cleanly (no hang) and the pool recovers.
throw new Error('worker failed to load')
