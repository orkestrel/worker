import type { QueueExecution } from '@orkestrel/queue'
import type { Guard, NodeThread, Reply } from './types.js'
import { Worker as ThreadWorker } from 'node:worker_threads'
import { isRecord } from '@orkestrel/contract'

// === The wire protocol (main ↔ thread)
//
// The main-side half of the run/abort/reply protocol `serveWorker` answers — spawning a
// pooled thread, narrowing its replies, and dispatching one job at a time. The envelope
// types ({@link Reply}, {@link NodeThread}) live in `./types.js` (AGENTS §5); the public
// bridge across the structured-clone boundary is the `input` / `result` `Guard`s, which
// narrow the envelopes' opaque `unknown` payloads with no assertion (AGENTS §14).

/**
 * Spawn one worker thread and resolve a live {@link NodeThread} once it comes online.
 *
 * @remarks
 * Constructs the thread with the `script` module and the cloned `workerData`, then
 * resolves on the thread's `online` event (rejecting on an early `error` OR an `exit`
 * that arrives before `online`, so the spawn promise is total — it can never dangle on a
 * thread that died without erroring). The wrapper attaches persistent `error` / `exit`
 * listeners that flip `alive` to `false` AND latch the first terminal event on
 * {@link NodeThread.death}: a crash is observable to an in-flight {@link dispatch} (via
 * its own listeners), to the pool's `validate` (via `alive`), and — crucially — to a
 * dispatch that attaches AFTER the death (via the latch). The latch closes a real race:
 * under event-loop pressure a dead thread's `online` + `error` + `exit` are delivered in
 * ONE synchronous exit-drain batch, so every death event fires before the microtask chain
 * resolving this spawn can hand the thread to `dispatch` — without the latch that job
 * would await events that already fired, forever. The pool's `create` hook calls this.
 *
 * @param script - The worker module each thread runs (must call `serveWorker`)
 * @param workerData - Opaque, structured-cloneable data handed to the thread at spawn
 * @returns A promise resolving the online {@link NodeThread}
 */
export function spawnThread(script: string | URL, workerData: unknown): Promise<NodeThread> {
	const worker = new ThreadWorker(script, { workerData })
	const thread: NodeThread = { worker, alive: true, death: undefined }
	// The persistent death latch — attached BEFORE any once-listener, so the first terminal
	// event records its cause on the record even when it fires inside a batched exit drain.
	worker.on('error', (error: Error) => {
		thread.alive = false
		if (thread.death === undefined) thread.death = error
	})
	worker.on('exit', (code: number) => {
		thread.alive = false
		if (thread.death === undefined) thread.death = new Error(`worker thread exited (code ${code})`)
	})
	return new Promise<NodeThread>((resolve, reject) => {
		const onOnline = (): void => {
			worker.off('error', onError)
			worker.off('exit', onExit)
			resolve(thread)
		}
		const onError = (error: Error): void => {
			worker.off('online', onOnline)
			worker.off('exit', onExit)
			reject(error)
		}
		const onExit = (code: number): void => {
			worker.off('online', onOnline)
			worker.off('error', onError)
			reject(new Error(`worker thread exited before coming online (code ${code})`))
		}
		worker.once('online', onOnline)
		worker.once('error', onError)
		worker.once('exit', onExit)
	})
}

/**
 * Narrow an inbound `message` to a {@link Reply} for a given job `id` — no assertion.
 *
 * @remarks
 * A total {@link Guard}-style predicate (never throws): a record whose `id` matches and
 * whose `ok` discriminant is well-formed (a `true` carries any `value`; a `false` carries a
 * string `error`). Anything else — another job's reply, a malformed payload — is `false`, so
 * a {@link dispatch} listener ignores it (a thread that chatters on the channel can't corrupt
 * a job).
 *
 * @param value - The inbound message to narrow
 * @param id - The job id a matching reply must carry
 * @returns `true` (narrowing `value` to {@link Reply}) when it is this job's well-formed reply
 */
export function isReply(value: unknown, id: string): value is Reply {
	if (!isRecord(value)) return false
	if (value.id !== id) return false
	if (value.ok === true) return true
	return value.ok === false && typeof value.error === 'string'
}

/**
 * Dispatch one job to a leased {@link NodeThread} and await its narrowed reply.
 *
 * @remarks
 * Mints a fresh `id`, posts a `run` envelope, and resolves when the thread replies for
 * that id: a success `value` is narrowed through `result` (a value that fails the guard
 * rejects — the zero-`as` type bridge), a failure rejects with the thread's error string.
 * A thread that ALREADY died rejects synchronously at entry from the latched
 * {@link NodeThread.death} — its death events fired before this dispatch existed (under
 * load they arrive in one batched exit drain) and will never fire again, so waiting on
 * the listeners below would dangle forever; the latch makes the death total across every
 * event ordering. If the thread `error`s / `exit`s mid-flight it is marked dead and the
 * job rejects. On `execution.signal` abort it posts an `abort` envelope (cooperative) AND
 * evicts the thread — `alive = false` + `terminate()` — because CPU-bound work cannot
 * honour the signal; the freed pool slot then gets a fresh thread. Every listener (the
 * thread's `message` / `error` / `exit` and the signal's `abort`) is removed on settle,
 * and a `settled` guard prevents a double-settle.
 *
 * @typeParam TResult - The reply type the `result` guard narrows to
 * @param thread - The leased thread to run the job on
 * @param input - The work payload (structured-cloned to the thread)
 * @param execution - The per-attempt handle; its `signal` aborts → terminate + evict
 * @param result - The {@link Guard} narrowing the reply value with no assertion
 * @returns A promise resolving the narrowed `TResult`, or rejecting on error / abort
 */
export function dispatch<TResult>(
	thread: NodeThread,
	input: unknown,
	execution: QueueExecution,
	result: Guard<TResult>,
): Promise<TResult> {
	const id = crypto.randomUUID()
	const worker = thread.worker
	return new Promise<TResult>((resolve, reject) => {
		// The latched-death entry check: a thread that died BEFORE this dispatch attached has
		// already emitted its `error` / `exit` (a batched exit drain delivers them before this
		// microtask runs) — no listener below will ever fire, and a `postMessage` to it is a
		// silent no-op. Reject NOW from the latch; this check + the attaches are synchronous,
		// so there is no gap a death can slip through.
		if (thread.death !== undefined || !thread.alive) {
			reject(thread.death ?? new Error('worker thread is dead'))
			return
		}
		let settled = false
		let detach = (): void => {}
		const settle = (action: () => void): void => {
			if (settled) return
			settled = true
			detach()
			action()
		}
		const onMessage = (value: unknown): void => {
			if (!isReply(value, id)) return
			if (value.ok) {
				const reply = value.value
				if (result(reply)) settle(() => resolve(reply))
				else settle(() => reject(new Error('reply did not satisfy result guard')))
				return
			}
			const message = value.error
			settle(() => reject(new Error(message)))
		}
		const onError = (error: Error): void => {
			thread.alive = false
			settle(() => reject(error))
		}
		const onExit = (): void => {
			thread.alive = false
			settle(() => reject(new Error('worker thread exited')))
		}
		// Cooperative abort first, then EVICT: CPU-bound work won't honour the signal, so
		// terminate the thread and mark it dead — the pool replaces it on the next acquire.
		// NOTE: this `terminate()` may run TWICE — once here, and again when the pool's
		// `destroy` hook (`thread.worker.terminate()`) tears down the now-dead thread its
		// `validate` evicts. A second `terminate()` on an already-terminated Node thread is a
		// safe, idempotent no-op (it resolves with the prior exit code), so do NOT "dedupe" it
		// by gating on `alive` — that would skip the pool's eviction and reuse a tainted thread.
		const onAbort = (): void => {
			worker.postMessage({ id, command: 'abort' })
			thread.alive = false
			void worker.terminate()
			settle(() => reject(new Error('job aborted')))
		}
		detach = (): void => {
			worker.off('message', onMessage)
			worker.off('error', onError)
			worker.off('exit', onExit)
			execution.signal.removeEventListener('abort', onAbort)
		}
		worker.on('message', onMessage)
		worker.on('error', onError)
		worker.on('exit', onExit)
		if (execution.signal.aborted) {
			onAbort()
			return
		}
		execution.signal.addEventListener('abort', onAbort, { once: true })
		// `postMessage` structured-clones `input`; a non-cloneable payload throws here —
		// settle-reject so the listeners detach (no leak) rather than escaping the executor.
		try {
			worker.postMessage({ id, command: 'run', input })
		} catch (error: unknown) {
			settle(() => reject(error instanceof Error ? error : new Error(String(error))))
		}
	})
}
