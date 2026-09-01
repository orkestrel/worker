import type { QueueExecution } from '@orkestrel/queue'
import type { Guard } from '@orkestrel/contract'
import type { NodeThread, Reply } from './types.js'
import { attempt, isRecord } from '@orkestrel/contract'
import { Dispatch } from './Dispatch.js'
import { Thread } from './Thread.js'

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
 * dispatch that attaches AFTER the death (via the latch). A `messageerror` is terminal too,
 * so a thread whose inbound payload could not be deserialized is never reused. The latch
 * closes a real race: a thread can become terminal before the readiness promise continuation
 * hands it to `dispatch`, leaving no future death event for that dispatch to observe. Without
 * the latch, that job would wait forever. The pool's `create` hook calls this.
 *
 * @param script - The worker module each thread runs (must call `serveWorker`)
 * @param workerData - Opaque, structured-cloneable data handed to the thread at spawn
 * @returns A promise resolving the online {@link NodeThread}
 */
export function spawnThread(script: string | URL, workerData: unknown): Promise<NodeThread> {
	return new Thread(script, workerData).promise
}

/**
 * Dispatch one job to a leased {@link NodeThread} and await its narrowed reply.
 *
 * @remarks
 * Mints a fresh per-dispatch correlation `id`, posts it with `job: execution.id`, and
 * resolves when the thread replies for that correlation id. The stable Queue job id reaches
 * the worker handler for idempotency across retries and restore; it is not caller identity or
 * authentication / authorization evidence. Per-job consumer context remains explicit,
 * structured-cloneable `input`; ambient context is not worker-thread transport. A success
 * `value` is narrowed through `result` (a value that fails the guard
 * rejects — the zero-`as` type bridge), a failure rejects with the thread's error string.
 * A thread that ALREADY died rejects synchronously at entry from the latched
 * {@link NodeThread.death} — its death events fired before this dispatch existed and will
 * never fire again, so waiting on the listeners below would dangle forever; the latch makes
 * death total across every event ordering. If the thread `error`s / `exit`s mid-flight it is
 * marked dead and the
 * job rejects. An inbound `messageerror` also evicts and terminates the thread before
 * rejection. On `execution.signal` abort it contains the cooperative `abort` post,
 * evicts the thread, and observes `terminate()` settlement because CPU-bound work cannot
 * honour the signal; the freed pool slot then gets a fresh thread. Every per-job listener
 * (`message` / `messageerror` / `error` / `exit` / `abort`) is removed on settle.
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
	return new Dispatch(thread, input, execution, result).promise
}

/**
 * Narrow an inbound `message` to a {@link Reply} for a given job `id` — no assertion.
 *
 * @remarks
 * A total predicate: a record whose `id` matches and whose `ok` discriminant is well-formed.
 * Anything else is rejected so a dispatch listener can ignore foreign or malformed messages.
 * It correlates against the `id` argument rather than narrowing one value alone, so it is a
 * correlated predicate rather than a `Guard<Reply>` and is not accepted where a `Guard` is.
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
