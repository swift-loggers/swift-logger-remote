/// Typed diagnostic surface for the engine-internal retry execution
/// loop.
///
/// Each case names the lifecycle step where the failure surfaced.
/// Cases either carry the underlying layer-typed error or name an
/// engine-side invariant / lifecycle failure directly. Internal
/// batch-state invalidity marks engine-side invariant failure,
/// not adapter transport invalidity. Sleep-injector
/// failures (e.g. cooperative cancellation propagated from
/// `Task.sleep(for:)`) surface as
/// ``ExecutionLoopError/sleepInterrupted`` — the underlying error is
/// intentionally dropped because the engine treats every
/// sleep-injector failure as equivalent at this layer.
///
/// The error is engine-internal. ``RemoteEngine/flush()`` is the
/// only production caller; `ExecutionLoop.runOnce(...)` throws
/// this surface for the engine-internal lifecycle (drain +
/// empty-release) that the engine wraps differently in
/// production. The public translation lives on
/// ``RemoteEngineError``; the execution-loop typed throw flows
/// otherwise only through the test target via `@testable
/// import`.
internal enum ExecutionLoopError: Error, Sendable {
    /// ``DurableRemoteQueue/drain(to:)`` failed; the queue's
    /// outstanding-batch state is preserved per the queue contract.
    case drainFailed(DurableRemoteQueueError)

    /// Parsing the drained queue export into entries through
    /// `BatchEngine.recoverEntries(from:)` failed
    /// closed before any transport call could be made.
    case recoverFailed(BatchEngineError)

    /// Splitting the recovered entry stream into batches through
    /// ``RemoteBatchPolicy`` failed (e.g. an oversized single entry
    /// or an engine-side batching-state defect).
    case batchSplitFailed(RemoteDeliveryError)

    /// The retry-delay calculation
    /// (``RemoteRetryPolicy/delayBeforeRetry(attempt:)`` or an
    /// injected equivalent) refused the engine's requested attempt
    /// count before any sleep could be scheduled. The
    /// ``RemoteRetryPolicy/make(maxAttempts:backoff:)`` factory
    /// already enforces that ``RemoteRetryPolicy/delayBeforeRetry(attempt:)``
    /// accepts every `attempt` value the executor passes from a
    /// validated policy, so this case names an engine-side or
    /// seam-injected invariant violation rather than a caller-
    /// actionable failure. The carried ``RemoteDeliveryError`` is
    /// the diagnostic raised by the delay calculation; the engine
    /// does not collapse it into
    /// ``ExecutionLoopError/sleepInterrupted`` because a policy-
    /// space failure is distinct from a sleep-injector failure and
    /// every diagnostic stays addressable.
    case invalidRetryDelay(RemoteDeliveryError)

    /// The empty-drain release path failed.
    ///
    /// ``ExecutionLoop/runOnce(queue:exportURL:batchPolicy:retryPolicy:transport:sleep:afterDrain:)``
    /// calls ``DurableRemoteQueue/acknowledge()`` after a drain
    /// whose ``DurableRemoteQueueBatch/byteCount`` is `0` so a
    /// polling caller does not get blocked on
    /// ``DurableRemoteQueueError/batchAlreadyOutstanding`` on the
    /// next pass. The release is keyed off the authoritative
    /// zero-byte signal the queue returns from drain, before any
    /// export-file read; a missing or unreadable export artifact
    /// after the queue's authoritative zero-byte drain signal
    /// cannot block this path. The empty release does not advance
    /// any destructive-removal of delivered queue payload bytes —
    /// there are no delivered queue payload bytes — so it stays
    /// distinct from the acknowledgement-to-removal lifecycle
    /// ``RemoteEngine/flush()`` runs for non-empty flush passes.
    /// A failure of that empty release surfaces here rather than
    /// being masked as ``ExecutionLoopError/drainFailed(_:)``.
    case emptyBatchReleaseFailed(DurableRemoteQueueError)

    /// The sleep injector threw between two batch dispatch
    /// rounds. Treated as an opaque interruption at this layer;
    /// the engine layer above (``RemoteEngine/flush()``) decides
    /// whether to retry the drained batch on the next
    /// caller-driven flush through the outstanding-reuse path.
    case sleepInterrupted

    /// The batch-round dispatcher reached a state that should be
    /// unreachable from validated inputs: at least one entry had
    /// no recorded outcome after dispatch rounds completed. This
    /// signals an engine-side active-set / outcome-tracking defect,
    /// not an adapter transport contract violation.
    case internalBatchStateInvalid

    /// ``RemoteTransport/sendBatch(_:)`` returned a result array
    /// whose count does not match the number of items the engine
    /// handed it. The engine fails closed rather than guessing
    /// which entries the surplus or missing results refer to.
    /// `expected` is the input item count the engine handed
    /// to ``RemoteTransport/sendBatch(_:)``; `actual` is the
    /// returned result count.
    case transportBatchCountMismatch(expected: Int, actual: Int)
}

extension ExecutionLoopError: Equatable {
    // `sleepInterrupted` drops the underlying error to keep the type
    // `Equatable` for test assertions; the engine treats every
    // sleep-injector failure as the same outcome at this layer, so
    // an associated value would carry information no consumer reads.
    static func == (lhs: ExecutionLoopError, rhs: ExecutionLoopError) -> Bool {
        switch (lhs, rhs) {
        case let (.drainFailed(left), .drainFailed(right)):
            return left == right
        case let (.recoverFailed(left), .recoverFailed(right)):
            return left == right
        case let (.batchSplitFailed(left), .batchSplitFailed(right)):
            return left == right
        case let (.invalidRetryDelay(left), .invalidRetryDelay(right)):
            return left == right
        case let (.emptyBatchReleaseFailed(left), .emptyBatchReleaseFailed(right)):
            return left == right
        case (.sleepInterrupted, .sleepInterrupted):
            return true
        case (.internalBatchStateInvalid, .internalBatchStateInvalid):
            return true
        case let (
            .transportBatchCountMismatch(leftExpected, leftActual),
            .transportBatchCountMismatch(rightExpected, rightActual)
        ):
            return leftExpected == rightExpected && leftActual == rightActual
        default:
            return false
        }
    }
}
