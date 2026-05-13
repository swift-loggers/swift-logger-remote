/// Engine-internal failure surface for the shared
/// `ExecutionLoop.deliver(batch:...)`
/// helper that `RemoteEngine.flush()` and
/// `ExecutionLoop.runOnce(...)` both drive.
///
/// Narrower than ``ExecutionLoopError`` on purpose: this surface
/// only carries the post-drain delivery-path failures (parse,
/// batch-split, retry-delay calc, sleep-injector, and transport
/// batch-response count mismatch), plus an internal batch-state
/// invariant marker. The drain step and the empty-release
/// acknowledge step are not part of
/// `deliver(batch:)` — they live in
/// `ExecutionLoop.runOnce(...)` and in
/// `RemoteEngine.flush()` respectively — so this enum
/// deliberately omits those cases instead of forcing the
/// public-side translation in ``RemoteEngine`` to handle
/// unreachable defensive fallbacks.
///
/// `RemoteEngine.flush()` translates each case into a public
/// ``RemoteEngineError`` value; `internalBatchStateInvalid`
/// projects as ``RemoteEngineError/batchFailed(_:)`` carrying
/// ``RemoteDeliveryError/invalidBatchState``. `runOnce`
/// translates each case into the broader ``ExecutionLoopError``
/// shape that also carries the drain / empty-release cases.
internal enum BatchDeliveryError: Error, Sendable, Equatable {
    /// `BatchEngine.recoverEntries(from:)` failed before any
    /// transport call could be made.
    case recoverFailed(BatchEngineError)

    /// `BatchEngine.makeBatches(from:policy:)` failed (e.g.
    /// oversized single entry surfaces
    /// ``RemoteDeliveryError/batchSizeExceeded(limit:actual:)``
    /// or an engine-side batching-state defect surfaces
    /// ``RemoteDeliveryError/invalidBatchState``).
    case batchSplitFailed(RemoteDeliveryError)

    /// The retry-delay calculation
    /// (`RemoteRetryPolicy.delayBeforeRetry(attempt:)` or an
    /// injected equivalent) refused the engine's requested
    /// attempt count before any sleep could be scheduled between
    /// two batch dispatch rounds.
    /// `RemoteRetryPolicy.make(maxAttempts:backoff:)` already
    /// enforces that the default calculator accepts every
    /// `attempt` value the engine passes from a validated
    /// policy, so this case names an engine-side /
    /// seam-injected invariant violation rather than a
    /// caller-actionable failure. `ExecutionLoop.runOnce(...)`
    /// maps this case to ``ExecutionLoopError/invalidRetryDelay(_:)``
    /// at the broader execution-loop surface.
    case invalidRetryDelay(RemoteDeliveryError)

    /// The sleep injector threw between two batch dispatch
    /// rounds. Treated as an opaque interruption at this layer;
    /// the engine layer above decides whether to retry the
    /// drained batch on the next caller-driven flush through the
    /// outstanding-reuse path.
    case sleepInterrupted

    /// The batch-round dispatcher reached a state that should be
    /// unreachable from validated inputs: at least one entry had
    /// no recorded outcome after dispatch rounds completed. This
    /// signals an engine-side active-set / outcome-tracking defect,
    /// not an adapter transport contract violation.
    case internalBatchStateInvalid

    /// The transport's `RemoteTransport.sendBatch(_:)` returned
    /// a result array whose count does not match the number of
    /// items the engine handed it. Per
    /// `RemoteTransport.sendBatch(_:)` the returned array MUST
    /// have one element per input item in the same order; a
    /// count mismatch is an adapter-contract violation the
    /// engine cannot map back to per-entry outcomes, so the
    /// engine fails closed rather than guessing which entries
    /// the surplus or missing results refer to. The carried
    /// `expected` is the input item count; `actual` is the
    /// returned result count.
    case transportBatchCountMismatch(expected: Int, actual: Int)
}
