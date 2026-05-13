/// Engine-internal failure surface for the shared
/// ``ExecutionLoop/deliver(batch:batchPolicy:retryPolicy:transport:sleep:)``
/// helper that `RemoteEngine.flush()` and
/// `ExecutionLoop.runOnce(...)` both drive.
///
/// Narrower than ``ExecutionLoopError`` on purpose: this surface
/// only carries the four cases the post-drain delivery path can
/// raise (parse, batch-split, retry-delay calc, sleep-injector).
/// The drain step and the empty-release acknowledge step are not
/// part of `deliver(batch:)` — they live in
/// ``ExecutionLoop/runOnce(...)`` and in
/// ``RemoteEngine/flush()`` respectively — so this enum
/// deliberately omits those cases instead of forcing the
/// public-side translation in ``RemoteEngine`` to handle
/// unreachable defensive fallbacks.
///
/// `RemoteEngine.flush()` translates each case into a public
/// ``RemoteEngineError`` value one-for-one; `runOnce`
/// translates each case into the broader ``ExecutionLoopError``
/// shape that also carries the drain / empty-release cases.
internal enum BatchDeliveryError: Error, Sendable, Equatable {
    /// `BatchEngine.recoverEntries(from:)` failed fail-closed
    /// before any transport call could be made.
    case recoverFailed(BatchEngineError)

    /// `BatchEngine.makeBatches(from:policy:)` failed (e.g.
    /// oversized single entry surfaces
    /// ``RemoteDeliveryError/batchSizeExceeded(limit:actual:)``
    /// or an engine-side batching-state defect surfaces
    /// ``RemoteDeliveryError/invalidBatchState``).
    case batchSplitFailed(RemoteDeliveryError)

    /// The retry-delay calculation
    /// (``RemoteRetryPolicy/delayBeforeRetry(attempt:)`` or an
    /// injected equivalent) refused the engine's requested
    /// attempt count before any sleep could be scheduled.
    /// ``RemoteRetryPolicy/make(maxAttempts:backoff:)`` already
    /// enforces that the default calculator accepts every
    /// `attempt` value the executor passes from a validated
    /// policy, so this case names an engine-side /
    /// seam-injected invariant violation rather than a
    /// caller-actionable failure. ``ExecutionLoop/runOnce(...)``
    /// maps this case to ``ExecutionLoopError/invalidRetryDelay(_:)``
    /// at the broader execution-loop surface.
    case invalidRetryDelay(RemoteDeliveryError)

    /// The sleep injector threw between two retryable attempts.
    /// Treated as an opaque interruption at this layer; the
    /// engine layer above decides whether to retry the drained
    /// batch on the next caller-driven flush through the
    /// outstanding-reuse path.
    case sleepInterrupted
}
