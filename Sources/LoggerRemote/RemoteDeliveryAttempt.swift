/// Per-entry outcome of the engine-internal retry execution loop.
///
/// Records the entry that was delivered, the final
/// ``RemoteDeliveryResult`` after the retry budget was consumed (or
/// `.success` / `.terminal` reached), and the number of attempts
/// that were actually consumed (1-indexed, never above
/// ``RemoteRetryPolicy/maxAttempts``).
///
/// The type is engine-internal: `LoggerRemote` exposes no public
/// per-entry attempt surface (the public ``RemoteFlushSummary``
/// returned by ``RemoteEngine/flush()`` only carries per-class
/// counts), so attempts flow only through internal machinery and
/// the test target via `@testable import`.
internal struct RemoteDeliveryAttempt: Sendable, Equatable {
    /// The entry the engine attempted to deliver.
    let entry: RemoteDeliveryEntry

    /// Final result for the entry after the retry budget was
    /// consumed or a terminal / success outcome was reached. A
    /// `.retryable` outcome here means the retry budget was
    /// exhausted with the last attempt still retryable; the engine
    /// does not retry beyond ``RemoteRetryPolicy/maxAttempts``.
    let outcome: RemoteDeliveryResult

    /// Number of attempts the engine actually consumed for the
    /// entry. Always `1 ... policy.maxAttempts`. A `.success` or
    /// `.terminal` outcome stops attempts as soon as it is reached;
    /// a `.retryable` outcome consumes the full budget.
    let attempts: Int
}
