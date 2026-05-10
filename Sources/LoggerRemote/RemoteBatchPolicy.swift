/// Defines deterministic batch-boundary decisions for remote delivery.
///
/// Both caps are enforced; whichever fires first closes the batch.
/// Boundary semantics are equal-to-cap fits, strictly-greater fires.
public struct RemoteBatchPolicy: Sendable, Equatable {
    /// Maximum number of entries per batch. Must be `>= 1`.
    public let maxEntryCount: Int

    /// Maximum total payload bytes per batch. Must be `>= 1`.
    public let maxByteCount: Int

    /// Creates a batch policy after validating its inputs.
    ///
    /// - Throws: ``RemoteDeliveryError/invalidBatchPolicy`` when
    ///   either cap is below `1`.
    public static func make(
        maxEntryCount: Int,
        maxByteCount: Int
    ) throws(RemoteDeliveryError) -> RemoteBatchPolicy {
        guard maxEntryCount >= 1, maxByteCount >= 1 else {
            throw .invalidBatchPolicy
        }
        return RemoteBatchPolicy(
            maxEntryCount: maxEntryCount,
            maxByteCount: maxByteCount
        )
    }

    /// Returns whether adding `nextEntryByteCount` bytes to a batch
    /// currently holding `currentEntryCount` entries and
    /// `currentByteCount` payload bytes would close the batch.
    ///
    /// The helper is internal: batch policy is the public contract
    /// model; the boundary helper is engine-side machinery, not
    /// part of the public API surface.
    ///
    /// - Throws:
    ///   - ``RemoteDeliveryError/invalidBatchState`` when
    ///     `currentEntryCount`, `currentByteCount`, or
    ///     `nextEntryByteCount` is negative, or when the current
    ///     batch state already exceeds either cap. The engine must
    ///     not reach a negative or beyond-policy state; surfacing
    ///     the violation surfaces an engine-side defect rather than
    ///     silently emitting an undefined boundary decision.
    ///   - ``RemoteDeliveryError/batchSizeExceeded(limit:actual:)``
    ///     when the candidate single entry's byte count alone
    ///     exceeds `maxByteCount`. An oversized single entry can
    ///     never fit any batch and is rejected explicitly rather
    ///     than treated as a perpetual boundary.
    internal func wouldExceed(
        currentEntryCount: Int,
        currentByteCount: Int,
        nextEntryByteCount: Int
    ) throws(RemoteDeliveryError) -> Bool {
        guard currentEntryCount >= 0,
              currentByteCount >= 0,
              nextEntryByteCount >= 0
        else {
            throw .invalidBatchState
        }
        guard currentEntryCount <= maxEntryCount,
              currentByteCount <= maxByteCount
        else {
            throw .invalidBatchState
        }
        if nextEntryByteCount > maxByteCount {
            throw .batchSizeExceeded(
                limit: maxByteCount, actual: nextEntryByteCount
            )
        }
        let (nextCount, countOverflow) = currentEntryCount.addingReportingOverflow(1)
        if countOverflow { return true }
        if nextCount > maxEntryCount { return true }
        let (nextBytes, byteOverflow) = currentByteCount.addingReportingOverflow(nextEntryByteCount)
        if byteOverflow { return true }
        return nextBytes > maxByteCount
    }

    private init(maxEntryCount: Int, maxByteCount: Int) {
        self.maxEntryCount = maxEntryCount
        self.maxByteCount = maxByteCount
    }
}
