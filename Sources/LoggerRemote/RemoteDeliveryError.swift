/// Typed diagnostic surface for remote-engine contract failures.
///
/// Adapters classify their own response models into these cases so
/// the engine's retry / terminal decision is sink-neutral.
public enum RemoteDeliveryError: Error, Sendable, Equatable {
    /// A batch was submitted with no entries.
    case batchEmpty
    /// A batch exceeded the configured byte cap. Includes the
    /// explicit oversized-single-entry rejection: a candidate entry
    /// whose byte count alone exceeds the batch byte cap can never
    /// fit any batch and is rejected with this error rather than
    /// being treated as a perpetual boundary.
    case batchSizeExceeded(limit: Int, actual: Int)
    /// A retry policy failed factory validation (e.g. non-positive
    /// attempt count, non-positive backoff seconds).
    case invalidRetryPolicy
    /// A batch policy failed factory validation (e.g. non-positive
    /// entry count or byte cap).
    case invalidBatchPolicy
    /// Internal batching state violated the non-negative and
    /// current-within-policy invariants the engine maintains. Signals
    /// an engine-side defect, not a caller-actionable validation
    /// failure.
    case invalidBatchState
    /// The transport reported a delivery failure the adapter
    /// classified as non-success. Status-code or body-code
    /// classification is adapter-owned and stays out of the
    /// sink-neutral engine error surface.
    case transportRejected
    /// The acknowledgement-tracking surface lost track of a delivered
    /// entry. Implementation-defect signal, not caller-actionable.
    case acknowledgementMissing
}
