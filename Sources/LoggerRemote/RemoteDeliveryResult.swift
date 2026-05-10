/// Outcome the engine assigns to one delivery attempt.
///
/// Adapters classify their own response models into these cases:
/// success, retryable failure (the engine should re-queue under the
/// configured retry policy), or terminal failure (the engine must
/// not retry).
public enum RemoteDeliveryResult: Sendable, Equatable {
    /// Delivery was acknowledged by the adapter.
    case success
    /// Delivery failed and may be retried by engine policy.
    case retryable(reason: RemoteDeliveryError)
    /// Delivery failed permanently and must not be retried.
    case terminal(reason: RemoteDeliveryError)
}
