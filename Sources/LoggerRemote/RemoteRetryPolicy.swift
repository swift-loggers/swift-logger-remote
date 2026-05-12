/// Retry policy for remote delivery attempts.
///
/// The policy is the public contract model — locked attempt-count
/// bounds, locked backoff schedule. Runtime execution is driven
/// by the engine-internal `RetryExecutor` / `ExecutionLoop`
/// and surfaced through ``RemoteEngine/flush()``.
public struct RemoteRetryPolicy: Sendable, Equatable {
    /// Upper bound on `maxAttempts`. Attempt counts above this are
    /// rejected at construction time.
    public static let maxSupportedAttempts = 100

    /// Upper bound on any single backoff value, in seconds. Backoff
    /// values above this are rejected at construction time and any
    /// exponential delay is clamped to `capSeconds` at runtime.
    public static let maxBackoffSeconds: Double = 86400

    /// Maximum number of delivery attempts (initial attempt plus
    /// retries). Must be `1 ... maxSupportedAttempts`.
    public let maxAttempts: Int

    /// Backoff schedule between retry attempts.
    public let backoff: BackoffSchedule

    /// Backoff schedule between retries.
    public enum BackoffSchedule: Sendable, Equatable {
        /// Wait the same number of seconds between every retry.
        case constant(seconds: Double)
        /// Exponential backoff capped at `capSeconds`. Wait time
        /// after attempt `n` (1-indexed) is
        /// `min(initialSeconds * multiplier^(n - 1), capSeconds)`.
        case exponential(initialSeconds: Double, multiplier: Double, capSeconds: Double)
    }

    /// Creates a retry policy after validating its inputs against
    /// the public bounds.
    ///
    /// - Throws: ``RemoteDeliveryError/invalidRetryPolicy`` when
    ///   `maxAttempts` is outside `1 ... maxSupportedAttempts`, or
    ///   when any backoff field is non-finite, non-positive, beyond
    ///   `maxBackoffSeconds`, or fails the schedule-specific
    ///   ordering invariants (`multiplier > 1`,
    ///   `capSeconds >= initialSeconds`).
    public static func make(
        maxAttempts: Int,
        backoff: BackoffSchedule
    ) throws(RemoteDeliveryError) -> RemoteRetryPolicy {
        guard maxAttempts >= 1, maxAttempts <= maxSupportedAttempts else {
            throw .invalidRetryPolicy
        }
        switch backoff {
        case let .constant(seconds):
            guard seconds.isFinite,
                  seconds > 0,
                  seconds <= maxBackoffSeconds
            else {
                throw .invalidRetryPolicy
            }
        case let .exponential(initialSeconds, multiplier, capSeconds):
            guard initialSeconds.isFinite,
                  initialSeconds > 0,
                  initialSeconds <= maxBackoffSeconds,
                  multiplier.isFinite,
                  multiplier > 1,
                  capSeconds.isFinite,
                  capSeconds >= initialSeconds,
                  capSeconds <= maxBackoffSeconds
            else {
                throw .invalidRetryPolicy
            }
        }
        return RemoteRetryPolicy(maxAttempts: maxAttempts, backoff: backoff)
    }

    /// Returns the delay after a failed 1-indexed attempt.
    internal func delayBeforeRetry(
        attempt: Int
    ) throws(RemoteDeliveryError) -> Double {
        guard attempt >= 1, attempt < maxAttempts else {
            throw .invalidRetryPolicy
        }
        switch backoff {
        case let .constant(seconds):
            return seconds
        case let .exponential(initialSeconds, multiplier, capSeconds):
            var raw = initialSeconds
            for _ in 1 ..< attempt {
                raw *= multiplier
                // Clamp overflow or cap breach to the configured cap.
                if !raw.isFinite || raw > capSeconds {
                    return capSeconds
                }
            }
            return raw
        }
    }

    private init(maxAttempts: Int, backoff: BackoffSchedule) {
        self.maxAttempts = maxAttempts
        self.backoff = backoff
    }
}
