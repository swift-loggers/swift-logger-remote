/// Engine-internal per-entry retry executor.
///
/// ``deliver(entry:transport:policy:sleep:delayCalculator:)`` drives
/// one ``RemoteDeliveryEntry`` through ``RemoteRetryPolicy``
/// semantics:
///
/// - Each attempt invokes ``RemoteTransport/send(payloadBytes:payloadMetadata:)``
///   with the entry's `payload` and `metadata`. The transport call is
///   per-entry; the engine never aggregates entry bytes into a single
///   transport payload (LGR-5: sink-neutrality). Batches from
///   ``BatchEngine/makeBatches(from:policy:)`` are the iteration unit
///   the higher-level ``ExecutionLoop`` walks, not a wire-request
///   atomic unit.
/// - ``RemoteTransport/classify(_:)`` maps the per-attempt result
///   (either a ``RemoteTransportResponse`` or a thrown error) into a
///   ``RemoteDeliveryResult``. Classification is sink-owned
///   (LGR-7 / LGR-9) and the engine never inspects HTTP status,
///   vendor body codes, or transport error types.
/// - `.success` and `.terminal` outcomes stop attempts immediately;
///   only `.retryable` consumes additional budget.
/// - Backoff between two retryable attempts is computed by the
///   injected `delayCalculator` (defaulting to
///   ``RemoteRetryPolicy/delayBeforeRetry(attempt:)``) and applied
///   through the injected `sleep` closure (LGR-3: the engine does
///   not own the wall-clock timer). Sleep happens **only** between
///   retryable attempts; the final attempt is followed by an
///   immediate return rather than a no-op sleep.
/// - A delay-calculation failure surfaces as
///   ``BatchDeliveryError/invalidRetryDelay(_:)`` carrying the
///   underlying ``RemoteDeliveryError``. The
///   ``RemoteRetryPolicy/make(maxAttempts:backoff:)`` factory
///   already enforces that the default
///   ``RemoteRetryPolicy/delayBeforeRetry(attempt:)`` accepts every
///   `attempt` value the executor passes, so the diagnostic is an
///   engine-side / seam-injected invariant marker — not a
///   `sleepInterrupted` collapse.
/// - A `sleep` failure between two retryable attempts surfaces as
///   ``BatchDeliveryError/sleepInterrupted``, distinct from a
///   delay-calculation failure.
///
/// The executor never invokes ``DurableRemoteQueue/acknowledge()``
/// and performs no destructive removal of delivered queue payload
/// bytes; the acknowledgement-to-removal lifecycle closure for
/// non-empty flush passes is driven by ``RemoteEngine/flush()``
/// above this layer.
internal enum RetryExecutor {
    /// Drives one entry through the retry loop.
    ///
    /// - Parameters:
    ///   - entry: Durable delivery entry to attempt.
    ///   - transport: Sink-neutral transport conformer; the executor
    ///     calls ``RemoteTransport/send(payloadBytes:payloadMetadata:)``
    ///     once per attempt with `entry.payload` and `entry.metadata`
    ///     and then ``RemoteTransport/classify(_:)`` to map the
    ///     result into ``RemoteDeliveryResult``.
    ///   - policy: Retry budget + backoff schedule. The executor
    ///     stops at `policy.maxAttempts` regardless of whether the
    ///     last attempt was retryable.
    ///   - sleep: Sleep injector applied between two retryable
    ///     attempts. The engine never sleeps after the final attempt.
    ///   - delayCalculator: Engine-internal seam returning the
    ///     backoff seconds for the just-completed retryable attempt.
    ///     Defaults to
    ///     ``RemoteRetryPolicy/delayBeforeRetry(attempt:)``; the
    ///     parameter exists so the test target can drive the
    ///     ``BatchDeliveryError/invalidRetryDelay(_:)`` branch that
    ///     ``RemoteRetryPolicy/make(maxAttempts:backoff:)`` makes
    ///     unreachable from the public API. Production callers
    ///     leave the default.
    /// - Returns: The per-entry ``RemoteDeliveryAttempt`` with the
    ///   final outcome and the number of attempts actually consumed
    ///   (1-indexed, never above `policy.maxAttempts`).
    /// - Throws:
    ///   - ``BatchDeliveryError/invalidRetryDelay(_:)`` when the
    ///     delay calculator refuses the requested attempt count.
    ///   - ``BatchDeliveryError/sleepInterrupted`` when the
    ///     `sleep` closure throws between two retryable attempts
    ///     (e.g. cooperative task cancellation propagated from
    ///     Swift concurrency sleep primitives).
    static func deliver(
        entry: RemoteDeliveryEntry,
        transport: any RemoteTransport,
        policy: RemoteRetryPolicy,
        sleep: @Sendable (Double) async throws -> Void,
        delayCalculator: @Sendable (RemoteRetryPolicy, Int) throws(RemoteDeliveryError) -> Double = { policy, attempt in
            try policy.delayBeforeRetry(attempt: attempt)
        }
    ) async throws(BatchDeliveryError) -> RemoteDeliveryAttempt {
        var attempt = 1
        while true {
            let result: Result<RemoteTransportResponse, any Error>
            do {
                let response = try await transport.send(
                    payloadBytes: entry.payload,
                    payloadMetadata: entry.metadata
                )
                result = .success(response)
            } catch {
                result = .failure(error)
            }
            let outcome = await transport.classify(result)
            switch outcome {
            case .success, .terminal:
                return RemoteDeliveryAttempt(
                    entry: entry, outcome: outcome, attempts: attempt
                )
            case .retryable:
                // Final retryable attempt: return without sleeping
                // so the engine never burns wall time after the
                // last attempt the policy permits.
                if attempt >= policy.maxAttempts {
                    return RemoteDeliveryAttempt(
                        entry: entry, outcome: outcome, attempts: attempt
                    )
                }
                let seconds: Double
                do {
                    seconds = try delayCalculator(policy, attempt)
                } catch {
                    // The default `delayCalculator` only throws
                    // when `attempt` is outside `1 ..< maxAttempts`,
                    // which `RemoteRetryPolicy.make(...)` makes
                    // unreachable from the public API (the loop
                    // above also proves the bound). A failure here
                    // is an engine-side or seam-injected invariant
                    // violation; surface it as
                    // `.invalidRetryDelay(_:)` so the policy-space
                    // failure stays distinct from a sleep-injector
                    // failure.
                    throw .invalidRetryDelay(error)
                }
                do {
                    try await sleep(seconds)
                } catch {
                    throw .sleepInterrupted
                }
                attempt += 1
            }
        }
    }
}
