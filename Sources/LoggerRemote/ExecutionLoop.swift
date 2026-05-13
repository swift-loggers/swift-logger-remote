import Foundation

/// Engine-internal one-shot execution loop driven over the existing
/// queue + batching + retry primitives.
///
/// ``runOnce(queue:exportURL:batchPolicy:retryPolicy:transport:sleep:afterDrain:)``
/// performs one full delivery pass:
///
/// 1. ``DurableRemoteQueue/drain(to:)`` captures the current
///    recoverable prefix as a byte-stable export file.
/// 2. ``BatchEngine/recoverEntries(from:)`` parses the
///    export back into an ordered ``RemoteDeliveryEntry`` stream.
/// 3. ``BatchEngine/makeBatches(from:policy:)`` splits the stream
///    into ordered batches under ``RemoteBatchPolicy``.
/// 4. For every entry in every batch, ``RetryExecutor/deliver(entry:transport:policy:sleep:delayCalculator:)``
///    drives the per-entry retry loop and produces one
///    ``RemoteDeliveryAttempt``. The retry loop calls
///    ``RemoteTransport/send(payloadBytes:payloadMetadata:)`` and
///    ``RemoteTransport/classify(_:)`` once per attempt.
///
/// The loop returns the ordered ``RemoteDeliveryAttempt`` array
/// across all batches. For non-empty flush passes the queue's
/// outstanding-batch state is preserved verbatim — ``runOnce``
/// **never** invokes ``DurableRemoteQueue/acknowledge()`` for a
/// non-empty drained batch, and performs no destructive removal
/// of delivered queue payload bytes; that lifecycle closure is
/// owned by ``RemoteEngine/flush()`` above this layer.
///
/// The empty-drain path is intentionally different: when the
/// queue reports ``DurableRemoteQueueBatch/byteCount`` `== 0`
/// after drain, ``runOnce`` short-circuits **before** invoking
/// ``BatchEngine/recoverEntries(from:)`` — there is nothing to
/// parse — and releases the held outstanding-batch boundary by
/// calling ``DurableRemoteQueue/acknowledge()`` before returning
/// `[]`. Because the release is keyed off the authoritative
/// zero-byte signal the queue returns rather than off "the
/// recovered entry stream is empty", a missing or unreadable
/// export artifact after the queue's authoritative zero-byte
/// drain signal cannot block the empty polling path. No delivered
/// queue payload bytes are removed by this layer — the empty
/// export contains none — but the in-memory boundary is cleared
/// so a polling caller does not get blocked on
/// ``DurableRemoteQueueError/batchAlreadyOutstanding`` on the
/// next pass. A failure of that empty release surfaces as
/// ``ExecutionLoopError/emptyBatchReleaseFailed(_:)`` rather than
/// being masked as ``ExecutionLoopError/drainFailed(_:)``.
///
/// LGR alignment:
/// - LGR-3 — the engine does not own the wall-clock timer; the
///   `sleep` injector decides how a backoff duration in seconds
///   becomes real wall time.
/// - LGR-5 — each ``RemoteTransport/send(payloadBytes:payloadMetadata:)``
///   call carries one entry's payload + metadata; the engine never
///   aggregates entry bytes into a wire-format-imposing batch
///   request.
/// - LGR-10 / LGR-11 — the loop reads accepted bytes only through
///   the persistence-owned ``DurableRemoteQueue/drain(to:)`` and
///   does not advance the destructive-removal boundary.
internal enum ExecutionLoop {
    // swiftlint:disable function_parameter_count
    // Reason: One-shot execution-loop entry point bundling the
    // durable queue, batching, retry, transport, and sleep
    // injectors LGR-3 / LGR-5 keep deliberately separated, plus
    // the engine-internal `afterDrain` test seam; folding them
    // into a config struct would only relocate the count without
    // simplifying the engine-internal call site.

    /// Runs one delivery pass over the queue's current recoverable
    /// prefix.
    ///
    /// - Parameters:
    ///   - queue: Persistence-backed durable queue providing the
    ///     drain boundary.
    ///   - exportURL: Destination file for the byte-stable export
    ///     ``DurableRemoteQueue/drain(to:)`` writes.
    ///   - batchPolicy: Public batch boundary contract consumed by
    ///     ``BatchEngine/makeBatches(from:policy:)``.
    ///   - retryPolicy: Public retry budget + backoff schedule
    ///     consumed by ``RetryExecutor``.
    ///   - transport: Sink-neutral transport conformer; called once
    ///     per entry attempt for ``RemoteTransport/send(payloadBytes:payloadMetadata:)``
    ///     and once per attempt for ``RemoteTransport/classify(_:)``.
    ///   - sleep: Sleep injector applied between two retryable
    ///     attempts; never invoked after the final attempt or after
    ///     a `.success` / `.terminal` outcome.
    ///   - afterDrain: Engine-internal seam invoked once with the
    ///     freshly captured ``DurableRemoteQueueBatch`` immediately
    ///     after ``DurableRemoteQueue/drain(to:)`` returns and
    ///     before the loop inspects ``DurableRemoteQueueBatch/byteCount``.
    ///     Defaults to a no-op; the test target uses it to drive
    ///     deterministic regressions of the empty-drain
    ///     short-circuit (e.g. removing an export artifact after
    ///     the queue's authoritative zero-byte drain signal).
    ///     Production callers leave the default.
    /// - Returns: Ordered per-entry ``RemoteDeliveryAttempt`` values
    ///   across every batch in batch traversal order and entry
    ///   traversal order within each batch, matching the drained
    ///   export traversal order. An empty queue produces `[]` and
    ///   the queue's outstanding-batch boundary is released so a
    ///   polling caller can run a subsequent `runOnce` without
    ///   tripping ``DurableRemoteQueueError/batchAlreadyOutstanding``.
    /// - Throws: ``ExecutionLoopError`` for drain, recovery,
    ///   batch-split, or empty-drain release failure, or for a
    ///   sleep-injector failure between two retryable attempts.
    static func runOnce(
        queue: DurableRemoteQueue,
        exportURL: URL,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy,
        transport: any RemoteTransport,
        sleep: @Sendable (Double) async throws -> Void,
        afterDrain: @Sendable (DurableRemoteQueueBatch) async -> Void = { _ in }
    ) async throws(ExecutionLoopError) -> [RemoteDeliveryAttempt] {
        let batch: DurableRemoteQueueBatch
        do {
            batch = try await queue.drain(to: exportURL)
        } catch {
            throw .drainFailed(error)
        }
        await afterDrain(batch)
        if batch.byteCount == 0 {
            // Short-circuit on the queue's authoritative zero-byte
            // signal before any export-file read. The queue is
            // still holding the outstanding-batch boundary from
            // the drain above; release it so a polling caller is
            // not blocked on `.batchAlreadyOutstanding` on the
            // next pass. Keying off `batch.byteCount == 0` rather
            // than "the recovered entry stream is empty" means a
            // missing or unreadable export artifact after the
            // queue's authoritative zero-byte drain signal cannot
            // block this path. The release does not advance any
            // destructive-removal of delivered queue payload bytes
            // (the empty export contains none) and stays distinct
            // from the non-empty acknowledgement-to-removal
            // lifecycle that `RemoteEngine.flush()` runs above
            // this layer.
            do {
                try await queue.acknowledge()
            } catch {
                throw .emptyBatchReleaseFailed(error)
            }
            return []
        }
        do {
            let outcome = try await deliver(
                batch: batch,
                batchPolicy: batchPolicy,
                retryPolicy: retryPolicy,
                transport: transport,
                sleep: sleep
            )
            return outcome.attempts
        } catch {
            // `BatchDeliveryError` → `ExecutionLoopError` for
            // `runOnce` callers that branch on the broader
            // surface that also carries drain / empty-release
            // cases.
            switch error {
            case let .recoverFailed(batchEngineError):
                throw .recoverFailed(batchEngineError)
            case let .batchSplitFailed(deliveryError):
                throw .batchSplitFailed(deliveryError)
            case let .invalidRetryDelay(deliveryError):
                throw .invalidRetryDelay(deliveryError)
            case .sleepInterrupted:
                throw .sleepInterrupted
            }
        }
    }

    // swiftlint:enable function_parameter_count

    /// Processes an already-captured non-empty
    /// ``DurableRemoteQueueBatch``: parses the byte-stable export
    /// via ``BatchEngine/recoverEntries(from:)``, partitions
    /// entries through ``BatchEngine/makeBatches(from:policy:)``,
    /// and drives every entry through
    /// ``RetryExecutor/deliver(entry:transport:policy:sleep:delayCalculator:)``
    /// sequentially without batch-level aggregation or shared
    /// retry state. Shared between ``runOnce(queue:exportURL:batchPolicy:retryPolicy:transport:sleep:afterDrain:)``
    /// (which captures the batch with a fresh drain) and
    /// ``RemoteEngine/flush()`` (which may reuse a still-held
    /// outstanding batch from a prior flush).
    ///
    /// The helper never touches the queue's
    /// ``DurableRemoteQueue/acknowledge()`` boundary; callers own
    /// the acknowledgement-to-removal decision after consuming the
    /// returned ``RemoteDeliveryAttempt`` array.
    static func deliver(
        batch: DurableRemoteQueueBatch,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy,
        transport: any RemoteTransport,
        sleep: @Sendable (Double) async throws -> Void
    ) async throws(BatchDeliveryError) -> BatchDeliveryOutcome {
        let entries: [RemoteDeliveryEntry]
        do {
            entries = try BatchEngine.recoverEntries(from: batch)
        } catch {
            throw .recoverFailed(error)
        }
        let groups: [[RemoteDeliveryEntry]]
        do {
            groups = try BatchEngine.makeBatches(
                from: entries, policy: batchPolicy
            )
        } catch {
            throw .batchSplitFailed(error)
        }
        var attempts: [RemoteDeliveryAttempt] = []
        for group in groups {
            for entry in group {
                let attempt = try await RetryExecutor.deliver(
                    entry: entry,
                    transport: transport,
                    policy: retryPolicy,
                    sleep: sleep
                )
                attempts.append(attempt)
            }
        }
        return BatchDeliveryOutcome(
            batchCount: groups.count, attempts: attempts
        )
    }
}

/// Engine-internal outcome of
/// ``ExecutionLoop/deliver(batch:batchPolicy:retryPolicy:transport:sleep:)``.
///
/// Carries both the batch count (number of groups
/// ``BatchEngine/makeBatches(from:policy:)`` produced) and the
/// flat per-entry attempts so callers can build the public
/// ``RemoteFlushSummary`` without re-counting groups themselves.
internal struct BatchDeliveryOutcome: Sendable, Equatable {
    let batchCount: Int
    let attempts: [RemoteDeliveryAttempt]
}
