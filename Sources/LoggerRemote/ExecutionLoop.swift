import Foundation

/// Engine-internal one-shot execution loop that drives delivery
/// as batch rounds over the existing queue + batching primitives.
///
/// ``runOnce(queue:exportURL:batchPolicy:retryPolicy:transport:sleep:afterDrain:)``
/// performs one full delivery pass:
///
/// 1. ``DurableRemoteQueue/drain(to:)`` captures the current
///    recoverable prefix as a byte-stable export file.
/// 2. ``BatchEngine/recoverEntries(from:)`` parses the export
///    back into an ordered ``RemoteDeliveryEntry`` stream.
/// 3. ``BatchEngine/makeBatches(from:policy:)`` splits the
///    stream into ordered batches under ``RemoteBatchPolicy``.
/// 4. For each batch, the loop dispatches active entries
///    to ``RemoteTransport/sendBatch(_:)`` in rounds: round 1
///    contains every entry in the batch, every subsequent round
///    re-dispatches only the entries whose previous classification
///    from the immediately preceding round was
///    ``RemoteDeliveryResult/retryable(reason:)``. Each
///    dispatched item is mapped through
///    ``RemoteTransport/classify(_:)`` to produce a
///    ``RemoteDeliveryResult``. The dispatcher stops when every
///    entry is resolved (``RemoteDeliveryResult/success`` or
///    ``RemoteDeliveryResult/terminal(reason:)``) or
///    `retryPolicy.maxAttempts` rounds have been spent.
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
/// - LGR-5 — ``RemoteTransport/sendBatch(_:)`` is sink-neutral:
///   the engine hands the adapter an ordered array of
///   ``RemoteTransportBatchItem`` values and the adapter decides
///   the wire shape (one HTTP request per item for single-event
///   sinks, one shared request for batch-aggregating sinks like
///   Elastic `_bulk`). The engine never inspects HTTP status,
///   vendor body codes, or transport error types.
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
    ///   - retryPolicy: Public retry budget + backoff schedule.
    ///     `maxAttempts` bounds the number of batch dispatch
    ///     rounds for each batch.
    ///   - transport: Sink-neutral transport conformer. The loop
    ///     calls ``RemoteTransport/sendBatch(_:)`` once per
    ///     dispatch round with the active items for that round
    ///     and ``RemoteTransport/classify(_:)`` once per returned
    ///     result.
    ///   - sleep: Sleep injector applied between two dispatch
    ///     rounds; never invoked before the first round or after
    ///     the final round.
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
    ///   batch-split, empty-drain release, sleep-injector failure
    ///   between two dispatch rounds, or transport batch-response
    ///   count mismatch.
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
            case .internalBatchStateInvalid:
                throw .internalBatchStateInvalid
            case let .transportBatchCountMismatch(expected, actual):
                throw .transportBatchCountMismatch(
                    expected: expected, actual: actual
                )
            }
        }
    }

    // swiftlint:enable function_parameter_count

    /// Processes an already-captured non-empty
    /// ``DurableRemoteQueueBatch``: parses the byte-stable export
    /// via ``BatchEngine/recoverEntries(from:)``, partitions
    /// entries through ``BatchEngine/makeBatches(from:policy:)``,
    /// and drives each batch through batch-round dispatch
    /// against ``RemoteTransport/sendBatch(_:)`` until every entry
    /// resolves or `retryPolicy.maxAttempts` rounds are spent.
    /// Shared between ``runOnce(queue:exportURL:batchPolicy:retryPolicy:transport:sleep:afterDrain:)``
    /// (which captures the batch with a fresh drain) and
    /// ``RemoteEngine/flush()`` (which may reuse a still-held
    /// outstanding batch from a prior flush).
    ///
    /// The helper never touches the queue's
    /// ``DurableRemoteQueue/acknowledge()`` boundary; callers own
    /// the acknowledgement-to-removal decision after consuming the
    /// returned ``RemoteDeliveryAttempt`` array.
    ///
    /// `delayCalculator` defaults to
    /// ``RemoteRetryPolicy/delayBeforeRetry(attempt:)`` and exists
    /// as an internal seam so test code can drive the
    /// ``BatchDeliveryError/invalidRetryDelay(_:)`` branch that
    /// ``RemoteRetryPolicy/make(maxAttempts:backoff:)`` makes
    /// unreachable from the public API.
    static func deliver(
        batch: DurableRemoteQueueBatch,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy,
        transport: any RemoteTransport,
        sleep: @Sendable (Double) async throws -> Void,
        delayCalculator: @Sendable (RemoteRetryPolicy, Int) throws(RemoteDeliveryError) -> Double = { policy, attempt in
            try policy.delayBeforeRetry(attempt: attempt)
        }
    ) async throws(BatchDeliveryError) -> BatchDeliveryOutcome {
        let entries: [RemoteDeliveryEntry]
        do {
            entries = try BatchEngine.recoverEntries(from: batch)
        } catch {
            throw .recoverFailed(error)
        }
        let batches: [[RemoteDeliveryEntry]]
        do {
            batches = try BatchEngine.makeBatches(
                from: entries, policy: batchPolicy
            )
        } catch {
            throw .batchSplitFailed(error)
        }
        var attempts: [RemoteDeliveryAttempt] = []
        for batch in batches {
            let batchAttempts = try await dispatchBatchRounds(
                batch: batch,
                transport: transport,
                retryPolicy: retryPolicy,
                sleep: sleep,
                delayCalculator: delayCalculator
            )
            attempts.append(contentsOf: batchAttempts)
        }
        return BatchDeliveryOutcome(
            batchCount: batches.count, attempts: attempts
        )
    }

    // swiftlint:disable function_body_length cyclomatic_complexity
    // Reason: One-shot batch-round dispatcher consolidates active-set
    // shrink, per-entry attempt accounting, round-delay sleep, whole-
    // batch send-throw fallback, count-mismatch fail-closed, and final
    // attempt assembly into a single linear lifecycle. Splitting this
    // would relocate state-coupled control flow across helpers without
    // reducing decision points and would obscure the round-by-round
    // invariant the engine depends on.

    /// Drives one batch through batch-round dispatch.
    ///
    /// Round 1 contains every entry in `batch`; each subsequent
    /// round re-dispatches only the entries whose previous
    /// classification from the immediately preceding round was
    /// ``RemoteDeliveryResult/retryable(reason:)``.
    /// The dispatcher stops on the first round where every entry
    /// resolves (``RemoteDeliveryResult/success`` or
    /// ``RemoteDeliveryResult/terminal(reason:)``) or after
    /// `retryPolicy.maxAttempts` rounds have been spent.
    /// Per-entry attempt counts equal the number of rounds the
    /// entry was active in; entries that resolve on round 1 carry
    /// `attempts == 1`, entries that retry through every round
    /// carry `attempts == retryPolicy.maxAttempts`. Returned
    /// ``RemoteDeliveryAttempt`` values are in the original
    /// `batch` order. An entry that is still
    /// ``RemoteDeliveryResult/retryable(reason:)`` after the
    /// final round (the per-entry retry budget exhausted without
    /// resolution) carries that `.retryable(reason:)` value as
    /// its final ``RemoteDeliveryAttempt/outcome``; the engine
    /// layer above (``RemoteEngine/flush()``) consumes the
    /// budget-exhausted outcome verbatim and blocks ACK for the
    /// flush pass on it.
    ///
    /// **Adapter contract.** ``RemoteTransport/sendBatch(_:)``
    /// MUST return one result per input item in the same order;
    /// a count mismatch is an adapter-contract violation surfaced
    /// fail-closed as
    /// ``BatchDeliveryError/transportBatchCountMismatch(expected:actual:)``.
    /// If ``RemoteTransport/sendBatch(_:)`` itself throws, the
    /// dispatcher treats the throw as a transport-level failure
    /// for every active item in the round: each item is routed
    /// through ``RemoteTransport/classify(_:)`` with the same
    /// `.failure(error)` value in input order, and the round
    /// counts toward each item's retry budget.
    private static func dispatchBatchRounds(
        batch: [RemoteDeliveryEntry],
        transport: any RemoteTransport,
        retryPolicy: RemoteRetryPolicy,
        sleep: @Sendable (Double) async throws -> Void,
        delayCalculator: @Sendable (RemoteRetryPolicy, Int) throws(RemoteDeliveryError) -> Double
    ) async throws(BatchDeliveryError) -> [RemoteDeliveryAttempt] {
        if batch.isEmpty { return [] }
        var outcomes: [RemoteDeliveryResult?] = Array(
            repeating: nil, count: batch.count
        )
        var attemptCounts: [Int] = Array(repeating: 0, count: batch.count)

        for round in 1 ... retryPolicy.maxAttempts {
            let activeIndices = activeRetryIndices(in: outcomes)
            if activeIndices.isEmpty { break }

            if round > 1 {
                let seconds: Double
                do {
                    seconds = try delayCalculator(retryPolicy, round - 1)
                } catch {
                    throw .invalidRetryDelay(error)
                }
                do {
                    try await sleep(seconds)
                } catch {
                    throw .sleepInterrupted
                }
            }

            let items = activeIndices.map { idx in
                RemoteTransportBatchItem(
                    payloadBytes: batch[idx].payload,
                    payloadMetadata: batch[idx].metadata
                )
            }

            let results: [Result<RemoteTransportResponse, any Error>]
            do {
                results = try await transport.sendBatch(items)
            } catch {
                // Whole-batch send failure: route every active
                // item through `classify(_:)` with the same
                // `.failure(error)` value and let the adapter
                // decide success / terminal / retryable in input
                // order. The round still counts toward each
                // active item's retry budget.
                for idx in activeIndices {
                    let outcome = await transport.classify(.failure(error))
                    outcomes[idx] = outcome
                    attemptCounts[idx] += 1
                }
                continue
            }

            guard results.count == items.count else {
                throw .transportBatchCountMismatch(
                    expected: items.count, actual: results.count
                )
            }

            for (j, idx) in activeIndices.enumerated() {
                let outcome = await transport.classify(results[j])
                outcomes[idx] = outcome
                attemptCounts[idx] += 1
            }
        }

        var attempts: [RemoteDeliveryAttempt] = []
        attempts.reserveCapacity(batch.count)
        for idx in 0 ..< batch.count {
            // Round 1 included every index in `activeIndices`
            // (the `nil` outcome state selects all entries on
            // the first iteration), so every index has an
            // outcome by the time the dispatch loop returns.
            // The defensive fallback fails closed if a future
            // change to the active-set predicate ever skipped an
            // entry on round 1.
            guard let outcome = outcomes[idx] else {
                throw .internalBatchStateInvalid
            }
            attempts.append(RemoteDeliveryAttempt(
                entry: batch[idx],
                outcome: outcome,
                attempts: attemptCounts[idx]
            ))
        }
        return attempts
    }

    // swiftlint:enable function_body_length cyclomatic_complexity

    private static func activeRetryIndices(
        in outcomes: [RemoteDeliveryResult?]
    ) -> [Int] {
        var indices: [Int] = []
        indices.reserveCapacity(outcomes.count)
        for (idx, outcome) in outcomes.enumerated() {
            switch outcome {
            case nil, .retryable:
                indices.append(idx)
            case .success, .terminal:
                continue
            }
        }
        return indices
    }
}

/// Engine-internal outcome of `ExecutionLoop.deliver(batch:...)`.
///
/// Carries both the batch count (number of batches
/// ``BatchEngine/makeBatches(from:policy:)`` produced) and the
/// flat per-entry attempts so callers can build the public
/// ``RemoteFlushSummary`` without re-counting batches themselves.
internal struct BatchDeliveryOutcome: Sendable, Equatable {
    let batchCount: Int
    let attempts: [RemoteDeliveryAttempt]
}
