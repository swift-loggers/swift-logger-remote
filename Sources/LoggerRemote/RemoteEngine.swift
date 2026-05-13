import Foundation

/// Public remote-delivery engine that drives one delivery pass per
/// caller-initiated ``flush()``.
///
/// The engine is **caller-driven**: it owns no timer, no platform
/// lifecycle observer, no autonomous scheduler. Host applications
/// integrate with whatever platform-specific lifecycle they care
/// about (e.g. `UIApplication` background notifications,
/// `NSWorkspace` power-off, app-specific manual triggers) and
/// invoke ``flush()`` from those hooks. Actor isolation serializes
/// concurrent ``flush()`` invocations so the engine never runs two
/// passes against the same queue simultaneously.
///
/// One ``flush()`` pass:
///
/// 1. Consults `currentOutstandingBatch()`.
///    - If the queue is still holding a previously drained batch
///      the engine reuses it and its already-written export
///      file; this path intentionally skips
///      ``DurableRemoteQueue/flush()`` because there are already
///      drained bytes to replay and an unrelated persistence-flush
///      failure must not block them.
///    - Otherwise the engine calls ``DurableRemoteQueue/flush()``
///      (so every admitted entry is on disk before the drain)
///      and then ``DurableRemoteQueue/drain(to:)`` into a fresh
///      scratch file inside the engine-owned `exportDirectory`.
/// 2. Drives the engine-internal
///    `ExecutionLoop.deliver(batch:...)`
///    helper over the captured batch:
///    `BatchEngine.recoverEntries(from:)` →
///    `BatchEngine.makeBatches(from:policy:)` → per-group
///    batch-round dispatch against
///    ``RemoteTransport/sendBatch(_:)`` with per-entry
///    classification through ``RemoteTransport/classify(_:)``.
///    Round 1 of each group contains every entry; every
///    subsequent round re-dispatches only the entries whose
///    previous classification was
///    ``RemoteDeliveryResult/retryable(reason:)``. Per-entry
///    attempt counts equal the number of rounds the entry was
///    active in. The sleep injector is invoked between rounds
///    only when at least one retryable entry remains.
/// 3. Aggregates per-entry outcomes into a
///    ``RemoteFlushSummary``.
/// 4. **Acknowledgement decision** (LGR-11): the engine invokes
///    ``DurableRemoteQueue/acknowledge()`` only when every recovered
///    entry across the entire flush pass reached a resolved
///    classification — ``RemoteDeliveryResult/success`` or
///    ``RemoteDeliveryResult/terminal(reason:)``. A single
///    ``RemoteDeliveryResult/retryable(reason:)`` outcome anywhere
///    in the pass (the per-entry retry budget exhausted without
///    resolution) keeps the drained batch held so the next
///    ``flush()`` retries the same bytes through the
///    outstanding-reuse path. There is **no per-batch
///    acknowledgement**: ACK is a pass-wide decision.
///    ``RemoteDeliveryResult/terminal(reason:)`` is treated as
///    resolved because the classifier — sink-owned (LGR-7,
///    LGR-9) — declared the entry permanently failed; removing
///    those queue payload bytes is forward progress, not data
///    loss.
///
/// On an empty drain
/// (``DurableRemoteQueueBatch/byteCount`` `== 0`) the engine
/// releases the boundary by calling
/// ``DurableRemoteQueue/acknowledge()`` and reports a summary with
/// zero counts and ``RemoteFlushSummary/acknowledgement``
/// `== .emptyReleased`; no delivered queue payload bytes are
/// removed because none existed.
///
/// The engine surface is sink-neutral. ``RemoteTransport`` owns
/// both ``RemoteTransport/sendBatch(_:)`` and
/// ``RemoteTransport/classify(_:)``; the engine itself never
/// inspects HTTP status, vendor body codes, or transport error
/// types. Single-event adapters (Splunk HEC, Loki single-event,
/// Datadog Logs HTTP intake) implement
/// ``RemoteTransport/sendBatch(_:)`` by dispatching each input
/// item independently. Batch-aggregating adapters (Elastic
/// `_bulk`, OTLP/HTTP batched) build one vendor request from the
/// whole batch and project the response back into per-input
/// results. Vendor-specific encoders, request builders, and
/// response classifiers live in adapter packages (LGR-9).
public actor RemoteEngine {
    private let queue: DurableRemoteQueue
    private let exportDirectory: URL
    private let transport: any RemoteTransport
    private let batchPolicy: RemoteBatchPolicy
    private let retryPolicy: RemoteRetryPolicy
    private let sleep: @Sendable (Double) async throws -> Void

    /// Test-only seam allowing the test target to drive
    /// ``RemoteEngineError/acknowledgementFailed(_:)`` deterministically.
    /// When set, ``flush()`` routes its `acknowledge` call through
    /// this closure instead of ``DurableRemoteQueue/acknowledge()``;
    /// the queue's outstanding-batch state is unchanged so the
    /// engine's "keep export on ack failure" branch can be
    /// exercised. Production callers leave this `nil`.
    internal var acknowledgeOverrideForTesting:
        (@Sendable () async throws(DurableRemoteQueueError) -> Void)?

    /// Test-only seam allowing the test target to drive
    /// ``RemoteEngineError/exportCleanupFailed(_:)`` deterministically.
    /// When set, ``flush()`` routes every `FileManager.removeItem`
    /// call through this closure instead of `FileManager`;
    /// throwing here makes the engine's cleanup helper surface
    /// ``RemoteEngineError/exportCleanupFailed(_:)``. Production
    /// callers leave this `nil`.
    internal var exportRemovalOverrideForTesting:
        (@Sendable (URL) throws -> Void)?

    /// Constructs a remote-delivery engine over an existing queue +
    /// transport + policies.
    ///
    /// - Parameters:
    ///   - queue: Persistence-backed durable queue providing the
    ///     drain boundary. The engine never touches persistence
    ///     outside this queue (LGR-10).
    ///   - exportDirectory: Caller-provided scratch directory
    ///     used exclusively by the engine for per-flush
    ///     byte-stable export files. The caller owns the
    ///     directory's lifecycle: it MUST already exist, MUST be
    ///     writable by the engine's process, and SHOULD be a
    ///     caller-controlled private location not shared with
    ///     other code paths. The engine
    ///     creates only unique scratch export files inside the
    ///     directory; it does not create the parent directory,
    ///     set or audit access-control policy, or sweep
    ///     pre-existing files. Each ``flush()`` either reuses the
    ///     queue's still-held outstanding batch (and its
    ///     already-written export file) through the
    ///     outstanding-reuse path, or, when there is no
    ///     outstanding batch, allocates a fresh unique filename
    ///     inside this directory and drains into it. The export
    ///     artifact is removed only on a successful empty release
    ///     or a successful non-empty acknowledge; on a
    ///     `.retryable`-exhausted tally or any parse / batch /
    ///     retry-interruption / ack failure after the engine owns
    ///     a reusable drained batch reference for the current
    ///     flush pass the retained export artifact stays so the
    ///     next ``flush()`` can replay it through the
    ///     outstanding-reuse path. The directory is the engine's
    ///     exclusive working area; callers should not co-locate
    ///     other files there.
    ///   - transport: Sink-neutral transport conformer. Adapters
    ///     implement ``RemoteTransport/sendBatch(_:)`` and
    ///     ``RemoteTransport/classify(_:)``; the engine never
    ///     interprets either result.
    ///   - batchPolicy: Validated boundary contract consumed by
    ///     `BatchEngine.makeBatches(from:policy:)`.
    ///   - retryPolicy: Validated per-entry retry budget +
    ///     backoff schedule consumed by the batch-round
    ///     dispatcher inside `ExecutionLoop`.
    public init(
        queue: DurableRemoteQueue,
        exportDirectory: URL,
        transport: any RemoteTransport,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy
    ) {
        self.init(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            batchPolicy: batchPolicy,
            retryPolicy: retryPolicy,
            sleep: { seconds in
                try await Task.sleep(
                    nanoseconds: Self.safeSleepNanoseconds(seconds: seconds)
                )
            }
        )
    }

    /// Maps a backoff delay in `Double` seconds to a `UInt64`
    /// nanosecond count safe to pass to `Task.sleep(nanoseconds:)`,
    /// without trapping on the corner cases a `Double` can carry.
    ///
    /// `Double → UInt64` traps on `NaN`, on `±infinity`, on
    /// negative values, and on any value at or above the
    /// representable `UInt64` range. The production sleep injector
    /// would otherwise crash the engine if a future seam ever fed
    /// in one of those values. The mapping is fail-safe:
    ///
    /// - Non-finite (`NaN` or `±infinity`) → `0`.
    /// - `<= 0` (zero or negative) → `0`.
    /// - When `seconds * 1_000_000_000` is at or above
    ///   `Double(UInt64.max)` → `UInt64.max` (the longest sleep
    ///   the platform can express).
    /// - Otherwise the truncating cast is safe.
    ///
    /// Exposed `internal` for test-target coverage of every branch;
    /// the default production sleep injector is the only caller.
    internal static func safeSleepNanoseconds(seconds: Double) -> UInt64 {
        guard seconds.isFinite else { return 0 }
        guard seconds > 0 else { return 0 }
        let nanos = seconds * 1_000_000_000
        guard nanos < Double(UInt64.max) else { return .max }
        return UInt64(nanos)
    }

    /// Designated initializer used by tests to inject a
    /// deterministic `sleep` closure in place of the production
    /// `Task.sleep(nanoseconds:)`-backed default. Internal so the
    /// public surface never exposes the wall-clock seam.
    internal init(
        queue: DurableRemoteQueue,
        exportDirectory: URL,
        transport: any RemoteTransport,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy,
        sleep: @escaping @Sendable (Double) async throws -> Void
    ) {
        self.queue = queue
        self.exportDirectory = exportDirectory
        self.transport = transport
        self.batchPolicy = batchPolicy
        self.retryPolicy = retryPolicy
        self.sleep = sleep
    }

    /// Runs one caller-driven delivery pass.
    ///
    /// Concurrent ``flush()`` calls serialize through actor
    /// isolation — only one pass executes against the queue at a
    /// time; the engine never runs two passes in parallel.
    ///
    /// Outstanding-batch lifecycle: every ``flush()`` first
    /// consults `currentOutstandingBatch()`.
    /// If the queue is still holding a previously drained batch
    /// (for example because the prior flush returned with
    /// ``RemoteFlushSummary/retryableEntries`` `> 0`), the engine
    /// reuses that batch — and its already-written export file —
    /// through the outstanding-reuse path; this path skips both
    /// ``DurableRemoteQueue/flush()`` and
    /// ``DurableRemoteQueue/drain(to:)`` so an unrelated
    /// persistence flush failure cannot block bytes that are
    /// already drained and need replaying, and so a second drain
    /// against the still-held boundary cannot surface
    /// ``DurableRemoteQueueError/batchAlreadyOutstanding``. Only
    /// when there is no outstanding batch does the engine run the
    /// fresh-drain path: it calls ``DurableRemoteQueue/flush()``
    /// so every admitted entry is on disk, allocates a fresh
    /// scratch URL inside `exportDirectory`, and drains into it
    /// through ``DurableRemoteQueue/drain(to:)``.
    ///
    /// Export-file cleanup is keyed off the acknowledgement
    /// decision:
    ///
    /// - Drain failure before the engine owns a reusable drained
    ///   batch reference for the current flush pass runs a
    ///   best-effort removal of the just-allocated scratch URL;
    ///   the ``RemoteEngineError/drainFailed(_:)`` diagnostic
    ///   stays primary and any filesystem cleanup failure on this
    ///   path is intentionally suppressed.
    /// - Empty drain release (`byteCount == 0`) acknowledges and
    ///   removes the export file.
    /// - Non-empty batch fully resolved (every entry is
    ///   `.success` or `.terminal`) acknowledges and removes the
    ///   export file.
    /// - Any `.retryable` outcome keeps the retained export
    ///   artifact so the next `flush()` replays it through the
    ///   outstanding-reuse path.
    /// - Parse / batch / retry-interruption / ack failure after
    ///   the engine owns a reusable drained batch reference for
    ///   the current flush pass keeps the retained export
    ///   artifact; the queue still holds the outstanding batch so
    ///   the next ``flush()`` reuses both through
    ///   `currentOutstandingBatch()`.
    ///
    /// - Returns: ``RemoteFlushSummary`` describing the per-class
    ///   entry counts and whether the engine acknowledged the
    ///   drained batch.
    /// - Throws: ``RemoteEngineError`` mapped from the lifecycle
    ///   step that surfaced the failure (flush, drain, parse,
    ///   batch, retry-interruption, or acknowledgement).
    public func flush() async throws(RemoteEngineError) -> RemoteFlushSummary {
        let batch = try await acquireBatch()
        if batch.byteCount == 0 {
            return try await releaseEmpty(batch: batch)
        }
        let outcome = try await processNonEmpty(batch: batch)
        return try await finalizeNonEmpty(batch: batch, outcome: outcome)
    }

    /// Reuses the queue's still-held outstanding batch when one
    /// exists, otherwise flushes the queue's writer and drains
    /// into a fresh scratch URL inside ``exportDirectory``.
    ///
    /// The outstanding-batch check runs **before**
    /// ``DurableRemoteQueue/flush()`` so an unrelated persistence
    /// flush failure cannot block bytes that were already drained
    /// and need replaying; ``DurableRemoteQueue/flush()`` only
    /// runs when the engine is about to issue a fresh drain and
    /// wants the writer buffer to be on disk first.
    ///
    /// On drain failure the engine runs a best-effort removal of
    /// the just-allocated scratch URL because the engine does not
    /// own a reusable drained batch reference for the current
    /// flush pass; that cleanup is intentionally suppressed (the
    /// ``RemoteEngineError/drainFailed(_:)``
    /// diagnostic stays primary), so a stale scratch file can
    /// linger if the filesystem removal itself also fails. Any
    /// later failure path (parse, batch, retry-interruption,
    /// ack) leaves the retained export artifact alone because the
    /// queue is still holding the outstanding batch and the next
    /// flush reuses both through ``DurableRemoteQueue/currentOutstandingBatch()``.
    private func acquireBatch() async throws(RemoteEngineError) -> DurableRemoteQueueBatch {
        if let outstanding = await queue.currentOutstandingBatch() {
            // Reuse the queue's still-held outstanding batch and
            // its already-written export file. Calling
            // `drain(to:)` here would surface
            // `.batchAlreadyOutstanding`; the engine must replay
            // the retained export artifact until the outcome
            // reaches a resolved tally and ack succeeds.
            // The queue's writer buffer is irrelevant on this
            // path — there are already drained bytes to retry —
            // so the engine intentionally skips
            // `DurableRemoteQueue.flush()` here.
            return outstanding
        }
        do {
            try await queue.flush()
        } catch {
            throw .flushFailed(error)
        }
        let scratchURL = exportDirectory.appendingPathComponent(
            "flush-\(UUID().uuidString).ndjson"
        )
        do {
            return try await queue.drain(to: scratchURL)
        } catch {
            // Drain failed before the engine owns a reusable
            // drained batch reference for the current flush pass.
            // The drain error is the primary diagnostic; cleanup
            // of the orphan scratch URL is best-effort and any
            // removal failure here is intentionally suppressed.
            removeExportArtifactBestEffort(at: scratchURL)
            throw .drainFailed(error)
        }
    }

    /// Empty-drain release path: no delivered queue payload bytes
    /// to acknowledge, but the queue still holds the
    /// outstanding-batch boundary. Acknowledge to release it,
    /// remove the (empty) export artifact, and report a
    /// zero-count summary. An ack failure here keeps the export
    /// file on disk so the next flush retries the same boundary.
    private func releaseEmpty(
        batch: DurableRemoteQueueBatch
    ) async throws(RemoteEngineError) -> RemoteFlushSummary {
        do {
            try await performQueueAcknowledge()
        } catch {
            throw .acknowledgementFailed(error)
        }
        // Ack succeeded; the empty export carries no delivered
        // queue payload bytes but the engine still owns the
        // scratch artifact's filesystem lifecycle. Surface any
        // cleanup failure as `.exportCleanupFailed` so a leaking
        // scratch file is addressable instead of silently
        // swallowed.
        try removeExportArtifact(at: batch.exportURL, phase: .emptyRelease)
        return RemoteFlushSummary(
            attemptedBatches: 0,
            attemptedEntries: 0,
            succeededEntries: 0,
            terminalEntries: 0,
            retryableEntries: 0,
            acknowledgement: .emptyReleased
        )
    }

    /// Non-empty completion path: tallies the per-entry outcomes
    /// and either acknowledges (every entry resolved → ack +
    /// remove export) or holds the boundary (any `.retryable` →
    /// keep export so the next flush can replay the same bytes
    /// through the outstanding-reuse path).
    private func finalizeNonEmpty(
        batch: DurableRemoteQueueBatch,
        outcome: BatchDeliveryOutcome
    ) async throws(RemoteEngineError) -> RemoteFlushSummary {
        let attempts = outcome.attempts
        let tally = Self.tally(attempts: attempts)
        if tally.retryable > 0 {
            return RemoteFlushSummary(
                attemptedBatches: outcome.batchCount,
                attemptedEntries: attempts.count,
                succeededEntries: tally.succeeded,
                terminalEntries: tally.terminal,
                retryableEntries: tally.retryable,
                acknowledgement: .notAcknowledged
            )
        }
        do {
            try await performQueueAcknowledge()
        } catch {
            throw .acknowledgementFailed(error)
        }
        // Ack succeeded — the persistence layer's destructive
        // removal already ran, so the queue payload bytes are
        // gone from accepted ordering. The engine's scratch
        // export file still contains a copy of those same bytes;
        // a cleanup failure here surfaces as
        // `.exportCleanupFailed`. Ack is final and the retained
        // artifact is a duplicate copy, never a retry source —
        // the engine never re-reads it and the caller MAY remove
        // the URL after observing the cleanup failure.
        try removeExportArtifact(
            at: batch.exportURL, phase: .acknowledgedNonEmpty
        )
        return RemoteFlushSummary(
            attemptedBatches: outcome.batchCount,
            attemptedEntries: attempts.count,
            succeededEntries: tally.succeeded,
            terminalEntries: tally.terminal,
            retryableEntries: tally.retryable,
            acknowledgement: .removedDeliveredBytes
        )
    }

    /// Routes acknowledge through the test-only seam when set,
    /// otherwise hits ``DurableRemoteQueue/acknowledge()`` directly.
    /// Production callers always go through the queue.
    private func performQueueAcknowledge() async throws(DurableRemoteQueueError) {
        if let override = acknowledgeOverrideForTesting {
            try await override()
        } else {
            try await queue.acknowledge()
        }
    }

    /// Surfaceable cleanup helper for the empty-release and
    /// non-empty-ack paths. A removal failure throws
    /// ``RemoteEngineError/exportCleanupFailed(_:)`` with the
    /// matching ``RemoteEngineExportCleanupContext/Phase``.
    /// Routes through the optional
    /// ``exportRemovalOverrideForTesting`` seam when set;
    /// production callers always go through `FileManager`.
    private func removeExportArtifact(
        at url: URL,
        phase: RemoteEngineExportCleanupContext.Phase
    ) throws(RemoteEngineError) {
        do {
            try performExportRemoval(at: url)
        } catch {
            let nsError = error as NSError
            throw .exportCleanupFailed(
                RemoteEngineExportCleanupContext(
                    exportURL: url,
                    phase: phase,
                    errorDomain: nsError.domain,
                    errorCode: nsError.code
                )
            )
        }
    }

    /// Best-effort cleanup helper used on the drain-failure path
    /// where the primary diagnostic is
    /// ``RemoteEngineError/drainFailed(_:)``. Routes through the
    /// same ``exportRemovalOverrideForTesting`` seam so test
    /// fixtures can observe the call, but suppresses the
    /// underlying error so the drain failure stays primary;
    /// callers that observe ``RemoteEngineError/drainFailed(_:)``
    /// may need to sweep the engine-owned directory because a
    /// stale scratch file can linger here.
    private func removeExportArtifactBestEffort(at url: URL) {
        try? performExportRemoval(at: url)
    }

    /// Single filesystem-removal call site so the test-only
    /// seam (``exportRemovalOverrideForTesting``) covers both the
    /// surfaceable cleanup and the best-effort drain-failure
    /// cleanup uniformly.
    private func performExportRemoval(at url: URL) throws {
        if let override = exportRemovalOverrideForTesting {
            try override(url)
        } else {
            try FileManager.default.removeItem(at: url)
        }
    }

    // swiftlint:disable identifier_name
    // Reason: Test-only seams mirroring the queue's
    // `_setExportSizeReaderForTesting` / `_setRecordEncoderForTesting`
    // naming so test code reads symmetrically across the
    // engine-internal seams.

    /// Test-only setter for the acknowledge seam. Mirrors the
    /// queue's `_setExportSizeReaderForTesting` /
    /// `_setRecordEncoderForTesting` shape so the seam can be
    /// installed and cleared from inside an actor-isolated test
    /// hop.
    internal func _setAcknowledgeOverrideForTesting(
        _ override: (@Sendable () async throws(DurableRemoteQueueError) -> Void)?
    ) {
        acknowledgeOverrideForTesting = override
    }

    /// Test-only setter for the export-removal seam used to drive
    /// ``RemoteEngineError/exportCleanupFailed(_:)`` deterministically.
    internal func _setExportRemovalOverrideForTesting(
        _ override: (@Sendable (URL) throws -> Void)?
    ) {
        exportRemovalOverrideForTesting = override
    }

    // swiftlint:enable identifier_name

    /// Drives the engine-internal `ExecutionLoop.deliver(batch:...)`
    /// helper over the captured non-empty batch. Failures after
    /// this point keep the queue's outstanding boundary held and
    /// the export artifact on disk so the next flush can reuse
    /// them; only the ack step decides removal.
    private func processNonEmpty(
        batch: DurableRemoteQueueBatch
    ) async throws(RemoteEngineError) -> BatchDeliveryOutcome {
        do {
            return try await ExecutionLoop.deliver(
                batch: batch,
                batchPolicy: batchPolicy,
                retryPolicy: retryPolicy,
                transport: transport,
                sleep: sleep
            )
        } catch {
            throw Self.mapDeliverError(error)
        }
    }

    private struct OutcomeTally {
        let succeeded: Int
        let terminal: Int
        let retryable: Int
    }

    private static func tally(
        attempts: [RemoteDeliveryAttempt]
    ) -> OutcomeTally {
        var succeeded = 0
        var terminal = 0
        var retryable = 0
        for attempt in attempts {
            switch attempt.outcome {
            case .success:
                succeeded += 1
            case .terminal:
                terminal += 1
            case .retryable:
                retryable += 1
            }
        }
        return OutcomeTally(
            succeeded: succeeded,
            terminal: terminal,
            retryable: retryable
        )
    }

    /// Public-side translation of the narrow
    /// `ExecutionLoop.deliver(batch:...)`
    /// engine-internal error surface
    /// (``BatchDeliveryError``) into ``RemoteEngineError`` so the
    /// internal types stay out of the public surface. Exhaustive
    /// over the six cases the helper can raise; the broader
    /// ``ExecutionLoopError`` surface (which also carries drain /
    /// empty-release cases) only flows through
    /// `ExecutionLoop.runOnce(...)` and never reaches this
    /// mapper.
    private static func mapDeliverError(
        _ error: BatchDeliveryError
    ) -> RemoteEngineError {
        switch error {
        case let .recoverFailed(batchEngineError):
            return .parseFailed(Self.mapBatchEngineError(batchEngineError))
        case let .batchSplitFailed(deliveryError):
            return .batchFailed(deliveryError)
        case let .invalidRetryDelay(deliveryError):
            return .retryInterrupted(.invalidRetryDelay(deliveryError))
        case .sleepInterrupted:
            return .retryInterrupted(.sleepInterrupted)
        case .internalBatchStateInvalid:
            return .batchFailed(.invalidBatchState)
        case let .transportBatchCountMismatch(expected, actual):
            return .transportBatchInvalid(
                expected: expected, actual: actual
            )
        }
    }

    /// Mirrors the engine-internal ``BatchEngineError`` taxonomy
    /// onto the public ``RemoteEngineParseError`` cases verbatim,
    /// preserving every associated value the parser carries.
    private static func mapBatchEngineError(
        _ error: BatchEngineError
    ) -> RemoteEngineParseError {
        switch error {
        case .exportFileReadFailed:
            return .exportFileReadFailed
        case let .exportByteCountMismatch(expected, actual):
            return .exportByteCountMismatch(expected: expected, actual: actual)
        case .exportByteCountUnavailable:
            return .exportByteCountUnavailable
        case .envelopeMalformed:
            return .envelopeMalformed
        case let .envelopeContentTypeMismatch(expected, found):
            return .envelopeContentTypeMismatch(expected: expected, found: found)
        case .recordPayloadBase64Invalid:
            return .recordPayloadBase64Invalid
        case .recordPayloadMalformed:
            return .recordPayloadMalformed
        case .recordFormatVersionMissing:
            return .recordFormatVersionMissing
        case let .recordFormatVersionUnsupported(found, supported):
            return .recordFormatVersionUnsupported(found: found, supported: supported)
        }
    }
}
