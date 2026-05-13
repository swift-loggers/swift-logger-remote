import Foundation
import LoggerFilePersistence
import LoggerPersistence

/// Durable persistence-backed accept / drain / acknowledge surface
/// for the remote-delivery engine.
///
/// `DurableRemoteQueue` is the persistence boundary of
/// `swift-logger-remote`: the only piece of the package that
/// touches `swift-logger-persistence`. Flush trigger semantics and
/// the acknowledgement-to-removal lifecycle are owned by
/// ``RemoteEngine/flush()``; entry recovery and batch construction
/// are owned by the engine-internal `BatchEngine`; per-entry retry
/// execution is owned by the engine-internal `RetryExecutor`;
/// transport implementations live in adapter packages outside the
/// queue. The queue itself just admits, drains, and acknowledges.
///
/// Destructive removal of accepted bytes happens exclusively
/// through ``acknowledge()`` and is driven only by
/// ``RemoteEngine/flush()`` when every recovered entry in a
/// non-empty flush pass reaches ``RemoteDeliveryResult/success``
/// or ``RemoteDeliveryResult/terminal(reason:)`` (LGR-10, LGR-11).
/// The empty-drain path also acknowledges to release the
/// in-memory outstanding-batch boundary, but removes no delivered
/// queue payload bytes because none existed.
///
/// - ``enqueue(_:)`` admits one ``RemoteDeliveryEntry`` into the
///   persistence layer through a package-owned internal queue
///   record so the entry's identifier, payload, and metadata
///   survive the byte-stable export boundary losslessly.
/// - ``drain(to:)`` captures the current recoverable prefix as a
///   byte-stable export file. A second drain before
///   ``acknowledge()`` succeeds is rejected so the in-memory
///   removal boundary always names the batch the caller is about
///   to acknowledge.
/// - ``acknowledge()`` consumes the in-memory removal boundary
///   captured by the most recent successful drain.
public actor DurableRemoteQueue {
    /// Queue-owned constant `contentType` value recorded on every
    /// persisted envelope. Locked as queue-internal so the
    /// batching engine's parser can validate envelopes it
    /// recovers fail-closed against the same constant. The value
    /// satisfies the persistence layer's content-type validation
    /// (visible ASCII, no whitespace, 1...128 UTF-8 bytes).
    internal static let envelopeContentType =
        "application/vnd.swift-loggers.remote-queue+json"

    private let store: FileLogStore
    private let recordEncoder: JSONEncoder
    private var nextSequence: UInt64
    private var outstandingBatch: DurableRemoteQueueBatch?

    /// TEST-ONLY override for the post-export size read so the
    /// stat-failure path is reachable without provoking real
    /// filesystem errors. Production code never assigns this.
    /// Thrown errors project to ``DurableRemoteQueueError/drainSizeReadFailed``.
    internal var exportSizeReaderForTesting: (@Sendable (URL) throws -> UInt64)?

    /// TEST-ONLY override for the queue-record encoder. The closure
    /// must produce the persistence-payload bytes for one record;
    /// thrown errors project to
    /// ``DurableRemoteQueueError/recordEncodingFailed``. Production
    /// code never assigns this. Reaching the override fires before
    /// the persistence layer is touched so an encode failure cannot
    /// advance the queue's private sequence allocator.
    internal var recordEncoderOverrideForTesting: (@Sendable (DurableRemoteQueueRecord) throws -> Data)?

    /// Creates a durable queue backed by a `FileLogStore` rooted at
    /// `directory`.
    ///
    /// - Parameters:
    ///   - directory: Configured persistence root. Path-confinement
    ///     and ancestor-symlink ownership are the caller's
    ///     responsibility per the persistence package contract.
    ///   - rotation: Segment-rotation policy forwarded to the
    ///     persistence layer.
    ///
    /// The queue intentionally does not expose a `contentType`
    /// parameter. The persistence envelope's `contentType` is
    /// queue-owned (locked as a queue-internal constant) so the
    /// batching engine's parser can validate every recovered
    /// envelope fail-closed against the same constant; caller-
    /// customizable content types would let the parser silently
    /// accept envelopes the queue did not produce.
    ///
    /// The queue intentionally does not expose a `retention`
    /// parameter. Persistence retention (`.maxSegments`,
    /// `.maxTotalBytes`, `.maxAge`) could delete bytes that were
    /// never acknowledged by the delivery loop, which would violate
    /// LGR-11 (acknowledgement is the only trigger for destructive
    /// removal). The queue hardcodes `.unlimited`; a bounded queue
    /// policy is a future contract separate from persistence
    /// retention.
    public init(
        directory: URL,
        rotation: RotationPolicy = .never
    ) {
        self.init(
            directory: directory,
            rotation: rotation,
            startingSequence: 1
        )
    }

    /// TEST-ONLY initializer that seeds the private sequence
    /// allocator. Production callers must use the public init,
    /// which starts at `1`.
    internal init(
        directory: URL,
        rotation: RotationPolicy,
        startingSequence: UInt64
    ) {
        store = FileLogStore(configuration: .init(
            directory: directory,
            rotation: rotation,
            retention: .unlimited
        ))
        nextSequence = startingSequence
        outstandingBatch = nil
        recordEncoder = JSONEncoder()
        // Stable key order helps human-readable diffs; the queue
        // does not depend on canonical bytes because the
        // persistence envelope's outer canonical encoding is what
        // owns byte stability across the export boundary.
        recordEncoder.outputFormatting = [.sortedKeys]
    }

    /// Persists `entry` as one accepted line.
    ///
    /// The entry is wrapped into a queue-owned record so the
    /// engine-local identifier and sink-owned metadata survive the
    /// byte-stable export boundary losslessly. Persistence sequence
    /// values come from a queue-private monotonic allocator and are
    /// not derived from `entry.identifier`.
    ///
    /// - Throws: ``DurableRemoteQueueError`` cases for a queue-record
    ///   encoding defect, envelope validation failure, or a
    ///   persistence-layer append failure.
    public func enqueue(
        _ entry: RemoteDeliveryEntry
    ) async throws(DurableRemoteQueueError) {
        // Reserve the persistence sequence before the I/O so an
        // allocator already marked exhausted (`0`) fails before any
        // bytes are admitted. Exhaustion after `UInt64.max` is
        // detected on the next reservation attempt.
        guard nextSequence != 0 else {
            throw .sequenceExhausted
        }
        let reservedSequence = nextSequence
        let record = DurableRemoteQueueRecord(
            formatVersion: DurableRemoteQueueRecord.currentFormatVersion,
            identifier: entry.identifier,
            payload: entry.payload,
            metadata: entry.metadata
        )
        let recordBytes: Data
        do {
            if let override = recordEncoderOverrideForTesting {
                recordBytes = try override(record)
            } else {
                recordBytes = try recordEncoder.encode(record)
            }
        } catch {
            throw .recordEncodingFailed
        }
        let envelope: PersistentLogEnvelope
        do {
            envelope = try PersistentLogEnvelope(
                id: UUID(),
                sequence: reservedSequence,
                createdAt: Self.millisecondAlignedNow(),
                contentType: Self.envelopeContentType,
                hints: [:],
                payload: recordBytes
            )
        } catch {
            throw .envelopeRejected(error)
        }
        do {
            try await store.append(envelope)
        } catch {
            throw .enqueueFailed(error)
        }
        // Advance the allocator only after a successful admission
        // so a rejected envelope or failed append leaves the next
        // sequence unchanged. `&+=` is used intentionally: at
        // `UInt64.max` the operator wraps the allocator to `0`,
        // and the exhaustion guard at the top of the next
        // ``enqueue(_:)`` is what catches the wrapped value and
        // surfaces ``DurableRemoteQueueError/sequenceExhausted``.
        // This is a deliberate two-step contract (`&+=` here +
        // guard there), not implicit reliance on Swift's general
        // wrapping-addition semantics anywhere else.
        nextSequence &+= 1
    }

    /// Flushes the recoverable prefix to disk so the next drain
    /// captures every byte the writer has admitted so far.
    public func flush() async throws(DurableRemoteQueueError) {
        do {
            try await store.flush()
        } catch {
            throw .flushFailed(error)
        }
    }

    /// Captures the current recoverable prefix as a byte-stable
    /// export written to `exportURL`. The captured boundary is held
    /// in memory by the persistence layer and consumed by
    /// ``acknowledge()``.
    ///
    /// The queue admits one outstanding batch at a time: a second
    /// `drain(to:)` before ``acknowledge()`` succeeds is rejected
    /// with ``DurableRemoteQueueError/batchAlreadyOutstanding`` so
    /// the persistence layer's in-memory removal boundary always
    /// names the batch the caller is about to acknowledge.
    ///
    /// - Parameter exportURL: Destination for the byte-stable
    ///   export. The destination parent must already exist; path
    ///   confinement and ancestor-symlink ownership remain the
    ///   caller's responsibility.
    /// - Returns: ``DurableRemoteQueueBatch`` carrying the export
    ///   URL and the exact post-export byte count.
    public func drain(
        to exportURL: URL
    ) async throws(DurableRemoteQueueError) -> DurableRemoteQueueBatch {
        guard outstandingBatch == nil else {
            throw .batchAlreadyOutstanding
        }
        do {
            try await store.exportLogs(to: exportURL)
        } catch {
            throw .drainFailed(error)
        }
        // The persistence in-memory removal boundary is set by the
        // successful `exportLogs(to:)`. From here on a queue-side
        // failure leaves the queue without a queue-held outstanding
        // batch reference: the persistence boundary may still
        // exist, but the queue neither holds it nor has handed it
        // to the caller. `acknowledge()` must refuse the
        // destructive remove in that state — see the
        // queue-held-batch guard in `acknowledge()`.
        let byteCount: UInt64
        if let override = exportSizeReaderForTesting {
            do {
                byteCount = try override(exportURL)
            } catch {
                throw .drainSizeReadFailed
            }
        } else {
            byteCount = try Self.exactExportByteCount(at: exportURL)
        }
        let batch = DurableRemoteQueueBatch(
            exportURL: exportURL,
            byteCount: byteCount
        )
        outstandingBatch = batch
        return batch
    }

    /// Consumes the in-memory removal boundary captured by the most
    /// recent successful ``drain(to:)``.
    ///
    /// A failed `acknowledge()` leaves the outstanding batch in
    /// place; the persistence layer keeps the removal boundary
    /// across the failed call and the caller can retry
    /// `acknowledge()` against the same outstanding batch. Only a
    /// successful `acknowledge()` clears the outstanding-batch
    /// state and re-opens the queue for the next ``drain(to:)``.
    ///
    /// The queue refuses the destructive remove when it holds no
    /// outstanding batch even if the persistence in-memory removal
    /// boundary exists. A queue-side failure in ``drain(to:)``
    /// that fires after the persistence `exportLogs(to:)` already
    /// succeeded would otherwise leave the persistence boundary
    /// pointing at bytes the queue never returned to the caller; a
    /// later `acknowledge()` would consume those bytes
    /// destructively. The queue-held-batch guard closes that
    /// window: `acknowledge()` is only valid against a batch the
    /// queue produced and handed to the caller.
    public func acknowledge() async throws(DurableRemoteQueueError) {
        guard outstandingBatch != nil else {
            throw .acknowledgeFailed(.noExportedRemovalBoundary)
        }
        do {
            try await store.removeExportedLogs()
        } catch {
            throw .acknowledgeFailed(error)
        }
        outstandingBatch = nil
    }

    /// Returns the currently outstanding (drained-but-not-yet-
    /// acknowledged) batch, if any. Internal so the public PR 2/N
    /// surface stays narrow.
    ///
    /// This is actor-local in-memory state for retrying
    /// ``acknowledge()`` against a drained batch **while the actor
    /// instance remains alive**. It is not crash-recovery state
    /// and not durable across actor-instance lifetime: deallocating
    /// or rebuilding the actor loses the in-memory batch
    /// reference, and the queue has no replay/query API to
    /// rebuild it. ``RemoteEngine/flush()`` consumes this value
    /// to decide whether to drain a fresh boundary or reuse the
    /// still-held outstanding batch for in-actor retry against
    /// the captured persistence boundary.
    internal func currentOutstandingBatch() -> DurableRemoteQueueBatch? {
        outstandingBatch
    }

    // swiftlint:disable identifier_name
    // Reason: Underscore prefix marks the test-only seam setters and
    // matches the persistence package's `_set...ForTesting`
    // convention; the rule disable cannot land on `disable:next`
    // because the doc comment sits between the directive and the
    // declaration.

    /// TEST-ONLY: installs a closure that replaces the post-export
    /// size read. Setting the closure to `nil` restores the
    /// default `FileManager`-backed exact-size measurement.
    internal func _setExportSizeReaderForTesting(
        _ hook: (@Sendable (URL) throws -> UInt64)?
    ) {
        exportSizeReaderForTesting = hook
    }

    /// TEST-ONLY: installs a closure that replaces the queue-record
    /// encoder. Setting the closure to `nil` restores the default
    /// `JSONEncoder` path. The override fires before the
    /// persistence layer is touched so an encode failure cannot
    /// advance the queue's private sequence allocator.
    internal func _setRecordEncoderForTesting(
        _ hook: (@Sendable (DurableRemoteQueueRecord) throws -> Data)?
    ) {
        recordEncoderOverrideForTesting = hook
    }

    // swiftlint:enable identifier_name

    /// Rounds the wall-clock to the nearest millisecond so the value
    /// satisfies the persistence layer's millisecond-aligned
    /// `createdAt` validation. Raw `Date()` carries sub-millisecond
    /// system-clock precision the canonical timestamp would reject.
    private static func millisecondAlignedNow() -> Date {
        let millis = (Date().timeIntervalSince1970 * 1000)
            .rounded(.toNearestOrAwayFromZero)
        return Date(timeIntervalSince1970: millis / 1000)
    }

    /// Reads the exact post-export file size. Exposed as
    /// `byteCount` on the returned batch summary; a missing,
    /// unreadable, or malformed `.size` value (including a
    /// negative `off_t`) surfaces as a typed
    /// ``DurableRemoteQueueError/drainSizeReadFailed`` rather than
    /// silently passing a wrap-converted unsigned byte count.
    private static func exactExportByteCount(
        at url: URL
    ) throws(DurableRemoteQueueError) -> UInt64 {
        guard let attrs = try? FileManager.default.attributesOfItem(
            atPath: url.path
        ) else {
            throw .drainSizeReadFailed
        }
        guard let size = attrs[.size] as? NSNumber else {
            throw .drainSizeReadFailed
        }
        // Extract the value as `Int64` via `CFNumberGetValue`,
        // which returns `false` when the stored value cannot be
        // represented exactly in the requested type. Plain
        // `NSNumber.int64Value` would silently coerce an
        // oversized or malformed numeric representation; the
        // checked extraction surfaces that as
        // `.drainSizeReadFailed` instead.
        var signed: Int64 = 0
        let extracted = CFNumberGetValue(
            size as CFNumber, .sInt64Type, &signed
        )
        guard extracted else {
            throw .drainSizeReadFailed
        }
        // Reject negative values explicitly so a malformed `.size`
        // (e.g. corrupt filesystem snapshot) does not round-trip
        // through `UInt64` as a ~`UInt64.max` byte count.
        guard signed >= 0 else {
            throw .drainSizeReadFailed
        }
        return UInt64(signed)
    }
}
