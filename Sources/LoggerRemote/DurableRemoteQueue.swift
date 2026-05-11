import Foundation
import LoggerFilePersistence
import LoggerPersistence

/// Durable persistence-backed accept / drain / acknowledge surface
/// for the remote-delivery engine.
///
/// `DurableRemoteQueue` is the only piece of `swift-logger-remote`
/// that touches `swift-logger-persistence`. The engine's delivery
/// loop, retry scheduler, and transport implementations consume the
/// queue's drained batches; they never call the persistence package
/// directly. Removal of accepted bytes happens exclusively through
/// ``acknowledge()`` after the transport classifies a drained batch
/// as successfully delivered (LGR-10, LGR-11).
///
/// Current scope is minimal:
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
///
/// Real HTTP transport, retry scheduling, batch-policy slicing,
/// flush trigger semantics, and lifecycle-observer wiring stay out
/// of scope for this milestone and ship with the engine delivery
/// loop.
public actor DurableRemoteQueue {
    private let store: FileLogStore
    private let contentType: String
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
    ///   - contentType: `contentType` recorded on each persisted
    ///     envelope. Must satisfy the persistence layer's
    ///     content-type validation (visible ASCII, no whitespace
    ///     or control characters, 1...128 UTF-8 bytes).
    ///   - rotation: Segment-rotation policy forwarded to the
    ///     persistence layer.
    ///
    /// The queue intentionally does not expose a `retention`
    /// parameter. Persistence retention (`.maxSegments`,
    /// `.maxTotalBytes`, `.maxAge`) could delete bytes that were
    /// never acknowledged by the delivery loop, which would violate
    /// LGR-11 (acknowledgement is the only trigger for durable
    /// removal). The queue hardcodes `.unlimited`; a bounded queue
    /// policy is a future contract separate from persistence
    /// retention.
    public init(
        directory: URL,
        contentType: String = "application/vnd.swift-loggers.remote-queue+json",
        rotation: RotationPolicy = .never
    ) {
        self.init(
            directory: directory,
            contentType: contentType,
            rotation: rotation,
            startingSequence: 1
        )
    }

    /// TEST-ONLY initializer that seeds the private sequence
    /// allocator. Production callers must use the public init,
    /// which starts at `1`.
    internal init(
        directory: URL,
        contentType: String,
        rotation: RotationPolicy,
        startingSequence: UInt64
    ) {
        self.contentType = contentType
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
        // Reserve the persistence sequence before the I/O so the
        // exhaustion guard runs first. The allocator never wraps
        // to `0`; an exhausted allocator surfaces `.sequenceExhausted`
        // and the queue must be rotated to a fresh directory.
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
                contentType: contentType,
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
        // sequence unchanged. `UInt64.max + 1` wraps to `0`, which
        // the exhaustion guard above catches on the next call.
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
        // failure leaves the queue without an `outstandingBatch`,
        // so `acknowledge()` must refuse the destructive remove
        // even though the persistence boundary may still exist
        // — see the queue-held-batch guard in `acknowledge()`.
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
    /// ``acknowledge()`` against a drained batch **within the same
    /// process**. It is not crash-recovery state — a process
    /// restart loses the in-memory batch reference and the queue
    /// has no replay/query API to rebuild it. The future delivery
    /// loop consumes this value only for in-process retry against
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
        // Read the signed value first; a negative `.size` is a
        // malformed attribute (e.g. corrupt filesystem snapshot)
        // and must not silently round-trip through `UInt64` as a
        // ~ `UInt64.max` value.
        let signed = size.int64Value
        guard signed >= 0 else {
            throw .drainSizeReadFailed
        }
        return UInt64(signed)
    }
}
