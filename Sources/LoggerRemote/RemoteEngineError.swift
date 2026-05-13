import Foundation

/// Public diagnostic surface for ``RemoteEngine/flush()``.
///
/// Wraps each engine-internal failure layer in a public-typed case
/// so callers can branch on the lifecycle step that surfaced the
/// failure without depending on engine-internal types like
/// `BatchEngineError` or `ExecutionLoopError`.
public enum RemoteEngineError: Error, Sendable, Equatable {
    /// ``DurableRemoteQueue/flush()`` failed on the fresh-drain
    /// path. ``RemoteEngine/flush()`` only calls
    /// ``DurableRemoteQueue/flush()`` when there is no
    /// outstanding batch to reuse; on that path it flushes the
    /// queue's writer buffer to disk so the captured prefix
    /// includes every admitted entry. The outstanding-reuse path
    /// skips this step entirely, so this case is only raised
    /// during fresh-drain flushes. The carried
    /// ``DurableRemoteQueueError`` is the persistence-layer
    /// reason verbatim.
    case flushFailed(DurableRemoteQueueError)

    /// ``DurableRemoteQueue/drain(to:)`` failed before the
    /// engine owns a reusable drained batch reference for the
    /// current flush pass. The engine runs a best-effort removal
    /// of the just-allocated scratch URL before throwing; if
    /// that filesystem cleanup also fails the engine
    /// intentionally suppresses it so this diagnostic stays
    /// primary, which means a stale scratch file at the
    /// engine-owned `exportDirectory` URL can linger across a
    /// drain failure. Callers that observe this case may want
    /// to sweep the directory before the next flush.
    case drainFailed(DurableRemoteQueueError)

    /// Parsing the byte-stable export through
    /// `BatchEngine.recoverEntries(from:)` failed closed
    /// before any transport call could be made. The public
    /// ``RemoteEngineParseError`` mirrors the engine-internal
    /// `BatchEngineError` cases one-for-one — including
    /// associated values — so the public type preserves the
    /// exact parser failure the engine refused to interpret
    /// further.
    case parseFailed(RemoteEngineParseError)

    /// Splitting the recovered entry stream into batches through
    /// ``RemoteBatchPolicy`` failed (e.g. an oversized single
    /// entry surfaces ``RemoteDeliveryError/batchSizeExceeded(limit:actual:)``
    /// or an engine-side batching-state defect surfaces
    /// ``RemoteDeliveryError/invalidBatchState``).
    case batchFailed(RemoteDeliveryError)

    /// The batch-round retry path was interrupted before every
    /// entry in the flush pass reached a resolved classification.
    /// Either the sleep injector threw between two dispatch
    /// rounds or the policy delay calculation refused the
    /// requested attempt; the public ``RemoteEngineRetryError``
    /// taxonomy preserves the distinction.
    case retryInterrupted(RemoteEngineRetryError)

    /// ``RemoteTransport/sendBatch(_:)`` returned a result array
    /// whose count does not match the number of items the engine
    /// handed it. The engine fails closed rather than guessing
    /// which entries the surplus or missing results refer to;
    /// the queue still holds the outstanding-batch boundary and
    /// the export artifact stays on disk so the next
    /// ``RemoteEngine/flush()`` can reuse the retained export
    /// artifact through the outstanding-reuse path once the
    /// adapter is fixed.
    /// `expected` is the input item count the engine handed
    /// to ``RemoteTransport/sendBatch(_:)``; `actual` is the
    /// returned result count.
    case transportBatchInvalid(expected: Int, actual: Int)

    /// Releasing the queue's outstanding-batch boundary by
    /// calling ``DurableRemoteQueue/acknowledge()`` failed.
    /// Reported by the engine for both the empty-drain release
    /// path (no delivered queue payload bytes existed) and the
    /// non-empty acknowledgement path (every entry was resolved
    /// and the engine acknowledged a fully-resolved non-empty
    /// flush pass). On any
    /// failure of this case the queue still holds the
    /// outstanding boundary and the export artifact stays on
    /// disk so the next ``RemoteEngine/flush()`` can reuse the
    /// retained export artifact through the outstanding-reuse
    /// path.
    case acknowledgementFailed(DurableRemoteQueueError)

    /// Removing the engine-owned export artifact from
    /// ``RemoteEngine`` `exportDirectory` failed.
    ///
    /// **Acknowledgement state when this case is raised:**
    /// - ``RemoteEngineExportCleanupContext/Phase/emptyRelease``
    ///   means the empty-drain ack
    ///   (``DurableRemoteQueue/acknowledge()``) **succeeded** and
    ///   the in-memory boundary is cleared; the (empty) export
    ///   artifact lingers but carries no delivered queue payload
    ///   bytes.
    /// - ``RemoteEngineExportCleanupContext/Phase/acknowledgedNonEmpty``
    ///   means the non-empty ack **succeeded** — the queue's
    ///   destructive removal already ran and persistence has
    ///   dropped the delivered queue payload bytes — but the
    ///   engine's scratch export file with those same bytes is
    ///   still on disk. Acknowledgement is final; the retained
    ///   artifact is a duplicate copy, not a retry source — the
    ///   engine never re-reads it and the next ``RemoteEngine/flush()``
    ///   sees no outstanding batch on the queue. Callers MAY
    ///   remove the artifact at
    ///   ``RemoteEngineExportCleanupContext/exportURL`` after
    ///   observing this case to drop the retained copy.
    ///
    /// The carried ``RemoteEngineExportCleanupContext`` preserves
    /// the export URL, the cleanup phase, and the underlying
    /// `NSError` domain / code so the failure stays
    /// addressable without leaking a non-`Sendable` `NSError`
    /// through the public type.
    case exportCleanupFailed(RemoteEngineExportCleanupContext)
}

/// Public diagnostic context attached to
/// ``RemoteEngineError/exportCleanupFailed(_:)``.
///
/// Captures the engine-owned export-file URL the cleanup
/// targeted, the lifecycle phase the cleanup ran in, and the
/// underlying `NSError` domain / code so callers can diagnose the
/// failure without depending on a non-`Sendable` `NSError`.
public struct RemoteEngineExportCleanupContext: Sendable, Equatable {
    /// Lifecycle phase the cleanup ran in.
    ///
    /// Only the phases that can actually surface a public
    /// ``RemoteEngineError/exportCleanupFailed(_:)`` are
    /// enumerated. The engine's drain-failure path also runs a
    /// best-effort scratch removal, but its cleanup failure is
    /// intentionally suppressed (the
    /// ``RemoteEngineError/drainFailed(_:)`` diagnostic stays
    /// primary), so that phase is engine-internal only and never
    /// appears here.
    public enum Phase: Sendable, Equatable {
        /// Cleanup of the empty export artifact after the engine
        /// acknowledged a zero-byte drain release. The empty
        /// export carries no delivered queue payload bytes; a
        /// failure here is a scratch-file leak only.
        case emptyRelease

        /// Cleanup of the non-empty export artifact after the
        /// engine acknowledged a fully-resolved non-empty flush
        /// pass. The queue's
        /// destructive removal already ran, so the retained
        /// artifact is a duplicate copy of bytes the persistence
        /// layer has already dropped — not a retry source: the
        /// engine never re-reads it and the next
        /// ``RemoteEngine/flush()`` sees no outstanding batch on
        /// the queue. Callers MAY remove the retained URL after
        /// observing ``RemoteEngineError/exportCleanupFailed(_:)``.
        case acknowledgedNonEmpty
    }

    /// Engine-owned export URL the cleanup targeted.
    public let exportURL: URL

    /// Lifecycle phase the cleanup ran in.
    public let phase: Phase

    /// `NSError.domain` for the underlying filesystem failure
    /// (e.g. `NSCocoaErrorDomain`, `NSPOSIXErrorDomain`).
    public let errorDomain: String

    /// `NSError.code` for the underlying filesystem failure.
    public let errorCode: Int

    public init(
        exportURL: URL,
        phase: Phase,
        errorDomain: String,
        errorCode: Int
    ) {
        self.exportURL = exportURL
        self.phase = phase
        self.errorDomain = errorDomain
        self.errorCode = errorCode
    }
}

/// Public parser-failure surface for ``RemoteEngineError/parseFailed(_:)``.
///
/// Mirrors the engine-internal `BatchEngineError` taxonomy so the
/// public type preserves the exact byte-stable-export failure the
/// engine refused to interpret further. Associated values match
/// the internal cases verbatim.
public enum RemoteEngineParseError: Error, Sendable, Equatable {
    /// The drained export file could not be read at all
    /// (filesystem read failure).
    case exportFileReadFailed

    /// The drained export file's on-disk size disagrees with the
    /// queue's captured ``DurableRemoteQueueBatch/byteCount``;
    /// the bytes the engine is about to interpret are not the
    /// bytes the queue drained.
    case exportByteCountMismatch(expected: UInt64, actual: UInt64)

    /// `Data.count` (a `Swift.Int`) for the export bytes cannot
    /// be represented as the queue's `UInt64` byte-count surface
    /// — an engine / platform invariant violation surfaced
    /// fail-closed.
    case exportByteCountUnavailable

    /// One persistence-envelope line in the byte-stable export
    /// could not be parsed as a JSON envelope object.
    case envelopeMalformed

    /// The envelope parsed but its `contentType` did not match
    /// the queue-owned envelope content type; a foreign envelope
    /// (one the queue did not produce for the queue-owned
    /// envelope `contentType`) is refused fail-closed at this
    /// validation stage, before the queue record's
    /// `formatVersion` field is decoded.
    case envelopeContentTypeMismatch(expected: String, found: String)

    /// The envelope's `payload` was present but its base64 text
    /// could not be decoded into queue-record bytes.
    case recordPayloadBase64Invalid

    /// The decoded queue-record bytes could not be parsed as a
    /// queue record (malformed JSON or shape mismatch).
    case recordPayloadMalformed

    /// The recovered queue record did not carry a `formatVersion`
    /// field at all.
    case recordFormatVersionMissing

    /// The recovered queue record carried a `formatVersion` the
    /// engine does not recognize. `found` is widened to `UInt64`
    /// so an integer outside the current `UInt8` schema-version
    /// space (e.g. `999`) is preserved verbatim instead of
    /// collapsing into a generic malformed-record category.
    case recordFormatVersionUnsupported(found: UInt64, supported: UInt8)
}

/// Public retry-failure surface for
/// ``RemoteEngineError/retryInterrupted(_:)``.
///
/// Splits the two engine-internal causes the batch-round retry
/// dispatcher can surface so callers can branch on the layer that
/// interrupted the retry budget.
public enum RemoteEngineRetryError: Error, Sendable, Equatable {
    /// The retry-delay calculation refused the requested attempt
    /// count between two batch dispatch rounds. The carried
    /// ``RemoteDeliveryError`` is the policy-space diagnostic;
    /// ``RemoteRetryPolicy/make(maxAttempts:backoff:)`` already
    /// enforces that the default calculator accepts every attempt
    /// the engine passes, so this case names an engine-side /
    /// seam-injected invariant violation rather than a
    /// caller-actionable failure.
    case invalidRetryDelay(RemoteDeliveryError)

    /// The sleep injector threw between two batch dispatch rounds
    /// (e.g. cooperative task cancellation propagated from
    /// Swift concurrency sleep primitives). The underlying error
    /// is intentionally dropped at this layer; the engine treats
    /// every sleep-injector failure as equivalent for retry-loop
    /// lifecycle purposes.
    case sleepInterrupted
}
