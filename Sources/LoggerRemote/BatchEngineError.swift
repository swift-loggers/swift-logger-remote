/// Typed diagnostic surface for the engine-internal batching path
/// that recovers ``RemoteDeliveryEntry`` values from a drained
/// queue export.
///
/// The error is intentionally not part of the public engine
/// surface; the batching engine is internal machinery and
/// surfaces failures back to the engine-internal
/// ``ExecutionLoop`` / ``RemoteEngine`` lifecycle. The public
/// translation lives on ``RemoteEngineParseError``, which mirrors
/// these cases one-for-one when surfaced through
/// ``RemoteEngine/flush()``.
internal enum BatchEngineError: Error, Sendable, Equatable {
    /// Reading the byte-stable export file produced by
    /// ``DurableRemoteQueue/drain(to:)`` failed.
    case exportFileReadFailed

    /// The export file's on-disk size disagrees with the
    /// ``DurableRemoteQueueBatch/byteCount`` the queue captured at
    /// drain time. The queue contract pins `byteCount` to the
    /// exact post-export size; a mismatch means the bytes the
    /// parser is about to interpret are not the bytes the queue
    /// drained.
    case exportByteCountMismatch(expected: UInt64, actual: UInt64)

    /// The export file's `Data.count` (a `Swift.Int`) cannot be
    /// represented as the queue's `UInt64` byte-count surface
    /// (e.g. a hypothetical negative or `UInt64`-overrange count
    /// that Swift's `Data` shape forbids today). Engine/platform
    /// invariant violation, surfaced fail-closed rather than
    /// trapping or wrap-casting. Distinct from
    /// ``BatchEngineError/exportFileReadFailed`` (the file could
    /// not be read at all) and
    /// ``BatchEngineError/exportByteCountMismatch(expected:actual:)``
    /// (the file was read but its size disagrees with the queue's
    /// recorded `byteCount`).
    case exportByteCountUnavailable

    /// One line of the byte-stable export could not be parsed as a
    /// persistence envelope JSON object (or the envelope did not
    /// expose a `payload` field).
    case envelopeMalformed

    /// The envelope parsed but its `contentType` did not match the
    /// queue-owned envelope content type. A foreign envelope (one
    /// the queue did not produce) is refused fail-closed rather
    /// than treated as a malformed queue record.
    case envelopeContentTypeMismatch(expected: String, found: String)

    /// The envelope's `payload` field was present but its base64
    /// text could not be decoded into queue-record bytes. The
    /// envelope itself parsed; the `payload` value is corrupt.
    case recordPayloadBase64Invalid

    /// The decoded queue-record bytes could not be parsed as a
    /// ``DurableRemoteQueueRecord``.
    case recordPayloadMalformed

    /// The queue record did not carry a `formatVersion` field at
    /// all. Older or hand-rolled records that predate the schema
    /// anchor are rejected fail-closed.
    case recordFormatVersionMissing

    /// The queue record carried a `formatVersion` the engine does
    /// not recognize. The engine-internal ``BatchEngine`` parser
    /// refuses to interpret unknown versions rather than silently
    /// treating new fields as missing. `found` is widened to
    /// `UInt64` so an integer outside the current `UInt8`
    /// schema-version space (e.g. `999`) routes to this diagnostic
    /// instead of being classified as a generic malformed record.
    case recordFormatVersionUnsupported(found: UInt64, supported: UInt8)
}
