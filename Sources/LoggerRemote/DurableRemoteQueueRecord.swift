import Foundation

/// Package-owned internal record used by ``DurableRemoteQueue`` to
/// persist one ``RemoteDeliveryEntry`` losslessly.
///
/// The persistence layer stores opaque payload bytes; to round-trip
/// the engine-local correlation identifier and sink-owned metadata
/// across the byte-stable export boundary the queue serializes the
/// entry into this record and uses the encoded bytes as the
/// persistence `payload`. The record is intentionally internal so
/// the public engine surface stays free of a queue-specific
/// container type; the engine-internal ``BatchEngine`` parser
/// reads the same record back through the same encoder.
///
/// The record is a persistent on-disk schema: bytes a writer of
/// one queue version persists must be parseable by a reader of
/// any later compatible queue version. ``formatVersion`` is the
/// schema-evolution anchor every record carries; the engine-
/// internal ``BatchEngine`` parser MUST inspect it before
/// decoding any other field and refuse to interpret an unknown
/// version rather than silently treating new fields as missing.
internal struct DurableRemoteQueueRecord: Codable, Sendable, Equatable {
    /// Current queue-record schema version. Increment when the
    /// on-disk shape changes; readers MUST reject unknown versions
    /// fail-closed.
    static let currentFormatVersion: UInt8 = 1

    /// On-disk schema version carried by every persisted record.
    /// Older readers reject a record whose version they do not
    /// recognize fail-closed; new fields land alongside a bump.
    let formatVersion: UInt8

    /// Mirrors `RemoteDeliveryEntry.identifier`; engine-local
    /// correlation only. Does not drive persistence ordering — the
    /// queue's own private sequence allocator owns that.
    let identifier: UInt64

    /// Mirrors `RemoteDeliveryEntry.payload` byte-for-byte.
    let payload: Data

    /// Mirrors `RemoteDeliveryEntry.metadata` verbatim. `[:]` is
    /// preserved as an empty object on the wire so the decoder
    /// always returns a definite map.
    let metadata: [String: String]
}
