import Foundation

/// One delivery unit the engine hands to
/// ``RemoteTransport/sendBatch(_:)`` for transport-level dispatch.
///
/// The engine builds one item per ``RemoteDeliveryEntry`` in the
/// batch group it is about to attempt. The transport sees the
/// entry's `payloadBytes` and `payloadMetadata` verbatim and never
/// observes the engine-local ``RemoteDeliveryEntry/identifier``;
/// per-entry correlation between input items and the returned
/// results array is by **position**, not by identifier.
public struct RemoteTransportBatchItem: Sendable, Equatable {
    /// Opaque pre-encoded bytes the engine hands to the transport.
    /// The engine never decodes them.
    public let payloadBytes: Data

    /// Sink-owned metadata the engine propagates byte-for-byte from
    /// ``RemoteDeliveryEntry/metadata`` to the transport. The engine
    /// never inspects `payloadMetadata` keys or values; it only
    /// forwards them to the transport.
    public let payloadMetadata: [String: String]

    public init(
        payloadBytes: Data,
        payloadMetadata: [String: String] = [:]
    ) {
        self.payloadBytes = payloadBytes
        self.payloadMetadata = payloadMetadata
    }
}
