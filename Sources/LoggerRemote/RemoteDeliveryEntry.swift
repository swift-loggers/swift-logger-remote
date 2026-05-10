import Foundation

/// Durable unit accepted by the remote delivery engine.
///
/// Carries opaque payload bytes and sink-owned metadata. The engine
/// does not store raw `LogRecord` values; encoding happens upstream
/// in vendor-specific adapters before the entry reaches the engine.
public struct RemoteDeliveryEntry: Sendable, Equatable {
    /// Engine-local correlation identifier only. It is not a
    /// persistence replay identity and carries no ordering contract.
    public let identifier: UInt64

    /// Encoded payload bytes; opaque to the engine.
    public let payload: Data

    /// Sink-owned metadata attached to the entry (request labels,
    /// routing hints, vendor-specific identifiers). The engine
    /// preserves these verbatim and does not interpret them.
    public let metadata: [String: String]

    public init(
        identifier: UInt64,
        payload: Data,
        metadata: [String: String] = [:]
    ) {
        self.identifier = identifier
        self.payload = payload
        self.metadata = metadata
    }
}
