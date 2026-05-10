import Foundation

/// Sink-neutral transport surface used by the remote-delivery engine.
///
/// Adapters bridge the engine's opaque payload bytes and metadata to
/// a vendor-specific transport.
public protocol RemoteTransport: Sendable {
    /// Sends `payloadBytes` plus `payloadMetadata` and returns the
    /// transport response. Adapters classify this response into a
    /// ``RemoteDeliveryResult`` (success / retryable / terminal)
    /// outside the engine.
    func send(
        payloadBytes: Data,
        payloadMetadata: [String: String]
    ) async throws -> RemoteTransportResponse
}

/// Sink-owned response returned by a `RemoteTransport`.
///
/// The engine does not interpret response bytes or metadata.
/// Adapters classify responses into `RemoteDeliveryResult`.
public struct RemoteTransportResponse: Sendable, Equatable {
    /// Opaque response bytes returned by the transport.
    public let responseBytes: Data
    /// Sink-owned response metadata returned by the transport.
    public let responseMetadata: [String: String]

    public init(
        responseBytes: Data,
        responseMetadata: [String: String] = [:]
    ) {
        self.responseBytes = responseBytes
        self.responseMetadata = responseMetadata
    }
}
