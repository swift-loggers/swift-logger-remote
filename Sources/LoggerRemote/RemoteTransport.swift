import Foundation

/// Sink-neutral transport surface used by the remote-delivery engine.
///
/// Adapters bridge the engine's opaque payload bytes and metadata to
/// a vendor-specific transport and own response classification.
public protocol RemoteTransport: Sendable {
    /// Sends `payloadBytes` plus `payloadMetadata` and returns the
    /// transport response. The engine never inspects the returned
    /// bytes or metadata; classification is owned by
    /// ``classify(_:)``.
    func send(
        payloadBytes: Data,
        payloadMetadata: [String: String]
    ) async throws -> RemoteTransportResponse

    /// Maps the result of ``send(payloadBytes:payloadMetadata:)``
    /// (either a returned ``RemoteTransportResponse`` or a thrown
    /// error) into a ``RemoteDeliveryResult``. Classification is
    /// sink-owned: the adapter consults HTTP status, vendor body
    /// codes, or transport error types to decide success /
    /// retryable / terminal; the engine itself never inspects any
    /// of those signals (LGR-5 / LGR-7 / LGR-9).
    ///
    /// **Adapter contract:**
    ///
    /// - **Deterministic within a flush pass.** For the same
    ///   adapter instance and the same `result` value, the
    ///   classifier MUST return the same ``RemoteDeliveryResult``
    ///   for every call inside one ``RemoteEngine/flush()``. The
    ///   engine relies on this when it tallies per-entry outcomes
    ///   and decides acknowledgement; a classifier that returns
    ///   different verdicts for identical inputs would let the
    ///   ack decision depend on call ordering rather than on the
    ///   sink's response shape.
    /// - **No ack or export-file lifecycle side effects.**
    ///   ``classify(_:)`` MUST NOT (directly or indirectly)
    ///   advance ``DurableRemoteQueue/acknowledge()``, mutate any
    ///   engine-owned export artifact, or otherwise observe or
    ///   touch the queue's outstanding-batch state. The engine
    ///   intentionally does not pass any of those primitives in
    ///   (the classifier sees only `Result<RemoteTransportResponse,
    ///   any Error>`), so the constraint is structural; the
    ///   adapter MUST also avoid reaching them through external
    ///   references.
    func classify(
        _ result: Result<RemoteTransportResponse, any Error>
    ) async -> RemoteDeliveryResult
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
