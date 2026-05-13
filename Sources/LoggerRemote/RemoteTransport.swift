import Foundation

/// Sink-neutral transport surface used by the remote-delivery
/// engine.
///
/// `sendBatch(_:)` is the only transport dispatch primitive in
/// `0.1.0`. The engine drives delivery as batch rounds: it hands
/// the transport an ordered array of
/// ``RemoteTransportBatchItem`` values, the transport dispatches
/// them in whatever vendor-specific shape it wants (one HTTP
/// request per item for Splunk HEC / Loki single-event, one
/// `_bulk` request for Elastic, etc.), and returns one
/// `Result<RemoteTransportResponse, any Error>` per input item in
/// the **same order** as the input. The engine then maps each
/// result through ``classify(_:)`` to a
/// ``RemoteDeliveryResult`` and aggregates per-entry outcomes.
///
/// Single-event adapters implement ``sendBatch(_:)`` by
/// dispatching each item independently and returning one result
/// per input item, preserving input order — no public `send`
/// primitive exists; the per-item loop lives inside the adapter.
/// Batch-aggregating adapters (Elastic `_bulk`, OTLP/HTTP batched)
/// build one vendor request from all items in the call and map
/// the vendor response back to a per-input result.
public protocol RemoteTransport: Sendable {
    /// Dispatches `items` to the sink and returns one
    /// `Result<RemoteTransportResponse, any Error>` per input
    /// item, in the same order as `items`.
    ///
    /// **Order contract.** The returned array MUST have exactly
    /// `items.count` elements; the result at index `i`
    /// corresponds to `items[i]`. The engine fails closed with
    /// ``RemoteEngineError/transportBatchInvalid(expected:actual:)``
    /// if the returned count differs from `items.count`.
    ///
    /// **Per-item independence.** Each returned `Result` reflects
    /// only the dispatch outcome for its own input item. Adapters
    /// that share a single vendor request across the batch
    /// (Elastic `_bulk`) project the vendor's item-level response
    /// into independent per-item results; one item's failure
    /// MUST NOT poison sibling items' classifications.
    ///
    /// **Throwing.** If the entire `sendBatch` call throws, the
    /// engine treats it as a transport-level failure for every
    /// item in the call (each item is run through
    /// ``classify(_:)`` with the same `.failure(error)` value).
    /// Throw only for whole-batch failures (DNS, TLS, request
    /// build); per-item failures travel as `.failure(error)`
    /// inside the returned array.
    func sendBatch(
        _ items: [RemoteTransportBatchItem]
    ) async throws -> [Result<RemoteTransportResponse, any Error>]

    /// Maps the per-item result of ``sendBatch(_:)`` (either a
    /// returned ``RemoteTransportResponse`` or a thrown error)
    /// into a ``RemoteDeliveryResult``. Classification is
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
    ///   and decides acknowledgement.
    /// - **No ack or export-file lifecycle side effects.**
    ///   ``classify(_:)`` MUST NOT (directly or indirectly)
    ///   advance ``DurableRemoteQueue/acknowledge()``, mutate any
    ///   engine-owned export artifact, or otherwise observe or
    ///   touch the queue's outstanding-batch state.
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
