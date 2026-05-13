import Foundation
@testable import LoggerRemote

// Test-only fixtures (not public API).
//
// `StubRemoteTransport`, `StubTransportError`, `SleepRecorder`, and
// `RecordingSleep.make(...)` are intentionally excluded from the
// production `LoggerRemote` module and must never surface as
// `public` types — the M3.4 milestone only releases the contract
// value types listed in `Docs/APIDesign.md` and the engine-internal
// machinery the test target reaches via `@testable import`.

/// Sendable marker error the stub transport surfaces inside the
/// per-item `Result.failure(_:)` slot when no scripted outcome is
/// available for the item (the outcome queue ran out). Tests
/// route this through the per-test classifier closure to decide
/// `.success` / `.retryable` / `.terminal`; the engine itself
/// never observes the error value.
internal struct StubTransportError: Error, Sendable, Equatable {}

/// Per-item outcome the stub transport surfaces inside one batch
/// dispatch call's returned `[Result<...>]` array.
internal enum StubTransportOutcome: Sendable {
    /// Transport reports this `RemoteTransportResponse` for the
    /// item; the classifier then maps it into a delivery result.
    case response(RemoteTransportResponse)
    /// Transport reports this Sendable error for the item; the
    /// classifier's `Result.failure(_:)` branch handles it.
    case failure(StubTransportError)
}

/// Record of one transport per-item dispatch carried inside a
/// ``StubBatchCallRecord``; tests assert against the ordered
/// per-item view of each batch call.
internal struct StubTransportCall: Sendable, Equatable {
    let payloadBytes: Data
    let payloadMetadata: [String: String]
}

/// Record of one ``RemoteTransport.sendBatch(_:)`` invocation the
/// engine made; tests assert against the ordered list of calls and
/// the per-item content of each call.
internal struct StubBatchCallRecord: Sendable, Equatable {
    let items: [StubTransportCall]
}

/// Programmable `RemoteTransport` fixture used by the batch
/// dispatch / execution-loop / engine tests.
///
/// Each ``RemoteTransport.sendBatch(_:)`` call consumes the next
/// `items.count` entries from `outcomes` in arrival order and
/// returns one mapped `Result<RemoteTransportResponse, any Error>`
/// per input item, preserving input order. Tests can therefore
/// drive multi-round behaviour by laying down a flat sequence of
/// outcomes whose prefix is consumed by each round's active set.
/// Once `outcomes` is exhausted, additional items fail through
/// ``StubTransportError`` so a runaway loop
/// does not silently re-enter an undefined slot.
///
/// Optional knobs:
/// - `dropResultsPerCall`: maps a 0-indexed call number to the
///   number of trailing results to omit from that call's return
///   array. Drives the
///   ``RemoteEngineError.transportBatchInvalid(expected:actual:)``
///   path deterministically.
/// - `onCall`: invoked at the top of each `sendBatch` with the
///   0-indexed call number. Throwing here causes the entire
///   `sendBatch` call to throw, which the engine treats as a
///   whole-batch transport failure and routes through
///   ``classify(_:)`` with `.failure(error)` for every active
///   item in the round.
///
/// Marked `final` + actor-isolated to satisfy
/// ``RemoteTransport``'s `Sendable` requirement; the fixture
/// itself is intentionally `internal`.
internal final actor StubRemoteTransport: RemoteTransport {
    private let outcomes: [StubTransportOutcome]
    private var outcomesReadIndex: Int = 0
    private var callCount: Int = 0
    private var calls: [StubBatchCallRecord] = []
    private let classifier:
        @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult
    private let dropResultsPerCall: [Int: Int]
    private let onCall: (@Sendable (Int) async throws -> Void)?

    init(
        outcomes: [StubTransportOutcome],
        classifier: @escaping @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult,
        dropResultsPerCall: [Int: Int] = [:],
        onCall: (@Sendable (Int) async throws -> Void)? = nil
    ) {
        self.outcomes = outcomes
        self.classifier = classifier
        self.dropResultsPerCall = dropResultsPerCall
        self.onCall = onCall
    }

    func sendBatch(
        _ items: [RemoteTransportBatchItem]
    ) async throws -> [Result<RemoteTransportResponse, any Error>] {
        let callIndex = callCount
        callCount += 1
        let record = StubBatchCallRecord(items: items.map { item in
            StubTransportCall(
                payloadBytes: item.payloadBytes,
                payloadMetadata: item.payloadMetadata
            )
        })
        calls.append(record)

        if let hook = onCall {
            try await hook(callIndex)
        }

        // Consume `items.count` outcomes from the queue via a read
        // index. Per-item ordering inside the call mirrors `items`.
        var consumed: [StubTransportOutcome] = []
        consumed.reserveCapacity(items.count)
        for _ in 0 ..< items.count {
            if outcomesReadIndex < outcomes.count {
                consumed.append(outcomes[outcomesReadIndex])
                outcomesReadIndex += 1
            } else {
                consumed.append(.failure(
                    StubTransportError()
                ))
            }
        }

        var results: [Result<RemoteTransportResponse, any Error>] = consumed.map { outcome in
            switch outcome {
            case let .response(response):
                return .success(response)
            case let .failure(error):
                return .failure(error)
            }
        }

        if let drop = dropResultsPerCall[callIndex], drop > 0 {
            let keep = max(0, results.count - drop)
            results = Array(results.prefix(keep))
        }

        return results
    }

    func classify(
        _ result: Result<RemoteTransportResponse, any Error>
    ) async -> RemoteDeliveryResult {
        await classifier(result)
    }

    /// Ordered per-call view of every ``RemoteTransport.sendBatch(_:)``
    /// invocation the engine made against the stub, including
    /// the ordered items inside each call. Use this for tests
    /// that assert batch-shape (e.g. "round 1 dispatched N items
    /// in one call, round 2 dispatched M ≤ N retryables").
    func recordedBatchCalls() -> [StubBatchCallRecord] {
        calls
    }

    /// Flattened per-item view across every batch call: the
    /// concatenation of `recordedBatchCalls()[i].items` in order.
    /// Convenience for tests that only care about the per-item
    /// payload sequence the engine drove through the transport,
    /// regardless of which batch call each item rode in.
    func recordedCalls() -> [StubTransportCall] {
        calls.flatMap(\.items)
    }
}

/// Captures the sequence of backoff `seconds` values the dispatcher
/// passed to its `sleep` injector, in order. Tests assert exact
/// match against the policy's expected backoff progression.
internal final actor SleepRecorder {
    private var sleeps: [Double] = []

    /// Appends `seconds` and returns the count of previously recorded
    /// sleeps in one actor-isolated step. ``RecordingSleep/make(recorder:throwOnSleepIndex:)``
    /// uses this to decide whether to throw at this sleep without a
    /// read/write split between two awaits.
    func recordAndReturnPreviousCount(_ seconds: Double) -> Int {
        let previousCount = sleeps.count
        sleeps.append(seconds)
        return previousCount
    }

    func recordedSleeps() -> [Double] {
        sleeps
    }
}

/// Builds a `@Sendable (Double) async throws -> Void` sleep closure
/// suitable for the dispatcher's `sleep` parameter. The default
/// shape only records; pass `throwOnSleepIndex` to make the closure
/// throw exactly once at that 0-indexed sleep so tests can drive
/// the sleep-interruption branches in execution loop and engine
/// tests.
internal enum RecordingSleep {
    static func make(
        recorder: SleepRecorder,
        throwOnSleepIndex: Int? = nil
    ) -> @Sendable (Double) async throws -> Void {
        { seconds in
            let priorCount = await recorder.recordAndReturnPreviousCount(seconds)
            if let threshold = throwOnSleepIndex, priorCount == threshold {
                throw StubTransportError()
            }
        }
    }
}
