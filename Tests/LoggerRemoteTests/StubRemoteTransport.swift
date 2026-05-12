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

/// Sendable error variants the stub transport throws so tests can
/// drive the retry executor's `Result.failure(_:)` classification
/// branch deterministically. The cases are not normative; tests pick
/// whichever variant reads best in their assertion narrative.
internal struct StubTransportError: Error, Sendable, Equatable {
    /// Kind of failure the stub transport raises on a given call.
    enum Kind: Sendable, Equatable {
        /// Generic connection-style failure used by retryable tests.
        case connectionFailure
        /// Permanent-style failure used by terminal-classifier tests.
        case permanentFailure
    }

    let kind: Kind
}

/// Per-call outcome the stub transport returns to the engine on the
/// matching ``StubRemoteTransport/send(payloadBytes:payloadMetadata:)``
/// invocation.
internal enum StubTransportOutcome: Sendable {
    /// Transport returns this `RemoteTransportResponse` to the
    /// caller; the classifier then maps it into a delivery result.
    case response(RemoteTransportResponse)
    /// Transport throws this Sendable error to the caller; the
    /// classifier's `Result.failure(_:)` branch handles it.
    case failure(StubTransportError)
}

/// Record of one transport `send(...)` invocation the engine made;
/// tests assert against the ordered list of calls.
internal struct StubTransportCall: Sendable, Equatable {
    let payloadBytes: Data
    let payloadMetadata: [String: String]
}

/// Programmable `RemoteTransport` fixture used by the retry /
/// execution-loop / engine tests.
///
/// Each `send(...)` call consumes the next entry from `outcomes`;
/// once `outcomes` is exhausted, additional calls fail through
/// ``StubTransportError/Kind/permanentFailure`` so a runaway loop
/// does not silently re-enter an undefined slot. Recorded calls are
/// exposed through ``recordedCalls()`` for ordered assertion.
///
/// Classification (``RemoteTransport/classify(_:)``) is sink-owned
/// in production; tests inject a `classifier` closure at init so
/// per-test classification logic stays adjacent to the per-test
/// transport-outcome scripting.
///
/// Marked `final` + actor-isolated to satisfy
/// ``RemoteTransport``'s `Sendable` requirement; the fixture itself
/// is intentionally `internal`.
internal final actor StubRemoteTransport: RemoteTransport {
    private var outcomes: [StubTransportOutcome]
    private var calls: [StubTransportCall] = []
    private let classifier:
        @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult

    init(
        outcomes: [StubTransportOutcome],
        classifier: @escaping @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult
    ) {
        self.outcomes = outcomes
        self.classifier = classifier
    }

    func send(
        payloadBytes: Data,
        payloadMetadata: [String: String]
    ) async throws -> RemoteTransportResponse {
        let index = calls.count
        calls.append(StubTransportCall(
            payloadBytes: payloadBytes,
            payloadMetadata: payloadMetadata
        ))
        guard index < outcomes.count else {
            throw StubTransportError(kind: .permanentFailure)
        }
        switch outcomes[index] {
        case let .response(response):
            return response
        case let .failure(error):
            throw error
        }
    }

    func classify(
        _ result: Result<RemoteTransportResponse, any Error>
    ) async -> RemoteDeliveryResult {
        await classifier(result)
    }

    func recordedCalls() -> [StubTransportCall] {
        calls
    }
}

/// Captures the sequence of backoff `seconds` values the executor
/// passed to its `sleep` injector, in order. Tests assert exact
/// match against the policy's expected backoff progression.
internal final actor SleepRecorder {
    private var sleeps: [Double] = []

    func record(_ seconds: Double) {
        sleeps.append(seconds)
    }

    func recordedSleeps() -> [Double] {
        sleeps
    }
}

/// Builds a `@Sendable (Double) async throws -> Void` sleep closure
/// suitable for the retry executor's `sleep` parameter. The default
/// shape only records; pass `throwOnSleepIndex` to make the closure
/// throw exactly once at that 0-indexed sleep so tests can drive
/// the sleep-interruption branches in retry executor, execution
/// loop, and engine tests.
internal enum RecordingSleep {
    static func make(
        recorder: SleepRecorder,
        throwOnSleepIndex: Int? = nil
    ) -> @Sendable (Double) async throws -> Void {
        { seconds in
            let priorCount = await recorder.recordedSleeps().count
            await recorder.record(seconds)
            if let threshold = throwOnSleepIndex, priorCount == threshold {
                throw StubTransportError(kind: .permanentFailure)
            }
        }
    }
}
