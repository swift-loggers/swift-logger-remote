import Foundation
import Testing
@testable import LoggerRemote

@Suite("RetryExecutor per-entry retry semantics")
struct RetryExecutorTests {}

// MARK: - Test classifier helpers

extension RetryExecutorTests {
    /// Sink-owned classifier shape used by every test: a
    /// `__test_class` metadata key on the transport response selects
    /// the delivery result, and any thrown transport error maps to
    /// `.retryable(reason: .transportRejected)`. Tests that need a
    /// different classification declare their own classifier inline.
    static let metadataClassifier:
        @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult = {
            switch $0 {
            case let .success(response):
                switch response.responseMetadata["__test_class"] {
                case "retryable":
                    return .retryable(reason: .transportRejected)
                case "terminal":
                    return .terminal(reason: .transportRejected)
                default:
                    return .success
                }
            case .failure:
                return .retryable(reason: .transportRejected)
            }
        }

    static func makeSuccessResponse() -> RemoteTransportResponse {
        RemoteTransportResponse(responseBytes: Data())
    }

    static func makeRetryableResponse() -> RemoteTransportResponse {
        RemoteTransportResponse(
            responseBytes: Data(),
            responseMetadata: ["__test_class": "retryable"]
        )
    }

    static func makeTerminalResponse() -> RemoteTransportResponse {
        RemoteTransportResponse(
            responseBytes: Data(),
            responseMetadata: ["__test_class": "terminal"]
        )
    }

    static func makeEntry(
        identifier: UInt64 = 1,
        payload: Data = Data([0x01]),
        metadata: [String: String] = [:]
    ) -> RemoteDeliveryEntry {
        RemoteDeliveryEntry(
            identifier: identifier, payload: payload, metadata: metadata
        )
    }

    static func makeConstantPolicy(
        maxAttempts: Int, seconds: Double
    ) throws -> RemoteRetryPolicy {
        try RemoteRetryPolicy.make(
            maxAttempts: maxAttempts, backoff: .constant(seconds: seconds)
        )
    }

    static func makeExponentialPolicy(
        maxAttempts: Int,
        initialSeconds: Double,
        multiplier: Double,
        capSeconds: Double
    ) throws -> RemoteRetryPolicy {
        try RemoteRetryPolicy.make(
            maxAttempts: maxAttempts,
            backoff: .exponential(
                initialSeconds: initialSeconds,
                multiplier: multiplier,
                capSeconds: capSeconds
            )
        )
    }
}

// MARK: - First-attempt success

extension RetryExecutorTests {
    @Test(
        "first-attempt success returns after exactly one call with no sleep",
        .tags(.lgr2, .lgr3, .lgr5)
    )
    func firstAttemptSuccess() async throws {
        let entry = RemoteDeliveryEntry(
            identifier: 1,
            payload: Data([0x01]),
            metadata: ["route": "primary"]
        )
        let transport = StubRemoteTransport(
            outcomes: [.response(Self.makeSuccessResponse())]
        )
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeConstantPolicy(maxAttempts: 3, seconds: 0.5)

        let attempt = try await RetryExecutor.deliver(
            entry: entry,
            transport: transport,
            policy: policy,
            classifier: Self.metadataClassifier,
            sleep: sleep
        )

        #expect(attempt.entry == entry)
        #expect(attempt.outcome == .success)
        #expect(attempt.attempts == 1)
        let calls = await transport.recordedCalls()
        #expect(calls.count == 1)
        #expect(calls[0].payloadBytes == entry.payload)
        #expect(calls[0].payloadMetadata == entry.metadata)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - Retryable then success

extension RetryExecutorTests {
    @Test(
        "two retryable responses followed by success consume three attempts and two sleeps",
        .tags(.lgr2, .lgr3, .lgr5)
    )
    func retryableThenSuccess() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse()),
            .response(Self.makeSuccessResponse())
        ])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeConstantPolicy(maxAttempts: 5, seconds: 0.25)

        let attempt = try await RetryExecutor.deliver(
            entry: entry,
            transport: transport,
            policy: policy,
            classifier: Self.metadataClassifier,
            sleep: sleep
        )

        #expect(attempt.outcome == .success)
        #expect(attempt.attempts == 3)
        let calls = await transport.recordedCalls()
        #expect(calls.count == 3)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.25, 0.25])
    }
}

// MARK: - Retryable exhausted

extension RetryExecutorTests {
    @Test(
        "retryable outcome on every attempt exhausts maxAttempts and skips the post-final sleep",
        .tags(.lgr2, .lgr3, .lgr5)
    )
    func retryableBudgetExhausted() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse())
        ])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeConstantPolicy(maxAttempts: 3, seconds: 0.1)

        let attempt = try await RetryExecutor.deliver(
            entry: entry,
            transport: transport,
            policy: policy,
            classifier: Self.metadataClassifier,
            sleep: sleep
        )

        #expect(attempt.outcome == .retryable(reason: .transportRejected))
        #expect(attempt.attempts == 3)
        let calls = await transport.recordedCalls()
        #expect(calls.count == 3)
        // Two sleeps between three attempts; never a third sleep
        // after the final attempt.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.1, 0.1])
    }
}

// MARK: - Terminal stops immediately

extension RetryExecutorTests {
    @Test(
        "terminal outcome on the first attempt stops immediately with no retry and no sleep",
        .tags(.lgr2, .lgr3, .lgr5)
    )
    func terminalNoRetry() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .response(Self.makeTerminalResponse()),
            // Extra responses present but the engine MUST NOT touch
            // them after a terminal classification on attempt 1.
            .response(Self.makeSuccessResponse())
        ])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeConstantPolicy(maxAttempts: 4, seconds: 0.1)

        let attempt = try await RetryExecutor.deliver(
            entry: entry,
            transport: transport,
            policy: policy,
            classifier: Self.metadataClassifier,
            sleep: sleep
        )

        #expect(attempt.outcome == .terminal(reason: .transportRejected))
        #expect(attempt.attempts == 1)
        let calls = await transport.recordedCalls()
        #expect(calls.count == 1)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - Thrown transport classified retryable

extension RetryExecutorTests {
    @Test(
        "transport throw classified as retryable participates in the retry budget like any other retryable response",
        .tags(.lgr2, .lgr3, .lgr5)
    )
    func thrownTransportClassifiedRetryable() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .failure(StubTransportError(kind: .connectionFailure)),
            .failure(StubTransportError(kind: .connectionFailure)),
            .response(Self.makeSuccessResponse())
        ])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeConstantPolicy(maxAttempts: 5, seconds: 0.05)

        let attempt = try await RetryExecutor.deliver(
            entry: entry,
            transport: transport,
            policy: policy,
            classifier: Self.metadataClassifier,
            sleep: sleep
        )

        #expect(attempt.outcome == .success)
        #expect(attempt.attempts == 3)
        let calls = await transport.recordedCalls()
        #expect(calls.count == 3)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.05, 0.05])
    }
}

// MARK: - Kind-inspecting classifier

extension RetryExecutorTests {
    @Test(
        "kind-inspecting classifier maps permanentFailure to terminal and stops immediately",
        .tags(.lgr2, .lgr3, .lgr5)
    )
    func kindInspectingClassifierMapsPermanentFailureToTerminal() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .failure(StubTransportError(kind: .permanentFailure)),
            // The executor must never reach this slot once the
            // permanent failure has been classified as terminal.
            .response(Self.makeSuccessResponse())
        ])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeConstantPolicy(maxAttempts: 4, seconds: 0.05)
        let classifier:
            @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult = {
                switch $0 {
                case .success:
                    return .success
                case let .failure(error):
                    guard let stub = error as? StubTransportError else {
                        return .retryable(reason: .transportRejected)
                    }
                    switch stub.kind {
                    case .connectionFailure:
                        return .retryable(reason: .transportRejected)
                    case .permanentFailure:
                        return .terminal(reason: .transportRejected)
                    }
                }
            }

        let attempt = try await RetryExecutor.deliver(
            entry: entry,
            transport: transport,
            policy: policy,
            classifier: classifier,
            sleep: sleep
        )

        #expect(attempt.outcome == .terminal(reason: .transportRejected))
        #expect(attempt.attempts == 1)
        let calls = await transport.recordedCalls()
        #expect(calls.count == 1)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - Backoff progression

extension RetryExecutorTests {
    @Test(
        "constant backoff produces the same seconds value before every retry",
        .tags(.lgr3)
    )
    func constantBackoffSequence() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse())
        ])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeConstantPolicy(maxAttempts: 4, seconds: 0.3)

        _ = try await RetryExecutor.deliver(
            entry: entry,
            transport: transport,
            policy: policy,
            classifier: Self.metadataClassifier,
            sleep: sleep
        )

        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.3, 0.3, 0.3])
    }

    @Test(
        "exponential backoff multiplies by `multiplier` each retry and clamps at `capSeconds`",
        .tags(.lgr3)
    )
    func exponentialBackoffSequence() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse())
        ])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeExponentialPolicy(
            maxAttempts: 5,
            initialSeconds: 0.1,
            multiplier: 2.0,
            capSeconds: 0.5
        )

        _ = try await RetryExecutor.deliver(
            entry: entry,
            transport: transport,
            policy: policy,
            classifier: Self.metadataClassifier,
            sleep: sleep
        )

        // Progression: 0.1 → 0.2 → 0.4 → clamp to 0.5.
        // Four sleeps total before the fifth (final) attempt.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.1, 0.2, 0.4, 0.5])
    }
}

// MARK: - Delay-calculator failure stays distinct from sleep-injector failure

extension RetryExecutorTests {
    @Test(
        "delayCalculator failure surfaces .invalidRetryDelay carrying the underlying error, not .sleepInterrupted",
        .tags(.lgr3, .lgr7)
    )
    func delayCalculatorFailureSurfacesInvalidRetryDelay() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .response(Self.makeRetryableResponse()),
            // Extra slot present but unreachable: the delay
            // calculator throws between attempts 1 and 2 so the
            // executor never reaches attempt 2.
            .response(Self.makeSuccessResponse())
        ])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)
        let policy = try Self.makeConstantPolicy(maxAttempts: 4, seconds: 0.05)

        var caught: ExecutionLoopError?
        do {
            // Test-only delay calculator seam: refuses every
            // attempt count so we drive the `.invalidRetryDelay`
            // branch the `RemoteRetryPolicy.make(_:_:)` factory
            // makes unreachable from the public API.
            _ = try await RetryExecutor.deliver(
                entry: entry,
                transport: transport,
                policy: policy,
                classifier: Self.metadataClassifier,
                sleep: sleep,
                delayCalculator: { (_: RemoteRetryPolicy, _: Int) throws(RemoteDeliveryError) -> Double in
                    throw .invalidRetryPolicy
                }
            )
            Issue.record("expected .invalidRetryDelay")
        } catch {
            caught = error
        }
        // The diagnostic carries the underlying policy-space error
        // verbatim and is NOT collapsed into `.sleepInterrupted`.
        #expect(caught == .invalidRetryDelay(.invalidRetryPolicy))
        // The executor stopped before the second transport call
        // and before any sleep could be recorded.
        let calls = await transport.recordedCalls()
        #expect(calls.count == 1)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - Sleep injector failure

extension RetryExecutorTests {
    @Test(
        "sleep injector failure between two retryable attempts surfaces .sleepInterrupted and stops",
        .tags(.lgr3, .lgr7)
    )
    func sleepInjectorFailureSurfaces() async throws {
        let entry = Self.makeEntry()
        let transport = StubRemoteTransport(outcomes: [
            .response(Self.makeRetryableResponse()),
            .response(Self.makeRetryableResponse()),
            .response(Self.makeSuccessResponse())
        ])
        let recorder = SleepRecorder()
        // Throw on the first sleep (between attempts 1 and 2) so the
        // executor cannot reach attempt 2 at all.
        let sleep = RecordingSleep.make(
            recorder: recorder, throwOnSleepIndex: 0
        )
        let policy = try Self.makeConstantPolicy(maxAttempts: 4, seconds: 0.05)

        var caught: ExecutionLoopError?
        do {
            _ = try await RetryExecutor.deliver(
                entry: entry,
                transport: transport,
                policy: policy,
                classifier: Self.metadataClassifier,
                sleep: sleep
            )
            Issue.record("expected .sleepInterrupted")
        } catch {
            caught = error
        }
        #expect(caught == .sleepInterrupted)
        // Guard the opposite taxonomy direction: a sleep-injector
        // failure must not collapse into the policy-space
        // `.invalidRetryDelay` diagnostic.
        #expect(caught != .invalidRetryDelay(.invalidRetryPolicy))
        // Exactly one transport call was made (the first attempt);
        // the executor never reached attempt 2.
        let calls = await transport.recordedCalls()
        #expect(calls.count == 1)
        // The sleep was recorded once before the injector threw,
        // carrying the constant-backoff value the policy returned
        // for the just-completed retryable attempt.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.05])
    }
}
