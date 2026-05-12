import Foundation
import Testing
@testable import LoggerRemote

@Suite("ExecutionLoop end-to-end retry pass")
struct ExecutionLoopTests {
    private static func uniqueDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("LoggerRemoteTests")
            .appendingPathComponent(UUID().uuidString)
    }

    private static func cleanup(_ directory: URL) {
        try? FileManager.default.removeItem(at: directory)
    }

    private static func makeExportURL() throws -> (url: URL, parent: URL) {
        let parent = uniqueDirectory()
        try FileManager.default.createDirectory(
            at: parent, withIntermediateDirectories: true
        )
        return (parent.appendingPathComponent("export.ndjson"), parent)
    }
}

// MARK: - Test helpers

extension ExecutionLoopTests {
    /// Mirrors the per-entry retry classifier used by the
    /// `RetryExecutor` suite so the integration tests share the
    /// same sink-owned classification shape.
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

    static func successResponse() -> RemoteTransportResponse {
        RemoteTransportResponse(responseBytes: Data())
    }

    static func retryableResponse() -> RemoteTransportResponse {
        RemoteTransportResponse(
            responseBytes: Data(),
            responseMetadata: ["__test_class": "retryable"]
        )
    }

    static func terminalResponse() -> RemoteTransportResponse {
        RemoteTransportResponse(
            responseBytes: Data(),
            responseMetadata: ["__test_class": "terminal"]
        )
    }

    static func batchPolicy(
        maxEntryCount: Int = 64, maxByteCount: Int = 4096
    ) throws -> RemoteBatchPolicy {
        try RemoteBatchPolicy.make(
            maxEntryCount: maxEntryCount, maxByteCount: maxByteCount
        )
    }

    static func retryPolicy(
        maxAttempts: Int = 3, seconds: Double = 0.05
    ) throws -> RemoteRetryPolicy {
        try RemoteRetryPolicy.make(
            maxAttempts: maxAttempts, backoff: .constant(seconds: seconds)
        )
    }

    /// Builds a `StubRemoteTransport` with the shared
    /// ``metadataClassifier`` already wired up. The execution-loop
    /// suite uses this everywhere; per-test custom classifiers are
    /// not needed for the integration cases this file covers.
    static func makeStubTransport(
        outcomes: [StubTransportOutcome]
    ) -> StubRemoteTransport {
        StubRemoteTransport(outcomes: outcomes, classifier: metadataClassifier)
    }
}

// MARK: - Empty queue

extension ExecutionLoopTests {
    @Test(
        "two consecutive empty-queue passes both return [] without transport calls or sleeps",
        .tags(.lgr10, .lgr11)
    )
    func emptyQueueDoesNotWedgePollingCaller() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let firstDestination = try Self.makeExportURL()
        defer { Self.cleanup(firstDestination.parent) }
        let secondDestination = try Self.makeExportURL()
        defer { Self.cleanup(secondDestination.parent) }

        let transport = Self.makeStubTransport(outcomes: [])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)

        let firstAttempts = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: firstDestination.url,
            batchPolicy: Self.batchPolicy(),
            retryPolicy: Self.retryPolicy(),
            transport: transport,
            sleep: sleep
        )

        #expect(firstAttempts.isEmpty)

        // A polling caller must be able to run a second pass on a
        // still-empty queue without tripping
        // `.batchAlreadyOutstanding` from the prior empty drain.
        let secondAttempts = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: secondDestination.url,
            batchPolicy: Self.batchPolicy(),
            retryPolicy: Self.retryPolicy(),
            transport: transport,
            sleep: sleep
        )

        #expect(secondAttempts.isEmpty)
        let calls = await transport.recordedCalls()
        #expect(calls.isEmpty)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }

    @Test(
        "empty drain short-circuits on byteCount == 0 before reading the export file",
        .tags(.lgr10, .lgr11)
    )
    func emptyDrainShortCircuitsBeforeExportRead() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let firstDestination = try Self.makeExportURL()
        defer { Self.cleanup(firstDestination.parent) }
        let secondDestination = try Self.makeExportURL()
        defer { Self.cleanup(secondDestination.parent) }

        let transport = Self.makeStubTransport(outcomes: [])
        let recorder = SleepRecorder()
        let sleep = RecordingSleep.make(recorder: recorder)

        // Engine-internal `afterDrain` seam removes the zero-byte
        // export artifact between drain and the would-be parse
        // step. If the loop reads the export file before keying
        // off `batch.byteCount == 0`, the recovery step would
        // throw `.exportFileReadFailed`. Keying off the queue's
        // authoritative zero-byte signal must instead skip the
        // read entirely and release the held boundary so the
        // polling caller never sees `.batchAlreadyOutstanding`.
        let firstAttempts = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: firstDestination.url,
            batchPolicy: Self.batchPolicy(),
            retryPolicy: Self.retryPolicy(),
            transport: transport,
            sleep: sleep,
            afterDrain: { batch in
                #expect(batch.byteCount == 0)
                try? FileManager.default.removeItem(at: batch.exportURL)
            }
        )

        #expect(firstAttempts.isEmpty)

        // The empty-drain release cleared the outstanding-batch
        // boundary even though the export file was gone, so a
        // second pass on the still-empty queue is admitted
        // without tripping `.batchAlreadyOutstanding`.
        let secondAttempts = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: secondDestination.url,
            batchPolicy: Self.batchPolicy(),
            retryPolicy: Self.retryPolicy(),
            transport: transport,
            sleep: sleep
        )

        #expect(secondAttempts.isEmpty)
        let calls = await transport.recordedCalls()
        #expect(calls.isEmpty)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - Single batch, first-attempt success

extension ExecutionLoopTests {
    @Test(
        "single batch first-attempt success: one transport call per entry, accepted ordering preserved",
        .tags(.lgr3, .lgr4, .lgr5, .lgr10)
    )
    func singleBatchAllSucceed() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let entries: [RemoteDeliveryEntry] = [
            RemoteDeliveryEntry(
                identifier: 10, payload: Data([0x10]),
                metadata: ["route": "alpha"]
            ),
            RemoteDeliveryEntry(
                identifier: 20, payload: Data([0x20]),
                metadata: ["route": "beta"]
            ),
            RemoteDeliveryEntry(
                identifier: 30, payload: Data([0x30]),
                metadata: ["route": "gamma"]
            )
        ]
        for entry in entries {
            try await queue.enqueue(entry)
        }
        try await queue.flush()
        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }

        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.successResponse()),
            .response(Self.successResponse()),
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()

        let attempts = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: destination.url,
            batchPolicy: Self.batchPolicy(),
            retryPolicy: Self.retryPolicy(),
            transport: transport,
            sleep: RecordingSleep.make(recorder: recorder)
        )

        #expect(attempts.count == 3)
        #expect(attempts.map(\.entry.identifier) == [10, 20, 30])
        #expect(attempts.allSatisfy { $0.outcome == .success })
        #expect(attempts.allSatisfy { $0.attempts == 1 })
        let calls = await transport.recordedCalls()
        #expect(calls.map(\.payloadBytes) == entries.map(\.payload))
        // Sink-owned metadata propagates byte-for-byte from the
        // entry to the transport call; the engine does not interpret
        // metadata keys.
        #expect(calls.map(\.payloadMetadata) == entries.map(\.metadata))
        // No retries on a first-attempt-success path → no sleeps.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - Mixed outcomes within one batch

extension ExecutionLoopTests {
    @Test(
        "mixed entries: per-entry budgets are independent and accepted ordering survives",
        .tags(.lgr3, .lgr4, .lgr5, .lgr10)
    )
    func mixedEntriesInOneBatch() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let entries: [RemoteDeliveryEntry] = [
            RemoteDeliveryEntry(identifier: 1, payload: Data([0xA1])),
            RemoteDeliveryEntry(identifier: 2, payload: Data([0xA2])),
            RemoteDeliveryEntry(identifier: 3, payload: Data([0xA3]))
        ]
        for entry in entries {
            try await queue.enqueue(entry)
        }
        try await queue.flush()
        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }

        // Per-entry transport script (per-entry retry budgets are
        // independent in this PR):
        //   entry 1: success on first attempt           (1 call)
        //   entry 2: retryable → retryable → success    (3 calls)
        //   entry 3: terminal on first attempt          (1 call)
        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.successResponse()),
            .response(Self.retryableResponse()),
            .response(Self.retryableResponse()),
            .response(Self.successResponse()),
            .response(Self.terminalResponse())
        ])
        let recorder = SleepRecorder()

        let attempts = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: destination.url,
            batchPolicy: Self.batchPolicy(),
            retryPolicy: Self.retryPolicy(maxAttempts: 4, seconds: 0.05),
            transport: transport,
            sleep: RecordingSleep.make(recorder: recorder)
        )

        #expect(attempts.count == 3)
        #expect(attempts.map(\.entry.identifier) == [1, 2, 3])
        #expect(attempts[0].outcome == .success)
        #expect(attempts[0].attempts == 1)
        #expect(attempts[1].outcome == .success)
        #expect(attempts[1].attempts == 3)
        #expect(attempts[2].outcome == .terminal(reason: .transportRejected))
        #expect(attempts[2].attempts == 1)
        let calls = await transport.recordedCalls()
        #expect(calls.count == 5)
        // Per-entry dispatch order on the wire: entry 1 succeeds on
        // its single call (one `0xA1`), entry 2 retries twice
        // before success (three `0xA2`s in a row), entry 3
        // terminates on its single call (one `0xA3`). The engine
        // never aggregates entry bytes; each call carries exactly
        // one entry's payload.
        #expect(calls.map(\.payloadBytes) == [
            Data([0xA1]), Data([0xA2]), Data([0xA2]), Data([0xA2]), Data([0xA3])
        ])
        // Only entry 2 produced retry sleeps (two of them); entries
        // 1 and 3 stopped on their first attempt.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.05, 0.05])
    }
}

// MARK: - Multi-batch traversal

extension ExecutionLoopTests {
    @Test(
        "multi-batch run: every entry across every batch produces an ordered attempt",
        .tags(.lgr3, .lgr4, .lgr5, .lgr10)
    )
    func multiBatchTraversal() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        // Five entries, batch policy that splits into 2 + 2 + 1.
        let entries: [RemoteDeliveryEntry] = (1 ... 5).map { id in
            RemoteDeliveryEntry(
                identifier: UInt64(id), payload: Data([UInt8(id)])
            )
        }
        for entry in entries {
            try await queue.enqueue(entry)
        }
        try await queue.flush()
        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }

        let transport = Self.makeStubTransport(
            outcomes: Array(
                repeating: .response(Self.successResponse()), count: 5
            )
        )
        let recorder = SleepRecorder()

        let attempts = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: destination.url,
            batchPolicy: Self.batchPolicy(maxEntryCount: 2),
            retryPolicy: Self.retryPolicy(),
            transport: transport,
            sleep: RecordingSleep.make(recorder: recorder)
        )

        // Three batches of size 2, 2, 1 — but `runOnce` returns the
        // ordered per-entry attempts, not the batch boundaries.
        #expect(attempts.count == 5)
        #expect(attempts.map(\.entry.identifier) == [1, 2, 3, 4, 5])
        #expect(attempts.allSatisfy { $0.outcome == .success })
        let calls = await transport.recordedCalls()
        #expect(calls.count == 5)
        #expect(calls.map(\.payloadBytes) == entries.map(\.payload))
        // Every entry succeeded on its first attempt across all
        // three batches, so the loop must never schedule backoff —
        // the sleep injector was not invoked.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - runOnce does not advance acknowledgement boundary

extension ExecutionLoopTests {
    @Test(
        "runOnce leaves the outstanding-batch state held; destructive removal is the engine flush's concern",
        .tags(.lgr10, .lgr11)
    )
    func runOnceDoesNotAcknowledge() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        try await queue.flush()
        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }

        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()

        _ = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: destination.url,
            batchPolicy: Self.batchPolicy(),
            retryPolicy: Self.retryPolicy(),
            transport: transport,
            sleep: RecordingSleep.make(recorder: recorder)
        )

        // A second drain before acknowledge must surface
        // `.batchAlreadyOutstanding`. This proves `runOnce` did NOT
        // invoke `acknowledge()` on its own; the destructive-removal
        // boundary stays for the engine's `flush()` to consume.
        let secondDestination = try Self.makeExportURL()
        defer { Self.cleanup(secondDestination.parent) }
        var captured: DurableRemoteQueueError?
        do {
            _ = try await queue.drain(to: secondDestination.url)
            Issue.record("expected .batchAlreadyOutstanding")
        } catch {
            captured = error
        }
        #expect(captured == .batchAlreadyOutstanding)
        // The export artifact produced by the first drain must
        // still be on disk: `runOnce` never removes the held
        // outstanding-batch file, so the engine's
        // acknowledgement-to-removal lifecycle (in
        // `RemoteEngine.flush()`) can read those bytes back
        // through the outstanding-reuse path.
        #expect(FileManager.default.fileExists(atPath: destination.url.path))
    }
}
