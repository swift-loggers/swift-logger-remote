// swiftlint:disable file_length - Batch-round dispatch is the engine's sole transport-side delivery primitive; keeping every contract clause (round-by-round shape, active-set shrink, per-entry attempt budget, count-mismatch fail-closed, whole-batch throw classifier, ACK lifecycle) in one suite preserves audit traceability across the 0.1.0 contract lock.
import Foundation
import Testing
@testable import LoggerRemote

/// Coverage for the batch-round dispatch contract on
/// ``RemoteTransport.sendBatch(_:)``: one batch call per round,
/// active-set shrinking across rounds, per-entry attempt
/// accounting, ordered per-entry outcomes, count-mismatch
/// fail-closed, and the pass-wide ACK lifecycle.
@Suite("Batch-round dispatch contract")
struct BatchDispatchTests {
    private static func uniqueDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("LoggerRemoteTests")
            .appendingPathComponent(UUID().uuidString)
    }

    private static func cleanup(_ directory: URL) {
        try? FileManager.default.removeItem(at: directory)
    }

    private static func makeExportDirectory() throws -> URL {
        let directory = uniqueDirectory()
        try FileManager.default.createDirectory(
            at: directory, withIntermediateDirectories: true
        )
        return directory
    }

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

    static func makeEngine(
        queue: DurableRemoteQueue,
        exportDirectory: URL,
        transport: any RemoteTransport,
        recorder: SleepRecorder,
        batchPolicy: RemoteBatchPolicy? = nil,
        retryPolicy: RemoteRetryPolicy? = nil
    ) throws -> RemoteEngine {
        try RemoteEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            batchPolicy: batchPolicy ?? Self.batchPolicy(),
            retryPolicy: retryPolicy ?? Self.retryPolicy(),
            sleep: RecordingSleep.make(recorder: recorder)
        )
    }
}

// MARK: - One batch call for N entries

extension BatchDispatchTests {
    @Test(
        "round 1 dispatches every entry in the group in one batch call when all resolve",
        .tags(.lgr5, .lgr6)
    )
    func roundOneSingleBatchCallForResolvedGroup() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let payloads = (1 ... 5).map { Data([UInt8($0)]) }
        for (index, payload) in payloads.enumerated() {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(index + 1), payload: payload
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = StubRemoteTransport(
            outcomes: Array(repeating: .response(Self.successResponse()), count: 5),
            classifier: Self.metadataClassifier
        )
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()
        let batchCalls = await transport.recordedBatchCalls()
        // Exactly one batch call dispatched all five items in
        // input order.
        #expect(batchCalls.count == 1)
        #expect(batchCalls[0].items.count == 5)
        #expect(batchCalls[0].items.map(\.payloadBytes) == payloads)
        // Pass-wide tally: all `.success`, ack ran, retained
        // export artifact removed.
        #expect(summary.attemptedBatches == 1)
        #expect(summary.attemptedEntries == 5)
        #expect(summary.succeededEntries == 5)
        #expect(summary.acknowledgement == .removedDeliveredBytes)
        // No retry sleeps because no entry stayed retryable.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - Active-set shrink across rounds

extension BatchDispatchTests {
    @Test(
        "round 2+ contains only the retryables from round 1 in their original drained-export order",
        .tags(.lgr3, .lgr5, .lgr10)
    )
    func roundTwoContainsOnlyRetryablesInOrder() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let payloads: [Data] = [
            Data([0x01]), Data([0x02]), Data([0x03]),
            Data([0x04]), Data([0x05])
        ]
        for (index, payload) in payloads.enumerated() {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(index + 1), payload: payload
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        // Round 1 outcomes for items [1..5]:
        //   1 → success, 2 → retryable, 3 → terminal,
        //   4 → retryable, 5 → success
        // Round 2 outcomes for active [2, 4]:
        //   2 → success, 4 → success
        let transport = StubRemoteTransport(
            outcomes: [
                .response(Self.successResponse()),
                .response(Self.retryableResponse()),
                .response(Self.terminalResponse()),
                .response(Self.retryableResponse()),
                .response(Self.successResponse()),
                .response(Self.successResponse()),
                .response(Self.successResponse())
            ],
            classifier: Self.metadataClassifier
        )
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()
        let batchCalls = await transport.recordedBatchCalls()
        #expect(batchCalls.count == 2)
        // Round 1: full group in drained order.
        #expect(batchCalls[0].items.map(\.payloadBytes) == payloads)
        // Round 2: only the entries whose round 1 outcome was
        // retryable, preserving their original drained-export
        // order (entry 2 before entry 4).
        #expect(batchCalls[1].items.map(\.payloadBytes) == [
            Data([0x02]), Data([0x04])
        ])
        // Pass-wide summary: 4 success + 1 terminal, ack ran.
        #expect(summary.attemptedEntries == 5)
        #expect(summary.succeededEntries == 4)
        #expect(summary.terminalEntries == 1)
        #expect(summary.retryableEntries == 0)
        #expect(summary.acknowledgement == .removedDeliveredBytes)
        // Exactly one sleep between rounds.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.05])
    }
}

// MARK: - Per-entry attempt counts across rounds

extension BatchDispatchTests {
    @Test(
        "per-entry attempt counts equal the number of rounds each entry was active in",
        .tags(.lgr3, .lgr5)
    )
    func perEntryAttemptCountsReflectRoundsActive() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0xA1])
        ))
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 2, payload: Data([0xA2])
        ))
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 3, payload: Data([0xA3])
        ))
        try await queue.flush()
        let destination = Self.uniqueDirectory().appendingPathComponent("export.ndjson")
        defer { Self.cleanup(destination.deletingLastPathComponent()) }
        try FileManager.default.createDirectory(
            at: destination.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        // Outcome script across rounds (maxAttempts = 3):
        //   Round 1: [success(1), retryable(2), retryable(3)]
        //   Round 2: [success(2), retryable(3)]
        //   Round 3: [success(3)]
        let transport = StubRemoteTransport(
            outcomes: [
                .response(Self.successResponse()),
                .response(Self.retryableResponse()),
                .response(Self.retryableResponse()),
                .response(Self.successResponse()),
                .response(Self.retryableResponse()),
                .response(Self.successResponse())
            ],
            classifier: Self.metadataClassifier
        )
        let recorder = SleepRecorder()

        let attempts = try await ExecutionLoop.runOnce(
            queue: queue,
            exportURL: destination,
            batchPolicy: Self.batchPolicy(),
            retryPolicy: Self.retryPolicy(maxAttempts: 3, seconds: 0.05),
            transport: transport,
            sleep: RecordingSleep.make(recorder: recorder)
        )
        // Returned attempts preserve drained-export order
        // (entries 1, 2, 3 in that order) regardless of
        // round-shrink dynamics.
        #expect(attempts.map(\.entry.identifier) == [1, 2, 3])
        // Entry 1 resolved on round 1.
        #expect(attempts[0].attempts == 1)
        #expect(attempts[0].outcome == .success)
        // Entry 2 resolved on round 2.
        #expect(attempts[1].attempts == 2)
        #expect(attempts[1].outcome == .success)
        // Entry 3 resolved on round 3.
        #expect(attempts[2].attempts == 3)
        #expect(attempts[2].outcome == .success)
        // Two sleeps between three rounds.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps == [0.05, 0.05])
    }
}

// MARK: - Response count mismatch fails closed

extension BatchDispatchTests {
    @Test(
        "sendBatch returning fewer results than items surfaces .transportBatchInvalid fail-closed",
        .tags(.lgr5, .lgr7)
    )
    func responseCountMismatchSurfacesTransportBatchInvalid() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let payloads = (1 ... 3).map { Data([UInt8($0)]) }
        for (index, payload) in payloads.enumerated() {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(index + 1), payload: payload
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        // Stub returns 2 results for an input of 3 items on the
        // first call → engine fails closed.
        let transport = StubRemoteTransport(
            outcomes: [
                .response(Self.successResponse()),
                .response(Self.successResponse()),
                .response(Self.successResponse())
            ],
            classifier: Self.metadataClassifier,
            dropResultsPerCall: [0: 1]
        )
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        var caught: RemoteEngineError?
        do throws(RemoteEngineError) {
            _ = try await engine.flush()
            Issue.record("expected .transportBatchInvalid")
        } catch {
            caught = error
        }
        #expect(caught == .transportBatchInvalid(expected: 3, actual: 2))
        // The queue keeps the outstanding-batch boundary: no ack
        // was issued and the retained export artifact stays on
        // disk for the next flush.
        let outstandingAfter = await queue.currentOutstandingBatch()
        #expect(outstandingAfter != nil)
        let leftoverFiles = try FileManager.default.contentsOfDirectory(
            at: exportDirectory, includingPropertiesForKeys: nil
        )
        #expect(leftoverFiles.count == 1)
    }
}

// MARK: - ACK / no-ACK lifecycle

extension BatchDispatchTests {
    @Test(
        "all entries .success or .terminal across the flush pass triggers acknowledge",
        .tags(.lgr6, .lgr11)
    )
    func passWideResolutionAcknowledges() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 2, payload: Data([0x02])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        // Round 1: [success(1), terminal(2)] — both resolved.
        let transport = StubRemoteTransport(
            outcomes: [
                .response(Self.successResponse()),
                .response(Self.terminalResponse())
            ],
            classifier: Self.metadataClassifier
        )
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()
        #expect(summary.acknowledgement == .removedDeliveredBytes)
        #expect(summary.succeededEntries == 1)
        #expect(summary.terminalEntries == 1)
        #expect(summary.retryableEntries == 0)
        // Acknowledge ran: queue clears outstanding boundary and
        // the export artifact is removed.
        let outstandingAfter = await queue.currentOutstandingBatch()
        #expect(outstandingAfter == nil)
        let leftoverFiles = try FileManager.default.contentsOfDirectory(
            at: exportDirectory, includingPropertiesForKeys: nil
        )
        #expect(leftoverFiles.isEmpty)
    }

    @Test(
        "any retryable across the flush pass blocks acknowledge and keeps outstanding-batch reuse",
        .tags(.lgr6, .lgr11)
    )
    func anyRetryableBlocksAcknowledge() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 2, payload: Data([0x02])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        // Outcome script: every round leaves entry 2 retryable
        // until the budget runs out. Round 1: [success, retryable];
        // Round 2: [retryable]; (maxAttempts=2, dispatcher stops).
        let transport = StubRemoteTransport(
            outcomes: [
                .response(Self.successResponse()),
                .response(Self.retryableResponse()),
                .response(Self.retryableResponse())
            ],
            classifier: Self.metadataClassifier
        )
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder,
            retryPolicy: Self.retryPolicy(maxAttempts: 2, seconds: 0.05)
        )

        let summary = try await engine.flush()
        #expect(summary.acknowledgement == .notAcknowledged)
        #expect(summary.succeededEntries == 1)
        #expect(summary.retryableEntries == 1)
        // No ack ran: queue still holds the outstanding-batch
        // boundary and the retained export artifact stays on
        // disk so the next `flush()` can reuse it through the
        // outstanding-batch path.
        let outstandingAfter = await queue.currentOutstandingBatch()
        #expect(outstandingAfter != nil)
        let leftoverFiles = try FileManager.default.contentsOfDirectory(
            at: exportDirectory, includingPropertiesForKeys: nil
        )
        #expect(leftoverFiles.count == 1)
    }
}

// MARK: - Per-item adapter fallback

extension BatchDispatchTests {
    /// Minimal single-event adapter that implements
    /// ``RemoteTransport.sendBatch(_:)`` by dispatching each item
    /// independently. Demonstrates that single-event sinks
    /// (Splunk HEC, Loki single-event, Datadog Logs HTTP intake)
    /// implement the sink-neutral batch primitive with a per-item
    /// loop inside `sendBatch` and rely on the engine for ACK and
    /// retry budget bookkeeping.
    private final actor PerItemAdapter: RemoteTransport {
        private let perItemDispatch:
            @Sendable (Data, [String: String]) async throws -> RemoteTransportResponse
        private(set) var observedBatchSizes: [Int] = []

        init(
            perItemDispatch: @escaping @Sendable (Data, [String: String]) async throws -> RemoteTransportResponse
        ) {
            self.perItemDispatch = perItemDispatch
        }

        func sendBatch(
            _ items: [RemoteTransportBatchItem]
        ) async throws -> [Result<RemoteTransportResponse, any Error>] {
            observedBatchSizes.append(items.count)
            var results: [Result<RemoteTransportResponse, any Error>] = []
            results.reserveCapacity(items.count)
            for item in items {
                do {
                    let response = try await perItemDispatch(
                        item.payloadBytes, item.payloadMetadata
                    )
                    results.append(.success(response))
                } catch {
                    results.append(.failure(error))
                }
            }
            return results
        }

        func classify(
            _ result: Result<RemoteTransportResponse, any Error>
        ) async -> RemoteDeliveryResult {
            switch result {
            case .success:
                return .success
            case .failure:
                return .retryable(reason: .transportRejected)
            }
        }

        func recordedBatchSizes() -> [Int] {
            observedBatchSizes
        }
    }

    @Test(
        "single-event adapter dispatching per item inside sendBatch sees engine ack/remove on all-success",
        .tags(.lgr5, .lgr6, .lgr11)
    )
    func singleEventAdapterPerItemDispatchAcks() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let payloads = (1 ... 4).map { Data([UInt8($0)]) }
        for (index, payload) in payloads.enumerated() {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(index + 1), payload: payload
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = PerItemAdapter(perItemDispatch: { _, _ in
            RemoteTransportResponse(responseBytes: Data())
        })
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()
        #expect(summary.acknowledgement == .removedDeliveredBytes)
        #expect(summary.succeededEntries == 4)
        // The adapter saw exactly one batch call of size 4 — the
        // engine's batch boundary handed the whole group in one
        // `sendBatch` invocation, even though the adapter
        // dispatched per item internally.
        let observed = await transport.recordedBatchSizes()
        #expect(observed == [4])
        // No retry sleeps; everything resolved on round 1.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}

// MARK: - Whole-batch sendBatch throw classifier path

extension BatchDispatchTests {
    /// Sendable error the whole-batch throw test injects through
    /// the stub's `onCall` hook. The `id` byte makes the value
    /// distinguishable so the test can verify the engine handed
    /// this exact value — not just any error of this type — to
    /// every per-item `classify(_:)` call in the round.
    private struct WholeBatchSendThrow: Error, Sendable, Equatable {
        let id: UInt8
    }

    /// Sendable-safe ordered recorder so the test's `@Sendable`
    /// classifier closure can capture the per-call `Result` it
    /// received and the enclosing test can assert against the
    /// flat per-item sequence (count, error identity, ordering).
    private final class ClassifyRecorder: @unchecked Sendable {
        private let lock = NSLock()
        private var values: [Result<RemoteTransportResponse, any Error>] = []

        func append(_ value: Result<RemoteTransportResponse, any Error>) {
            lock.lock()
            defer { lock.unlock() }
            values.append(value)
        }

        var snapshot: [Result<RemoteTransportResponse, any Error>] {
            lock.lock()
            defer { lock.unlock() }
            return values
        }
    }

    @Test(
        "whole-batch sendBatch throw routes the same error through classify per active item in input order",
        .tags(.lgr5, .lgr7)
    )
    func wholeBatchSendThrowRoutesSameErrorPerActiveItemInOrder() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let payloads = (1 ... 3).map { Data([UInt8($0)]) }
        for (index, payload) in payloads.enumerated() {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(index + 1), payload: payload
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }

        let recorder = ClassifyRecorder()
        let injectedError = WholeBatchSendThrow(id: 0x42)
        let classifier:
            @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult = { result in
                recorder.append(result)
                // Every classified item lands as `.terminal` so
                // round 1 fully resolves; the dispatcher returns
                // without further rounds.
                return .terminal(reason: .transportRejected)
            }
        let transport = StubRemoteTransport(
            outcomes: [],
            classifier: classifier,
            onCall: { _ in
                throw injectedError
            }
        )
        let sleepRecorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: sleepRecorder
        )

        let summary = try await engine.flush()
        #expect(summary.terminalEntries == 3)
        // `sendBatch` was called once; on throw the engine routes
        // each of the 3 active items through `classify(_:)` with
        // the same `.failure(error)` value in input order.
        let observed = recorder.snapshot
        #expect(observed.count == 3)
        for value in observed {
            switch value {
            case .success:
                Issue.record("expected .failure(WholeBatchSendThrow)")
            case let .failure(error):
                let cast = error as? WholeBatchSendThrow
                #expect(cast?.id == injectedError.id)
            }
        }
        // The stub recorded exactly one `sendBatch` call that
        // carried all three items in drained-export order; the
        // classify recordings above mirror that input order.
        let calls = await transport.recordedBatchCalls()
        #expect(calls.count == 1)
        #expect(calls[0].items.map(\.payloadBytes) == payloads)
    }
}

// MARK: - First-round active-set predicate

extension BatchDispatchTests {
    @Test(
        "round 1 dispatches every entry in drained-export order because the initial outcome state selects all",
        .tags(.lgr3, .lgr5, .lgr10)
    )
    func firstRoundActiveSetIncludesEveryEntry() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let payloads = (1 ... 4).map { Data([UInt8($0)]) }
        for (index, payload) in payloads.enumerated() {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(index + 1), payload: payload
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        // Round 1 outcomes: every entry resolves on first call
        // (`.success`). The first round MUST dispatch every entry
        // even though none of them carries a previous classification
        // — the active-set predicate selects `nil`-outcome entries
        // on the initial iteration.
        let transport = StubRemoteTransport(
            outcomes: Array(repeating: .response(Self.successResponse()), count: 4),
            classifier: Self.metadataClassifier
        )
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()
        let batchCalls = await transport.recordedBatchCalls()
        // Exactly one batch call for the only round; the call's
        // item array is the full group in drained-export order.
        #expect(batchCalls.count == 1)
        #expect(batchCalls[0].items.map(\.payloadBytes) == payloads)
        #expect(summary.attemptedEntries == 4)
        #expect(summary.succeededEntries == 4)
        #expect(summary.acknowledgement == .removedDeliveredBytes)
        // No sleeps because no round 2 was needed.
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)
    }
}
