// swiftlint:disable file_length - LGR-6 / LGR-11 lifecycle-closure test inventory kept in a single file for traceability; the per-flush ack-decision, outstanding-reuse, and error-mapping cases share helper fixtures and read better adjacent than split across files.

import Foundation
import LoggerFilePersistence
import Testing
@testable import LoggerRemote

@Suite("RemoteEngine public flush lifecycle")
struct RemoteEngineTests {
    private static func uniqueDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("LoggerRemoteTests")
            .appendingPathComponent(UUID().uuidString)
    }

    private static func cleanup(_ directory: URL) {
        try? FileManager.default.removeItem(at: directory)
    }

    /// Builds a fresh directory the engine can use as its
    /// per-flush export-scratch area. Each call returns a unique
    /// directory so tests stay isolated.
    static func makeExportDirectory() throws -> URL {
        let directory = uniqueDirectory()
        try FileManager.default.createDirectory(
            at: directory, withIntermediateDirectories: true
        )
        return directory
    }
}

// MARK: - Test fixtures

extension RemoteEngineTests {
    /// Same metadata-keyed classifier the retry-executor and
    /// execution-loop suites use: `__test_class` on the response
    /// metadata selects `.success` / `.retryable` / `.terminal`,
    /// and a thrown transport error defaults to `.retryable`.
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

    static func makeStubTransport(
        outcomes: [StubTransportOutcome]
    ) -> StubRemoteTransport {
        StubRemoteTransport(outcomes: outcomes, classifier: metadataClassifier)
    }

    /// Builds an engine with a deterministic recording sleep so
    /// tests assert backoff behavior without burning wall time.
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

    /// Async seam-cleanup wrapper. Installs a test-only seam via
    /// `install`, runs `body`, and guarantees `cleanup` runs on
    /// both the success path and any throw before `body` returns —
    /// so a seam installed on an actor never lingers past the
    /// test even if an intermediate `try` throws unexpectedly.
    /// `defer` cannot host `await`, so this helper is the file's
    /// unified pattern for scoped async seam cleanup.
    /// Typed-throws over `Failure` so the caller's `catch error`
    /// sees the same concrete error type the body throws (e.g.
    /// `RemoteEngineError`).
    static func withSeamCleanup<R, Failure: Error>(
        install: () async -> Void,
        cleanup: () async -> Void,
        body: () async throws(Failure) -> R
    ) async throws(Failure) -> R {
        await install()
        do {
            let result = try await body()
            await cleanup()
            return result
        } catch {
            await cleanup()
            throw error
        }
    }
}

// MARK: - Empty queue flush

extension RemoteEngineTests {
    @Test(
        "flush on an empty queue acknowledges the empty boundary and reports zero attempts",
        .tags(.lgr6, .lgr10, .lgr11)
    )
    func emptyQueueFlush() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }

        let transport = Self.makeStubTransport(outcomes: [])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()

        #expect(summary == RemoteFlushSummary(
            attemptedBatches: 0,
            attemptedEntries: 0,
            succeededEntries: 0,
            terminalEntries: 0,
            retryableEntries: 0,
            acknowledgement: .emptyReleased
        ))
        let calls = await transport.recordedCalls()
        #expect(calls.isEmpty)
        let sleeps = await recorder.recordedSleeps()
        #expect(sleeps.isEmpty)

        // A second flush on the still-empty queue must succeed
        // because the first empty-drain release already cleared
        // the outstanding-batch boundary.
        let secondSummary = try await engine.flush()
        #expect(secondSummary.acknowledgement == .emptyReleased)
        #expect(secondSummary.attemptedEntries == 0)
        #expect(secondSummary.attemptedBatches == 0)
    }
}

// MARK: - All-success flush acknowledges

extension RemoteEngineTests {
    @Test(
        "flush with every entry succeeding acknowledges the drained batch",
        .tags(.lgr2, .lgr5, .lgr11)
    )
    func allSuccessFlushAcknowledges() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        for value: UInt8 in [0x01, 0x02, 0x03] {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(value), payload: Data([value])
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }

        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.successResponse()),
            .response(Self.successResponse()),
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()

        #expect(summary == RemoteFlushSummary(
            attemptedBatches: 1,
            attemptedEntries: 3,
            succeededEntries: 3,
            terminalEntries: 0,
            retryableEntries: 0,
            acknowledgement: .removedDeliveredBytes
        ))
        // After ack, a follow-up flush on the now-empty queue
        // returns zero attempts (the boundary cleared) and
        // reports the empty-release acknowledgement case.
        let secondSummary = try await engine.flush()
        #expect(secondSummary.attemptedEntries == 0)
        #expect(secondSummary.attemptedBatches == 0)
        #expect(secondSummary.acknowledgement == .emptyReleased)
    }
}

// MARK: - All-terminal flush acknowledges

extension RemoteEngineTests {
    @Test(
        "flush with every entry classified terminal acknowledges the drained batch",
        .tags(.lgr2, .lgr11)
    )
    func allTerminalFlushAcknowledges() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        for value: UInt8 in [0x10, 0x20] {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(value), payload: Data([value])
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }

        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.terminalResponse()),
            .response(Self.terminalResponse())
        ])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()

        #expect(summary.attemptedEntries == 2)
        #expect(summary.succeededEntries == 0)
        #expect(summary.terminalEntries == 2)
        #expect(summary.retryableEntries == 0)
        // Terminal entries are sink-decided permanent failure; the
        // engine treats them as resolved and acknowledges the
        // drained batch. Removing those bytes is forward progress
        // per the LGR-11 contract.
        #expect(summary.acknowledgement == .removedDeliveredBytes)
    }
}

// MARK: - Mixed success and terminal acknowledges

extension RemoteEngineTests {
    @Test(
        "flush mixing success and terminal entries still acknowledges (no retryable)",
        .tags(.lgr2, .lgr11)
    )
    func mixedSuccessTerminalAcknowledges() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        for value: UInt8 in [0xA0, 0xA1, 0xA2] {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(value), payload: Data([value])
            ))
        }
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }

        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.successResponse()),
            .response(Self.terminalResponse()),
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let summary = try await engine.flush()

        #expect(summary == RemoteFlushSummary(
            attemptedBatches: 1,
            attemptedEntries: 3,
            succeededEntries: 2,
            terminalEntries: 1,
            retryableEntries: 0,
            acknowledgement: .removedDeliveredBytes
        ))
    }
}

// MARK: - Retryable exhaustion holds boundary

extension RemoteEngineTests {
    // swiftlint:disable function_body_length
    // Reason: Two consecutive `engine.flush()` calls plus their
    // ordered transport-call / sleep / leftover-file assertions
    // belong in a single test because the second flush's outcome
    // is only meaningful in the context of the first flush's
    // retryable-exhausted state. Splitting the body would obscure
    // the outstanding-reuse contract this test pins end-to-end.

    @Test(
        "any retryable-exhausted entry holds the export and the next flush replays the retained export artifact",
        .tags(.lgr3, .lgr11)
    )
    func retryableExhaustedThenRetriedAcknowledges() async throws {
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

        // Per-entry budget = 1. First flush: entry 1 succeeds
        // (call 1), entry 2 is classified retryable on its only
        // attempt and the budget is exhausted (call 2). No ack.
        // Second flush: engine reuses the outstanding batch and
        // replays both queue payload bytes; both succeed
        // (calls 3 and 4) and the engine acknowledges.
        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.successResponse()),
            .response(Self.retryableResponse()),
            .response(Self.successResponse()),
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder,
            retryPolicy: Self.retryPolicy(maxAttempts: 1, seconds: 0.01)
        )

        let firstSummary = try await engine.flush()
        #expect(firstSummary.attemptedEntries == 2)
        #expect(firstSummary.succeededEntries == 1)
        #expect(firstSummary.terminalEntries == 0)
        #expect(firstSummary.retryableEntries == 1)
        #expect(firstSummary.acknowledgement == .notAcknowledged)

        // The outstanding-batch boundary must still be held: a
        // probe drain straight against the queue surfaces
        // `.batchAlreadyOutstanding`, proving the engine left
        // the queue's boundary intact.
        let probeDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(probeDirectory) }
        let probeURL = probeDirectory.appendingPathComponent("probe.ndjson")
        var captured: DurableRemoteQueueError?
        do {
            _ = try await queue.drain(to: probeURL)
            Issue.record("expected .batchAlreadyOutstanding")
        } catch {
            captured = error
        }
        #expect(captured == .batchAlreadyOutstanding)

        // Second flush must reuse the outstanding batch and its
        // already-written export file. The engine must NOT call
        // `queue.drain(to:)` again (which would surface
        // `.batchAlreadyOutstanding` and fail the flush); it
        // must replay the same two queue payload bytes against
        // the transport and acknowledge once both resolve.
        let secondSummary = try await engine.flush()
        #expect(secondSummary.attemptedEntries == 2)
        #expect(secondSummary.succeededEntries == 2)
        #expect(secondSummary.terminalEntries == 0)
        #expect(secondSummary.retryableEntries == 0)
        #expect(secondSummary.acknowledgement == .removedDeliveredBytes)

        // The transport saw the same payload bytes in both
        // flushes (entry 1 then entry 2, twice) — proving the
        // second flush replayed the retained export artifact
        // verbatim.
        let calls = await transport.recordedCalls()
        #expect(calls.count == 4)
        #expect(calls.map(\.payloadBytes) == [
            Data([0x01]), Data([0x02]), Data([0x01]), Data([0x02])
        ])

        // The export directory is empty after the resolved ack
        // (the engine removed the artifact when it acknowledged
        // on the second flush).
        let leftoverFiles = try FileManager.default.contentsOfDirectory(
            at: exportDirectory, includingPropertiesForKeys: nil
        )
        #expect(leftoverFiles.isEmpty)
    }

    // swiftlint:enable function_body_length
}

// MARK: - Enqueue-while-boundary-held ordering

extension RemoteEngineTests {
    @Test(
        "enqueue while a boundary is held defers the new entry until after the outstanding batch is acknowledged",
        .tags(.lgr10, .lgr11)
    )
    func enqueueWhileBoundaryHeldIsDeferredToNextFreshDrain() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0xA1])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }

        // Per-entry budget = 1. Transport script:
        //  call 1 → A retryable → engine holds boundary, no ack
        //  call 2 → A success (outstanding replay) → engine ack
        //  call 3 → B success (fresh drain) → engine ack
        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.retryableResponse()),
            .response(Self.successResponse()),
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder,
            retryPolicy: Self.retryPolicy(maxAttempts: 1, seconds: 0.01)
        )

        // First flush: A exhausts retry budget, boundary held.
        let first = try await engine.flush()
        #expect(first.attemptedEntries == 1)
        #expect(first.retryableEntries == 1)
        #expect(first.acknowledgement == .notAcknowledged)
        let outstandingAfterFirst = await queue.currentOutstandingBatch()
        #expect(outstandingAfterFirst != nil)

        // Enqueue B *while* the boundary is held. The queue must
        // accept the enqueue (writer-side admission is independent
        // of drain/ack state), but B must NOT participate in the
        // next flush — that flush is the outstanding replay of A.
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 2, payload: Data([0xB2])
        ))

        // Second flush: outstanding-reuse path replays exactly A
        // and acknowledges; B is still admitted-but-not-drained
        // on the queue's writer side.
        let second = try await engine.flush()
        #expect(second.attemptedEntries == 1)
        #expect(second.succeededEntries == 1)
        #expect(second.retryableEntries == 0)
        #expect(second.acknowledgement == .removedDeliveredBytes)
        let outstandingAfterSecond = await queue.currentOutstandingBatch()
        #expect(outstandingAfterSecond == nil)

        // Third flush: fresh drain captures B (queued during the
        // hold), dispatches it, acknowledges.
        let third = try await engine.flush()
        #expect(third.attemptedEntries == 1)
        #expect(third.succeededEntries == 1)
        #expect(third.acknowledgement == .removedDeliveredBytes)

        // Transport ordering proof: A is dispatched twice (first
        // flush retryable + second flush replay), then B is
        // dispatched once on the third flush. The engine never
        // interleaves the two entries — outstanding-reuse and
        // fresh-drain stay separate.
        let calls = await transport.recordedCalls()
        #expect(calls.count == 3)
        #expect(calls.map(\.payloadBytes) == [
            Data([0xA1]), Data([0xA1]), Data([0xB2])
        ])

        // Export directory is empty after the third resolved ack
        // — every flush that acknowledged also cleaned up its
        // export artifact.
        let leftoverFiles = try FileManager.default.contentsOfDirectory(
            at: exportDirectory, includingPropertiesForKeys: nil
        )
        #expect(leftoverFiles.isEmpty)
    }
}

// MARK: - Sequential flushes acknowledge each non-empty batch

extension RemoteEngineTests {
    @Test(
        "sequential flushes each acknowledge their own resolved batches",
        .tags(.lgr11)
    )
    func sequentialFlushesAcknowledgeEachBatch() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }

        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.successResponse()),
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )

        let firstSummary = try await engine.flush()
        #expect(firstSummary.acknowledgement == .removedDeliveredBytes)
        #expect(firstSummary.succeededEntries == 1)

        // Enqueue another entry between flushes and verify the
        // second flush admits it (the boundary cleared on ack).
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 2, payload: Data([0x02])
        ))
        let secondSummary = try await engine.flush()
        #expect(secondSummary.acknowledgement == .removedDeliveredBytes)
        #expect(secondSummary.succeededEntries == 1)
        #expect(secondSummary.attemptedEntries == 1)

        let calls = await transport.recordedCalls()
        #expect(calls.count == 2)
        #expect(calls.map(\.payloadBytes) == [Data([0x01]), Data([0x02])])
    }
}

// MARK: - Error mapping coverage

extension RemoteEngineTests {
    @Test(
        "queue drain failure surfaces .drainFailed carrying the underlying queue error",
        .tags(.lgr7, .lgr10)
    )
    func drainFailureSurfacesDrainFailed() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = Self.makeStubTransport(outcomes: [])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )
        // Inject a throwing export-size reader so the queue's
        // `drain(to:)` surfaces `.drainSizeReadFailed` after the
        // persistence-layer export step succeeds but before the
        // queue captures an outstanding-batch boundary. Scoped
        // cleanup guarantees the seam is reset even if the body
        // throws before the explicit assertion.
        await Self.withSeamCleanup(
            install: {
                await queue._setExportSizeReaderForTesting { _ in
                    throw NSError(domain: "test", code: 1)
                }
            },
            cleanup: {
                await queue._setExportSizeReaderForTesting(nil)
            },
            body: {
                var caught: RemoteEngineError?
                do throws(RemoteEngineError) {
                    _ = try await engine.flush()
                    Issue.record("expected .drainFailed")
                } catch {
                    caught = error
                }
                #expect(caught == .drainFailed(.drainSizeReadFailed))
            }
        )
    }

    @Test(
        "malformed queue record bytes surface .parseFailed(.recordPayloadMalformed)",
        .tags(.lgr7, .lgr10)
    )
    func parseFailureSurfacesParseFailed() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = Self.makeStubTransport(outcomes: [])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )
        // Inject a record encoder that emits non-JSON bytes for
        // the queue payload before enqueue. The persistence layer
        // wraps those bytes into a valid envelope (envelope JSON
        // + base64 payload), so drain succeeds; the parser then
        // fails when it tries to decode the inner queue record.
        // Scoped cleanup guarantees the encoder seam is reset
        // even if the body throws before the assertions complete.
        try await Self.withSeamCleanup(
            install: {
                await queue._setRecordEncoderForTesting { _ in
                    Data([0xFF, 0xFE, 0xFD])
                }
            },
            cleanup: {
                await queue._setRecordEncoderForTesting(nil)
            },
            body: {
                try await queue.enqueue(RemoteDeliveryEntry(
                    identifier: 1, payload: Data([0x01])
                ))

                var caught: RemoteEngineError?
                do throws(RemoteEngineError) {
                    _ = try await engine.flush()
                    Issue.record("expected .parseFailed")
                } catch {
                    caught = error
                }
                // The malformed bytes are valid base64 (the queue's
                // envelope encoder base64-wraps the payload bytes)
                // but JSON-decode fails as a queue record, so the
                // public diagnostic mirrors
                // `BatchEngineError.recordPayloadMalformed`.
                #expect(caught == .parseFailed(.recordPayloadMalformed))
                // Parse failed AFTER the engine owned a reusable
                // drained batch reference for this flush pass, so
                // the retained export artifact stays on disk and
                // the queue still holds the outstanding batch; the
                // next flush would reuse both through the
                // outstanding-reuse path.
                let leftoverFiles = try FileManager.default.contentsOfDirectory(
                    at: exportDirectory, includingPropertiesForKeys: nil
                )
                #expect(leftoverFiles.count == 1)
                let outstandingAfter = await queue.currentOutstandingBatch()
                #expect(outstandingAfter != nil)
            }
        )
    }

    @Test(
        "oversized single entry surfaces .batchFailed(.batchSizeExceeded(_:_:))",
        .tags(.lgr4, .lgr7)
    )
    func batchFailureSurfacesBatchFailed() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let oversizedPayload = Data(repeating: 0x42, count: 4)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: oversizedPayload
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = Self.makeStubTransport(outcomes: [])
        let recorder = SleepRecorder()
        // Batch policy with byte cap = 1 byte; the single 4-byte
        // entry alone exceeds the cap and the batcher surfaces
        // `.batchSizeExceeded(limit: 1, actual: 4)`.
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder,
            batchPolicy: Self.batchPolicy(maxEntryCount: 64, maxByteCount: 1)
        )

        var caught: RemoteEngineError?
        do {
            _ = try await engine.flush()
            Issue.record("expected .batchFailed")
        } catch {
            caught = error
        }
        #expect(caught == .batchFailed(.batchSizeExceeded(limit: 1, actual: 4)))
        // Batch split failed AFTER drain; the export artifact and
        // the queue's outstanding boundary both stay so the next
        // flush would reuse them.
        let leftoverFiles = try FileManager.default.contentsOfDirectory(
            at: exportDirectory, includingPropertiesForKeys: nil
        )
        #expect(leftoverFiles.count == 1)
        let outstandingAfter = await queue.currentOutstandingBatch()
        #expect(outstandingAfter != nil)
    }

    // swiftlint:disable function_body_length
    // Reason: This test covers the ack-failure lifecycle
    // end-to-end — failure mapping, export-and-boundary
    // retention, seam clear, replay against the same outstanding
    // bytes, and final ack — because each step is only
    // meaningful in the context of the previous one. Splitting
    // would obscure the lifecycle contract.

    @Test(
        "acknowledgement failure keeps the export held; clearing the seam lets the next flush replay and acknowledge",
        .tags(.lgr7, .lgr11)
    )
    func acknowledgementFailureSurfacesAcknowledgementFailed() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = Self.makeStubTransport(outcomes: [
            // First flush: send the entry once → success.
            .response(Self.successResponse()),
            // Second flush (after the test clears the ack
            // override): outstanding-reuse replays the same entry
            // once more → success.
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )
        // Engine-internal seam: the acknowledge call routes
        // through the override and throws, so the engine
        // surfaces `.acknowledgementFailed` and must keep the
        // export artifact on disk. The thrown failure mirrors a
        // realistic persistence-layer outcome — the queue's
        // remove-exported-logs step refusing to run because the
        // removal boundary was already consumed — rather than an
        // unreachable case the queue cannot actually raise.
        // Scoped cleanup resets the ack seam after the
        // first-flush phase even if its assertions throw, so the
        // follow-up flush always runs against a clean engine.
        try await Self.withSeamCleanup(
            install: {
                // swiftformat:disable:next redundantParens
                await engine._setAcknowledgeOverrideForTesting { @Sendable () async throws(DurableRemoteQueueError) in
                    throw .acknowledgeFailed(.noExportedRemovalBoundary)
                }
            },
            cleanup: {
                await engine._setAcknowledgeOverrideForTesting(nil)
            },
            body: {
                var caught: RemoteEngineError?
                do throws(RemoteEngineError) {
                    _ = try await engine.flush()
                    Issue.record("expected .acknowledgementFailed")
                } catch {
                    caught = error
                }
                #expect(caught == .acknowledgementFailed(.acknowledgeFailed(.noExportedRemovalBoundary)))

                // Export file must still exist; the queue's
                // outstanding-batch boundary is held and the next
                // flush will retry through the outstanding-reuse path.
                let leftoverFiles = try FileManager.default.contentsOfDirectory(
                    at: exportDirectory, includingPropertiesForKeys: nil
                )
                #expect(leftoverFiles.count == 1)
                let outstandingAfter = await queue.currentOutstandingBatch()
                #expect(outstandingAfter != nil)
            }
        )

        // Re-flush after the scoped helper cleared the ack seam:
        // the engine must reuse the outstanding batch (no fresh
        // drain — the queue is still holding the boundary), replay
        // the same queue payload bytes, acknowledge for real this
        // time, and clean up the export artifact.
        let summary = try await engine.flush()
        #expect(summary == RemoteFlushSummary(
            attemptedBatches: 1,
            attemptedEntries: 1,
            succeededEntries: 1,
            terminalEntries: 0,
            retryableEntries: 0,
            acknowledgement: .removedDeliveredBytes
        ))
        let calls = await transport.recordedCalls()
        #expect(calls.count == 2)
        #expect(calls.map(\.payloadBytes) == [Data([0x01]), Data([0x01])])
        let finalFiles = try FileManager.default.contentsOfDirectory(
            at: exportDirectory, includingPropertiesForKeys: nil
        )
        #expect(finalFiles.isEmpty)
        let outstandingFinal = await queue.currentOutstandingBatch()
        #expect(outstandingFinal == nil)
    }

    // swiftlint:enable function_body_length

    @Test(
        "sleep-injector failure during flush surfaces .retryInterrupted(.sleepInterrupted)",
        .tags(.lgr3)
    )
    func sleepInjectorFailureSurfacesRetryInterrupted() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }

        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.retryableResponse()),
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()
        // Throw on the first sleep so the engine cannot reach
        // attempt 2 and the public error must surface
        // `.retryInterrupted(.sleepInterrupted)`.
        let throwingSleep = RecordingSleep.make(
            recorder: recorder, throwOnSleepIndex: 0
        )
        let engine = RemoteEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            batchPolicy: try Self.batchPolicy(),
            retryPolicy: try Self.retryPolicy(maxAttempts: 3, seconds: 0.05),
            sleep: throwingSleep
        )

        var caught: RemoteEngineError?
        do {
            _ = try await engine.flush()
            Issue.record("expected .retryInterrupted(.sleepInterrupted)")
        } catch {
            caught = error
        }
        #expect(caught == .retryInterrupted(.sleepInterrupted))
        // Retry interruption fired AFTER drain; the export
        // artifact and outstanding boundary both stay so the
        // next flush would retry through the outstanding-reuse
        // path.
        let leftoverFiles = try FileManager.default.contentsOfDirectory(
            at: exportDirectory, includingPropertiesForKeys: nil
        )
        #expect(leftoverFiles.count == 1)
        let outstandingAfter = await queue.currentOutstandingBatch()
        #expect(outstandingAfter != nil)
    }

    // swiftlint:disable function_body_length
    // Reason: Test pins the end-to-end .exportCleanupFailed(.acknowledgedNonEmpty) contract — the surfaced error + diagnostic context, the queue-side ack-final state, and the follow-up flush proving the retained orphan artifact is not a retry source — as a single connected scenario; splitting the assertions across tests would weaken the contractual continuity and obscure the duplicate-not-retry-source invariant.
    @Test(
        "non-empty ack cleanup failure surfaces .exportCleanupFailed; ack is final and the retained artifact lingers",
        .tags(.lgr7, .lgr11)
    )
    func nonEmptyAckCleanupFailureSurfacesExportCleanupFailed() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0xCA, 0xFE])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = Self.makeStubTransport(outcomes: [
            .response(Self.successResponse())
        ])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )
        // Engine-internal seam: the cleanup helper routes through
        // the override and throws an `NSError` with a known
        // domain/code so the test can assert the diagnostic
        // context the engine attaches to `.exportCleanupFailed`.
        // Scoped cleanup resets the seam after the first-flush
        // phase even if its assertions throw, so the follow-up
        // flush always runs against a clean engine.
        let injectedDomain = "RemoteEngineTests.cleanup"
        let injectedCode = 73
        let capturedURL = LockedURL()
        let removalOverride: @Sendable (URL) throws -> Void = { url in
            capturedURL.set(url)
            throw NSError(domain: injectedDomain, code: injectedCode)
        }
        await Self.withSeamCleanup(
            install: {
                await engine._setExportRemovalOverrideForTesting(removalOverride)
            },
            cleanup: {
                await engine._setExportRemovalOverrideForTesting(nil)
            },
            body: {
                var caught: RemoteEngineError?
                do throws(RemoteEngineError) {
                    _ = try await engine.flush()
                    Issue.record("expected .exportCleanupFailed")
                } catch {
                    caught = error
                }

                // The cleanup phase MUST be `acknowledgedNonEmpty` —
                // the queue's destructive removal already ran when
                // the ack succeeded; the engine's retained scratch
                // file is now an orphan copy of bytes the
                // persistence layer has dropped.
                guard case let .exportCleanupFailed(context) = caught else {
                    Issue.record(
                        "expected .exportCleanupFailed; got \(String(describing: caught))"
                    )
                    return
                }
                #expect(context.phase == .acknowledgedNonEmpty)
                #expect(context.errorDomain == injectedDomain)
                #expect(context.errorCode == injectedCode)
                #expect(context.exportURL == capturedURL.value)

                // Acknowledgement is final: the queue's
                // outstanding-batch state is cleared (ack succeeded)
                // and a follow-up flush would re-drain from the
                // writer side. The orphan artifact stays on disk
                // for the caller to clean up.
                let outstandingAfter = await queue.currentOutstandingBatch()
                #expect(outstandingAfter == nil)
                #expect(FileManager.default.fileExists(atPath: context.exportURL.path))
            }
        )

        // Follow-up flush after the cleanup seam is cleared by
        // the scoped helper: the engine must NOT replay the
        // already-acknowledged payload through the transport, and
        // must reach the empty-drain fresh path since the queue
        // carries no further bytes.
        let followupSummary = try await engine.flush()
        #expect(followupSummary.acknowledgement == .emptyReleased)
        #expect(followupSummary.attemptedBatches == 0)
        #expect(followupSummary.attemptedEntries == 0)
        #expect(await queue.currentOutstandingBatch() == nil)
        // Transport saw exactly one call across both flushes — the
        // original successful dispatch. The retained orphan
        // artifact is a duplicate copy, never a retry source.
        let callsAfter = await transport.recordedCalls()
        #expect(callsAfter.count == 1)
        #expect(callsAfter.map(\.payloadBytes) == [Data([0xCA, 0xFE])])
    }

    // swiftlint:enable function_body_length

    @Test(
        "empty release cleanup failure surfaces .exportCleanupFailed without delivered queue payload bytes",
        .tags(.lgr7, .lgr11)
    )
    func emptyReleaseCleanupFailureSurfacesExportCleanupFailed() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = Self.makeStubTransport(outcomes: [])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )
        let injectedDomain = "RemoteEngineTests.cleanup"
        let injectedCode = 12
        let removalOverride: @Sendable (URL) throws -> Void = { _ in
            throw NSError(domain: injectedDomain, code: injectedCode)
        }
        await Self.withSeamCleanup(
            install: {
                await engine._setExportRemovalOverrideForTesting(removalOverride)
            },
            cleanup: {
                await engine._setExportRemovalOverrideForTesting(nil)
            },
            body: {
                var caught: RemoteEngineError?
                do throws(RemoteEngineError) {
                    _ = try await engine.flush()
                    Issue.record("expected .exportCleanupFailed")
                } catch {
                    caught = error
                }
                guard case let .exportCleanupFailed(context) = caught else {
                    Issue.record(
                        "expected .exportCleanupFailed; got \(String(describing: caught))"
                    )
                    return
                }
                // The cleanup phase distinguishes the empty release
                // path from the non-empty ack path: no delivered
                // queue payload bytes were carried by the export,
                // so this is a scratch-file leak only.
                #expect(context.phase == .emptyRelease)
                #expect(context.errorDomain == injectedDomain)
                #expect(context.errorCode == injectedCode)

                // The empty-drain ack already cleared the boundary
                // on the queue's side; only the empty scratch file
                // lingers.
                let outstandingAfter = await queue.currentOutstandingBatch()
                #expect(outstandingAfter == nil)
            }
        )
    }

    @Test(
        "drain failure with a throwing scratch cleanup keeps .drainFailed primary and suppresses the cleanup error",
        .tags(.lgr7, .lgr10)
    )
    func drainFailureWithCleanupFailureKeepsDrainFailedPrimary() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        let exportDirectory = try Self.makeExportDirectory()
        defer { Self.cleanup(exportDirectory) }
        let transport = Self.makeStubTransport(outcomes: [])
        let recorder = SleepRecorder()
        let engine = try Self.makeEngine(
            queue: queue,
            exportDirectory: exportDirectory,
            transport: transport,
            recorder: recorder
        )
        // Cleanup seam also throws so the test can prove the
        // drain-failure path swallows the cleanup error and keeps
        // `.drainFailed` primary. Scoped cleanup guarantees both
        // seams (queue size-reader + engine removal override) are
        // reset on any throw during the body.
        let capturedURL = LockedURL()
        let cleanupOverride: @Sendable (URL) throws -> Void = { url in
            capturedURL.set(url)
            throw NSError(
                domain: "RemoteEngineTests.drainCleanup", code: 99
            )
        }
        await Self.withSeamCleanup(
            install: {
                // Drive `drain(to:)` to surface
                // `.drainSizeReadFailed` after the
                // persistence-layer export step succeeds — matches
                // the existing drain-failure seam used elsewhere
                // in this file. The queue intentionally never
                // reaches a queue-held outstanding-batch state on
                // this path, so the engine runs its best-effort
                // scratch cleanup helper.
                await queue._setExportSizeReaderForTesting { _ in
                    throw NSError(domain: "test", code: 7)
                }
                await engine._setExportRemovalOverrideForTesting(cleanupOverride)
            },
            cleanup: {
                await queue._setExportSizeReaderForTesting(nil)
                await engine._setExportRemovalOverrideForTesting(nil)
            },
            body: {
                var caught: RemoteEngineError?
                do throws(RemoteEngineError) {
                    _ = try await engine.flush()
                    Issue.record("expected .drainFailed")
                } catch {
                    caught = error
                }
                // The cleanup helper failed, but the drain-side
                // diagnostic stays primary: callers see
                // `.drainFailed`, not `.exportCleanupFailed`. The
                // cleanup branch on this path is intentionally
                // best-effort.
                #expect(caught == .drainFailed(.drainSizeReadFailed))
                // The cleanup seam DID run — its URL was captured
                // — so the engine genuinely attempted the scratch
                // cleanup before suppressing the cleanup error.
                #expect(capturedURL.value != nil)
                // No queue-held outstanding batch on this path;
                // the next flush would re-enter the fresh-drain
                // path.
                let outstandingAfter = await queue.currentOutstandingBatch()
                #expect(outstandingAfter == nil)
            }
        )
    }
}

// MARK: - Safe sleep-nanosecond conversion

extension RemoteEngineTests {
    @Test(
        "safeSleepNanoseconds maps non-finite, non-positive, and oversized Double seconds without trapping",
        .tags(.lgr3)
    )
    func safeSleepNanosecondsFailSafeBranches() {
        // NaN / ±infinity collapse to no-sleep so the default
        // production injector cannot trap on a poisoned policy
        // value or a future seam returning a non-finite Double.
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: .nan) == 0)
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: .infinity) == 0)
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: -.infinity) == 0)

        // Negative and zero seconds map to no-sleep; the loop
        // already proves it only sleeps between retryable
        // attempts, so a misconfigured calculator cannot drive a
        // negative-cast trap.
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: -1.0) == 0)
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: 0.0) == 0)
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: -1e9) == 0)

        // A well-shaped sub-`UInt64.max` value rounds through the
        // truncating cast without surprise.
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: 1.0) == 1_000_000_000)
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: 0.5) == 500_000_000)

        // A delay so large the nanosecond product is at or above
        // the representable `UInt64` range clamps to `UInt64.max`
        // so the cast cannot trap; the engine takes the longest
        // expressible sleep instead of crashing.
        let huge = Double(UInt64.max)
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: huge) == .max)
        #expect(RemoteEngine.safeSleepNanoseconds(seconds: .greatestFiniteMagnitude) == .max)
    }
}

/// Sendable-safe URL capture so a `@Sendable` cleanup-override
/// closure can record the URL it was invoked with for the
/// enclosing test's assertion.
private final class LockedURL: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: URL?

    func set(_ url: URL) {
        lock.lock()
        defer { lock.unlock() }
        _value = url
    }

    var value: URL? {
        lock.lock()
        defer { lock.unlock() }
        return _value
    }
}
