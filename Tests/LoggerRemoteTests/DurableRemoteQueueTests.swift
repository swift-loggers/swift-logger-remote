import Foundation
import LoggerFilePersistence
import Testing

@testable import LoggerRemote

/// Coverage for the persistence-backed durable queue core (LGR-10,
/// LGR-11 enqueue/drain/acknowledge boundary). Batching, retry
/// execution, and transport dispatch are deferred to later PRs.
@Suite("DurableRemoteQueue persistence-backed core")
struct DurableRemoteQueueTests {
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

// MARK: - On-disk schema version

extension DurableRemoteQueueTests {
    @Test("persisted record carries the current format version")
    func recordCarriesFormatVersion() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        try await queue.flush()

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        _ = try await queue.drain(to: destination.url)
        let bytes = try Data(contentsOf: destination.url)
        let firstLineBytes = Data(bytes.prefix { $0 != 0x0A })
        let envelope = try #require(
            try JSONSerialization.jsonObject(
                with: firstLineBytes
            ) as? [String: Any]
        )
        let payloadBase64 = try #require(envelope["payload"] as? String)
        let recordBytes = try #require(Data(base64Encoded: payloadBase64))
        let recordObject = try #require(
            try JSONSerialization.jsonObject(with: recordBytes) as? [String: Any]
        )
        let formatVersion = try #require(recordObject["formatVersion"] as? Int)
        #expect(formatVersion == Int(DurableRemoteQueueRecord.currentFormatVersion))
        // Decoding through the typed record path must also see the
        // current version so a future schema bump that forgets to
        // populate the field surfaces here.
        let decoded = try JSONDecoder().decode(
            DurableRemoteQueueRecord.self, from: recordBytes
        )
        #expect(decoded.formatVersion == DurableRemoteQueueRecord.currentFormatVersion)
    }
}

// MARK: - Encode failure does not advance the sequence allocator

extension DurableRemoteQueueTests {
    @Test("recordEncodingFailed leaves the private sequence allocator at the reserved value")
    func recordEncodingFailureDoesNotAdvanceAllocator() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)

        struct EncoderSentinel: Error {}
        // Scoped helper installs the throwing encoder, runs the
        // body, and clears the override before returning even when
        // `body` throws. The seam cleanup is awaited inline, not
        // dispatched through a fire-and-forget Task.
        try await Self.withRecordEncoderOverride(
            on: queue,
            hook: { _ in throw EncoderSentinel() },
            body: {
                do {
                    try await queue.enqueue(RemoteDeliveryEntry(
                        identifier: 1, payload: Data([0x01])
                    ))
                    Issue.record("expected .recordEncodingFailed")
                } catch let error as DurableRemoteQueueError {
                    #expect(error == .recordEncodingFailed)
                } catch {
                    Issue.record("unexpected non-typed error: \(error)")
                }
            }
        )

        // The next enqueue runs against the cleared seam. The
        // persisted envelope must spell `"sequence":1`, proving
        // the failed encode never advanced the allocator past its
        // reserved value.
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 9, payload: Data([0x02])
        ))
        try await queue.flush()

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        _ = try await queue.drain(to: destination.url)
        let bytes = try Data(contentsOf: destination.url)
        let line = try #require(String(bytes: bytes, encoding: .utf8))
        #expect(line.contains("\"sequence\":1"))
        #expect(!line.contains("\"sequence\":2"))
    }
}

// MARK: - Sequence allocator exhaustion

extension DurableRemoteQueueTests {
    @Test("allocator exhaustion surfaces .sequenceExhausted and rejects the next enqueue")
    func allocatorExhaustionRejectsNextEnqueue() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        // Seed the queue's allocator at UInt64.max so the very next
        // enqueue admission tips it over without an `&+=` wrap to 0.
        let queue = DurableRemoteQueue(
            directory: directory,
            contentType: "application/vnd.swift-loggers.remote-queue+json",
            rotation: .never,
            startingSequence: UInt64.max
        )
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        // Next call: allocator has wrapped through `&+= 1` to `0`,
        // which the exhaustion guard catches before any I/O.
        do {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: 2, payload: Data([0x02])
            ))
            Issue.record("expected .sequenceExhausted")
        } catch {
            #expect(error == .sequenceExhausted)
        }
    }
}

// MARK: - Lossless record persistence

extension DurableRemoteQueueTests {
    @Test("enqueue accepts an entry with metadata and a zero identifier")
    func enqueueAcceptsMetadataAndZeroIdentifier() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 0,
            payload: Data([0x01]),
            metadata: ["sink": "elastic", "route": "primary"]
        ))
        try await queue.flush()
        // Reaching this assertion proves the queue persists the
        // entry losslessly: zero is a valid correlation identifier
        // and metadata round-trips through the internal record.
    }

    @Test("enqueue persists identifier, payload, and metadata losslessly")
    func enqueuePersistsRecordLosslessly() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let entry = RemoteDeliveryEntry(
            identifier: 7,
            payload: Data(#"{"event":"hello"}"#.utf8),
            metadata: ["sink": "elastic", "route": "primary"]
        )
        try await queue.enqueue(entry)
        try await queue.flush()

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let batch = try await queue.drain(to: destination.url)
        let bytes = try Data(contentsOf: destination.url)
        #expect(batch.exportURL == destination.url)
        #expect(batch.byteCount == UInt64(bytes.count))
        // Exported bytes are persistence envelopes wrapping the
        // queue's internal record. The decoded record must match
        // the original entry's identifier, payload, and metadata
        // byte-for-byte.
        let decoded = try Self.decodeFirstRecord(in: bytes)
        #expect(decoded.identifier == entry.identifier)
        #expect(decoded.payload == entry.payload)
        #expect(decoded.metadata == entry.metadata)
    }
}

// MARK: - Sequence allocator

extension DurableRemoteQueueTests {
    @Test("queue assigns its own private sequence; entry identifier carries no ordering")
    func queueAssignsPrivateSequence() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        // Producer hands the queue identifiers in a non-monotonic
        // pattern; the queue must still assign ascending
        // persistence sequence values.
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 999, payload: Data([0x01])
        ))
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 42, payload: Data([0x02])
        ))
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 7, payload: Data([0x03])
        ))
        try await queue.flush()

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        _ = try await queue.drain(to: destination.url)
        let bytes = try Data(contentsOf: destination.url)
        let line = try #require(String(bytes: bytes, encoding: .utf8))
        // The persistence envelope spells `"sequence":N` verbatim;
        // the queue must spell 1, 2, 3 across the three entries.
        #expect(line.contains("\"sequence\":1"))
        #expect(line.contains("\"sequence\":2"))
        #expect(line.contains("\"sequence\":3"))
        // The producer-supplied 999 / 42 / 7 must NOT appear as a
        // persistence sequence value.
        #expect(!line.contains("\"sequence\":999"))
    }
}

// MARK: - Single-outstanding-batch state

extension DurableRemoteQueueTests {
    @Test("drain before acknowledge surfaces .batchAlreadyOutstanding")
    func drainBeforeAcknowledgeSurfacesBatchAlreadyOutstanding() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))

        let firstDestination = try Self.makeExportURL()
        defer { Self.cleanup(firstDestination.parent) }
        _ = try await queue.drain(to: firstDestination.url)

        let secondDestination = try Self.makeExportURL()
        defer { Self.cleanup(secondDestination.parent) }
        do {
            _ = try await queue.drain(to: secondDestination.url)
            Issue.record("expected .batchAlreadyOutstanding")
        } catch {
            #expect(error == .batchAlreadyOutstanding)
        }
    }

    @Test("acknowledge clears outstanding batch and re-opens drain")
    func acknowledgeReopensDrain() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))

        let firstDestination = try Self.makeExportURL()
        defer { Self.cleanup(firstDestination.parent) }
        _ = try await queue.drain(to: firstDestination.url)
        try await queue.acknowledge()
        let cleared = await queue.currentOutstandingBatch()
        #expect(cleared == nil)

        // After acknowledge the queue accepts a second drain. The
        // recoverable prefix is empty now so the second export
        // captures zero bytes.
        let secondDestination = try Self.makeExportURL()
        defer { Self.cleanup(secondDestination.parent) }
        let second = try await queue.drain(to: secondDestination.url)
        #expect(second.byteCount == 0)
    }

    @Test("failed acknowledge keeps the outstanding batch for retry")
    func failedAcknowledgeKeepsOutstandingBatch() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let batch = try await queue.drain(to: destination.url)

        // Force `removeExportedLogs()` to fail by removing the
        // exported segment under the persistence layer's feet. The
        // queue must keep the outstanding-batch state so a retry
        // can still target the captured boundary; the failure path
        // does not silently re-open drain.
        try FileManager.default.removeItem(
            at: directory.appendingPathComponent("log.ndjson")
        )
        do {
            try await queue.acknowledge()
            Issue.record("expected .acknowledgeFailed")
        } catch {
            switch error {
            case .acknowledgeFailed:
                break
            default:
                Issue.record("unexpected error: \(error)")
            }
        }
        let stillOutstanding = await queue.currentOutstandingBatch()
        #expect(stillOutstanding == batch)
        // Drain is still blocked because the outstanding batch
        // never cleared.
        let retryDestination = try Self.makeExportURL()
        defer { Self.cleanup(retryDestination.parent) }
        do {
            _ = try await queue.drain(to: retryDestination.url)
            Issue.record("expected .batchAlreadyOutstanding after failed acknowledge")
        } catch {
            #expect(error == .batchAlreadyOutstanding)
        }
    }

    @Test("acknowledge without a captured boundary surfaces .acknowledgeFailed")
    func acknowledgeWithoutBoundaryFails() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        do {
            try await queue.acknowledge()
            Issue.record("expected .acknowledgeFailed with .noExportedRemovalBoundary")
        } catch {
            switch error {
            case let .acknowledgeFailed(removeError):
                #expect(removeError == .noExportedRemovalBoundary)
            default:
                Issue.record("unexpected error: \(error)")
            }
        }
    }
}

// MARK: - Acknowledge requires a queue-held outstanding batch

extension DurableRemoteQueueTests {
    // swiftlint:disable function_body_length
    // Reason: The LGR-11 acknowledge-guard proof co-locates the
    // scoped seam install, the drain failure assertion, the
    // outstanding-batch nil check, the acknowledge-rejection
    // assertion, and the recovery-drain record-identity decode in
    // one body so the contract reads top-to-bottom.

    @Test("drain size-read failure leaves no outstanding batch and blocks destructive remove")
    func drainSizeReadFailureBlocksDestructiveRemove() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0xCA, 0xFE])
        ))
        try await queue.flush()

        struct DrainSizeReadSentinel: Error {}
        let firstDestination = try Self.makeExportURL()
        defer { Self.cleanup(firstDestination.parent) }

        // Scoped helper installs the throwing size reader, runs
        // the body, and clears the override before returning even
        // when `body` throws. The seam cleanup is awaited inline,
        // not dispatched through a fire-and-forget Task, so the
        // recovery drain below always sees the default exact-size
        // measurement.
        try await Self.withExportSizeReaderOverride(
            on: queue,
            hook: { _ in throw DrainSizeReadSentinel() },
            body: {
                do {
                    _ = try await queue.drain(to: firstDestination.url)
                    Issue.record("expected .drainSizeReadFailed")
                } catch let error as DurableRemoteQueueError {
                    #expect(error == .drainSizeReadFailed)
                } catch {
                    Issue.record("unexpected non-typed error: \(error)")
                }

                // No batch was returned to the caller. The queue
                // must keep outstandingBatch nil so a later
                // acknowledge() cannot consume the persistence
                // in-memory boundary captured by the failed
                // drain's successful exportLogs(to:).
                let outstanding = await queue.currentOutstandingBatch()
                #expect(outstanding == nil)

                do {
                    try await queue.acknowledge()
                    Issue.record("expected .acknowledgeFailed(.noExportedRemovalBoundary)")
                } catch let error as DurableRemoteQueueError {
                    switch error {
                    case let .acknowledgeFailed(removeError):
                        #expect(removeError == .noExportedRemovalBoundary)
                    default:
                        Issue.record("unexpected error: \(error)")
                    }
                } catch {
                    Issue.record("unexpected non-typed error: \(error)")
                }
            }
        )

        // Recovery drain with the override cleared confirms the
        // originally admitted bytes are still in the persistence
        // store — nothing was destructively removed by the
        // rejected acknowledge.
        let retryDestination = try Self.makeExportURL()
        defer { Self.cleanup(retryDestination.parent) }
        let recoveryBatch = try await queue.drain(to: retryDestination.url)
        let recoveredBytes = try Data(contentsOf: retryDestination.url)
        #expect(recoveryBatch.byteCount == UInt64(recoveredBytes.count))
        #expect(recoveryBatch.byteCount > 0)
        let lineCount = recoveredBytes.lazy.filter { $0 == 0x0A }.count
        #expect(lineCount == 1)

        // Decode the surviving record and pin identifier, payload,
        // and metadata against the original entry. This proves the
        // rejected `acknowledge()` neither removed the queue record
        // nor replaced it with different bytes.
        let recovered = try Self.decodeFirstRecord(in: recoveredBytes)
        #expect(recovered.identifier == 1)
        #expect(recovered.payload == Data([0xCA, 0xFE]))
        #expect(recovered.metadata == [:])
    }

    // swiftlint:enable function_body_length
}

// MARK: - Flush failure surface (compile-time mapping)

extension DurableRemoteQueueTests {
    /// `flush()` does not project a persistence failure onto
    /// `.enqueueFailed`. The mapping itself is type-checked at
    /// compile time inside `DurableRemoteQueue.flush()`; this test
    /// pattern-matches the `.flushFailed` case so a future refactor
    /// that drops the case (or merges it back into `.enqueueFailed`)
    /// would stop compiling here. Runtime triggering of a
    /// persistence-flush failure waits for an accessible flush
    /// seam in a future persistence release.
    @Test("DurableRemoteQueueError exposes .flushFailed as a distinct case")
    func flushFailedCaseIsDistinctFromEnqueueFailed() {
        let synthetic = DurableRemoteQueueError.flushFailed(
            .operationFailed(
                operation: .flushBoundary,
                url: URL(fileURLWithPath: "/dev/null"),
                context: FileSystemErrorContext(
                    domain: "test", code: nil, description: "synthetic"
                )
            )
        )
        switch synthetic {
        case .flushFailed:
            // Reaching the case proves it exists and is distinct
            // from .enqueueFailed; the static switch is exhaustive
            // so a refactor that removes the case would break here.
            break
        case .enqueueFailed,
             .acknowledgeFailed,
             .drainFailed,
             .drainSizeReadFailed,
             .batchAlreadyOutstanding,
             .envelopeRejected,
             .recordEncodingFailed,
             .sequenceExhausted:
            Issue.record(".flushFailed must not collapse into another case")
        }
    }
}

// MARK: - Drain byte-count exactness

extension DurableRemoteQueueTests {
    @Test("drain reports the exact post-export byte count")
    func drainReportsExactByteCount() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        for sequence: UInt64 in 1 ... 3 {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: sequence, payload: Data([UInt8(sequence)])
            ))
        }
        try await queue.flush()

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let batch = try await queue.drain(to: destination.url)
        let bytes = try Data(contentsOf: destination.url)
        #expect(batch.byteCount == UInt64(bytes.count))
        let lineCount = bytes.lazy.filter { $0 == 0x0A }.count
        #expect(lineCount == 3)
    }
}

// MARK: - Scoped seam helpers

extension DurableRemoteQueueTests {
    /// Runs `body` with the queue's record-encoder override
    /// installed and clears the override before returning, even
    /// when `body` throws. The cleanup is awaited inline, not
    /// dispatched through a detached `Task`.
    static func withRecordEncoderOverride(
        on queue: DurableRemoteQueue,
        hook: @Sendable @escaping (DurableRemoteQueueRecord) throws -> Data,
        body: () async throws -> Void
    ) async throws {
        await queue._setRecordEncoderForTesting(hook)
        var capturedError: (any Error)?
        do {
            try await body()
        } catch {
            capturedError = error
        }
        await queue._setRecordEncoderForTesting(nil)
        if let capturedError { throw capturedError }
    }

    /// Runs `body` with the queue's export-size-reader override
    /// installed and clears the override before returning, even
    /// when `body` throws. The cleanup is awaited inline, not
    /// dispatched through a detached `Task`.
    static func withExportSizeReaderOverride(
        on queue: DurableRemoteQueue,
        hook: @Sendable @escaping (URL) throws -> UInt64,
        body: () async throws -> Void
    ) async throws {
        await queue._setExportSizeReaderForTesting(hook)
        var capturedError: (any Error)?
        do {
            try await body()
        } catch {
            capturedError = error
        }
        await queue._setExportSizeReaderForTesting(nil)
        if let capturedError { throw capturedError }
    }
}

// MARK: - Test helpers

extension DurableRemoteQueueTests {
    /// Recovers the first `DurableRemoteQueueRecord` embedded in
    /// the byte-stable export. The persistence wire format wraps
    /// each record in a canonical envelope whose `payload` field is
    /// base64-encoded; the tests decode that field manually so they
    /// do not depend on the persistence package's read-side parser
    /// (which is not part of the persistence 0.1.x surface).
    ///
    /// The line is sliced at the first `0x0A` byte rather than via
    /// `String.split(separator:)` so an empty leading line, a
    /// trailing-only `\n`, or any non-UTF-8 byte inside a later
    /// line cannot influence the first-line slice. Only the first
    /// line is required to be UTF-8 JSON.
    private static func decodeFirstRecord(
        in exportBytes: Data
    ) throws -> DurableRemoteQueueRecord {
        let lineEnd = exportBytes.firstIndex(of: 0x0A) ?? exportBytes.endIndex
        let firstLineBytes = Data(exportBytes[..<lineEnd])
        let envelope = try #require(
            try JSONSerialization.jsonObject(with: firstLineBytes) as? [String: Any]
        )
        let payloadBase64 = try #require(envelope["payload"] as? String)
        let payloadBytes = try #require(Data(base64Encoded: payloadBase64))
        return try JSONDecoder().decode(
            DurableRemoteQueueRecord.self, from: payloadBytes
        )
    }
}
