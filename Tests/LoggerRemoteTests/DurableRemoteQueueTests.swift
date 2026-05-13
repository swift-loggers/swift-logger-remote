import Foundation
import LoggerFilePersistence
import Testing

@testable import LoggerRemote

/// Coverage for the persistence-backed durable queue core (LGR-10,
/// LGR-11 enqueue/drain/acknowledge boundary). Batching and
/// batch-round dispatch live in `BatchEngineTests` /
/// `BatchDispatchTests` / `ExecutionLoopTests`; the public flush
/// lifecycle that ties drain / dispatch / acknowledge together
/// lives in `RemoteEngineTests`.
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
    @Test(
        "persisted record carries the current format version",
        .tags(.lgr10)
    )
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
        let decoded = try Self.decodeFirstRecord(in: bytes)
        #expect(decoded.formatVersion == DurableRemoteQueueRecord.currentFormatVersion)
    }
}

// MARK: - Encode failure does not advance the sequence allocator

extension DurableRemoteQueueTests {
    @Test(
        "recordEncodingFailed leaves the private sequence allocator at the reserved value",
        .tags(.lgr10)
    )
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
        // persisted envelope must spell sequence `1`, proving the
        // failed encode never advanced the allocator past its
        // reserved value.
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 9, payload: Data([0x02])
        ))
        try await queue.flush()

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        _ = try await queue.drain(to: destination.url)
        let bytes = try Data(contentsOf: destination.url)
        let sequences = try Self.decodePersistenceSequences(in: bytes)
        #expect(sequences == [1])
    }
}

// MARK: - Sequence allocator exhaustion

extension DurableRemoteQueueTests {
    @Test(
        "allocator exhaustion surfaces .sequenceExhausted and rejects the next enqueue",
        .tags(.lgr10)
    )
    func allocatorExhaustionRejectsNextEnqueue() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        // Seed the queue's allocator at UInt64.max so the first
        // enqueue is admitted with the largest valid sequence; the
        // post-admission allocator advance then wraps to 0 for the
        // next reservation guard.
        let queue = DurableRemoteQueue(
            directory: directory,
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
        // Confirm the admitted record carries the full `UInt64.max`
        // sequence on the wire — not a silently downgraded value —
        // by draining the queue and decoding the byte-stable export.
        try await queue.flush()
        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        _ = try await queue.drain(to: destination.url)
        let bytes = try Data(contentsOf: destination.url)
        let sequences = try Self.decodePersistenceSequences(in: bytes)
        #expect(sequences == [UInt64.max])
    }
}

// MARK: - Lossless record persistence

extension DurableRemoteQueueTests {
    @Test(
        "enqueue accepts an entry with metadata and a zero identifier",
        .tags(.lgr1, .lgr10)
    )
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
        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        _ = try await queue.drain(to: destination.url)
        let bytes = try Data(contentsOf: destination.url)
        let decoded = try Self.decodeFirstRecord(in: bytes)
        #expect(decoded.identifier == 0)
        #expect(decoded.metadata == ["sink": "elastic", "route": "primary"])
        #expect(decoded.payload == Data([0x01]))
    }

    @Test(
        "enqueue persists identifier, payload, and metadata losslessly",
        .tags(.lgr1, .lgr10)
    )
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
    @Test(
        "queue assigns its own private sequence; entry identifier carries no ordering",
        .tags(.lgr1, .lgr10)
    )
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
        // The queue must assign `[1, 2, 3]` regardless of the
        // producer-supplied `999`, `42`, `7` identifiers.
        let sequences = try Self.decodePersistenceSequences(in: bytes)
        #expect(sequences == [1, 2, 3])
    }
}

// MARK: - Single-outstanding-batch state

extension DurableRemoteQueueTests {
    @Test(
        "drain before acknowledge surfaces .batchAlreadyOutstanding",
        .tags(.lgr11)
    )
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

    @Test(
        "acknowledge clears outstanding batch and re-opens drain",
        .tags(.lgr11)
    )
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

    @Test(
        "failed acknowledge keeps the outstanding batch for retry",
        .tags(.lgr11)
    )
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

    @Test(
        "acknowledge without a captured boundary surfaces .acknowledgeFailed",
        .tags(.lgr11)
    )
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

    @Test(
        "drain size-read failure leaves no outstanding batch and blocks destructive remove",
        .tags(.lgr10, .lgr11)
    )
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
    @Test(
        "drain reports the exact post-export byte count",
        .tags(.lgr10)
    )
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
    /// `String.split(separator:)` so any non-UTF-8 byte inside a
    /// later line cannot influence the first-line slice. Only the
    /// first line is required to be UTF-8 JSON, and that line MUST
    /// be LF-terminated — an unterminated buffer fails the test
    /// rather than being treated as a valid first-line slice.
    private static func decodeFirstRecord(
        in exportBytes: Data
    ) throws -> DurableRemoteQueueRecord {
        let lineEnd = try #require(exportBytes.firstIndex(of: 0x0A))
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

    /// Returns the top-level persistence-envelope `sequence` value
    /// for every NDJSON line in `exportBytes`, in line order.
    /// Lets tests assert the queue's private sequence allocator
    /// values without falling back to substring matching against
    /// the canonical envelope text.
    ///
    /// Framing matches the byte-stable export contract the queue
    /// owns: every non-empty line MUST end with `0x0A`, a missing
    /// trailing LF on the last line fails the test, and an empty
    /// line (consecutive `0x0A`s or a leading `0x0A`) fails the
    /// test rather than being skipped.
    ///
    /// `sequence` is decoded through a minimal `Decodable` envelope
    /// so the full `UInt64` allocator range — including `UInt64.max`
    /// — round-trips losslessly. A negative, non-integer, or
    /// out-of-`UInt64`-range value fails the test through the
    /// decoder rather than being silently coerced.
    static func decodePersistenceSequences(
        in exportBytes: Data
    ) throws -> [UInt64] {
        let decoder = JSONDecoder()
        var cursor = exportBytes.startIndex
        var sequences: [UInt64] = []
        while cursor < exportBytes.endIndex {
            let lineEnd = try #require(
                exportBytes[cursor...].firstIndex(of: 0x0A)
            )
            let lineBytes = Data(exportBytes[cursor ..< lineEnd])
            cursor = exportBytes.index(after: lineEnd)
            try #require(!lineBytes.isEmpty)
            let envelope = try decoder.decode(
                PersistenceSequenceEnvelope.self, from: lineBytes
            )
            sequences.append(envelope.sequence)
        }
        return sequences
    }

    private struct PersistenceSequenceEnvelope: Decodable {
        let sequence: UInt64
    }
}
