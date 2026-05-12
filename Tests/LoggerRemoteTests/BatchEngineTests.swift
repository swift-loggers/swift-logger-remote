// swiftlint:disable file_length - LGR-4 / LGR-10 / LGR-11 batching-engine test inventory kept in a single file for traceability; per-LGR Swift Testing tag annotations expanded `@Test(...)` headers, growing the file past the default 500-line cap without adding new test cases.

import Foundation
import Testing

@testable import LoggerRemote

/// Coverage for the engine-internal batching machinery: pure
/// queue-export parser and pure entry-stream batcher. The
/// batching engine is internal in this milestone; tests reach it
/// through `@testable import`.
@Suite("BatchEngine batching machinery")
struct BatchEngineTests {
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

// MARK: - makeBatches: count cap

extension BatchEngineTests {
    @Test(
        "count cap splits the entry stream into batches of at most maxEntryCount entries",
        .tags(.lgr4)
    )
    func countCapSplitsEntryStream() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 2, maxByteCount: .max
        )
        let entries = (1 ... 5).map { value in
            RemoteDeliveryEntry(
                identifier: UInt64(value),
                payload: Data([UInt8(value)])
            )
        }
        let batches = try BatchEngine.makeBatches(
            from: entries, policy: policy
        )
        #expect(batches.map(\.count) == [2, 2, 1])
        // Accepted ordering survives the split.
        #expect(batches.flatMap { $0 }.map(\.identifier) == [1, 2, 3, 4, 5])
    }
}

// MARK: - makeBatches: byte cap

extension BatchEngineTests {
    @Test(
        "byte cap splits the entry stream when payload bytes would exceed maxByteCount",
        .tags(.lgr4)
    )
    func byteCapSplitsEntryStream() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: .max, maxByteCount: 4
        )
        // Sizes: 2, 2, 1, 4, 3. With cap 4: [2+2], [1+? 1+4 > 4 → close],
        // so [1] alone, then [4] alone, then [3] alone.
        let payloads: [Data] = [
            Data(repeating: 0xAA, count: 2),
            Data(repeating: 0xBB, count: 2),
            Data(repeating: 0xCC, count: 1),
            Data(repeating: 0xDD, count: 4),
            Data(repeating: 0xEE, count: 3)
        ]
        let entries = payloads.enumerated().map { index, payload in
            RemoteDeliveryEntry(
                identifier: UInt64(index + 1), payload: payload
            )
        }
        let batches = try BatchEngine.makeBatches(
            from: entries, policy: policy
        )
        #expect(batches.map { $0.map(\.payload.count) } == [[2, 2], [1], [4], [3]])
        #expect(batches.flatMap { $0 }.map(\.identifier) == [1, 2, 3, 4, 5])
    }
}

// MARK: - makeBatches: equal-to-cap fits, strictly-greater fires

extension BatchEngineTests {
    @Test(
        "equal-to-cap fits in the current batch and strictly-greater starts the next",
        .tags(.lgr4)
    )
    func boundaryEqualToCapFits() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: .max, maxByteCount: 10
        )
        // Two entries totalling exactly 10 fit in one batch.
        let exact = (1 ... 2).map { _ in
            RemoteDeliveryEntry(
                identifier: 1, payload: Data(repeating: 0x01, count: 5)
            )
        }
        let exactBatches = try BatchEngine.makeBatches(
            from: exact, policy: policy
        )
        #expect(exactBatches.count == 1)
        #expect(exactBatches[0].count == 2)

        // Two entries totalling 11 split: the second's 6 bytes
        // would push the running total over 10.
        let over = [
            RemoteDeliveryEntry(identifier: 1, payload: Data(repeating: 0x01, count: 5)),
            RemoteDeliveryEntry(identifier: 2, payload: Data(repeating: 0x02, count: 6))
        ]
        let overBatches = try BatchEngine.makeBatches(
            from: over, policy: policy
        )
        #expect(overBatches.map { $0.map(\.payload.count) } == [[5], [6]])
    }
}

// MARK: - makeBatches: oversized single entry

extension BatchEngineTests {
    @Test(
        "oversized single entry surfaces .batchSizeExceeded(limit:actual:)",
        .tags(.lgr4, .lgr7)
    )
    func oversizedSingleEntryRejected() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: .max, maxByteCount: 8
        )
        let entries = [
            RemoteDeliveryEntry(identifier: 1, payload: Data([0x01])),
            RemoteDeliveryEntry(
                identifier: 2, payload: Data(repeating: 0xFF, count: 9)
            )
        ]
        do {
            _ = try BatchEngine.makeBatches(from: entries, policy: policy)
            Issue.record("expected .batchSizeExceeded")
        } catch {
            #expect(error == .batchSizeExceeded(limit: 8, actual: 9))
        }
    }
}

// MARK: - makeBatches: ordering + duplicate identifiers

extension BatchEngineTests {
    @Test(
        "accepted ordering from the byte-stable queue export is preserved across batches",
        .tags(.lgr4, .lgr10)
    )
    func acceptedOrderingPreservedAcrossBatches() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 2, maxByteCount: .max
        )
        let identifiers: [UInt64] = [42, 7, 999, 1, 88]
        let entries = identifiers.map {
            RemoteDeliveryEntry(identifier: $0, payload: Data([0x00]))
        }
        let flat = try BatchEngine.makeBatches(
            from: entries, policy: policy
        ).flatMap { $0 }
        #expect(flat.map(\.identifier) == identifiers)
    }

    @Test(
        "duplicate identifiers survive batching verbatim",
        .tags(.lgr4, .lgr10)
    )
    func duplicateIdentifiersPreserved() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 2, maxByteCount: .max
        )
        let entries: [RemoteDeliveryEntry] = [
            RemoteDeliveryEntry(identifier: 7, payload: Data([0xAA])),
            RemoteDeliveryEntry(identifier: 7, payload: Data([0xBB])),
            RemoteDeliveryEntry(identifier: 7, payload: Data([0xCC]))
        ]
        let flat = try BatchEngine.makeBatches(
            from: entries, policy: policy
        ).flatMap { $0 }
        #expect(flat.map(\.identifier) == [7, 7, 7])
        #expect(flat.map(\.payload) == [Data([0xAA]), Data([0xBB]), Data([0xCC])])
    }
}

// MARK: - recoverEntries: queue-driven end-to-end

extension BatchEngineTests {
    @Test(
        "queue export -> recovered entries -> batches preserves identifier/payload/metadata",
        .tags(.lgr4, .lgr10)
    )
    func endToEndExportToBatches() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        let originals: [RemoteDeliveryEntry] = [
            RemoteDeliveryEntry(
                identifier: 1,
                payload: Data(#"{"event":"a"}"#.utf8),
                metadata: ["sink": "elastic"]
            ),
            RemoteDeliveryEntry(
                identifier: 2,
                payload: Data(#"{"event":"b"}"#.utf8),
                metadata: [:]
            ),
            RemoteDeliveryEntry(
                identifier: 1, // duplicate identifier
                payload: Data(#"{"event":"c"}"#.utf8),
                metadata: ["sink": "splunk"]
            )
        ]
        for entry in originals {
            try await queue.enqueue(entry)
        }
        try await queue.flush()

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let batchHandle = try await queue.drain(to: destination.url)

        let recovered = try BatchEngine.recoverEntries(from: batchHandle)
        #expect(recovered == originals)

        // Batch the recovered stream and confirm every original
        // entry appears once, in order.
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 2, maxByteCount: .max
        )
        let batches = try BatchEngine.makeBatches(
            from: recovered, policy: policy
        )
        #expect(batches.map(\.count) == [2, 1])
        #expect(batches.flatMap { $0 } == originals)
    }
}

// MARK: - recoverEntries: empty export

extension BatchEngineTests {
    @Test(
        "empty export produces no entries, no batches, and no acknowledgement side effect",
        .tags(.lgr11)
    )
    func emptyExportProducesNoBatches() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        // No enqueue; drain an empty queue.
        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let batchHandle = try await queue.drain(to: destination.url)

        let recovered = try BatchEngine.recoverEntries(from: batchHandle)
        #expect(recovered.isEmpty)

        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 4, maxByteCount: 1024
        )
        let batches = try BatchEngine.makeBatches(
            from: recovered, policy: policy
        )
        #expect(batches.isEmpty)

        // The batching engine never invokes `acknowledge()` and
        // performs no destructive removal: the active segment
        // remains empty (zero bytes) regardless of the queue's
        // internal outstanding-batch state.
        let activeSegment = directory.appendingPathComponent("log.ndjson")
        if FileManager.default.fileExists(atPath: activeSegment.path) {
            let bytes = try Data(contentsOf: activeSegment)
            #expect(bytes.isEmpty)
        }
    }
}

// MARK: - recoverEntries: malformed export bytes

extension BatchEngineTests {
    @Test(
        "malformed envelope JSON surfaces .envelopeMalformed",
        .tags(.lgr10)
    )
    func malformedEnvelopeRejected() throws {
        let bytes = Data("not-json\n".utf8)
        do {
            _ = try BatchEngine.recoverEntries(from: bytes)
            Issue.record("expected .envelopeMalformed")
        } catch {
            #expect(error == .envelopeMalformed)
        }
    }

    @Test(
        "envelope payload that is not valid base64 surfaces .recordPayloadBase64Invalid",
        .tags(.lgr10)
    )
    func envelopeWithNonBase64PayloadRejected() throws {
        // Envelope-shape JSON whose `payload` field is present but
        // whose value cannot be base64-decoded. The `contentType`
        // field is set to the queue-owned constant so the
        // contentType check passes and the parser reaches the
        // base64 step under test.
        let contentType = DurableRemoteQueue.envelopeContentType
        let bytes = Data(
            #"{"contentType":"\#(contentType)","payload":"!!!not-base64@@"}\#n"#.utf8
        )
        do {
            _ = try BatchEngine.recoverEntries(from: bytes)
            Issue.record("expected .recordPayloadBase64Invalid")
        } catch {
            #expect(error == .recordPayloadBase64Invalid)
        }
    }

    @Test(
        "envelope payload decoding to non-JSON bytes surfaces .recordPayloadMalformed",
        .tags(.lgr10)
    )
    func envelopeWithNonJSONRecordPayloadRejected() throws {
        let base64 = Data([0xFF, 0xFE]).base64EncodedString()
        let contentType = DurableRemoteQueue.envelopeContentType
        let line = #"{"contentType":"\#(contentType)","payload":"\#(base64)"}"#
        let bytes = Data(line.utf8) + Data([0x0A])
        do {
            _ = try BatchEngine.recoverEntries(from: bytes)
            Issue.record("expected .recordPayloadMalformed")
        } catch {
            #expect(error == .recordPayloadMalformed)
        }
    }

    @Test(
        "envelope with a foreign contentType surfaces .envelopeContentTypeMismatch",
        .tags(.lgr10)
    )
    func envelopeWithForeignContentTypeRejected() throws {
        // Hand-rolled envelope JSON carrying a contentType the
        // queue does not produce. The parser must refuse it
        // fail-closed before treating its `payload` as queue-record
        // bytes.
        let foreign = "application/vnd.unrelated+json"
        let recordBase64 = Data([0x01]).base64EncodedString()
        let line = #"{"contentType":"\#(foreign)","payload":"\#(recordBase64)"}"#
        let bytes = Data(line.utf8) + Data([0x0A])
        do {
            _ = try BatchEngine.recoverEntries(from: bytes)
            Issue.record("expected .envelopeContentTypeMismatch")
        } catch {
            #expect(
                error == .envelopeContentTypeMismatch(
                    expected: DurableRemoteQueue.envelopeContentType,
                    found: foreign
                )
            )
        }
    }
}

// MARK: - recoverEntries: byteCount cross-check

extension BatchEngineTests {
    @Test(
        "recoverEntries surfaces .exportByteCountMismatch when batch byteCount disagrees with the export file",
        .tags(.lgr10)
    )
    func batchByteCountMismatchRejected() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await queue.enqueue(RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        ))
        try await queue.flush()
        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let trueBatch = try await queue.drain(to: destination.url)
        // Construct a batch handle pointing at the same export file
        // but with a deliberately wrong byteCount; the parser must
        // refuse to interpret the bytes.
        let tamperedBatch = DurableRemoteQueueBatch(
            exportURL: trueBatch.exportURL,
            byteCount: trueBatch.byteCount &+ 1
        )
        do {
            _ = try BatchEngine.recoverEntries(from: tamperedBatch)
            Issue.record("expected .exportByteCountMismatch")
        } catch {
            #expect(error == .exportByteCountMismatch(
                expected: trueBatch.byteCount &+ 1,
                actual: trueBatch.byteCount
            ))
        }
    }
}

// MARK: - recoverEntries: framing rules

extension BatchEngineTests {
    @Test(
        "empty export bytes parse to no entries",
        .tags(.lgr10)
    )
    func emptyBytesParseToNoEntries() async throws {
        let entries = try BatchEngine.recoverEntries(from: Data())
        #expect(entries.isEmpty)
        // Pin the helper's `entryCount: 0` shape: draining a queue
        // that has admitted nothing must produce the same zero-byte
        // export the parser treats as valid empty.
        let validEmptyExport = try await Self.makeValidExportBytes(entryCount: 0)
        #expect(validEmptyExport == Data())
    }

    @Test(
        "a single bare newline is rejected as an empty line",
        .tags(.lgr10)
    )
    func bareNewlineRejected() throws {
        let bytes = Data([0x0A])
        do {
            _ = try BatchEngine.recoverEntries(from: bytes)
            Issue.record("expected .envelopeMalformed for bare newline")
        } catch {
            #expect(error == .envelopeMalformed)
        }
    }

    @Test(
        "a blank line between two valid records is rejected fail-closed",
        .tags(.lgr10)
    )
    func blankLineInMiddleRejected() async throws {
        // Drain a real queue with TWO records so each per-line
        // decode succeeds and the parser actually walks past the
        // first valid envelope into the blank-line iteration
        // between two valid records (not a trailing blank line).
        let validBytes = try await Self.makeValidExportBytes(entryCount: 2)
        let firstLF = try #require(validBytes.firstIndex(of: 0x0A))
        // Insert a `0x0A` byte after the first newline so the
        // parser sees: <envelope1>\n\n<envelope2>\n.
        var tampered = validBytes[..<validBytes.index(after: firstLF)]
        tampered.append(0x0A)
        tampered.append(contentsOf: validBytes[validBytes.index(after: firstLF)...])
        do {
            _ = try BatchEngine.recoverEntries(from: Data(tampered))
            Issue.record("expected .envelopeMalformed for blank middle line")
        } catch {
            #expect(error == .envelopeMalformed)
        }
    }

    @Test(
        "a last JSON line missing its trailing LF is rejected fail-closed",
        .tags(.lgr10)
    )
    func lastLineMissingLFRejected() async throws {
        // Drain a real queue and drop the trailing `0x0A` so the
        // parser sees a non-empty trailing chunk with no
        // terminator.
        let validBytes = try await Self.makeValidExportBytes(entryCount: 1)
        #expect(validBytes.last == 0x0A)
        let truncated = validBytes.prefix(validBytes.count - 1)
        do {
            _ = try BatchEngine.recoverEntries(from: Data(truncated))
            Issue.record("expected .envelopeMalformed for last line missing LF")
        } catch {
            #expect(error == .envelopeMalformed)
        }
    }
}

// MARK: - recoverEntries: formatVersion fail-closed

extension BatchEngineTests {
    @Test(
        "record without a formatVersion field surfaces .recordFormatVersionMissing",
        .tags(.lgr10)
    )
    func recordMissingFormatVersionRejected() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        // Install an encoder override that produces a queue-record
        // JSON without the formatVersion key.
        try await Self.withRecordEncoderOverride(
            on: queue,
            hook: { record in
                // Match DurableRemoteQueueRecord shape minus
                // formatVersion. Payload is the entry payload
                // base64-encoded by JSON encoding of Data.
                let json: [String: Any] = [
                    "identifier": record.identifier,
                    "payload": record.payload.base64EncodedString(),
                    "metadata": record.metadata
                ]
                return try JSONSerialization.data(
                    withJSONObject: json, options: [.sortedKeys]
                )
            },
            body: {
                try await queue.enqueue(RemoteDeliveryEntry(
                    identifier: 1, payload: Data([0x01])
                ))
                try await queue.flush()
            }
        )

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let batchHandle = try await queue.drain(to: destination.url)
        do {
            _ = try BatchEngine.recoverEntries(from: batchHandle)
            Issue.record("expected .recordFormatVersionMissing")
        } catch {
            #expect(error == .recordFormatVersionMissing)
        }
    }

    @Test(
        "record with an unknown formatVersion surfaces .recordFormatVersionUnsupported",
        .tags(.lgr10)
    )
    func recordWithUnknownFormatVersionRejected() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await Self.withRecordEncoderOverride(
            on: queue,
            hook: { record in
                let json: [String: Any] = [
                    "formatVersion": 99,
                    "identifier": record.identifier,
                    "payload": record.payload.base64EncodedString(),
                    "metadata": record.metadata
                ]
                return try JSONSerialization.data(
                    withJSONObject: json, options: [.sortedKeys]
                )
            },
            body: {
                try await queue.enqueue(RemoteDeliveryEntry(
                    identifier: 1, payload: Data([0x01])
                ))
                try await queue.flush()
            }
        )

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let batchHandle = try await queue.drain(to: destination.url)
        do {
            _ = try BatchEngine.recoverEntries(from: batchHandle)
            Issue.record("expected .recordFormatVersionUnsupported")
        } catch {
            #expect(
                error == .recordFormatVersionUnsupported(
                    found: 99,
                    supported: DurableRemoteQueueRecord.currentFormatVersion
                )
            )
        }
    }

    @Test(
        "record with a formatVersion above UInt8.max still surfaces .recordFormatVersionUnsupported",
        .tags(.lgr10)
    )
    func recordWithAboveUInt8FormatVersionRejected() async throws {
        // Pin the diagnostic taxonomy: an integer outside the
        // current `UInt8` schema-version space must still classify
        // as an unsupported version, not as a generic malformed
        // record. `999` is the canonical test value because it
        // exceeds `UInt8.max` (255) while still decoding as a
        // valid `UInt64`.
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        try await Self.withRecordEncoderOverride(
            on: queue,
            hook: { record in
                let json: [String: Any] = [
                    "formatVersion": 999,
                    "identifier": record.identifier,
                    "payload": record.payload.base64EncodedString(),
                    "metadata": record.metadata
                ]
                return try JSONSerialization.data(
                    withJSONObject: json, options: [.sortedKeys]
                )
            },
            body: {
                try await queue.enqueue(RemoteDeliveryEntry(
                    identifier: 1, payload: Data([0x01])
                ))
                try await queue.flush()
            }
        )

        let destination = try Self.makeExportURL()
        defer { Self.cleanup(destination.parent) }
        let batchHandle = try await queue.drain(to: destination.url)
        do {
            _ = try BatchEngine.recoverEntries(from: batchHandle)
            Issue.record("expected .recordFormatVersionUnsupported")
        } catch {
            #expect(
                error == .recordFormatVersionUnsupported(
                    found: 999,
                    supported: DurableRemoteQueueRecord.currentFormatVersion
                )
            )
        }
    }
}

// MARK: - Test helpers

extension BatchEngineTests {
    /// Drains a real `DurableRemoteQueue` populated with
    /// `entryCount` entries so framing tests have export bytes
    /// where each per-line decode actually succeeds and the parser
    /// reaches the framing assertion the test wants to exercise.
    /// `entryCount: 0` is supported and returns the empty export.
    static func makeValidExportBytes(entryCount: Int) async throws -> Data {
        let directory = uniqueDirectory()
        defer { cleanup(directory) }
        let queue = DurableRemoteQueue(directory: directory)
        for offset in 0 ..< entryCount {
            try await queue.enqueue(RemoteDeliveryEntry(
                identifier: UInt64(offset),
                payload: Data([UInt8(truncatingIfNeeded: offset)])
            ))
        }
        try await queue.flush()
        let destination = try makeExportURL()
        defer { cleanup(destination.parent) }
        _ = try await queue.drain(to: destination.url)
        return try Data(contentsOf: destination.url)
    }
}

// MARK: - Scoped seam helper

extension BatchEngineTests {
    /// Scoped record-encoder override cleared before returning.
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
}
