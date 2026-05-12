import Foundation

/// Engine-internal batching machinery driven by the engine-
/// internal ``ExecutionLoop`` and ``RemoteEngine/flush()``.
///
/// Two pure steps:
///
/// 1. ``recoverEntries(from:)`` parses a byte-stable export file
///    produced by ``DurableRemoteQueue/drain(to:)`` into an
///    ordered array of ``RemoteDeliveryEntry`` values. The parser
///    inspects each queue record's ``DurableRemoteQueueRecord/formatVersion``
///    schema-evolution anchor before decoding any other queue-record
///    field and refuses unknown or missing versions fail-closed.
/// 2. ``makeBatches(from:policy:)`` splits an ordered entry stream
///    into ordered batches that honor ``RemoteBatchPolicy``: equal-
///    to-cap fits, strictly-greater starts the next batch, and an
///    oversized single entry surfaces ``RemoteDeliveryError/batchSizeExceeded(limit:actual:)``.
///
/// The engine is intentionally internal: no transport classification,
/// no retry scheduling, no acknowledgement-to-removal lifecycle, no
/// dedupe, no sort, no replay/query API. Accepted ordering from
/// the byte-stable queue export is preserved verbatim; duplicate
/// ``RemoteDeliveryEntry/identifier`` values survive both steps in
/// the same order they were enqueued.
internal enum BatchEngine {
    /// Parses one drained queue export into the ordered entry
    /// stream the batching step consumes.
    ///
    /// The on-disk size is cross-checked against
    /// ``DurableRemoteQueueBatch/byteCount`` before the bytes are
    /// interpreted; a mismatch surfaces
    /// ``BatchEngineError/exportByteCountMismatch(expected:actual:)``
    /// fail-closed so the parser cannot silently consume a file
    /// that was truncated, extended, or replaced between drain and
    /// parse.
    static func recoverEntries(
        from batch: DurableRemoteQueueBatch
    ) throws(BatchEngineError) -> [RemoteDeliveryEntry] {
        let bytes: Data
        do {
            bytes = try Data(contentsOf: batch.exportURL)
        } catch {
            throw .exportFileReadFailed
        }
        let actualByteCount = try byteCount(of: bytes)
        guard actualByteCount == batch.byteCount else {
            throw .exportByteCountMismatch(
                expected: batch.byteCount,
                actual: actualByteCount
            )
        }
        return try recoverEntries(from: bytes)
    }

    /// Converts the export file's `Data.count` (a `Swift.Int`) into
    /// the queue's `UInt64` byte-count surface using
    /// `UInt64(exactly:)`. A conversion that cannot succeed is an
    /// engine/platform invariant violation (negative or
    /// `UInt64`-overrange `Int.count`, which Swift's `Data` shape
    /// forbids today) and is surfaced fail-closed as a typed
    /// error distinct from
    /// ``BatchEngineError/exportFileReadFailed`` (the file could
    /// not be read at all) and
    /// ``BatchEngineError/exportByteCountMismatch(expected:actual:)``
    /// (the file was read but its size disagrees with the queue's
    /// recorded `byteCount`).
    private static func byteCount(
        of bytes: Data
    ) throws(BatchEngineError) -> UInt64 {
        guard let byteCount = UInt64(exactly: bytes.count) else {
            throw .exportByteCountUnavailable
        }
        return byteCount
    }

    /// Same parser as ``recoverEntries(from:)`` but reading export
    /// bytes directly. Exposed so the engine-internal
    /// ``ExecutionLoop`` and ``RemoteEngine`` can run the parser
    /// against an in-memory buffer; internal-only.
    ///
    /// Framing rules (byte-stable LF-delimited NDJSON):
    ///
    /// - `Data()` (zero bytes) is a valid empty export and parses
    ///   to no entries.
    /// - Every non-empty entry line MUST end with `0x0A`. A
    ///   trailing-LF-less last line is rejected fail-closed.
    /// - Empty lines (consecutive `0x0A` bytes, or `\n` as the
    ///   first byte) are rejected fail-closed.
    static func recoverEntries(
        from exportBytes: Data
    ) throws(BatchEngineError) -> [RemoteDeliveryEntry] {
        var entries: [RemoteDeliveryEntry] = []
        var cursor = exportBytes.startIndex
        while cursor < exportBytes.endIndex {
            guard let lineEnd = exportBytes[cursor...].firstIndex(of: 0x0A) else {
                // Remaining bytes carry no terminator. Byte-stable
                // export requires every accepted line to end with
                // `0x0A`; reject fail-closed.
                throw .envelopeMalformed
            }
            let lineBytes = Data(exportBytes[cursor ..< lineEnd])
            if lineBytes.isEmpty {
                throw .envelopeMalformed
            }
            entries.append(try recoverEntry(fromLine: lineBytes))
            cursor = exportBytes.index(after: lineEnd)
        }
        return entries
    }

    /// Splits an ordered entry stream into ordered batches under
    /// `policy`. Each batch is closed when adding the next entry
    /// would exceed `policy.maxEntryCount` or `policy.maxByteCount`;
    /// the boundary helper enforces equal-to-cap fits and
    /// strictly-greater starts the next batch. An oversized single
    /// entry whose payload alone exceeds the byte cap surfaces
    /// ``RemoteDeliveryError/batchSizeExceeded(limit:actual:)`` from
    /// the helper rather than being treated as a perpetual boundary.
    static func makeBatches(
        from entries: [RemoteDeliveryEntry],
        policy: RemoteBatchPolicy
    ) throws(RemoteDeliveryError) -> [[RemoteDeliveryEntry]] {
        try makeBatches(
            from: entries,
            boundaryExceeded: policy.wouldExceed
        )
    }

    /// Engine-internal batching loop parameterized by the boundary
    /// decision so the test target can exercise the empty-`current`
    /// invariant guard without weakening
    /// ``RemoteBatchPolicy/wouldExceed(currentEntryCount:currentByteCount:nextEntryByteCount:)``.
    /// Production callers always reach this through
    /// ``makeBatches(from:policy:)``; the boundary closure overload
    /// exists so test code can inject a deterministic
    /// `wouldExceed`-shaped decision (e.g. one that returns `true`
    /// over an empty running batch) and assert the batcher refuses
    /// fail-closed.
    internal static func makeBatches(
        from entries: [RemoteDeliveryEntry],
        boundaryExceeded: (Int, Int, Int) throws(RemoteDeliveryError) -> Bool
    ) throws(RemoteDeliveryError) -> [[RemoteDeliveryEntry]] {
        var batches: [[RemoteDeliveryEntry]] = []
        var current: [RemoteDeliveryEntry] = []
        var currentByteCount = 0
        for entry in entries {
            let nextSize = entry.payload.count
            let exceeded = try boundaryExceeded(
                current.count, currentByteCount, nextSize
            )
            if exceeded {
                // The boundary helper already throws
                // `.batchSizeExceeded` when a single entry's bytes
                // alone exceed the cap. Reaching this branch means
                // the cap fits the entry but not on top of the
                // running batch, so we close `current` and start a
                // fresh batch with this entry. A boundary
                // `exceeded == true` over an empty `current` would
                // emit an empty batch — an engine-side invariant
                // violation forbidden by the batching contract —
                // so the batcher refuses fail-closed rather than
                // relying on the helper's first-entry contract.
                guard !current.isEmpty else {
                    throw .invalidBatchState
                }
                batches.append(current)
                current = [entry]
                currentByteCount = nextSize
            } else {
                // `boundaryExceeded == false` already proves the
                // sum fits both the byte cap and Swift's `Int`
                // range, but the queue update path keeps an
                // overflow-safe add so a future change to the
                // boundary helper cannot silently trap or wrap
                // here. An overflow that nonetheless slips through
                // surfaces as `.invalidBatchState` — an
                // engine-side invariant violation, not a
                // caller-actionable failure.
                let (updatedByteCount, overflow) =
                    currentByteCount.addingReportingOverflow(nextSize)
                guard !overflow else {
                    throw .invalidBatchState
                }
                current.append(entry)
                currentByteCount = updatedByteCount
            }
        }
        if !current.isEmpty {
            batches.append(current)
        }
        return batches
    }

    /// Reads one envelope-line, validates the envelope `contentType`,
    /// base64-decodes the queue-record bytes, then decodes the full
    /// record in a single keyed-container pass that inspects
    /// ``DurableRemoteQueueRecord/formatVersion`` before reading any
    /// other field. The projection into ``RemoteDeliveryEntry``
    /// happens at the call site.
    private static func recoverEntry(
        fromLine lineBytes: Data
    ) throws(BatchEngineError) -> RemoteDeliveryEntry {
        let envelope = try decodeEnvelope(lineBytes: lineBytes)
        let recordBytes = try decodeRecordBytes(payloadBase64: envelope.payload)
        let record = try decodeRecord(recordBytes: recordBytes)
        return RemoteDeliveryEntry(
            identifier: record.identifier,
            payload: record.payload,
            metadata: record.metadata
        )
    }

    private static func decodeEnvelope(
        lineBytes: Data
    ) throws(BatchEngineError) -> ExportEnvelope {
        let envelope: ExportEnvelope
        do {
            envelope = try JSONDecoder().decode(
                ExportEnvelope.self, from: lineBytes
            )
        } catch {
            throw .envelopeMalformed
        }
        // Validate the envelope's `contentType` against the queue-
        // owned constant before treating its `payload` as queue-
        // record bytes. A foreign envelope (e.g. a future persistence
        // package writing a different envelope kind into the export
        // path) is refused fail-closed rather than being decoded as
        // a malformed queue record.
        let expected = DurableRemoteQueue.envelopeContentType
        guard envelope.contentType == expected else {
            throw .envelopeContentTypeMismatch(
                expected: expected, found: envelope.contentType
            )
        }
        return envelope
    }

    private static func decodeRecordBytes(
        payloadBase64: String
    ) throws(BatchEngineError) -> Data {
        guard let bytes = Data(base64Encoded: payloadBase64) else {
            throw .recordPayloadBase64Invalid
        }
        return bytes
    }

    /// Single decode pass that inspects `formatVersion` first and
    /// only proceeds to read `identifier`, `payload`, `metadata`
    /// from the same keyed container once the schema version is
    /// the one this engine recognizes. Schema-version diagnostics
    /// are raised from ``ParsedRecord/init(from:)`` as
    /// ``BatchEngineError`` and re-thrown by the typed `catch`;
    /// any other decoder failure (malformed JSON, type mismatch on
    /// `identifier` / `payload` / `metadata`) collapses into
    /// ``BatchEngineError/recordPayloadMalformed``.
    private static func decodeRecord(
        recordBytes: Data
    ) throws(BatchEngineError) -> ParsedRecord {
        do {
            return try JSONDecoder().decode(
                ParsedRecord.self, from: recordBytes
            )
        } catch let error as BatchEngineError {
            throw error
        } catch {
            throw .recordPayloadMalformed
        }
    }
}

/// Parser-internal view of a ``DurableRemoteQueueRecord`` that
/// folds schema-version validation and field decoding into one
/// keyed-container pass.
///
/// `formatVersion` is read first as `UInt64?` so an integer outside
/// the current `UInt8` schema-version space (e.g. `999`) routes to
/// ``BatchEngineError/recordFormatVersionUnsupported(found:supported:)``
/// with the actual decoded value, and a missing field routes to
/// ``BatchEngineError/recordFormatVersionMissing`` — neither case
/// collapses into the generic ``BatchEngineError/recordPayloadMalformed``.
/// The remaining fields are decoded from the same container only
/// after the version check passes; the type intentionally never
/// exposes the raw `formatVersion` because the engine consumes
/// only the post-validation projection.
private struct ParsedRecord: Decodable {
    let identifier: UInt64
    let payload: Data
    let metadata: [String: String]

    private enum CodingKeys: String, CodingKey {
        case formatVersion
        case identifier
        case payload
        case metadata
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let rawVersion = try container.decodeIfPresent(
            UInt64.self, forKey: .formatVersion
        )
        guard let found = rawVersion else {
            throw BatchEngineError.recordFormatVersionMissing
        }
        let supported = DurableRemoteQueueRecord.currentFormatVersion
        // Compare in `UInt64` space so a record carrying a
        // formatVersion outside the current `UInt8` schema range
        // (e.g. `999`) routes to `.recordFormatVersionUnsupported`
        // with the actual decoded value, not to
        // `.recordPayloadMalformed`.
        guard found == UInt64(supported) else {
            throw BatchEngineError.recordFormatVersionUnsupported(
                found: found, supported: supported
            )
        }
        identifier = try container.decode(
            UInt64.self, forKey: .identifier
        )
        payload = try container.decode(Data.self, forKey: .payload)
        metadata = try container.decode(
            [String: String].self, forKey: .metadata
        )
    }
}

/// Minimal Codable view of a persistence envelope that the
/// batching parser needs: the `contentType` field (validated
/// against the queue-owned constant) plus the base64 `payload`
/// field. Other envelope fields (`id`, `sequence`, `createdAt`,
/// `hints`) are part of the persistence wire contract but the
/// batching engine does not consume them.
private struct ExportEnvelope: Decodable {
    let contentType: String
    let payload: String
}
