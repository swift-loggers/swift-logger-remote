import Foundation

/// One drained batch captured by ``DurableRemoteQueue/drain(to:)``.
///
/// Carries the export-file URL whose bytes the transport layer
/// delivers, plus a byte-count summary the engine can use for
/// batch-policy accounting. The exported bytes are encoded
/// `PersistentLogEnvelope` lines per the persistence wire format;
/// envelope parsing is a transport-layer concern that ships with
/// the engine delivery loop.
public struct DurableRemoteQueueBatch: Sendable, Equatable {
    /// URL of the file written by the underlying byte-stable export.
    public let exportURL: URL

    /// Exact post-export size of the file in bytes, read after
    /// `exportLogs(to:)` returned. Equal to
    /// `try Data(contentsOf: exportURL).count` at capture time. A
    /// drain whose post-export size cannot be measured surfaces
    /// ``DurableRemoteQueueError/drainSizeReadFailed`` rather than
    /// reporting `0`, so consumers can trust this value for
    /// batch-policy accounting without re-reading the file.
    public let byteCount: UInt64

    public init(exportURL: URL, byteCount: UInt64) {
        self.exportURL = exportURL
        self.byteCount = byteCount
    }
}
