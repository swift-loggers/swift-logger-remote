import LoggerFilePersistence
import LoggerPersistence

/// Typed diagnostic surface for ``DurableRemoteQueue`` failures.
///
/// Cases name the lifecycle step where the failure surfaced. Each
/// persistence-layer error type the queue depends on is carried as
/// an associated value so the caller can recover the underlying
/// diagnostic without parsing a string description.
public enum DurableRemoteQueueError: Error, Sendable, Equatable {
    /// `drain(to:)` was called while a previously drained batch had
    /// not yet been acknowledged. The queue keeps the existing
    /// in-memory removal boundary; the caller must `acknowledge()`
    /// the outstanding batch before draining again.
    case batchAlreadyOutstanding

    /// Encoding the package-owned internal queue record into
    /// persistence-payload bytes failed. Signals a queue-layer
    /// implementation defect, not a caller-actionable validation
    /// failure.
    case recordEncodingFailed

    /// The queue-private persistence sequence allocator has reached
    /// `UInt64.max` and cannot mint a fresh sequence value. The
    /// queue refuses to wrap to `0` (which the persistence layer
    /// reserves and rejects) and the caller must rotate the queue
    /// to a fresh directory.
    case sequenceExhausted

    /// Building a `PersistentLogEnvelope` from the encoded queue
    /// record failed pre-admission validation (timestamp shape,
    /// content-type shape, payload byte limit). The persistence-layer
    /// diagnostic is carried verbatim.
    case envelopeRejected(PersistentLogEnvelopeValidationError)

    /// Persistence-layer `append` failed after envelope validation.
    case enqueueFailed(FileLogStoreError)

    /// Persistence-layer `flush` failed.
    case flushFailed(FileLogStoreError)

    /// Persistence-layer `exportLogs(to:)` failed.
    case drainFailed(FileLogStoreExportError)

    /// `exportLogs(to:)` succeeded but the post-export file size
    /// could not be read. The drained batch's exact byte count is
    /// part of the queue contract; an unmeasurable batch is surfaced
    /// rather than reported as `0`.
    case drainSizeReadFailed

    /// Persistence-layer `removeExportedLogs()` failed. The
    /// in-memory removal boundary remains held by the persistence
    /// layer; the caller may retry `acknowledge()` against the same
    /// outstanding batch.
    case acknowledgeFailed(FileLogStoreRemoveError)
}
