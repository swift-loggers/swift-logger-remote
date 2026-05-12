/// Outcome summary produced by ``RemoteEngine/flush()``.
///
/// Reports per-classification entry counts plus the engine's
/// acknowledgement decision for the flush pass. The summary is
/// sink-neutral: it carries no transport status codes, no vendor
/// body details, and no per-entry diagnostics — those stay
/// sink-owned inside adapter classifiers (LGR-7, LGR-9).
public struct RemoteFlushSummary: Sendable, Equatable {
    /// Number of batches the engine iterated through for the
    /// flush pass. Equal to the number of groups
    /// `BatchEngine.makeBatches(from:policy:)` produced from the
    /// drained entry stream. `0` on an empty drain.
    public let attemptedBatches: Int

    /// Total entries the loop processed across every batch in the
    /// drained prefix. `0` on an empty drain.
    public let attemptedEntries: Int

    /// Number of entries the classifier ultimately mapped to
    /// `RemoteDeliveryResult.success`.
    public let succeededEntries: Int

    /// Number of entries the classifier ultimately mapped to
    /// `RemoteDeliveryResult.terminal(reason:)`. The engine does
    /// not retry these and treats them as resolved for the
    /// acknowledgement decision.
    public let terminalEntries: Int

    /// Number of entries whose final classification was still
    /// `RemoteDeliveryResult.retryable(reason:)` after the per-entry
    /// retry budget (``RemoteRetryPolicy/maxAttempts``) was
    /// exhausted. A non-zero value forces ``acknowledgement`` to
    /// be ``RemoteFlushAcknowledgement/notAcknowledged``; the
    /// queue's outstanding batch is held so the next flush can
    /// retry the same drained bytes.
    public let retryableEntries: Int

    /// Engine's acknowledgement decision for the flush pass.
    /// See ``RemoteFlushAcknowledgement`` for the three cases
    /// the engine can report.
    public let acknowledgement: RemoteFlushAcknowledgement

    public init(
        attemptedBatches: Int,
        attemptedEntries: Int,
        succeededEntries: Int,
        terminalEntries: Int,
        retryableEntries: Int,
        acknowledgement: RemoteFlushAcknowledgement
    ) {
        self.attemptedBatches = attemptedBatches
        self.attemptedEntries = attemptedEntries
        self.succeededEntries = succeededEntries
        self.terminalEntries = terminalEntries
        self.retryableEntries = retryableEntries
        self.acknowledgement = acknowledgement
    }
}

/// Engine acknowledgement decision the public
/// ``RemoteEngine/flush()`` surface reports through
/// ``RemoteFlushSummary/acknowledgement``.
///
/// Distinguishes the three lifecycle outcomes the engine reaches
/// for the flush pass: empty-drain release, non-empty resolved
/// (every entry `.success` or `.terminal` → destructive removal
/// ran), and non-empty unresolved (any `.retryable` budget
/// exhausted → outstanding boundary held for the next flush).
public enum RemoteFlushAcknowledgement: Sendable, Equatable {
    /// The flush captured a zero-byte drain
    /// (``DurableRemoteQueueBatch/byteCount`` `== 0`). The engine
    /// released the in-memory outstanding-batch boundary by
    /// calling ``DurableRemoteQueue/acknowledge()``; no delivered
    /// queue payload bytes were removed because none existed.
    case emptyReleased

    /// The flush drained a non-empty batch, every recovered
    /// entry reached a resolved classification
    /// (``RemoteDeliveryResult/success`` or
    /// ``RemoteDeliveryResult/terminal(reason:)``), and the
    /// engine invoked ``DurableRemoteQueue/acknowledge()`` —
    /// persistence dropped the delivered queue payload bytes.
    /// Acknowledgement is final; the engine never re-reads these
    /// bytes.
    case removedDeliveredBytes

    /// The flush drained a non-empty batch but at least one
    /// recovered entry exhausted the retry budget without
    /// resolution. The engine did not acknowledge; the queue's
    /// outstanding-batch boundary is held so the next
    /// ``RemoteEngine/flush()`` replays the same drained bytes
    /// through the outstanding-reuse path.
    case notAcknowledged
}
