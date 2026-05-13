# API Design

This document defines the current API-observable contract surface for
the remote-delivery engine. Public API is locked by accepted API/spec
review, then verified by implementation and conformance tests.

`APIDesign.md` owns API shape and API-observable contracts only.
Persistence/file-format contracts live in
[`swift-logger-persistence/Docs/FileFormatSpec.md`](https://github.com/swift-loggers/swift-logger-persistence/blob/main/Docs/FileFormatSpec.md).

## Current Scope

`0.1.0` locks the M3.4 remote-delivery engine surface: contract
value types, the persistence-backed ``DurableRemoteQueue``,
engine-internal batching and retry execution, batch-round dispatch
through ``RemoteTransport.sendBatch(_:)``, the sink-owned
``RemoteTransport.classify(_:)`` classification hook, and the public
``RemoteEngine.flush()`` lifecycle that closes acknowledgement-to-removal
for non-empty flush passes.
`0.1.0` closes M3.4.
The `swift-logger-elastic` migration and the vendor-specific
adapter family ship in later milestones.

## Engine Boundary

The remote engine is **sink-neutral**:

- Accepted ordering is owned by `swift-logger-persistence` `0.1.x`,
  consumed in this package exclusively through ``DurableRemoteQueue``
  (LGR-10). The engine never creates a second store for accepted
  persistence bytes; its only on-disk copy outside the store is the
  queue-owned byte-stable export artifact.
- Delivery acknowledgement is the **only** trigger for destructive
  removal of delivered queue payload bytes from the persistence layer
  (``DurableRemoteQueue.acknowledge()``, LGR-11). Retention policy
  is a separate concern owned by the persistence layer and, in the
  `0.1.0` scope, never consumes the acknowledgement boundary.
- Vendor-specific encoders, request builders, and response validators
  live in adapter packages (Elastic, Splunk HEC, Loki, Datadog Logs,
  Dynatrace Log Monitoring) — **not** inside the core engine.
- The engine does **not** know whether the wire format is NDJSON
  bulk, single JSON event, line-delimited proto, or anything else.

## Persistence Coupling

The package depends on
[`swift-loggers/swift-logger-persistence`](https://github.com/swift-loggers/swift-logger-persistence)
at the released `0.1.x` SemVer line via
`.upToNextMinor(from: "0.1.0")`. The dependency is bounded to the
persistence package's public products; no branch, revision, or SHA
pin is used.

``DurableRemoteQueue`` is the only type in `LoggerRemote` that
touches the persistence layer. Internally it owns a
`FileLogStore` configured against a caller-supplied directory and:

- wraps each ``RemoteDeliveryEntry`` (identifier + payload +
  metadata) into a package-owned queue record (internal persistence
  payload wrapper) encoded as the persistence `payload` so the
  engine-local identifier and sink-owned metadata survive the
  byte-stable export boundary losslessly;
- assigns persistence `sequence` values from a queue-private
  monotonic allocator, persisted only through persistence sequence
  fields, so ``RemoteDeliveryEntry.identifier`` retains its
  "engine-local correlation only, no ordering contract" shape from
  PR 1/N;
- captures the current recoverable prefix as a byte-stable export
  file through `FileLogStore.exportLogs(to:)`;
- consumes the in-memory removal boundary through
  `FileLogStore.removeExportedLogs()` when the engine acknowledges
  a fully-resolved non-empty flush pass.

```text
public actor DurableRemoteQueue {
    public init(
        directory: URL,
        rotation: RotationPolicy = .never
    )

    public func enqueue(
        _ entry: RemoteDeliveryEntry
    ) async throws(DurableRemoteQueueError)

    public func flush() async throws(DurableRemoteQueueError)

    public func drain(
        to exportURL: URL
    ) async throws(DurableRemoteQueueError) -> DurableRemoteQueueBatch

    public func acknowledge() async throws(DurableRemoteQueueError)

    // Internal: outstanding-batch inspection is reserved for
    // engine-internal delivery machinery and is not part of the
    // PR 2/N public surface.
}

public struct DurableRemoteQueueBatch: Sendable, Equatable {
    public let exportURL: URL
    public let byteCount: UInt64
}

public enum DurableRemoteQueueError: Error, Sendable, Equatable {
    case batchAlreadyOutstanding
    case recordEncodingFailed
    case sequenceExhausted
    case envelopeRejected(PersistentLogEnvelopeValidationError)
    case enqueueFailed(FileLogStoreError)
    case flushFailed(FileLogStoreError)
    case drainFailed(FileLogStoreExportError)
    case drainSizeReadFailed
    case acknowledgeFailed(FileLogStoreRemoveError)
}
```

The persistence envelope's `contentType` is queue-owned (locked
as a queue-internal constant) so the batching engine's parser can
validate every recovered envelope fail-closed against the same
constant. Caller-customizable content types would let the parser
silently accept envelopes the queue did not produce.

The queue scope remains intentionally narrow:

- The queue never exposes a `retention` parameter. Persistence
  retention (`.maxSegments`, `.maxTotalBytes`, `.maxAge`) could
  delete bytes that were never acknowledged by the delivery loop,
  which would violate LGR-11. The queue hardcodes
  `RetentionPolicy.unlimited` for the queue-owned `FileLogStore`; a
  bounded queue policy is outside the `0.1.0` contract and separate
  from persistence retention.
- The queue admits one outstanding-batch boundary at a time. A second
  ``DurableRemoteQueue.drain(to:)`` before
  ``DurableRemoteQueue.acknowledge()`` succeeds surfaces
  ``DurableRemoteQueueError.batchAlreadyOutstanding`` and leaves
  the persistence-layer removal boundary intact. A failed
  `acknowledge()` keeps the outstanding-batch boundary held so the
  caller can reuse it; only a successful `acknowledge()` clears the
  state and re-opens drain.
- ``DurableRemoteQueueBatch.byteCount`` is the exact post-export
  file size. The queue rejects a drain whose post-export size
  cannot be read (``DurableRemoteQueueError.drainSizeReadFailed``)
  rather than reporting `0`.
- ``DurableRemoteQueue.drain(to:)`` writes a byte-stable export
  whose payload bytes are queue records, not raw transport input.
  The engine-internal `BatchEngine` parser recovers the original
  ``RemoteDeliveryEntry`` (identifier + payload + metadata) from
  the queue record. The queue itself ships no envelope parser and
  exposes no public export-inspection or query API; envelope parsing is a
  `BatchEngine` concern.
- Every persisted queue record carries an explicit
  `formatVersion: UInt8` schema-evolution anchor (initial value
  `1`). The engine-internal `BatchEngine` parser MUST inspect this
  queue-record field before decoding any other queue-record field and
  MUST refuse to interpret an unknown version fail-closed rather than
  treating new fields as missing.
  Adding new fields to the record requires a `formatVersion` bump
  in the same commit for persisted queue-record compatibility.
- The queue's private persistence-sequence allocator never wraps
  to `0`. Once the allocator reaches `UInt64.max` the next
  ``DurableRemoteQueue.enqueue(_:)`` surfaces
  ``DurableRemoteQueueError.sequenceExhausted``; the caller must
  rotate the queue to a fresh directory. The persistence layer
  reserves `sequence == 0` for accepted envelopes and rejects it
  during envelope validation.

## Batching Engine (engine-internal)

The batching engine is internal machinery the engine-internal
execution loop drives. Its public surface is the existing
``RemoteBatchPolicy`` value type; the parser and the batcher are
both `internal` and reached through `@testable import` in the
test target. PR 3/N intentionally adds no new public types
because the queue surface (PR 2/N) and the batch-policy value
type (PR 1/N) already carry the public contract.

Two pure steps drive one drained queue export:

1. **`BatchEngine.recoverEntries(from:)`** parses a
   ``DurableRemoteQueueBatch`` (or its export bytes) into an
   ordered array of ``RemoteDeliveryEntry``. Each line's envelope
   `contentType` is validated against the queue-owned constant
   before its `payload` is treated as queue-record bytes; a
   foreign envelope is refused fail-closed for the queue-owned content type
   (``BatchEngineError.envelopeContentTypeMismatch(expected:found:)``).
   The parser then preserves accepted ordering from the byte-stable
   queue export and duplicate-identifier multiplicity verbatim,
   and inspects each queue record's
   ``DurableRemoteQueueRecord.formatVersion`` schema-evolution
   anchor before decoding any other queue-record field. A missing
   or unknown version is refused fail-closed
   (``BatchEngineError.recordFormatVersionMissing`` or
   ``BatchEngineError.recordFormatVersionUnsupported(found:supported:)``).
2. **`BatchEngine.makeBatches(from:policy:)`** splits the entry
   stream into ordered batches under ``RemoteBatchPolicy``:
   equal-to-cap fits in the current batch and strictly-greater
   starts the next; an oversized single entry whose payload alone
   exceeds the byte cap surfaces
   ``RemoteDeliveryError.batchSizeExceeded(limit:actual:)`` from
   the existing boundary helper, not as a perpetual boundary.

The batching engine never deduplicates, sorts, or classifies
entries; it never calls ``DurableRemoteQueue.acknowledge()`` and never
performs destructive removal of delivered queue payload bytes.
Caller-driven lifecycle closure belongs to ``RemoteEngine.flush()``;
production transport adapter
integration belongs to adapter packages and later milestones.

## Retry / Execution Loop (engine-internal)

The retry / execution loop is engine-internal machinery for
caller-driven delivery passes. The public surface is made up of the
``RemoteRetryPolicy``, ``RemoteBatchPolicy``, ``RemoteDeliveryEntry``,
``RemoteDeliveryResult``, ``RemoteTransport``,
``RemoteTransportResponse``, and ``RemoteTransportBatchItem`` public
types and protocols; the engine-internal loop has no public types
of its own.

The loop drives each batch as **batch rounds** against
``RemoteTransport.sendBatch(_:)``. Round 1 dispatches every entry in
the batch; later rounds dispatch only entries classified
``RemoteDeliveryResult.retryable(reason:)`` in the immediately
preceding round, preserving drained-export order. The dispatcher
stops when every entry resolves
(``RemoteDeliveryResult.success`` or
``RemoteDeliveryResult.terminal(reason:)``) or when each active entry
has consumed `retryPolicy.maxAttempts`. The batch model is the
`0.1.0` engine contract: there is no per-entry sequential retry path
and no opt-in dispatch-shape flag.

Batch-round dispatcher invariants:

- **Retry accounting.** Each active entry records `+1` attempt for
  each round in which it is dispatched; the recorded count is the
  number of active rounds within the current flush pass.
- **Retry progression.** Resolved entries are not re-presented to
  the transport. Before every retry round,
  ``RemoteRetryPolicy.delayBeforeRetry(attempt:)`` is consulted using
  each active entry's just-completed retryable attempt count, and the
  injected `sleep` closure applies the delay. No sleep fires after the
  final round, and retry-delay state never crosses flush-pass
  boundaries.
- **Response mapping.** The dispatcher pairs
  ``RemoteTransport.sendBatch(_:)``'s returned `Result` array with
  the round's active items by position. A count mismatch is
  an adapter-contract violation surfaced fail-closed as
  ``BatchDeliveryError.transportBatchCountMismatch(expected:actual:)``,
  projected to public
  ``RemoteEngineError.transportBatchInvalid(expected:actual:)``. If
  ``RemoteTransport.sendBatch(_:)`` itself throws, the dispatcher routes
  every active item through
  ``RemoteTransport.classify(_:)`` with the same
  `.failure(error)` value in input order and counts the round toward each
  active item's retry budget.
- **Classification constraints.** ``RemoteTransport.classify(_:)`` is
  deterministic for the same transport result from the same adapter
  implementation within a flush pass. It MUST NOT mutate queue
  acknowledgement state or export-file lifecycle state directly or indirectly
  (LGR-5 / LGR-7 / LGR-9).

`ExecutionLoop.runOnce(queue:exportURL:batchPolicy:retryPolicy:transport:sleep:afterDrain:)`
composes one full delivery pass:

- ``DurableRemoteQueue.drain(to:)`` captures the current
  recoverable prefix as a byte-stable export.
- `afterDrain` is an engine-internal/test seam invoked after a
  successful drain and before empty/non-empty handling. It does
  not own acknowledgement or destructive removal. The closure is
  non-throwing, so ``ExecutionLoopError`` has no corresponding
  error case.
- `BatchEngine.recoverEntries(from:)` parses the export back
  into an ordered ``RemoteDeliveryEntry`` stream.
- `BatchEngine.makeBatches(from:policy:)` splits the stream
  under ``RemoteBatchPolicy``.
- Each batch is driven through the batch-round dispatcher
  described above, yielding one ``RemoteDeliveryAttempt`` per
  entry in drained-export order.
- The aggregated ``RemoteDeliveryAttempt`` array is returned
  deterministically in batch traversal order and entry order
  within each batch. An empty queue produces `[]`.

For non-empty exports, the execution loop never releases the
outstanding-batch boundary and never performs destructive removal of
delivered queue payload bytes. That acknowledgement-to-removal closure
is owned by ``RemoteEngine.flush()`` above this layer. A boundary held
by the queue from a preceding `runOnce` call surfaces as
``DurableRemoteQueueError.batchAlreadyOutstanding`` on the next
`runOnce` until engine-owned lifecycle code acknowledges it.

The empty-drain path is intentionally different. When
``DurableRemoteQueueBatch.byteCount`` is `0`, ``runOnce``
short-circuits before ``BatchEngine.recoverEntries(from:)`` and
releases the outstanding-batch boundary with
``DurableRemoteQueue.acknowledge()`` before returning `[]`. The release
is keyed off the queue's authoritative zero-byte drain signal, so a
missing or unreadable export artifact cannot block it. No delivered
queue payload bytes are removed for the empty export. A release failure
surfaces as ``ExecutionLoopError.emptyBatchReleaseFailed(_:)`` rather
than being masked as ``ExecutionLoopError.drainFailed(_:)``.

```text
internal struct RemoteDeliveryAttempt: Sendable, Equatable {
    let entry: RemoteDeliveryEntry
    let outcome: RemoteDeliveryResult
    let attempts: Int
}

internal enum BatchDeliveryError: Error, Sendable, Equatable {
    case recoverFailed(BatchEngineError)
    case batchSplitFailed(RemoteDeliveryError)
    case invalidRetryDelay(RemoteDeliveryError)
    case sleepInterrupted
    case internalBatchStateInvalid
    case transportBatchCountMismatch(expected: Int, actual: Int)
}

internal enum ExecutionLoopError: Error, Sendable, Equatable {
    case drainFailed(DurableRemoteQueueError)
    case recoverFailed(BatchEngineError)
    case batchSplitFailed(RemoteDeliveryError)
    case invalidRetryDelay(RemoteDeliveryError)
    case emptyBatchReleaseFailed(DurableRemoteQueueError)
    case sleepInterrupted
    case internalBatchStateInvalid
    case transportBatchCountMismatch(expected: Int, actual: Int)
}

internal struct BatchDeliveryOutcome: Sendable, Equatable {
    let batchCount: Int
    let attempts: [RemoteDeliveryAttempt]
}

internal enum ExecutionLoop {
    static func runOnce(
        queue: DurableRemoteQueue,
        exportURL: URL,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy,
        transport: any RemoteTransport,
        sleep: @Sendable (Double) async throws -> Void,
        afterDrain: @Sendable (DurableRemoteQueueBatch) async -> Void = { _ in }
    ) async throws(ExecutionLoopError) -> [RemoteDeliveryAttempt]

    static func deliver(
        batch: DurableRemoteQueueBatch,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy,
        transport: any RemoteTransport,
        sleep: @Sendable (Double) async throws -> Void,
        delayCalculator: @Sendable (RemoteRetryPolicy, Int) throws(RemoteDeliveryError) -> Double
            = { policy, attempt in try policy.delayBeforeRetry(attempt: attempt) }
    ) async throws(BatchDeliveryError) -> BatchDeliveryOutcome
}
```

## Contract Value Types

`RemoteDeliveryEntry.identifier` is an engine-local correlation
identifier only. It is not a persistence sequence identity and carries
no ordering contract.

`RemoteTransportResponse` does not expose an HTTP status code field.
Status codes, vendor body codes, and other transport-specific
signals are adapter-owned and travel as `responseMetadata` entries
(or stay inside the adapter's classifier). They are not core fields
on the engine surface.

`RemoteDeliveryError.transportRejected` is the sink-neutral signal
for a transport-classified delivery failure. The engine does not
expose a status-code variant; that classification belongs to the
adapter, including whole-batch ``RemoteTransport.sendBatch(_:)``
failures projected through ``RemoteTransport.classify(_:)``.

```text
public struct RemoteDeliveryEntry: Sendable, Equatable {
    /// Engine-local correlation identifier only. Not a persistence
    /// sequence identity; carries no ordering contract.
    public let identifier: UInt64
    public let payload: Data
    public let metadata: [String: String]
}

public enum RemoteDeliveryResult: Sendable, Equatable {
    case success
    case retryable(reason: RemoteDeliveryError)
    case terminal(reason: RemoteDeliveryError)
}

public struct RemoteRetryPolicy: Sendable, Equatable {
    public static let maxSupportedAttempts: Int
    public static let maxBackoffSeconds: Double
    public let maxAttempts: Int
    public let backoff: BackoffSchedule
    public enum BackoffSchedule: Sendable, Equatable {
        case constant(seconds: Double)
        case exponential(initialSeconds: Double, multiplier: Double, capSeconds: Double)
    }
}
// Retry policy is the public contract model with locked validation
// bounds (`1 ... maxSupportedAttempts`, backoff fields finite,
// positive, and `<= maxBackoffSeconds`; exponential `multiplier > 1`
// and `capSeconds >= initialSeconds`). The pure `delayBeforeRetry`
// calculation is engine-side machinery and is not part of the
// public API surface; retry execution is driven by the
// engine-internal `ExecutionLoop` batch-round dispatcher over the
// queue + batching + transport primitives. `maxAttempts` bounds each
// retained entry's active dispatch rounds within a batch.

public struct RemoteBatchPolicy: Sendable, Equatable {
    public let maxEntryCount: Int
    public let maxByteCount: Int
}
// Batch policy is the public contract model. The boundary helper is
// engine-side machinery and is not part of the public API surface.
// An oversized single entry whose byte count alone exceeds the byte
// cap is rejected as `.batchSizeExceeded(limit:actual:)` rather than
// treated as a perpetual boundary.

public protocol RemoteTransport: Sendable {
    // `sendBatch(_:)` is the engine's only transport dispatch
    // primitive. It must return exactly one result per input item,
    // in the same order. A count mismatch is an adapter-contract
    // violation the engine fails closed on as
    // `RemoteEngineError.transportBatchInvalid(expected:actual:)`.
    func sendBatch(
        _ items: [RemoteTransportBatchItem]
    ) async throws -> [Result<RemoteTransportResponse, any Error>]

    // Sink-owned response classification. `0.1.0` locks this hook
    // as part of the public engine surface. Classification must be
    // deterministic for the same transport result from the same
    // adapter implementation within a flush pass. Repeated
    // classification invocations for the same result within a flush
    // pass MUST return the same decision. It MUST NOT mutate queue
    // acknowledgement state or export-file lifecycle state directly
    // or indirectly.
    func classify(
        _ result: Result<RemoteTransportResponse, any Error>
    ) async -> RemoteDeliveryResult
}

public struct RemoteTransportBatchItem: Sendable, Equatable {
    public let payloadBytes: Data
    public let payloadMetadata: [String: String]

    public init(
        payloadBytes: Data,
        payloadMetadata: [String: String] = [:]
    )
}

public struct RemoteTransportResponse: Sendable, Equatable {
    public let responseBytes: Data
    public let responseMetadata: [String: String]

    public init(
        responseBytes: Data,
        responseMetadata: [String: String] = [:]
    )
}

public enum RemoteDeliveryError: Error, Sendable, Equatable {
    case batchEmpty
    case batchSizeExceeded(limit: Int, actual: Int)
    case invalidRetryPolicy
    case invalidBatchPolicy
    case invalidBatchState
    case transportRejected
    case acknowledgementMissing
}
```

## Engine Surface

``RemoteEngine`` is the public delivery surface. It is a sink-
neutral, caller-driven dispatch actor:

- **Caller-driven.** The engine owns no timer, no platform
  lifecycle observer, no autonomous scheduler. Host applications
  decide when to call ``flush()`` (LGR-3). Concurrent ``flush()``
  invocations serialize through actor isolation; the engine never
  runs two passes against the same queue simultaneously.
- **Sink-neutral.** ``RemoteTransport`` owns both
  ``sendBatch(_:)`` and ``classify(_:)``; the engine never
  inspects HTTP status, vendor body codes, or transport error
  types (LGR-5 / LGR-7 / LGR-9). The adapter decides the wire
  shape and returns one per-item ``Result`` in input order.
  Classification output drives engine lifecycle decisions; the
  hook's determinism and side-effect constraints are locked on
  ``RemoteTransport``.
- **Flush lifecycle and retained export artifact.** The engine takes
  an `exportDirectory: URL`; the caller owns the directory, and the
  engine writes only unique scratch export files inside it. Each
  ``flush()`` first checks
  ``DurableRemoteQueue.currentOutstandingBatch()``. If the queue
  holds an outstanding-batch boundary, the engine reuses the retained
  export artifact without flushing or draining new queue bytes. If no
  boundary exists, the engine calls ``DurableRemoteQueue.flush()``,
  creates a fresh scratch export file, and drains through
  ``DurableRemoteQueue.drain(to:)``.
- **Acknowledgement rule (LGR-11).** After a non-empty flush pass,
  the engine acknowledges only when every recovered entry resolves as
  ``RemoteDeliveryResult.success`` or
  ``RemoteDeliveryResult.terminal(reason:)``. That returns
  ``RemoteFlushAcknowledgement.removedDeliveredBytes``. Any
  ``RemoteDeliveryResult.retryable(reason:)`` outcome returns
  ``RemoteFlushAcknowledgement.notAcknowledged`` and leaves the
  outstanding-batch boundary held.
  ``RemoteDeliveryResult.terminal(reason:)`` is sink-decided
  permanent failure, so removing those bytes is forward progress.
- **Cleanup and empty drains.** The retained export artifact is
  removed only after a successful empty release or a successful
  non-empty acknowledgement. Cleanup failures surface as
  ``RemoteEngineError.exportCleanupFailed(_:)`` after the phase's
  acknowledgement state is final and MUST NOT trigger retry of
  already acknowledged delivered queue payload bytes. On
  ``DurableRemoteQueueBatch.byteCount`` `== 0`, the execution loop
  releases the outstanding-batch boundary, returns
  ``RemoteFlushAcknowledgement.emptyReleased`` with
  ``RemoteFlushSummary.attemptedBatches`` `== 0`, and removes no
  delivered queue payload bytes.

```text
public actor RemoteEngine {
    public init(
        queue: DurableRemoteQueue,
        exportDirectory: URL,
        transport: any RemoteTransport,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy
    )

    public func flush() async throws(RemoteEngineError) -> RemoteFlushSummary
}

public struct RemoteFlushSummary: Sendable, Equatable {
    public let attemptedBatches: Int
    public let attemptedEntries: Int
    public let succeededEntries: Int
    public let terminalEntries: Int
    public let retryableEntries: Int
    public let acknowledgement: RemoteFlushAcknowledgement

    public init(
        attemptedBatches: Int,
        attemptedEntries: Int,
        succeededEntries: Int,
        terminalEntries: Int,
        retryableEntries: Int,
        acknowledgement: RemoteFlushAcknowledgement
    )
}

public enum RemoteFlushAcknowledgement: Sendable, Equatable {
    case emptyReleased
    case removedDeliveredBytes
    case notAcknowledged
}

public enum RemoteEngineError: Error, Sendable, Equatable {
    // Public projection of a `DurableRemoteQueue.flush()` failure.
    // The engine does not reach through the queue boundary or expose
    // `FileLogStoreError` directly.
    case flushFailed(DurableRemoteQueueError)

    // Public projection of a `DurableRemoteQueue.drain(to:)`
    // failure before the engine owns a reusable drained batch
    // reference for the current flush pass.
    case drainFailed(DurableRemoteQueueError)

    case parseFailed(RemoteEngineParseError)

    // Public projection of batch construction / policy failure
    // from `BatchEngine.makeBatches(from:policy:)`, such as an
    // oversized entry or invalid batch state. This is not a
    // transport failure; transport batch count mismatches surface
    // through `transportBatchInvalid`.
    case batchFailed(RemoteDeliveryError)

    case retryInterrupted(RemoteEngineRetryError)

    // Acknowledgement failed after an outstanding-batch boundary was
    // captured. The queue keeps the boundary held, and subsequent
    // flushes reuse the retained export artifact until acknowledgement
    // succeeds.
    case acknowledgementFailed(DurableRemoteQueueError)

    // Export artifact cleanup failed after empty release or
    // acknowledged non-empty removal. Cleanup failure MUST NOT cause
    // retry of already acknowledged delivered queue payload bytes.
    case exportCleanupFailed(RemoteEngineExportCleanupContext)

    // `RemoteTransport.sendBatch(_:)` returned a result array whose
    // count does not match the number of items the engine handed
    // it. Adapter-contract violation; the engine fails closed so the
    // queue keeps the outstanding-batch boundary and the retained
    // export artifact can be reused by a later flush once the adapter
    // returns a valid per-item result count.
    case transportBatchInvalid(expected: Int, actual: Int)
}

public struct RemoteEngineExportCleanupContext: Sendable, Equatable {
    public enum Phase: Sendable, Equatable {
        case emptyRelease
        case acknowledgedNonEmpty
    }
    public let exportURL: URL
    public let phase: Phase
    // Captured from the bridged `NSError` representation.
    public let errorDomain: String
    public let errorCode: Int
}

// Public projection of the engine-internal BatchEngineError
// taxonomy. This enum preserves parser failure categories at the
// public RemoteEngine boundary; it does not define a second parser
// contract independent from the engine-internal batching parser.
// Detail-free cases such as `envelopeMalformed` intentionally do
// not expose raw JSON/parser diagnostics.
public enum RemoteEngineParseError: Error, Sendable, Equatable {
    case exportFileReadFailed
    case exportByteCountMismatch(expected: UInt64, actual: UInt64)
    case exportByteCountUnavailable
    case envelopeMalformed
    case envelopeContentTypeMismatch(expected: String, found: String)
    case recordPayloadBase64Invalid
    case recordPayloadMalformed
    case recordFormatVersionMissing
    case recordFormatVersionUnsupported(found: UInt64, supported: UInt8)
}

public enum RemoteEngineRetryError: Error, Sendable, Equatable {
    case invalidRetryDelay(RemoteDeliveryError)
    case sleepInterrupted
}
```

### Lifecycle integration (host responsibility)

The engine intentionally stays platform-neutral: it imports no
`UIKit`, no `AppKit`, no `WatchKit`, no `NotificationCenter`
observer. Host applications wire lifecycle integration on their
own side:

- iOS / iPadOS / tvOS: observe
  `UIApplication.didEnterBackgroundNotification` (or
  `BGTaskScheduler` events) and call ``RemoteEngine.flush()``.
- macOS: observe `NSWorkspace.willPowerOffNotification` or
  app-specific shutdown hooks and call ``RemoteEngine.flush()``.
- Server-side: drive ``flush()`` from a graceful-shutdown
  signal handler or a periodic task.

Because the engine actor serializes concurrent ``flush()``
invocations, hosts can call it from multiple lifecycle hooks
without coordination; overlapping calls simply queue and run
sequentially.

## Sink-Neutrality Acceptance

Adapter APIs beyond the core engine surface remain outside `0.1.0`;
before those adapter APIs are released, the engine contract must
be validated against at least two non-equivalent backend response
models so the engine is not silently shaped by a single vendor
semantics:

- **Elastic `_bulk`** — batch-aggregating: one shared NDJSON
  `_bulk` HTTP request carrying every input item, item-level
  success / failure response classification mapped back into
  one per-input ``Result`` in the engine's
  ``RemoteTransport.sendBatch(_:)`` return array. This is a
  first-class adapter shape on the `0.1.0` transport surface,
  not a workaround on top of a per-entry contract.
- **Splunk HEC** — single-event: one HTTP request per input
  item dispatched inside the adapter's ``sendBatch(_:)``
  implementation, status / body-code classification distinct
  from Elastic `_bulk` item-level results.

`0.1.0` adds these as **internal test fixtures**. The second
remote-adapter protocol sanity check lands in M4; the
HTTP-backed adapter family lands in M5.

## Out Of Scope

- No autonomous timer, scheduler, or platform lifecycle observer.
  Hosts call ``RemoteEngine.flush()`` from their own hooks; retry
  backoff remains engine-internal sleep machinery (LGR-3).
- No production network transport or vendor-specific encoder /
  request-builder / response-validator in this package (LGR-9).
  The engine dispatches only through
  ``RemoteTransport.sendBatch(_:)``; adapter-internal aggregation
  and fan-out remain adapter-owned. `0.1.0` coverage uses the
  test-only ``StubRemoteTransport`` fixture. Concrete adapters
  (Elastic `_bulk`, Splunk HEC, …) ship later.
- No `swift-logger-elastic` migration.
- No Datadog/Splunk/Loki/Dynatrace adapter packages.
- No SDK-backed adapters (Datadog mobile SDK, Splunk RUM SDK,
  Dynatrace OneAgent) — those bypass this engine by design.

## Milestone Map

| Milestone | Scope |
| --- | --- |
| M3.4 PR 3/N | Batching engine: deterministic batch construction, entry/byte caps, oversized-entry behavior, byte-stable export → entry-stream parser with fail-closed `formatVersion` validation. Engine-internal machinery. |
| M3.4 PR 4/N | Retry / execution loop: per-entry retry budget across batch rounds over the queue + batching + transport primitives via batch-round dispatch against `RemoteTransport.sendBatch(_:)`, backoff progression in `Double` seconds through an injected sleep closure between rounds, per-entry attempt accounting, terminal vs retryable routing, test-only `StubRemoteTransport` fixture explicitly marked as such (not public API). Engine-internal `ExecutionLoop`, `BatchDeliveryError`, `RemoteDeliveryAttempt`, `ExecutionLoopError` machinery. |
| M3.4 PR 5/N (landed in 0.1.0) | Public delivery surface: `RemoteEngine` actor + `flush()` over the engine-internal loop, `RemoteFlushSummary`, `RemoteEngineError` including `.transportBatchInvalid(expected:actual:)`, plus `RemoteEngineParseError` and `RemoteEngineRetryError`, `RemoteTransport.sendBatch(_:)` batch-round primary dispatch primitive, `RemoteTransport.classify(_:)` sink-owned classification, `RemoteTransportBatchItem` per-item input shape, acknowledgement-to-removal lifecycle closure for non-empty flush passes (engine acknowledges when every recovered entry across the pass is `.success` or `.terminal`; any `.retryable` keeps the boundary held for the next flush). Closes M3.4. |
| M3.5 | Migrate `swift-logger-elastic` onto this engine; Elastic-specific code stays in the adapter (ECS encoder, `_bulk` request builder, `_bulk` response validator). |
| M4 | Second remote adapter (Datadog Logs intake or Splunk HEC) as a protocol sanity check. |
| M5 | HTTP-backed adapter family built on top of this engine. |
