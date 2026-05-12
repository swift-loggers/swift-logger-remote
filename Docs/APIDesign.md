# API Design

This document defines the current API-observable contract surface for
the remote-delivery engine. Public API is locked by accepted API/spec
review, then verified by implementation and conformance tests.

`APIDesign.md` owns API shape and API-observable contracts only.
Persistence/file-format contracts live in
[`swift-logger-persistence/Docs/FileFormatSpec.md`](https://github.com/swift-loggers/swift-logger-persistence/blob/main/Docs/FileFormatSpec.md).

## Current Scope

Current scope is contract value types (PR 1/N), a minimal
persistence-backed durable queue core (PR 2/N), engine-internal
batching machinery that recovers entries from a drained queue
export and splits them into deterministic batches under
``RemoteBatchPolicy`` (PR 3/N), an engine-internal retry /
execution loop that drives the per-entry retry budget over the
existing queue + batching + transport primitives under
``RemoteRetryPolicy`` (PR 4/N), and the public engine surface
(M3.4 PR 5/N) — ``RemoteEngine`` + ``flush()`` — that wraps the
engine-internal loop with the acknowledgement-to-removal lifecycle
closure for non-empty flush passes and the
``RemoteTransport/classify(_:)`` sink-owned classification hook.
PR 5/N closes M3.4.
The `swift-logger-elastic` migration and the vendor-specific
adapter family ship in later milestones.

## Engine Boundary

The remote engine is **sink-neutral**:

- Accepted ordering is owned by `swift-logger-persistence` `0.1.x`,
  consumed in this package exclusively through ``DurableRemoteQueue``
  (LGR-10). The engine never persists accepted bytes outside the
  persistence package except through the persistence package's
  byte-stable export contract.
- Delivery acknowledgement is the **only** trigger for destructive
  removal of accepted bytes from the persistence layer
  (``DurableRemoteQueue/acknowledge()``, LGR-11). Retention policy
  is a separate concern owned by the persistence layer and, in the
  current milestone scope, never consumes the acknowledgement
  boundary.
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
  fields, so ``RemoteDeliveryEntry/identifier`` retains its
  "engine-local correlation only, no ordering contract" shape from
  PR 1/N;
- captures the current recoverable prefix as a byte-stable export
  file through `FileLogStore.exportLogs(to:)`;
- consumes the in-memory removal boundary through
  `FileLogStore.removeExportedLogs()` when the engine acknowledges
  a drained batch.

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
  bounded queue policy is a future contract separate from persistence
  retention.
- The queue admits one outstanding (drained-but-not-yet-
  acknowledged) batch at a time. A second
  ``DurableRemoteQueue/drain(to:)`` before
  ``DurableRemoteQueue/acknowledge()`` succeeds surfaces
  ``DurableRemoteQueueError/batchAlreadyOutstanding`` and leaves
  the persistence-layer removal boundary intact. A failed
  `acknowledge()` keeps the outstanding batch held so the caller
  can retry against the still-held outstanding-batch boundary;
  only a successful `acknowledge()` clears the state and re-opens
  drain.
- ``DurableRemoteQueueBatch/byteCount`` is the exact post-export
  file size. The queue rejects a drain whose post-export size
  cannot be read (``DurableRemoteQueueError/drainSizeReadFailed``)
  rather than reporting `0`.
- ``DurableRemoteQueue/drain(to:)`` writes a byte-stable export
  whose payload bytes are queue records, not raw transport input.
  The engine-internal ``BatchEngine`` parser recovers the original
  ``RemoteDeliveryEntry`` (identifier + payload + metadata) from
  the queue record. The queue itself ships no envelope parser and
  exposes no replay/query API; envelope parsing is a
  ``BatchEngine`` concern.
- Every persisted queue record carries an explicit
  `formatVersion: UInt8` schema-evolution anchor (initial value
  `1`). The engine-internal ``BatchEngine`` parser MUST inspect this
  queue-record field before decoding any other queue-record field and
  MUST refuse to interpret an unknown version fail-closed rather than
  treating new fields as missing.
  Adding new fields to the record requires a `formatVersion` bump
  in the same commit for persisted queue-record compatibility.
- The queue's private persistence-sequence allocator never wraps
  to `0`. Once the allocator reaches `UInt64.max` the next
  ``DurableRemoteQueue/enqueue(_:)`` surfaces
  ``DurableRemoteQueueError/sequenceExhausted``; the caller must
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
   foreign envelope is refused fail-closed
   (``BatchEngineError/envelopeContentTypeMismatch(expected:found:)``).
   The parser then preserves accepted ordering from the byte-stable
   queue export and duplicate-identifier multiplicity verbatim,
   and inspects each queue record's
   ``DurableRemoteQueueRecord/formatVersion`` schema-evolution
   anchor before decoding any other queue-record field. A missing
   or unknown version is refused fail-closed
   (``BatchEngineError/recordFormatVersionMissing`` or
   ``BatchEngineError/recordFormatVersionUnsupported(found:supported:)``).
2. **`BatchEngine.makeBatches(from:policy:)`** splits the entry
   stream into ordered batches under ``RemoteBatchPolicy``:
   equal-to-cap fits in the current batch and strictly-greater
   starts the next; an oversized single entry whose payload alone
   exceeds the byte cap surfaces
   ``RemoteDeliveryError/batchSizeExceeded(limit:actual:)`` from
   the existing boundary helper, not as a perpetual boundary.

The engine never deduplicates, sorts, or classifies entries; it
never calls ``DurableRemoteQueue/acknowledge()`` and never
performs any destructive removal. Caller-driven lifecycle closure
belongs to ``RemoteEngine/flush()``; production transport adapter
integration belongs to adapter packages and later milestones.

## Retry / Execution Loop (engine-internal)

The retry / execution loop is engine-internal machinery for
caller-driven delivery passes. Its public surface is the existing
``RemoteRetryPolicy``, ``RemoteBatchPolicy``, ``RemoteDeliveryEntry``,
``RemoteDeliveryResult``, ``RemoteTransport``, and
``RemoteTransportResponse`` value types from PR 1/N; PR 4/N
intentionally adds no new public types because the contract value
types already carry the engine surface.

Two engine-internal namespaces compose the loop:

1. **`RetryExecutor.deliver(entry:transport:policy:sleep:delayCalculator:)`**
   drives one ``RemoteDeliveryEntry`` through the
   ``RemoteRetryPolicy`` budget:
   - Each attempt invokes ``RemoteTransport/send(payloadBytes:payloadMetadata:)``
     with the entry's `payload` and `metadata`. The transport call
     is per-entry; the engine never aggregates entry bytes into a
     single wire payload (LGR-5: the engine does not impose NDJSON,
     line-delimited proto, or any other wire format on adapters).
   - ``RemoteTransport/classify(_:)`` maps each transport result
     (``RemoteTransportResponse`` or thrown error) into a
     ``RemoteDeliveryResult``. Classification is sink-owned (LGR-7,
     LGR-9); the engine never inspects HTTP status, vendor body
     codes, or transport error types.
   - `.success` and `.terminal` stop attempts immediately; only
     `.retryable` consumes additional budget.
   - Backoff between two retryable attempts is calculated only
     after `.retryable` classification for the just-completed
     attempt. The delay comes from
     ``RemoteRetryPolicy/delayBeforeRetry(attempt:)`` (in seconds)
     and is applied through the injected `sleep` closure as
     `Double` seconds (LGR-3: the engine does not own the
     wall-clock timer). Sleep happens **only** between retryable
     attempts; the final attempt is followed by an immediate return
     rather than a no-op sleep.
   - A delay-calculation failure (the engine seam over
     ``RemoteRetryPolicy/delayBeforeRetry(attempt:)``) surfaces
     ``BatchDeliveryError/invalidRetryDelay(_:)`` carrying the
     underlying ``RemoteDeliveryError``. The
     ``RemoteRetryPolicy/make(maxAttempts:backoff:)`` factory
     already enforces that the default calculation accepts every
     `attempt` value the executor passes from a validated policy,
     so this case names an engine-side / seam-injected invariant
     violation and is never collapsed into
     ``BatchDeliveryError/sleepInterrupted``.
   - A sleep-injector failure between two retryable attempts
     surfaces ``BatchDeliveryError/sleepInterrupted``, distinct
     from a delay-calculation failure.
   - ``RetryExecutor.deliver(...)`` throws
     ``BatchDeliveryError`` directly. The broader
     ``ExecutionLoopError`` surface is reached only at the
     ``ExecutionLoop/runOnce(...)`` mapping layer, which projects
     the four narrow ``BatchDeliveryError`` cases plus its own
     drain / empty-release cases into a single
     ``runOnce``-callable error type.
2. **`ExecutionLoop.runOnce(queue:exportURL:batchPolicy:retryPolicy:transport:sleep:afterDrain:)`**
   composes one full delivery pass:
   - ``DurableRemoteQueue/drain(to:)`` captures the current
     recoverable prefix as a byte-stable export.
   - `afterDrain` is an engine-internal/test seam invoked after a
     successful drain and before empty/non-empty handling. It does
     not own acknowledgement or destructive removal.
   - `afterDrain` cannot fail because its closure is non-throwing, so
     ``ExecutionLoopError`` has no corresponding error case.
   - `BatchEngine.recoverEntries(from:)` parses the
     export back into an ordered ``RemoteDeliveryEntry`` stream.
   - `BatchEngine.makeBatches(from:policy:)` splits the stream
     under ``RemoteBatchPolicy``.
   - For every entry across every batch, ``RetryExecutor`` drives
     the per-entry retry loop and yields one
     ``RemoteDeliveryAttempt``.
   - The aggregated ``RemoteDeliveryAttempt`` array is returned
     deterministically in batch traversal order and entry order
     within each batch, matching the drained export traversal
     order. An empty queue produces `[]`.

The loop does **not** invoke ``DurableRemoteQueue/acknowledge()``
on a non-empty drained batch and performs no destructive removal
of delivered queue payload bytes; the acknowledgement-to-removal
lifecycle closure is owned by ``RemoteEngine/flush()`` above this
layer. A non-empty batch held by the queue from a preceding `runOnce`
call therefore surfaces as
``DurableRemoteQueueError/batchAlreadyOutstanding`` on the next
`runOnce` (the queue contract from PR 2/N) until engine-owned lifecycle
code performs the explicit acknowledgement.

The empty-drain path is intentionally different. When the queue
returns ``DurableRemoteQueueBatch/byteCount`` `== 0`, ``runOnce``
short-circuits **before** invoking
``BatchEngine.recoverEntries(from:)`` — there is nothing to
parse — and releases the held outstanding-batch boundary by
calling ``DurableRemoteQueue/acknowledge()`` before returning
`[]`. The release is keyed off the authoritative zero-byte
signal the queue returns from drain rather than off "the
recovered entry stream is empty"; a missing or unreadable export
artifact after the queue's authoritative zero-byte drain signal cannot
block this path.
The empty drain boundary release
does not advance any destructive-removal of delivered queue
payload bytes (there are none) and stays distinct from the non-empty
acknowledgement-to-removal lifecycle ``RemoteEngine/flush()`` runs; it exists
only so a polling caller does not get blocked on
``DurableRemoteQueueError/batchAlreadyOutstanding`` on the next
empty pass. A failure of that empty drain boundary release
surfaces as ``ExecutionLoopError/emptyBatchReleaseFailed(_:)``
rather than being masked as ``ExecutionLoopError/drainFailed(_:)``.

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
}

internal enum ExecutionLoopError: Error, Sendable, Equatable {
    case drainFailed(DurableRemoteQueueError)
    case recoverFailed(BatchEngineError)
    case batchSplitFailed(RemoteDeliveryError)
    case invalidRetryDelay(RemoteDeliveryError)
    case emptyBatchReleaseFailed(DurableRemoteQueueError)
    case sleepInterrupted
}

internal enum RetryExecutor {
    static func deliver(
        entry: RemoteDeliveryEntry,
        transport: any RemoteTransport,
        policy: RemoteRetryPolicy,
        sleep: @Sendable (Double) async throws -> Void,
        delayCalculator: @Sendable (RemoteRetryPolicy, Int) throws(RemoteDeliveryError) -> Double
            = { policy, attempt in try policy.delayBeforeRetry(attempt: attempt) }
    ) async throws(BatchDeliveryError) -> RemoteDeliveryAttempt
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
}
```

## Contract Value Types

`RemoteDeliveryEntry.identifier` is an engine-local correlation
identifier only. It is not a persistence replay identity and carries
no ordering contract.

`RemoteTransportResponse` does not expose an HTTP status code field.
Status codes, vendor body codes, and other transport-specific
signals are adapter-owned and travel as `responseMetadata` entries
(or stay inside the adapter's classifier). They are not core fields
on the engine surface.

`RemoteDeliveryError.transportRejected` is the sink-neutral signal
for a transport-classified delivery failure. The engine does not
expose a status-code variant; that classification belongs to the
adapter.

```text
public struct RemoteDeliveryEntry: Sendable, Equatable {
    /// Engine-local correlation identifier only. Not a persistence
    /// replay identity; carries no ordering contract.
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
// engine-internal `RetryExecutor` / `ExecutionLoop` machinery (PR
// 4/N) over the existing queue + batching + transport primitives.

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
    func send(
        payloadBytes: Data,
        payloadMetadata: [String: String]
    ) async throws -> RemoteTransportResponse

    // Sink-owned response classification. This is pre-release API
    // and is locked by PR 5/N before the first tag. Classification
    // must be deterministic for the same transport result within a
    // flush pass. Classification MUST NOT
    // call queue acknowledgement / removal APIs and MUST NOT mutate
    // engine lifecycle state; it only maps a send result to a
    // delivery result.
    func classify(
        _ result: Result<RemoteTransportResponse, any Error>
    ) async -> RemoteDeliveryResult
}

public struct RemoteTransportResponse: Sendable, Equatable {
    public let responseBytes: Data
    public let responseMetadata: [String: String]
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
  ``send(payloadBytes:payloadMetadata:)`` and ``classify(_:)``;
  the engine never inspects HTTP status, vendor body codes, or
  transport error types (LGR-5 / LGR-7 / LGR-9). Classification
  is a pure lifecycle input to the engine's decision and must be
  deterministic for the same transport result within a flush pass:
  it MUST NOT call queue acknowledgement / removal APIs and MUST
  NOT mutate engine lifecycle state.
- **Per-flush export scratch.** The engine takes an
  `exportDirectory: URL` at init. The caller owns the directory's
  lifecycle: it MUST already exist, MUST be writable by the
  engine's process, and SHOULD be a caller-controlled private
  location not shared with other code paths. The engine creates
  only unique scratch export files inside the directory; it does
  not create the parent directory, set or audit access-control
  policy, or sweep pre-existing files. Every ``flush()`` first
  consults ``DurableRemoteQueue/currentOutstandingBatch()``:
  when the queue is still holding a previously drained batch
  the engine reuses that batch (and its already-written export
  file) through the outstanding-reuse path without flushing or
  draining new bytes. The fresh-drain path runs only when there
  is no outstanding batch: the engine calls
  ``DurableRemoteQueue/flush()`` so every admitted entry is on
  disk, allocates a fresh unique filename inside the directory,
  and drains into it through ``DurableRemoteQueue/drain(to:)``.
  A queue-flush failure on this fresh-drain path surfaces as
  ``RemoteEngineError/flushFailed(_:)`` before the engine owns
  a reusable drained batch reference for the current flush pass.
  The export artifact is removed **only** after a successful
  empty release or a successful non-empty acknowledge; any
  `.retryable`-exhausted tally and any parse / batch /
  retry-interruption / acknowledge failure after the engine owns
  a reusable drained batch reference for the current flush pass
  keep the retained export artifact so the next ``flush()``
  replays it through the outstanding-reuse path. Cleanup failures on the removal step
  surface as ``RemoteEngineError/exportCleanupFailed(_:)`` rather
  than being silently swallowed; the carried
  ``RemoteEngineExportCleanupContext`` records the export URL,
  the cleanup phase, and the error domain / code captured from
  the bridged `NSError` representation. Cleanup failure fires
  only after the final acknowledgement state for the phase is
  already reached, so it MUST NOT trigger retry of already
  acknowledged bytes: callers branch on the surfaced error
  without re-entering the delivery lifecycle for the same bytes.
  On ``RemoteEngineExportCleanupContext/Phase/emptyRelease``
  the empty-drain boundary is already cleared and no delivered
  queue payload bytes existed, so the empty scratch artifact is
  a leftover; callers MAY remove the retained URL after observing
  this case. On
  ``RemoteEngineExportCleanupContext/Phase/acknowledgedNonEmpty``
  the queue's destructive removal has already run — the
  persistence layer has dropped the delivered queue payload
  bytes — and the retained artifact is a duplicate copy, not a
  retry source: the engine never re-reads it and the next
  ``RemoteEngine/flush()`` sees no outstanding batch on the
  queue. Callers MAY remove the retained URL after observing this
  case. Callers must keep the directory engine-exclusive.
- **Acknowledgement-to-removal lifecycle (LGR-11).** After every
  non-empty flush pass, the engine inspects all
  ``RemoteDeliveryAttempt`` outcomes from the drained export. If
  every recovered entry is
  resolved (``RemoteDeliveryResult/success`` or
  ``RemoteDeliveryResult/terminal(reason:)``) the engine calls
  ``DurableRemoteQueue/acknowledge()`` and returns
  ``RemoteFlushAcknowledgement/removedDeliveredBytes``. A single
  ``RemoteDeliveryResult/retryable(reason:)`` outcome (budget
  exhausted without resolution) keeps the outstanding-batch
  boundary held so the next ``flush()`` retries the same drained
  bytes; the summary acknowledgement is then
  ``RemoteFlushAcknowledgement/notAcknowledged``.
  If ``DurableRemoteQueue/acknowledge()`` fails, subsequent
  ``flush()`` calls reuse the same outstanding batch and export
  artifact until acknowledgement succeeds.
  ``RemoteDeliveryResult/terminal(reason:)`` is sink-decided
  permanent failure — the classifier owns the judgment — so
  removing those bytes is forward progress, not data loss.
- **Empty drain release.** On
  ``DurableRemoteQueueBatch/byteCount`` `== 0` the engine
  short-circuits inside the execution loop, releases the held
  boundary, and reports a summary with zero counts and
  ``RemoteFlushAcknowledgement/emptyReleased``. An empty drain is
  not an attempted batch: ``RemoteFlushSummary/attemptedBatches``
  is `0` because ``BatchEngine`` `.makeBatches(from:policy:)` is
  not invoked over zero delivered queue payload bytes. No
  delivered queue payload bytes are removed because none existed.

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
    // failure before the engine owns a delivered batch reference.
    case drainFailed(DurableRemoteQueueError)

    case parseFailed(RemoteEngineParseError)
    case batchFailed(RemoteDeliveryError)
    case retryInterrupted(RemoteEngineRetryError)

    // Acknowledge failed after a boundary was captured. The queue
    // keeps the outstanding batch held and the export artifact stays
    // on disk; subsequent flushes reuse the same outstanding batch /
    // export artifact until acknowledgement succeeds.
    case acknowledgementFailed(DurableRemoteQueueError)

    // Export artifact cleanup failed after empty release or
    // acknowledged non-empty removal. Cleanup failure MUST NOT cause
    // retry of bytes that were already acknowledged.
    case exportCleanupFailed(RemoteEngineExportCleanupContext)
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
  `BGTaskScheduler` events) and call ``RemoteEngine/flush()``.
- macOS: observe `NSWorkspace.willPowerOffNotification` or
  app-specific shutdown hooks and call ``RemoteEngine/flush()``.
- Server-side: drive ``flush()`` from a graceful-shutdown
  signal handler or a periodic task.

Because the engine actor serializes concurrent ``flush()``
invocations, hosts can call it from multiple lifecycle hooks
without coordination; overlapping calls simply queue and run
sequentially.

## Sink-Neutrality Acceptance

Before adapter APIs are locked beyond the core engine surface, the
contract must be validated against at least two non-equivalent
backend response models so the engine is not silently shaped only by
Elastic semantics:

- **Elastic `_bulk`** — NDJSON request shape, item-level success /
  failure response classification.
- **Splunk HEC** — non-bulk JSON event shape, status / body-code
  model distinct from Elastic `_bulk` item-level results.

The current scope adds these as **internal test fixtures**. The
second remote-adapter protocol sanity check lands in M4; the
HTTP-backed adapter family lands in M5.

## Out Of Scope

- No autonomous timer or scheduler in the public engine. The
  wall-clock backoff is taken from an engine-internal sleep
  injector backed by `Task.sleep` (LGR-3); the
  engine never observes platform lifecycle events on its own.
- No platform-specific lifecycle observer (`UIApplication`,
  `NSWorkspace`, `NotificationCenter`, etc.). Hosts wire those
  on their side and call ``RemoteEngine/flush()`` from the
  appropriate hook.
- No production network transport in this package. The engine
  dispatches through whatever ``RemoteTransport`` conformer the
  caller hands in; the current milestone exercises only the
  test-only ``StubRemoteTransport`` fixture. Concrete adapters
  (Elastic `_bulk`, Splunk HEC, …) ship in later milestones.
- No vendor-specific encoders, request builders, or response
  validators in the core engine — those live in adapter packages
  (LGR-9).
- No `swift-logger-elastic` migration.
- No Datadog/Splunk/Loki/Dynatrace adapter packages.
- No SDK-backed adapters (Datadog mobile SDK, Splunk RUM SDK,
  Dynatrace OneAgent) — those bypass this engine by design.
- No tags or releases.

## Milestone Map

| Milestone | Scope |
| --- | --- |
| M3.4 PR 3/N | Batching engine: deterministic batch construction, entry/byte caps, oversized-entry behavior, byte-stable export → entry-stream parser with fail-closed `formatVersion` validation. Engine-internal machinery. |
| M3.4 PR 4/N | Retry / execution loop: per-entry retry budget over the existing queue + batching + transport primitives, backoff progression in `Double` seconds through an injected sleep closure, attempt accounting, terminal vs retryable routing, test-only `StubRemoteTransport` fixture explicitly marked as such (not public API). Engine-internal `RetryExecutor`, `ExecutionLoop`, `RemoteDeliveryAttempt`, `ExecutionLoopError` machinery; no public API change in PR 4/N itself. |
| M3.4 PR 5/N (this milestone) | Public delivery surface: `RemoteEngine` actor + `flush()` over the engine-internal loop, `RemoteFlushSummary`, `RemoteEngineError` (+ `RemoteEngineParseError`, `RemoteEngineRetryError`), `RemoteTransport.classify(_:)` sink-owned classification, acknowledgement-to-removal lifecycle closure for non-empty flush passes (engine acknowledges when every recovered entry is `.success` or `.terminal`; any `.retryable` keeps the boundary held for the next flush). Closes M3.4. |
| M3.5 | Migrate `swift-logger-elastic` onto this engine; Elastic-specific code stays in the adapter (ECS encoder, `_bulk` request builder, `_bulk` response validator). |
| M4 | Second remote adapter (Datadog Logs intake or Splunk HEC) as a protocol sanity check. |
| M5 | HTTP-backed adapter family built on top of this engine. |
