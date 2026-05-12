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
``RemoteBatchPolicy`` (PR 3/N), and an engine-internal retry /
execution loop that drives the per-entry retry budget over the
existing queue + batching + transport primitives under
``RemoteRetryPolicy`` (M3.4 PR 4/N). The retry / execution loop
adds no public surface beyond the existing contract value types;
``RetryExecutor``, ``ExecutionLoop``, ``RemoteDeliveryAttempt``,
and ``ExecutionLoopError`` are internal machinery the test target
reaches through `@testable import`. Flush / lifecycle observer,
production ``RemoteTransport`` adapter integration, the
acknowledgement-to-removal lifecycle closure, and the
`swift-logger-elastic` migration ship in later PRs.

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
  can retry against the same captured boundary; only a successful
  `acknowledge()` clears the state and re-opens drain.
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
performs any destructive removal. Lifecycle hooks and production
transport integration ship in later PRs.

## Retry / Execution Loop (engine-internal)

The retry / execution loop is engine-internal machinery for
caller-driven delivery passes. Its public surface is the existing
``RemoteRetryPolicy``, ``RemoteBatchPolicy``, ``RemoteDeliveryEntry``,
``RemoteDeliveryResult``, ``RemoteTransport``, and
``RemoteTransportResponse`` value types from PR 1/N; PR 4/N
intentionally adds no new public types because the contract value
types already carry the engine surface.

Two engine-internal namespaces compose the loop:

1. **`RetryExecutor.deliver(entry:transport:policy:classifier:sleep:)`**
   drives one ``RemoteDeliveryEntry`` through the
   ``RemoteRetryPolicy`` budget:
   - Each attempt invokes ``RemoteTransport/send(payloadBytes:payloadMetadata:)``
     with the entry's `payload` and `metadata`. The transport call
     is per-entry; the engine never aggregates entry bytes into a
     single wire payload (LGR-5: the engine does not impose NDJSON,
     line-delimited proto, or any other wire format on adapters).
   - The injected `classifier` maps each transport result
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
     ``ExecutionLoopError/invalidRetryDelay(_:)`` carrying the
     underlying ``RemoteDeliveryError``. The
     ``RemoteRetryPolicy/make(maxAttempts:backoff:)`` factory
     already enforces that the default calculation accepts every
     `attempt` value the executor passes from a validated policy,
     so this case names an engine-side / seam-injected invariant
     violation and is never collapsed into
     ``ExecutionLoopError/sleepInterrupted``.
   - A sleep-injector failure between two retryable attempts
     surfaces ``ExecutionLoopError/sleepInterrupted``, distinct
     from a delay-calculation failure.
2. **`ExecutionLoop.runOnce(queue:exportURL:batchPolicy:retryPolicy:transport:classifier:sleep:afterDrain:)`**
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
on a non-empty delivered batch and performs no destructive removal
of delivered queue payload bytes; the acknowledgement-to-removal
lifecycle closure belongs to PR 5/N. A non-empty batch held by the queue
from a preceding `runOnce` call therefore surfaces as
``DurableRemoteQueueError/batchAlreadyOutstanding`` on the next
`runOnce` (the queue contract from PR 2/N) until the PR 5/N
lifecycle wires the explicit acknowledgement.

The empty-drain path is intentionally different. When the queue
returns ``DurableRemoteQueueBatch/byteCount`` `== 0`, ``runOnce``
short-circuits **before** invoking
``BatchEngine/recoverEntries(from:)`` — there is nothing to
parse — and releases the held outstanding-batch boundary by
calling ``DurableRemoteQueue/acknowledge()`` before returning
`[]`. The release is keyed off the authoritative zero-byte
signal the queue returns from drain rather than off "the
recovered entry stream is empty"; a missing or unreadable export
artifact after a zero-byte drain signal cannot block this path.
The empty drain boundary release
does not advance any destructive-removal of delivered queue
payload bytes (there are none) and stays distinct from the non-empty
acknowledgement-to-removal lifecycle PR 5/N wires; it exists
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
        classifier: @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult,
        sleep: @Sendable (Double) async throws -> Void,
        delayCalculator: @Sendable (RemoteRetryPolicy, Int) throws(RemoteDeliveryError) -> Double
            = { policy, attempt in try policy.delayBeforeRetry(attempt: attempt) }
    ) async throws(ExecutionLoopError) -> RemoteDeliveryAttempt
}

internal enum ExecutionLoop {
    static func runOnce(
        queue: DurableRemoteQueue,
        exportURL: URL,
        batchPolicy: RemoteBatchPolicy,
        retryPolicy: RemoteRetryPolicy,
        transport: any RemoteTransport,
        classifier: @Sendable (Result<RemoteTransportResponse, any Error>) async -> RemoteDeliveryResult,
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

`RemoteEngine` is a placeholder type with no dispatch lifecycle and
no persistence ownership in the current milestone. It does not run a
delivery loop and exposes no public dispatch API yet. The shape of
the public engine surface is intentionally deferred until the contract
value types are reviewed and locked.

```text
public actor RemoteEngine {
    // Public dispatch API lands in a later M3.4 milestone.
}
```

## Sink-Neutrality Acceptance

Before any public API is locked beyond the value types above, the
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

- No public ``RemoteEngine`` dispatch lifecycle in the current
  milestone scope. The engine-internal ``RetryExecutor`` /
  ``ExecutionLoop`` machinery (PR 4/N) drives one delivery pass
  per `runOnce(...)` call from the test target via
  `@testable import`; ``RemoteEngine`` itself stays a placeholder.
- No autonomous timer or production network transport. The
  wall-clock backoff is taken from a caller-injected
  `Double`-seconds sleep closure (LGR-3) and the
  ``RemoteTransport`` calls are dispatched through whatever
  conformer the caller hands in; the current PR exercises only the
  test-only ``StubRemoteTransport`` fixture.
- No acknowledgement-to-removal lifecycle closure for non-empty
  delivered batches. ``ExecutionLoop/runOnce(...)`` performs an
  empty drain boundary release so a polling caller does not get
  blocked, but that zero-byte empty drain boundary release is the
  only ``DurableRemoteQueue/acknowledge()`` call in the current
  milestone. The loop never invokes `acknowledge()` on a
  delivered non-empty batch — that lifecycle wires in PR 5/N.
- No network implementation, no `URLSession`, no socket transport.
- No `swift-logger-elastic` migration.
- No Datadog/Splunk/Loki/Dynatrace adapter packages.
- No SDK-backed adapters (Datadog mobile SDK, Splunk RUM SDK,
  Dynatrace OneAgent) — those bypass this engine by design.
- No tags or releases.

## Deferred: Flush Lifecycle

A flush-lifecycle vocabulary (e.g. opportunistic / lifecycle-driven /
manual) is intentionally deferred. Naming a `RemoteFlushPolicy.onLifecycle`
case would prematurely commit to platform lifecycle semantics
(`UIApplication`/`NSWorkspace`/etc.) before the host-supplied
lifecycle-observer surface is designed. The flush-trigger
vocabulary and the lifecycle-observer surface stay PR 5/N
concerns; the engine-internal retry / execution loop from PR 4/N
is purely caller-driven (one `runOnce(...)` call per pass) and
intentionally exposes no flush trigger of its own.

## Milestone Map

| Milestone | Scope |
| --- | --- |
| M3.4 PR 3/N | Batching engine: deterministic batch construction, entry/byte caps, oversized-entry behavior, byte-stable export → entry-stream parser with fail-closed `formatVersion` validation. Engine-internal machinery. |
| M3.4 PR 4/N (this milestone) | Retry / execution loop: per-entry retry budget over the existing queue + batching + transport primitives, backoff progression in `Double` seconds through an injected sleep closure, attempt accounting, terminal vs retryable routing, test-only `StubRemoteTransport` fixture explicitly marked as such (not public API). No public API change; `RetryExecutor`, `ExecutionLoop`, `RemoteDeliveryAttempt`, and `ExecutionLoopError` are engine-internal. No `acknowledge()` call for non-empty delivered batches; empty drain boundary release occurs without destructive removal of delivered queue payload bytes. |
| M3.4 PR 5/N | Flush trigger semantics, lifecycle hook surface, production `RemoteTransport` adapter integration, acknowledgement-to-removal lifecycle, final docs / coverage / CI closure for M3.4. |
| M3.5 | Migrate `swift-logger-elastic` onto this engine; Elastic-specific code stays in the adapter (ECS encoder, `_bulk` request builder, `_bulk` response validator). |
| M4 | Second remote adapter (Datadog Logs intake or Splunk HEC) as a protocol sanity check. |
| M5 | HTTP-backed adapter family built on top of this engine. |
