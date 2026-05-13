# Requirements

Lightweight traceability for `swift-logger-remote`. Each requirement
carries an `LGR-N` identifier for ADRs, implementation work, and
future tests to reference. The "Target" column records the milestone
or documentation owner associated with the requirement; implementation
status is tracked by the roadmap and coverage documents.

## Core Contract

| ID | Requirement | Target |
| --- | --- | --- |
| LGR-1 | Engine accepts a durable delivery unit that carries payload bytes, sink-owned metadata, and an engine-local correlation identifier (not a persistence replay identity, no ordering contract); raw `LogRecord` is not stored in the engine. | M3.4 |
| LGR-2 | Delivery result is one of success, retryable failure, or terminal failure. | M3.4 |
| LGR-3 | Retry policy exposes a retry limit and a backoff schedule model; the engine does not own the timer or scheduler. | M3.4 |
| LGR-4 | Batch policy exposes a max entry count and a max byte count and produces deterministic boundary behavior. | M3.4 |
| LGR-5 | Transport surface accepts ordered batch items (payload bytes + metadata per item) through `RemoteTransport.sendBatch(_:)` and returns one `Result<RemoteTransportResponse, any Error>` per input item in input order. Transport is sink-neutral and does not impose HTTP semantics on the core contract; HTTP status is not a core response field. Single-event sinks dispatch per item inside `sendBatch`, batch-aggregating sinks (Elastic `_bulk`, OTLP/HTTP batched) build one shared vendor request from the input batch. | M3.4 |
| LGR-6 | Flush is caller-driven through `RemoteEngine.flush()`. The engine owns no timer and no platform lifecycle observer in the public delivery surface; host applications drive lifecycle integration on their side and invoke `flush()` from the appropriate hook (background notifications, shutdown signals, periodic tasks). Actor isolation serializes concurrent invocations so the engine never runs two passes against the same queue simultaneously. | M3.4 |
| LGR-7 | Delivery error surface is a typed sink-neutral diagnostic enum suitable for adapter classification; HTTP / vendor body codes stay inside adapter classifiers. | M3.4 |

## Sink Neutrality

| ID | Requirement | Target |
| --- | --- | --- |
| LGR-8 | The engine API shape is validated against at least two non-equivalent backend response models (Elastic `_bulk` NDJSON item-level vs. Splunk HEC non-bulk JSON status/body-code) before public surface is locked. | M3.4 |
| LGR-9 | Vendor-specific encoders, request builders, and response validators stay in adapter packages, not in the engine. | M3.4 |

## Persistence Coupling

| ID | Requirement | Target |
| --- | --- | --- |
| LGR-10 | Accepted ordering is owned by `swift-logger-persistence` through its byte-stable export contract; the engine reads accepted bytes through the persistence surface, not through ad-hoc storage. | M3.4 |
| LGR-11 | Delivery acknowledgement is the only trigger for destructive removal of accepted bytes from the persistence layer. | M3.4 |

## Notes

- **Core contract.** [`Docs/APIDesign.md`](APIDesign.md)
  carries the current locked contract value types, the persistence
  coupling surface (`DurableRemoteQueue`), and the current M3.4
  scope boundary.
- **Persistence dependency.** The package depends on
  [`swift-loggers/swift-logger-persistence`](https://github.com/swift-loggers/swift-logger-persistence)
  at the released `0.1.x` SemVer line via
  `.upToNextMinor(from: "0.1.0")`. No branch, revision, or SHA pin.
- **Durable queue core (PR 2/N).** `DurableRemoteQueue` admits one
  `RemoteDeliveryEntry` per `enqueue(_:)` by wrapping the entry
  (identifier + payload + metadata) into a package-owned internal
  queue record persisted as the persistence `payload`. Persistence
  `sequence` values come from a queue-private monotonic allocator;
  `RemoteDeliveryEntry.identifier` retains its
  correlation-only / no-ordering shape from PR 1/N across
  persistence sequence allocation. The queue does not expose a
  `retention` parameter (hardcoded
  `RetentionPolicy.unlimited`) so LGR-11 holds: persistence
  retention cannot delete bytes that were never acknowledged.
  `drain(to:)` captures the recoverable prefix as a byte-stable
  export through the persistence package and admits one outstanding
  batch at a time; for a non-empty outstanding batch, a second
  `drain(to:)` before `acknowledge()` succeeds is rejected, and a
  failed `acknowledge()` keeps the outstanding batch for retry.
  `byteCount` on the returned batch is the exact post-export file
  size; unmeasurable batches surface as a typed drain failure
  without acknowledgement state advancement.
- **Batching engine (PR 3/N).** `BatchEngine` is engine-internal
  machinery the engine-internal execution loop drives.
  `recoverEntries(from:)` parses a drained queue export back into
  an ordered array of `RemoteDeliveryEntry` values; the parser
  validates each line's
  envelope `contentType` against the queue-owned constant
  (`DurableRemoteQueue.envelopeContentType`) before treating its
  `payload` as queue-record bytes, then validates each queue
  record's `formatVersion` schema-evolution anchor fail-closed
  before decoding any other queue-record field.
  `makeBatches(from:policy:)` splits the entry stream into ordered
  batches under `RemoteBatchPolicy`: equal-to-cap fits in the
  current batch, strictly-greater starts the next, and an
  oversized single entry surfaces
  `.batchSizeExceeded(limit:actual:)`. Accepted ordering from the
  byte-stable queue export and duplicate-identifier multiplicity
  survive both steps verbatim. The engine never dedupes, sorts, or
  classifies; it never invokes
  `DurableRemoteQueue.acknowledge()` and performs no destructive
  removal. LGR-4 owns the batch-policy boundary contract this
  engine consumes; LGR-10 / LGR-11 hold across the batching path.
- **Retry / execution loop (PR 4/N).** `ExecutionLoop` adds an
  engine-internal batch-round retry / execution pass over the
  queue + batching + transport primitives. The dispatcher drives
  each batch from `BatchEngine.makeBatches(from:policy:)`
  as **rounds** against `RemoteTransport.sendBatch(_:)`: round 1
  dispatches every entry in the batch, every subsequent round
  re-dispatches only the entries whose previous classification
  in the immediately preceding round was `.retryable(reason:)`
  (active-set shrink, preserving
  drained-export order). The transport sees an ordered array of
  `RemoteTransportBatchItem` per round and returns one
  `Result<RemoteTransportResponse, any Error>` per input item in
  the same order; the engine maps each result through
  `RemoteTransport.classify(_:)` to a `RemoteDeliveryResult`.
  `.success` / `.terminal` resolve an entry; `.retryable`
  consumes another round of the per-entry budget. Per-entry
  attempt counts equal the number of rounds the entry was active
  in (1-indexed, never above `policy.maxAttempts`). The engine
  does not own wall-clock timing (LGR-3); retry delay comes from
  `RemoteRetryPolicy.delayBeforeRetry(attempt:)` consulted before
  every retry round using each retained active entry's
  just-completed retryable attempt count, and is applied
  through an injected sleep closure (backed by Swift concurrency
  sleep primitives in the public engine).
  Sleep fires between rounds only when the previous round left
  at least one entry retryable in the retained active-set. A count
  mismatch between
  `sendBatch` input items and returned results is an
  adapter-contract violation surfaced fail-closed as
  `RemoteEngineError.transportBatchInvalid(expected:actual:)`;
  a whole-batch `sendBatch` throw is routed through
  `RemoteTransport.classify(_:)` with the same `.failure(error)`
  value for every active item in the round, in input order, and
  counts toward each item's budget.
  `ExecutionLoop.runOnce(...)` composes
  `DurableRemoteQueue.drain(to:)` →
  `BatchEngine.recoverEntries(from:)` →
  `BatchEngine.makeBatches(from:policy:)` → per-batch
  batch-round dispatch and returns a deterministically ordered
  `[RemoteDeliveryAttempt]` matching queue export traversal
  order, without mutating queue acknowledgement state for
  non-empty drained exports. The
  engine-internal loop never invokes `acknowledge()` for a
  non-empty drained export — that lifecycle is owned by
  `RemoteEngine.flush()` above
  this layer (see PR 5/N). On the empty-drain path the loop
  short-circuits on the queue's authoritative zero-byte signal
  (`DurableRemoteQueueBatch.byteCount == 0`) **before** invoking
  `BatchEngine.recoverEntries(from:)` and releases the held
  outstanding-batch boundary by calling `acknowledge()`. Keying the
  release off `byteCount == 0` rather than off "the recovered
  entry stream is empty" means a missing or unreadable export
  artifact after the queue's authoritative zero-byte drain signal
  cannot block this path. For the empty export, the empty drain
  release carries no delivered queue payload bytes in the drained
  export file and is not a destructive-removal step. A failure of
  that empty release surfaces as
  `ExecutionLoopError.emptyBatchReleaseFailed(_:)` rather than
  being masked as `drainFailed`. The PR carries a test-only
  `StubRemoteTransport` fixture explicitly marked as such, not
  public API.
- **Public delivery engine (PR 5/N).** `RemoteEngine` is the
  public delivery surface: an actor whose `flush()` method runs
  one caller-driven delivery pass over an `exportDirectory` it
  manages exclusively. The caller owns the directory's lifecycle:
  it MUST already exist, MUST be writable by the engine's process,
  and SHOULD be a caller-controlled private location not shared
  with other code paths. The engine creates only unique scratch
  export files inside the directory; it does not create the parent
  directory, set or audit access-control policy, or sweep
  pre-existing files. Each `flush()` first consults
  `DurableRemoteQueue.currentOutstandingBatch()` before allocating a
  fresh scratch export: when the queue is still holding a drained
  batch, the engine reuses that batch (and its already-written export
  file) without flushing or draining new bytes;
  only when no outstanding batch exists does the engine enter the
  fresh path. First, it calls `DurableRemoteQueue.flush()` against
  the queue. Then it drains into a fresh unique scratch file inside
  the export directory owned exclusively by the engine actor for that
  flush pass. The engine then orchestrates
  `BatchEngine.recoverEntries(from:)`,
  `BatchEngine.makeBatches(from:policy:)`, and per-batch
  batch-round dispatch against `RemoteTransport.sendBatch(_:)`.
  After all dispatch rounds across every batch complete,
  it decides acknowledgement for the drained export file based
  on the per-classification tally across the entire flush pass, without
  batch-level acknowledgement decisions. The engine calls
  `DurableRemoteQueue.acknowledge()` only when each recovered entry for
  that drained export
  reaches a fully-resolved classification outcome:
  `RemoteDeliveryResult.success` or
  `RemoteDeliveryResult.terminal(reason:)`. A single
  `RemoteDeliveryResult.retryable(reason:)` outcome (per-entry
  budget exhausted without resolution) across the entire flush pass
  keeps the boundary held for the drained export so the same drained
  export bytes are retried through the outstanding-reuse path.
  Export-file cleanup for the drained export artifact is keyed off the
  acknowledgement decision: drain failure before
  the engine owns a reusable drained batch reference for the current
  flush pass runs best-effort cleanup of the just-allocated scratch
  URL (the drain error is the primary diagnostic, so cleanup failure
  is suppressed on this path);
  empty release and
  non-empty fully-resolved + acknowledged delivery passes remove
  the export file, and
  any cleanup failure surfaces as
  `RemoteEngineError.exportCleanupFailed(RemoteEngineExportCleanupContext)`
  after acknowledgement state is already final for that phase, with
  the export URL, the lifecycle phase, and the underlying `NSError`
  domain / code, and MUST NOT trigger retry of already acknowledged
  bytes. On the `acknowledgedNonEmpty` phase
  the queue's destructive removal already ran before export cleanup
  begins; ack is final for the acknowledged non-empty pass. For the
  acknowledged non-empty pass, the retained scratch artifact is a
  duplicate copy of the delivered queue payload bytes from the
  drained export after acknowledgement already completed, not a
  retry source. Callers MAY remove the retained URL manually after
  observing
  `RemoteEngineError.exportCleanupFailed(RemoteEngineExportCleanupContext)`.
  Every other path (any `.retryable`-exhausted outcome, or any
  parse / batch / retry-interruption / ack failure after the engine
  owns a reusable drained batch reference for the current flush
  pass) keeps the retained export artifact as the retry source.
  The next flush reuses that retained export artifact through
  `DurableRemoteQueue.currentOutstandingBatch()` for the current
  outstanding batch without draining new queue bytes from
  persistence. The retained artifact is still
  parsed again through `BatchEngine.recoverEntries(from:)` as the
  retry source for that flush pass.
  `.terminal` is sink-decided permanent failure — the classifier
  owns that judgment — so removing those bytes is forward
  progress, not data loss; LGR-11 holds because acknowledgement is
  still the only destructive-removal trigger. Before the first
  public release tag, the classifier hook moved onto the public
  `RemoteTransport.classify(_:)` method so adapters own both
  transport dispatch and delivery classification. For
  `RemoteTransport.classify(_:)`, classification is deterministic
  within a flush pass for the same adapter implementation and
  transport result.
  Classification MUST NOT directly mutate queue acknowledgement
  state or export-file lifecycle state through
  `RemoteTransport.classify(_:)` for the current flush pass.
  It also MUST NOT cause equivalent mutations indirectly through
  callbacks, shared state, or transport side effects (LGR-5 /
  LGR-7 / LGR-9).
  The public failure taxonomy is `RemoteEngineError`,
  `RemoteEngineParseError`, `RemoteEngineRetryError`, and
  `RemoteEngineExportCleanupContext` as public projections of
  engine-internal diagnostics rather than exposing engine-internal
  error types directly;
  the engine-internal `BatchEngineError` and `ExecutionLoopError`
  types stay engine-internal and are translated by `RemoteEngine`
  into the public projection taxonomy at the boundary.
  The engine owns no timer and no platform lifecycle observer in
  the public delivery surface despite internal retry-delay
  scheduling (LGR-3, LGR-6); host applications drive integration
  from their own lifecycle hooks. PR 5/N closes M3.4.
- **Adapter migrations.** `swift-logger-elastic` migration lands in
  M3.5; HTTP adapters built on this engine ship in M5 outside the
  current milestone scope.
