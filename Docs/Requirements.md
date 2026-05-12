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
| LGR-5 | Transport surface accepts payload bytes plus metadata and returns response bytes plus sink-owned response metadata without imposing HTTP semantics on the core contract; transport is sink-neutral and does not expose HTTP status as a core response field. | M3.4 |
| LGR-6 | Flush lifecycle vocabulary is deferred until the flush / lifecycle contract ships; the lifecycle-observer surface remains deferred until the same boundary. | Deferred (Future M3.4 milestone) |
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
- **Retry / execution loop (PR 4/N).** `RetryExecutor` and
  `ExecutionLoop` add an engine-internal retry / execution pass
  over the existing queue + batching + transport primitives.
  `RetryExecutor.deliver(entry:transport:policy:classifier:sleep:)`
  drives one `RemoteDeliveryEntry` through the
  `RemoteRetryPolicy` budget independently from other entries in
  the same batch: each attempt dispatches exactly one
  `RemoteTransport.send(payloadBytes:payloadMetadata:)` call for
  the delivery entry with the entry's `payload` and `metadata`
  (per-entry dispatch, LGR-5 sink-neutrality, without batch-level
  transport encoding or aggregation), an injected classifier maps
  each result into `RemoteDeliveryResult`,
  `.success` / `.terminal` stop attempts immediately, and
  `.retryable` consumes additional budget until
  `policy.maxAttempts` (minimum 1, including the first attempt).
  The classifier runs before any sleep decision; backoff is
  scheduled only after a retryable classification for the
  just-completed attempt, so `.success` and `.terminal` outcomes
  never schedule backoff. The engine does not own wall-clock timing
  (LGR-3); retry delay is applied through an injected sleep
  closure after retryable classifier evaluation for the
  just-completed attempt. Backoff between two retryable attempts is
  taken from `RemoteRetryPolicy.delayBeforeRetry(attempt:)` using
  the just-completed retryable attempt count for the delivery entry.
  Sleep happens only between retryable attempts; the final attempt
  is followed by an immediate return from
  `RetryExecutor.deliver(...)` without additional backoff
  scheduling.
  `ExecutionLoop.runOnce(...)` composes
  `DurableRemoteQueue.drain(to:)` →
  `BatchEngine.recoverEntries(from:)` →
  `BatchEngine.makeBatches(from:policy:)` → per-entry
  `RetryExecutor.deliver(...)` sequentially without batch-level
  transport aggregation or shared retry state and returns a
  deterministically ordered `[RemoteDeliveryAttempt]` matching
  queue export traversal order for the drained export across batch
  traversal order and entry traversal order within each batch,
  without mutating queue acknowledgement state. During PR 4/N, the
  loop never invokes `acknowledge()` on a non-empty delivered
  batch, even when every delivery attempt succeeds, and performs no
  destructive removal of delivered queue payload bytes;
  the acknowledgement-to-removal lifecycle for non-empty delivered
  queue payload batches stays a PR 5/N concern (a non-empty
  drained-but-unacknowledged batch from a preceding `runOnce`
  surfaces as `.batchAlreadyOutstanding` on the next call). On the
  empty-drain path the loop short-circuits on the
  queue's authoritative zero-byte signal
  (`DurableRemoteQueueBatch.byteCount == 0`) **before** invoking
  `BatchEngine.recoverEntries(from:)` — there is nothing to
  parse — and releases the held outstanding-batch boundary by
  calling `acknowledge()` before returning `[]`. Keying the
  release off `byteCount == 0` rather than off "the recovered
  entry stream is empty" means a missing or unreadable
  export artifact after the queue's authoritative zero-byte drain
  signal cannot block this path. The empty drain export carries no
  delivered queue payload bytes in the drained export file and
  therefore no destructive-removal candidate bytes, so this is not
  a destructive-removal step; the in-memory boundary must still
  clear so a polling caller is not blocked by
  `.batchAlreadyOutstanding` on the next empty pass. A failure
  of that empty release surfaces as
  `ExecutionLoopError.emptyBatchReleaseFailed(_:)` rather than
  being masked as `drainFailed`. The PR carries a test-only
  `StubRemoteTransport` fixture explicitly marked as such, not
  public API.
- **Deferred — flush / lifecycle / transport integration (PR 5/N).**
  Flush trigger semantics, lifecycle hook surface, production
  `RemoteTransport` adapter integration on top of the
  engine-internal `ExecutionLoop`, and the
  acknowledgement-to-removal lifecycle close M3.4 along with final
  docs / coverage / CI readiness.
- **Adapter migrations.** `swift-logger-elastic` migration lands in
  M3.5; HTTP adapters built on this engine ship in M5 outside the
  current milestone scope.
