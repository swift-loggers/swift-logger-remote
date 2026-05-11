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
| LGR-6 | Flush lifecycle vocabulary is deferred until the engine loop contract ships; the lifecycle-observer surface remains deferred until the same boundary. | Deferred (Future M3.4 milestone) |
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
  batch at a time; a second `drain(to:)` before `acknowledge()`
  succeeds is rejected, and a failed `acknowledge()` keeps the
  outstanding batch for retry. `byteCount` on the returned batch is
  the exact post-export file size; unmeasurable batches surface as a
  typed drain failure without acknowledgement state advancement.
- **Batching engine (PR 3/N).** `BatchEngine` is engine-internal
  machinery the future delivery loop drives. `recoverEntries(from:)`
  parses a drained queue export back into an ordered array of
  `RemoteDeliveryEntry` values; the parser validates each line's
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
  survive both steps verbatim. The engine never dedupes, sorts, or classifies; it
  never invokes `DurableRemoteQueue.acknowledge()` and performs
  no destructive removal. LGR-4 owns the batch-policy boundary
  contract this engine consumes; LGR-10 / LGR-11 hold across the
  batching path.
- **Deferred — retry scheduler (PR 4/N).** Retry policy execution,
  backoff progression, attempt accounting, and terminal vs.
  retryable routing land after batching. The PR carries a test-only
  transport fixture explicitly marked as such, not public API.
- **Deferred — flush / lifecycle / transport integration (PR 5/N).**
  Flush trigger semantics, lifecycle hook surface,
  `RemoteTransport` dispatch integration (future engine-owned
  dispatch loop), and the
  acknowledgement-to-removal lifecycle close M3.4 along with final
  docs / coverage / CI readiness.
- **Adapter migrations.** `swift-logger-elastic` migration lands in
  M3.5; HTTP adapters built on this engine ship in M5 outside the
  current milestone scope.
