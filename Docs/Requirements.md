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
| LGR-6 | Flush lifecycle vocabulary is defined together with the engine loop; the lifecycle-observer surface remains deferred until the engine loop contract ships. | Deferred (Future M3.4 milestone) |
| LGR-7 | Delivery error surface is a typed sink-neutral diagnostic enum suitable for adapter classification; HTTP / vendor body codes stay inside adapter classifiers. | M3.4 |

## Sink Neutrality

| ID | Requirement | Target |
| --- | --- | --- |
| LGR-8 | The engine API shape is validated against at least two non-equivalent backend response models (Elastic `_bulk` NDJSON item-level vs. Splunk HEC non-bulk JSON status/body-code) before public surface is locked. | M3.4 |
| LGR-9 | Vendor-specific encoders, request builders, and response validators stay in adapter packages, not in the engine. | M3.4 |

## Persistence Coupling

| ID | Requirement | Target |
| --- | --- | --- |
| LGR-10 | Accepted ordering is owned by `swift-logger-persistence` through its byte-stable export contract; the engine reads accepted-line bytes through the persistence surface, not through ad-hoc storage. | M3.4 |
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
- **Deferred — batching engine (PR 3/N).** Deterministic batch
  construction, entry/byte caps, and oversized-entry behavior land
  in the next PR with test-only fixtures and no transport dispatch.
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
