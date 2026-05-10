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
| LGR-3 | Retry policy exposes a retry limit and a backoff schedule model; the engine does not own the timer. | M3.4 |
| LGR-4 | Batch policy exposes a max entry count and a max byte count and produces deterministic boundary behavior. | M3.4 |
| LGR-5 | Transport surface accepts payload bytes plus metadata and returns response bytes plus sink-owned response metadata; transport is sink-neutral and does not expose HTTP status as a core response field. | M3.4 |
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
| LGR-10 | Accepted ordering is owned by `swift-logger-persistence`; the engine reads accepted-line bytes through the persistence surface, not through ad-hoc storage. | M3.4 |
| LGR-11 | Delivery acknowledgement is the only trigger for durable removal of accepted bytes from the persistence layer. | M3.4 |

## Notes

- **Core contract.** [`Docs/APIDesign.md`](APIDesign.md)
  carries the current locked contract value types and the current
  M3.4 scope boundary.
- **Delivery loop.** Deferred until later M3.4 milestones that ship
  the engine execution loop.
- **Adapter migrations.** `swift-logger-elastic` migration lands in a
  future M3 milestone (M3.5); HTTP adapters built on this engine ship
  in M5.
