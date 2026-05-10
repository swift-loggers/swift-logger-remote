# LoggerRemote Coverage Map

This file maps `LoggerRemote` contract areas to requirement IDs and
test coverage.

This document is non-normative and does not define behavior.
[`Docs/Requirements.md`](../../Docs/Requirements.md) owns requirement
IDs.

## Contract Coverage Index

| Contract area | Requirement IDs | Status | Primary test location |
| --- | --- | --- | --- |
| Durable delivery entry shape | LGR-1 | Covered | `RemoteDeliveryContractTests.swift` |
| Delivery result taxonomy | LGR-2 | Covered | `RemoteDeliveryContractTests.swift`, `RemoteBackendShapeTests.swift` |
| Retry policy validation | LGR-3 | Covered | `RemoteDeliveryContractTests.swift` — factory validation pins attempt-count bounds (`1 ... maxSupportedAttempts`), constant backoff bounds (finite, `> 0`, `<= maxBackoffSeconds`), and exponential schedule bounds (`multiplier > 1`, `capSeconds >= initialSeconds`, `capSeconds <= maxBackoffSeconds`). Pure `delayBeforeRetry(attempt:)` calculation pins constant returns same seconds, exponential progresses as `initial * multiplier^(n - 1)`, exponential clamps at `capSeconds` during iterative growth, with defensive non-finite protection, and invalid attempts outside `1 ..< maxAttempts` throw `.invalidRetryPolicy`. Retry scheduling lands together with the engine loop in a later M3.4 milestone. |
| Batch policy boundary behavior | LGR-4 | Covered | `RemoteDeliveryContractTests.swift` — boundary proof covers validated state (negative inputs and current-state-beyond-policy surface `.invalidBatchState`), overflow safety (entry-count and byte-count overflow fire the boundary instead of trapping), and oversized-single-entry rejection as `.batchSizeExceeded(limit:actual:)`. The boundary helper is engine-side machinery and is not part of the public API surface; the public contract model is the `RemoteBatchPolicy` value type. |
| Sink-neutral transport surface | LGR-5 | Covered | `RemoteDeliveryContractTests.swift` (Sendable proof); deferred runtime conformance to a later M3.4 milestone |
| Flush lifecycle vocabulary | LGR-6 | Future scope | Lands together with the engine loop in a future M3.4 milestone; no current engine-loop type or test |
| Delivery error surface | LGR-7 | Covered | `RemoteDeliveryContractTests.swift` (Sendable proof + factory rejection paths); HTTP / vendor body codes stay out of the engine surface (verified in `RemoteBackendShapeTests.swift`) |
| Backend-shape sanity (Elastic `_bulk` + Splunk HEC) | LGR-8 | Covered | `RemoteBackendShapeTests.swift` |
| Vendor encoders/builders/validators stay in adapters | LGR-9 | Covered | `RemoteBackendShapeTests.swift` (classifiers are internal test fixtures, not engine API) |
| Persistence-owned ordering | LGR-10 | Future scope | Lands when the engine consumes [`swift-logger-persistence`](https://github.com/swift-loggers/swift-logger-persistence) accepted-line bytes in a future M3.4 milestone |
| Delivery acknowledgement is the only removal trigger | LGR-11 | Future scope | Lands with the delivery loop in a future M3.4 milestone |

## Implementation File Index

| Source file | Covered contract areas | Primary test location |
| --- | --- | --- |
| `RemoteDeliveryEntry.swift` | Durable delivery entry shape, sink-owned metadata preservation, identifier correlation semantics (engine-local correlation only; participates in `Equatable` but carries no ordering contract and is not a persistence replay identity). | `RemoteDeliveryContractTests.swift` |
| `RemoteDeliveryResult.swift` | Delivery result taxonomy used by adapter classifiers | `RemoteDeliveryContractTests.swift`, `RemoteBackendShapeTests.swift` |
| `RemoteRetryPolicy.swift` | Retry policy factory validation (locked attempt-count and backoff bounds), backoff schedule model, pure `delayBeforeRetry` calculation (engine-side machinery, not public API). Retry scheduling deferred to the future engine loop. | `RemoteDeliveryContractTests.swift` |
| `RemoteBatchPolicy.swift` | Batch policy factory validation, deterministic boundary behavior, validated batching state, entry-count and byte-count overflow safety, oversized-single-entry rejection. Boundary helper is internal engine-side machinery, not public API. | `RemoteDeliveryContractTests.swift` |
| `RemoteTransport.swift` | Sink-neutral transport surface, transport response model (no HTTP status field; opaque response bytes and metadata preservation, default metadata is empty) | `RemoteDeliveryContractTests.swift`, `RemoteBackendShapeTests.swift` |
| `RemoteDeliveryError.swift` | Typed sink-neutral diagnostic surface for adapter classification | `RemoteDeliveryContractTests.swift`, `RemoteBackendShapeTests.swift` |
| `RemoteEngine.swift` | Current placeholder actor; no public dispatch surface yet | `RemoteDeliveryContractTests.swift` (constructibility proof only) |

## Maintenance Rules

- Keep this file as an index, not a second specification.
- Keep requirement IDs aligned with `Docs/Requirements.md`.
- Do not mark a future area as covered until its implementation and
  tests land.
- Backend-shape sanity coverage stays at least two non-equivalent
  models (Elastic `_bulk` + Splunk HEC); a third model can be added
  but not as a substitute.
