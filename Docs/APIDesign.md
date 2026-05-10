# API Design

This document defines the current API-observable contract surface for
the remote-delivery engine. Public API is locked by accepted API/spec
review, then verified by implementation and conformance tests.

`APIDesign.md` owns API shape and API-observable contracts only.
Persistence/file-format contracts live in
[`swift-logger-persistence/Docs/FileFormatSpec.md`](https://github.com/swift-loggers/swift-logger-persistence/blob/main/Docs/FileFormatSpec.md).

## Current Scope

Current scope is scaffold + contract only. It locks the shape of the
durable-delivery contract value types and the sink-neutral engine
boundary. The delivery loop, retry scheduler, batching engine,
transport implementations, and `swift-logger-elastic` migration are
out of scope and ship in later M3.4 milestones.

## Engine Boundary

The remote engine is **sink-neutral**:

- Accepted ordering is owned by `swift-logger-persistence`. Once
  integrated, the engine consumes accepted-line bytes from the
  persistence layer; it does not store its own ordering. The
  current scope does not declare a persistence package dependency;
  the integration lands in a future M3.4 milestone (LGR-10).
- Delivery acknowledgement is the **only** trigger for durable
  removal of accepted bytes from the persistence layer (LGR-11,
  future scope).
- Vendor-specific encoders, request builders, and response validators
  live in adapter packages (Elastic, Splunk HEC, Loki, Datadog Logs,
  Dynatrace Log Monitoring) — **not** inside the core engine.
- The engine does **not** know whether the wire format is NDJSON
  bulk, single JSON event, line-delimited proto, or anything else.

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
// public API surface; retry scheduling lands together with the
// engine loop in a later M3.4 milestone.

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

`RemoteEngine` is a placeholder type. It does not run a
delivery loop and exposes no public dispatch API yet. The shape of
the public engine surface is intentionally deferred until the
contract value types are reviewed and locked.

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

- No real delivery loop (no timer, no scheduler, no in-flight queue).
- No network implementation, no `URLSession`, no socket transport.
- No `swift-logger-elastic` migration.
- No Datadog/Splunk/Loki/Dynatrace adapter packages.
- No SDK-backed adapters (Datadog mobile SDK, Splunk RUM SDK,
  Dynatrace OneAgent) — those bypass this engine by design.
- No tags or releases.

## Deferred -- Flush Lifecycle

A flush-lifecycle vocabulary (e.g. opportunistic / lifecycle-driven /
manual) is intentionally deferred. Naming a `RemoteFlushPolicy.onLifecycle`
case would prematurely commit to platform lifecycle semantics
(`UIApplication`/`NSWorkspace`/etc.) before the host-supplied
lifecycle-observer surface is designed. The flush vocabulary lands
together with the engine loop in a later M3.4 milestone.

## Future Milestones

| Milestone | Scope |
| --- | --- |
| Future M3.4 milestone | Delivery loop, retry scheduler, batching engine, flush lifecycle vocabulary. |
| M3.5 | Migrate `swift-logger-elastic` onto this engine; Elastic-specific code stays in the adapter (ECS encoder, `_bulk` request builder, `_bulk` response validator). |
| M4 | Second remote adapter (Datadog Logs intake or Splunk HEC) as a protocol sanity check. |
| M5 | HTTP-backed adapter family built on top of this engine. |
