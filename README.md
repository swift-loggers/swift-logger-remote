# swift-logger-remote

Durable remote delivery engine for [`swift-loggers`](https://github.com/swift-loggers/swift-logger).

The package owns the sink-neutral remote-delivery contract that
vendor-specific remote adapters (Elasticsearch, Splunk HEC, Loki,
Dynatrace Log Monitoring, Datadog Logs, …) build on top of. It
depends on [`swift-logger-persistence`](https://github.com/swift-loggers/swift-logger-persistence)
at the released `0.1.x` SemVer line via
`.upToNextMinor(from: "0.1.0")` and consumes the byte-stable queue
export from the persistence layer exclusively through
``DurableRemoteQueue``. Destructive removal of accepted bytes from
the persistence layer runs only after the engine acknowledges a
drained batch (LGR-11).

## Status

Pre-1.0 scaffold for the durable remote-delivery contract layer.
Current scope is the contract value types, a persistence-backed
``DurableRemoteQueue`` core, and engine-internal ``BatchEngine``
machinery that recovers entries from a drained queue export and
splits them into deterministic batches under ``RemoteBatchPolicy``.
Retry scheduler, real ``RemoteTransport`` dispatch, flush /
lifecycle integration, vendor adapters, and tagged releases ship
in later milestones.

## Queue envelope contract

``DurableRemoteQueue`` owns the persistence envelope `contentType`
as a queue-internal constant. Callers cannot customize it. The
engine-internal ``BatchEngine`` validates the envelope
`contentType` against the same constant fail-closed before
decoding any queue records, so a foreign envelope (one the queue
did not produce) is refused at recovery time rather than silently
decoded as a malformed queue record. No deprecated `contentType:`
initializer overload is kept: `swift-logger-remote` has no
released tag yet, so the public API is being locked before the
first release.

## Non-goals

- No retry scheduler and no real ``RemoteTransport`` dispatch in
  the core engine yet (no timer, no network, no in-flight queue).
- No vendor-specific encoders, request builders, or response
  validators in the core engine — those live in adapter packages.
- No Datadog/Splunk/Loki/Dynatrace adapter packages here.
- No SDK-backed adapters (those bypass this engine by design).
- No tags or releases yet.

## Documentation

- [`Docs/APIDesign.md`](Docs/APIDesign.md) — remote-delivery
  contract surface and sink-neutral ownership boundaries.
- [`Docs/Requirements.md`](Docs/Requirements.md) — `LGR-1 …` requirements
  catalog with milestone status.
