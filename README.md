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
successfully drained batch (LGR-11).

## Status

Pre-release durable remote-delivery engine package. The core
contract surfaces are locked, the persistence-backed
``DurableRemoteQueue`` core is in place, engine-internal
``BatchEngine`` machinery recovers entries from a drained queue
export and splits them into deterministic batches under
``RemoteBatchPolicy``, and an engine-internal retry / execution
loop (``RetryExecutor`` / ``ExecutionLoop``) drives the per-entry
retry budget over the queue + batching + transport primitives
under ``RemoteRetryPolicy`` without performing the destructive
acknowledgement-to-removal lifecycle yet. Production
``RemoteTransport`` adapter integration, flush and lifecycle
integration, the
acknowledgement-to-removal lifecycle closure, vendor adapters, and
tagged releases ship in later milestones.

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

- No production transport integration yet. The engine-internal
  retry / execution loop exists and dispatches through the
  ``RemoteTransport`` abstraction, but this milestone exercises it
  through a test-only ``StubRemoteTransport`` fixture (not a
  production adapter); production adapter and network integration
  lands in a later PR.
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
