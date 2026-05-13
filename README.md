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
fully-resolved non-empty flush pass (LGR-11).

## Status

`0.1.0` establishes the public durable remote-delivery engine
surface for the `0.1.x` line. M3.4 is complete: the core contract
surfaces are locked, the persistence-backed
``DurableRemoteQueue`` core is in place, engine-internal
``BatchEngine`` machinery recovers entries from a drained queue
export and splits them into deterministic batches under
``RemoteBatchPolicy``, the engine-internal ``ExecutionLoop``
drives delivery as **batch rounds** against
``RemoteTransport.sendBatch(_:)`` as the sole transport dispatch
primitive (one ordered ``sendBatch(_:)`` invocation per round
against the retained active-set; the retained active-set carries
only the entries whose previous classification was retryable
from the previous round, preserving their original input
ordering; the ``RemoteRetryPolicy`` budget is tracked per entry
across batch rounds for the lifetime of the flush pass,
advancing one attempt for each entry that stays in the
active-set per round), and the public ``RemoteEngine`` actor
wraps that loop with the caller-driven ``flush()`` surface, the
acknowledgement-to-removal lifecycle closure for non-empty flush
passes, and the ``RemoteTransport.classify(_:)`` sink-owned
per-item classification hook for ``sendBatch(_:)`` results owned
by the transport adapter (deterministic within a flush pass for
the same transport result and adapter implementation, and that
MUST NOT mutate acknowledgement or export-file lifecycle state
directly or indirectly). Concrete vendor adapters (Elastic `_bulk`,
Splunk HEC, …) ship as separate packages in later milestones.

## Queue envelope contract

``DurableRemoteQueue`` owns the persistence envelope `contentType`
as a queue-internal constant. Callers cannot customize it. The
engine-internal ``BatchEngine`` validates the envelope
`contentType` against the same constant fail-closed before
decoding any queue records, so a foreign envelope (one the queue
did not produce for the queue-owned envelope `contentType` for
the current queue format version) is refused at recovery time
rather than silently decoded as a malformed queue record. `0.1.0` ships without a
deprecated `contentType:` initializer overload; the public API is
locked around the queue-owned envelope contract.

## Non-goals

- No autonomous timer or scheduler in the public engine surface.
  Retry backoff still uses injected Swift concurrency sleep
  primitives internally. The engine is caller-driven: hosts
  decide when to invoke ``RemoteEngine/flush()`` from their own
  lifecycle hooks (`UIApplication` background notifications,
  `NSWorkspace` power-off, shutdown signals, periodic tasks).
- No concrete vendor adapter (Elastic `_bulk`, Splunk HEC, …) in
  this package. The engine dispatches through any
  ``RemoteTransport`` conformer implementing the batch-round
  transport contract through ``RemoteTransport.sendBatch(_:)`` as
  the sole transport dispatch primitive that satisfies the
  deterministic classification contract; concrete adapters ship
  as separate packages in later milestones. `0.1.0` test coverage
  exercises the engine through a test-only
  ``StubRemoteTransport`` fixture.
- No vendor-specific encoders, request builders, or response
  validators in the core engine — those live in adapter packages.
- No Datadog/Splunk/Loki/Dynatrace adapter packages here.
- No SDK-backed adapters (those bypass this engine by design).

## Documentation

- [`Docs/APIDesign.md`](Docs/APIDesign.md) — remote-delivery
  contract surface and sink-neutral ownership boundaries.
- [`Docs/Requirements.md`](Docs/Requirements.md) — `LGR-1 …` requirements
  catalog with milestone status.
