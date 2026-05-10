# swift-logger-remote

Durable remote delivery engine for [`swift-loggers`](https://github.com/swift-loggers/swift-logger).

The package owns the sink-neutral remote-delivery contract that
vendor-specific remote adapters (Elasticsearch, Splunk HEC, Loki,
Dynatrace Log Monitoring, Datadog Logs, …) build on top of.
[`swift-logger-persistence`](https://github.com/swift-loggers/swift-logger-persistence)
will own accepted ordering once the engine integrates with it: the
delivery engine is designed to consume accepted-line bytes from the
persistence layer and report delivery acknowledgement back so durable
removal can run. That integration lands in a future M3.4 milestone;
the current scope does not declare a persistence package dependency.

## Status

Pre-1.0 scaffold for the durable remote-delivery contract layer.
Current scope is contract value types only; delivery execution,
retry scheduling, batching, transports, and vendor adapters ship in
later milestones.

## Non-goals

- No real delivery loop yet (no timer, no scheduler, no network).
- No vendor-specific encoders, request builders, or response
  validators in the core engine — those live in adapter packages.
- No Datadog/Splunk/Loki/Dynatrace adapter packages here.
- No SDK-backed adapters (those bypass this engine by design).

## Documentation

- [`Docs/APIDesign.md`](Docs/APIDesign.md) — remote-delivery
  contract surface and sink-neutral ownership boundaries.
- [`Docs/Requirements.md`](Docs/Requirements.md) — `LGR-1 …` requirements
  catalog with milestone status.
