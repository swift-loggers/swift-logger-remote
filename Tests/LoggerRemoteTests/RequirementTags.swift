import Testing

/// Swift Testing tags for `Docs/Requirements.md` requirement IDs.
///
/// One tag per LGR ID; tags without test references are retained
/// so the catalog stays 1:1 with the spec.
extension Tag {
    // MARK: Core Contract

    /// Durable delivery unit carries payload bytes, sink-owned metadata,
    /// and an engine-local correlation identifier.
    @Tag public static var lgr1: Self

    /// Delivery result is success, retryable failure, or terminal failure.
    @Tag public static var lgr2: Self

    /// Retry policy exposes retry limits and backoff schedule model.
    @Tag public static var lgr3: Self

    /// Batch policy exposes deterministic entry-count and byte-count boundaries.
    @Tag public static var lgr4: Self

    /// Transport surface is sink-neutral and does not impose HTTP semantics.
    @Tag public static var lgr5: Self

    /// Flush is caller-driven through `RemoteEngine.flush()`; the
    /// engine owns no timer or platform lifecycle observer and
    /// serializes concurrent calls via actor isolation.
    @Tag public static var lgr6: Self

    /// Delivery error surface is typed and sink-neutral.
    @Tag public static var lgr7: Self

    // MARK: Sink Neutrality

    /// Engine API shape is validated against non-equivalent backend models.
    @Tag public static var lgr8: Self

    /// Vendor encoders, request builders, and validators stay in adapters.
    @Tag public static var lgr9: Self

    // MARK: Persistence Coupling

    /// Accepted ordering is owned by swift-logger-persistence byte-stable export.
    @Tag public static var lgr10: Self

    /// Delivery acknowledgement is the only destructive removal trigger.
    @Tag public static var lgr11: Self
}
