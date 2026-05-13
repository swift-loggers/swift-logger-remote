// swiftlint:disable file_length - Contract value-type catalog covers every locked public type (LGR-1 through LGR-7 plus the new PR 5/N public engine surface) in a single file so the public-API compile-shape proofs stay adjacent; splitting them would scatter the lock catalog without reducing maintenance burden.

import Foundation
import Testing

@testable import LoggerRemote

/// Coverage for remote-engine contract value types.
@Suite("Remote engine contract")
struct RemoteDeliveryContractTests {}

// MARK: - Sendable conformance

extension RemoteDeliveryContractTests {
    // swiftlint:disable function_body_length
    // Reason: Single compile-shape Sendable proof covers every locked public value type (LGR-1…LGR-7) including the PR 5/N public engine surface; splitting into per-type tests would scatter the lock catalog without adding coverage.
    @Test(
        "Contract value types are Sendable",
        .tags(.lgr1, .lgr2, .lgr3, .lgr4, .lgr5, .lgr6, .lgr7)
    )
    func contractValueTypesAreSendable() throws {
        let entry: any Sendable = RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        )
        let result: any Sendable = RemoteDeliveryResult.success
        let retry: any Sendable = try RemoteRetryPolicy.make(
            maxAttempts: 1, backoff: .constant(seconds: 1)
        )
        let batch: any Sendable = try RemoteBatchPolicy.make(
            maxEntryCount: 1, maxByteCount: 1
        )
        let response: any Sendable = RemoteTransportResponse(
            responseBytes: Data()
        )
        let batchItem: any Sendable = RemoteTransportBatchItem(
            payloadBytes: Data([0x01]),
            payloadMetadata: ["sink": "example"]
        )
        let error: any Sendable = RemoteDeliveryError.batchEmpty
        let summary: any Sendable = RemoteFlushSummary(
            attemptedBatches: 0,
            attemptedEntries: 0,
            succeededEntries: 0,
            terminalEntries: 0,
            retryableEntries: 0,
            acknowledgement: .emptyReleased
        )
        let acknowledgement: any Sendable = RemoteFlushAcknowledgement
            .removedDeliveredBytes
        let engineError: any Sendable = RemoteEngineError
            .retryInterrupted(.sleepInterrupted)
        let parseError: any Sendable = RemoteEngineParseError
            .recordPayloadMalformed
        let retryError: any Sendable = RemoteEngineRetryError
            .sleepInterrupted
        let cleanupContext: any Sendable = RemoteEngineExportCleanupContext(
            exportURL: URL(fileURLWithPath: "/dev/null"),
            phase: .acknowledgedNonEmpty,
            errorDomain: "test",
            errorCode: 0
        )
        let cleanupPhase: any Sendable =
            RemoteEngineExportCleanupContext.Phase.acknowledgedNonEmpty
        // Reaching this assertion proves each type satisfies the
        // existential `Sendable` requirement at compile time.
        #expect(entry is RemoteDeliveryEntry)
        #expect(result is RemoteDeliveryResult)
        #expect(retry is RemoteRetryPolicy)
        #expect(batch is RemoteBatchPolicy)
        #expect(response is RemoteTransportResponse)
        #expect(batchItem is RemoteTransportBatchItem)
        #expect(error is RemoteDeliveryError)
        #expect(summary is RemoteFlushSummary)
        #expect(acknowledgement is RemoteFlushAcknowledgement)
        #expect(engineError is RemoteEngineError)
        #expect(parseError is RemoteEngineParseError)
        #expect(retryError is RemoteEngineRetryError)
        #expect(cleanupContext is RemoteEngineExportCleanupContext)
        #expect(cleanupPhase is RemoteEngineExportCleanupContext.Phase)
    }

    // swiftlint:enable function_body_length

    @Test(
        "Engine-internal contract types are Sendable",
        .tags(.lgr3, .lgr5, .lgr7)
    )
    func internalEngineContractTypesAreSendable() {
        // `BatchDeliveryError` and `ExecutionLoopError` are
        // engine-internal but locked diagnostic surfaces the
        // batch-round dispatcher and the one-shot execution loop
        // raise. They reach the test target through
        // `@testable import` and MUST satisfy the existential
        // `Sendable` requirement so engine code can throw them
        // across the actor boundary `RemoteEngine.flush()` runs on
        // without `Sendable`-conformance noise.
        let batchDeliveryError: any Sendable = BatchDeliveryError
            .sleepInterrupted
        let executionLoopError: any Sendable = ExecutionLoopError
            .sleepInterrupted
        #expect(batchDeliveryError is BatchDeliveryError)
        #expect(executionLoopError is ExecutionLoopError)
    }
}

// MARK: - Public engine surface Equatable shape

extension RemoteDeliveryContractTests {
    // swiftlint:disable function_body_length
    // Reason: One per-type compile-shape Equatable proof for every
    // new public engine surface (`RemoteFlushSummary`,
    // `RemoteEngineError`, `RemoteEngineParseError`,
    // `RemoteEngineRetryError`) belongs in a single test because
    // the cases interlock — splitting would scatter the
    // associated-value mirroring proofs across multiple tests
    // without adding coverage.

    @Test(
        "Public engine surface types are Equatable across resolved + failure outcomes",
        .tags(.lgr6, .lgr7)
    )
    func engineSurfaceTypesAreEquatable() {
        // RemoteFlushSummary: identical fields compare equal,
        // single-field mutation compares unequal.
        let leftSummary = RemoteFlushSummary(
            attemptedBatches: 1,
            attemptedEntries: 2,
            succeededEntries: 1,
            terminalEntries: 1,
            retryableEntries: 0,
            acknowledgement: .removedDeliveredBytes
        )
        let rightSummary = RemoteFlushSummary(
            attemptedBatches: 1,
            attemptedEntries: 2,
            succeededEntries: 1,
            terminalEntries: 1,
            retryableEntries: 0,
            acknowledgement: .removedDeliveredBytes
        )
        #expect(leftSummary == rightSummary)
        let mutatedSummary = RemoteFlushSummary(
            attemptedBatches: 1,
            attemptedEntries: 2,
            succeededEntries: 1,
            terminalEntries: 1,
            retryableEntries: 0,
            acknowledgement: .notAcknowledged
        )
        #expect(leftSummary != mutatedSummary)

        // RemoteFlushAcknowledgement: all three lifecycle cases
        // are distinct.
        #expect(
            RemoteFlushAcknowledgement.emptyReleased
                != RemoteFlushAcknowledgement.removedDeliveredBytes
        )
        #expect(
            RemoteFlushAcknowledgement.removedDeliveredBytes
                != RemoteFlushAcknowledgement.notAcknowledged
        )

        // RemoteEngineError: every case is distinct; associated
        // values participate in equality.
        #expect(
            RemoteEngineError.flushFailed(.batchAlreadyOutstanding)
                == RemoteEngineError.flushFailed(.batchAlreadyOutstanding)
        )
        #expect(
            RemoteEngineError.flushFailed(.batchAlreadyOutstanding)
                != RemoteEngineError.drainFailed(.batchAlreadyOutstanding)
        )
        #expect(
            RemoteEngineError.parseFailed(.recordPayloadMalformed)
                != RemoteEngineError.parseFailed(.recordPayloadBase64Invalid)
        )
        #expect(
            RemoteEngineError.retryInterrupted(.sleepInterrupted)
                != RemoteEngineError.retryInterrupted(
                    .invalidRetryDelay(.invalidRetryPolicy)
                )
        )
        #expect(
            RemoteEngineError.acknowledgementFailed(.batchAlreadyOutstanding)
                == RemoteEngineError.acknowledgementFailed(.batchAlreadyOutstanding)
        )
        // `batchFailed` carries a `RemoteDeliveryError` verbatim;
        // identical inner cases compare equal, distinct inner
        // cases compare unequal.
        #expect(
            RemoteEngineError.batchFailed(.invalidBatchState)
                == RemoteEngineError.batchFailed(.invalidBatchState)
        )
        #expect(
            RemoteEngineError.batchFailed(.invalidBatchState)
                != RemoteEngineError.batchFailed(
                    .batchSizeExceeded(limit: 1, actual: 2)
                )
        )
        // `transportBatchInvalid` carries `expected` / `actual`
        // verbatim; both fields participate in equality.
        #expect(
            RemoteEngineError.transportBatchInvalid(expected: 5, actual: 4)
                == RemoteEngineError.transportBatchInvalid(expected: 5, actual: 4)
        )
        #expect(
            RemoteEngineError.transportBatchInvalid(expected: 5, actual: 4)
                != RemoteEngineError.transportBatchInvalid(expected: 5, actual: 3)
        )
        #expect(
            RemoteEngineError.transportBatchInvalid(expected: 5, actual: 4)
                != RemoteEngineError.transportBatchInvalid(expected: 6, actual: 4)
        )

        // RemoteTransportBatchItem: every stored field participates
        // in equality so per-item correlation between adapter
        // input and adapter output is round-trip-stable.
        let leftItem = RemoteTransportBatchItem(
            payloadBytes: Data([0x01, 0x02]),
            payloadMetadata: ["sink": "example"]
        )
        let sameItem = RemoteTransportBatchItem(
            payloadBytes: Data([0x01, 0x02]),
            payloadMetadata: ["sink": "example"]
        )
        #expect(leftItem == sameItem)
        #expect(
            leftItem != RemoteTransportBatchItem(
                payloadBytes: Data([0x01]),
                payloadMetadata: ["sink": "example"]
            )
        )
        #expect(
            leftItem != RemoteTransportBatchItem(
                payloadBytes: Data([0x01, 0x02]),
                payloadMetadata: ["sink": "other"]
            )
        )

        // RemoteEngineParseError: associated-value mirroring
        // preserves expected/actual / expected/found / found/supported.
        #expect(
            RemoteEngineParseError.exportByteCountMismatch(expected: 1, actual: 2)
                != RemoteEngineParseError.exportByteCountMismatch(expected: 1, actual: 3)
        )
        #expect(
            RemoteEngineParseError.envelopeContentTypeMismatch(
                expected: "application/x-test", found: "application/x-other"
            )
                != RemoteEngineParseError.envelopeContentTypeMismatch(
                    expected: "application/x-test", found: "application/x-different"
                )
        )
        #expect(
            RemoteEngineParseError.recordFormatVersionUnsupported(found: 999, supported: 1)
                != RemoteEngineParseError.recordFormatVersionUnsupported(found: 2, supported: 1)
        )

        // RemoteEngineRetryError: two distinct cases compare
        // unequal; the `invalidRetryDelay` associated value
        // participates in equality.
        #expect(
            RemoteEngineRetryError.sleepInterrupted
                != RemoteEngineRetryError.invalidRetryDelay(.invalidRetryPolicy)
        )
        #expect(
            RemoteEngineRetryError.invalidRetryDelay(.invalidRetryPolicy)
                != RemoteEngineRetryError.invalidRetryDelay(.invalidBatchPolicy)
        )

        // RemoteEngineExportCleanupContext: every stored field
        // participates in equality so the diagnostic context
        // round-trips across error boundaries verbatim.
        let leftContext = RemoteEngineExportCleanupContext(
            exportURL: URL(fileURLWithPath: "/tmp/a.ndjson"),
            phase: .acknowledgedNonEmpty,
            errorDomain: "NSCocoaErrorDomain",
            errorCode: 4
        )
        let sameContext = RemoteEngineExportCleanupContext(
            exportURL: URL(fileURLWithPath: "/tmp/a.ndjson"),
            phase: .acknowledgedNonEmpty,
            errorDomain: "NSCocoaErrorDomain",
            errorCode: 4
        )
        #expect(leftContext == sameContext)
        #expect(
            leftContext
                != RemoteEngineExportCleanupContext(
                    exportURL: URL(fileURLWithPath: "/tmp/b.ndjson"),
                    phase: .acknowledgedNonEmpty,
                    errorDomain: "NSCocoaErrorDomain",
                    errorCode: 4
                )
        )
        // `phase` participates in context equality: a context
        // identical to `leftContext` except for the cleanup phase
        // must compare unequal so the diagnostic case
        // round-trips the lifecycle phase, not just the URL +
        // domain + code triple.
        let phaseDifferentContext = RemoteEngineExportCleanupContext(
            exportURL: URL(fileURLWithPath: "/tmp/a.ndjson"),
            phase: .emptyRelease,
            errorDomain: "NSCocoaErrorDomain",
            errorCode: 4
        )
        #expect(leftContext != phaseDifferentContext)
        // `errorDomain` participates in context equality: a
        // context identical to `leftContext` except for the
        // bridged `NSError.domain` must compare unequal so the
        // diagnostic preserves the failure namespace.
        let domainDifferentContext = RemoteEngineExportCleanupContext(
            exportURL: URL(fileURLWithPath: "/tmp/a.ndjson"),
            phase: .acknowledgedNonEmpty,
            errorDomain: "NSPOSIXErrorDomain",
            errorCode: 4
        )
        #expect(leftContext != domainDifferentContext)
        // `errorCode` participates in context equality: a context
        // identical to `leftContext` except for the bridged
        // `NSError.code` must compare unequal so the diagnostic
        // preserves the failure code verbatim.
        let codeDifferentContext = RemoteEngineExportCleanupContext(
            exportURL: URL(fileURLWithPath: "/tmp/a.ndjson"),
            phase: .acknowledgedNonEmpty,
            errorDomain: "NSCocoaErrorDomain",
            errorCode: 5
        )
        #expect(leftContext != codeDifferentContext)
        #expect(
            RemoteEngineExportCleanupContext.Phase.emptyRelease
                != RemoteEngineExportCleanupContext.Phase.acknowledgedNonEmpty
        )
        // Cleanup-failure case round-trips through `RemoteEngineError`
        // equality with the inner context value participating.
        #expect(
            RemoteEngineError.exportCleanupFailed(leftContext)
                == RemoteEngineError.exportCleanupFailed(sameContext)
        )
        #expect(
            RemoteEngineError.exportCleanupFailed(leftContext)
                != RemoteEngineError.exportCleanupFailed(phaseDifferentContext)
        )
        #expect(
            RemoteEngineError.exportCleanupFailed(leftContext)
                != RemoteEngineError.acknowledgementFailed(.batchAlreadyOutstanding)
        )
    }

    // swiftlint:enable function_body_length
}

// MARK: - RemoteRetryPolicy validation

extension RemoteDeliveryContractTests {
    @Test(
        "`RemoteRetryPolicy.make` rejects a zero attempt count",
        .tags(.lgr3, .lgr7)
    )
    func retryPolicyRejectsZeroAttempts() {
        do {
            _ = try RemoteRetryPolicy.make(
                maxAttempts: 0, backoff: .constant(seconds: 1)
            )
            Issue.record("expected .invalidRetryPolicy for maxAttempts = 0")
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
    }

    @Test(
        "`RemoteRetryPolicy.make` rejects maxAttempts above maxSupportedAttempts",
        .tags(.lgr3, .lgr7)
    )
    func retryPolicyRejectsAttemptsAboveSupported() {
        do {
            _ = try RemoteRetryPolicy.make(
                maxAttempts: RemoteRetryPolicy.maxSupportedAttempts + 1,
                backoff: .constant(seconds: 1)
            )
            Issue.record(
                "expected .invalidRetryPolicy for maxAttempts > maxSupportedAttempts"
            )
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
    }

    @Test(
        "`RemoteRetryPolicy.make` rejects a non-positive constant backoff",
        .tags(.lgr3, .lgr7)
    )
    func retryPolicyRejectsNonPositiveConstantBackoff() {
        do {
            _ = try RemoteRetryPolicy.make(
                maxAttempts: 3, backoff: .constant(seconds: 0)
            )
            Issue.record("expected .invalidRetryPolicy for constant 0 seconds")
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
    }

    @Test(
        "`RemoteRetryPolicy.make` rejects constant backoff above maxBackoffSeconds",
        .tags(.lgr3, .lgr7)
    )
    func retryPolicyRejectsConstantBackoffAboveBound() {
        do {
            _ = try RemoteRetryPolicy.make(
                maxAttempts: 3,
                backoff: .constant(seconds: RemoteRetryPolicy.maxBackoffSeconds + 1)
            )
            Issue.record(
                "expected .invalidRetryPolicy for constant > maxBackoffSeconds"
            )
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
    }

    @Test(
        "`RemoteRetryPolicy.make` rejects exponential multiplier == 1",
        .tags(.lgr3, .lgr7)
    )
    func retryPolicyRejectsExponentialMultiplierEqualOne() {
        do {
            _ = try RemoteRetryPolicy.make(
                maxAttempts: 3,
                backoff: .exponential(
                    initialSeconds: 1, multiplier: 1, capSeconds: 8
                )
            )
            Issue.record(
                "expected .invalidRetryPolicy for exponential multiplier == 1"
            )
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
    }

    @Test(
        "`RemoteRetryPolicy.make` rejects exponential cap below initial seconds",
        .tags(.lgr3, .lgr7)
    )
    func retryPolicyRejectsExponentialCapBelowInitial() {
        do {
            _ = try RemoteRetryPolicy.make(
                maxAttempts: 3,
                backoff: .exponential(
                    initialSeconds: 2, multiplier: 2, capSeconds: 1
                )
            )
            Issue.record("expected .invalidRetryPolicy when cap < initial")
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
    }

    @Test(
        "`RemoteRetryPolicy.make` rejects exponential cap above maxBackoffSeconds",
        .tags(.lgr3, .lgr7)
    )
    func retryPolicyRejectsExponentialCapAboveBound() {
        do {
            _ = try RemoteRetryPolicy.make(
                maxAttempts: 3,
                backoff: .exponential(
                    initialSeconds: 1,
                    multiplier: 2,
                    capSeconds: RemoteRetryPolicy.maxBackoffSeconds + 1
                )
            )
            Issue.record(
                "expected .invalidRetryPolicy for exponential cap > maxBackoffSeconds"
            )
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
    }

    @Test(
        "`RemoteRetryPolicy.make` accepts a well-formed exponential schedule",
        .tags(.lgr3)
    )
    func retryPolicyAcceptsWellFormedExponential() throws {
        let policy = try RemoteRetryPolicy.make(
            maxAttempts: 3,
            backoff: .exponential(
                initialSeconds: 0.5, multiplier: 2, capSeconds: 8
            )
        )
        #expect(policy.maxAttempts == 3)
        #expect(policy.backoff == .exponential(
            initialSeconds: 0.5, multiplier: 2, capSeconds: 8
        ))
    }
}

// MARK: - RemoteRetryPolicy delayBeforeRetry

extension RemoteDeliveryContractTests {
    @Test(
        "`delayBeforeRetry` constant: every valid attempt returns the same seconds",
        .tags(.lgr3)
    )
    func delayBeforeRetryConstantReturnsSameSeconds() throws {
        let policy = try RemoteRetryPolicy.make(
            maxAttempts: 4, backoff: .constant(seconds: 2.5)
        )
        for attempt in 1 ..< policy.maxAttempts {
            #expect(try policy.delayBeforeRetry(attempt: attempt) == 2.5)
        }
    }

    @Test(
        "`delayBeforeRetry` exponential: attempts progress as initial * multiplier^(n - 1)",
        .tags(.lgr3)
    )
    func delayBeforeRetryExponentialProgression() throws {
        let policy = try RemoteRetryPolicy.make(
            maxAttempts: 5,
            backoff: .exponential(
                initialSeconds: 1, multiplier: 2, capSeconds: 100
            )
        )
        #expect(try policy.delayBeforeRetry(attempt: 1) == 1)
        #expect(try policy.delayBeforeRetry(attempt: 2) == 2)
        #expect(try policy.delayBeforeRetry(attempt: 3) == 4)
        #expect(try policy.delayBeforeRetry(attempt: 4) == 8)
    }

    @Test(
        "`delayBeforeRetry` exponential: clamps at capSeconds when raw exceeds cap",
        .tags(.lgr3)
    )
    func delayBeforeRetryExponentialClampsAtCap() throws {
        let policy = try RemoteRetryPolicy.make(
            maxAttempts: 50,
            backoff: .exponential(
                initialSeconds: 1, multiplier: 2, capSeconds: 16
            )
        )
        // attempt 5 → 16 (== cap, fits)
        #expect(try policy.delayBeforeRetry(attempt: 5) == 16)
        // attempt 6 → 32 > 16 → clamp to 16
        #expect(try policy.delayBeforeRetry(attempt: 6) == 16)
        // Later attempts exceed capSeconds through iterative growth;
        // helper clamps to capSeconds.
        #expect(try policy.delayBeforeRetry(attempt: 49) == 16)
    }

    @Test(
        "`delayBeforeRetry` rejects an attempt outside `1 ..< maxAttempts`",
        .tags(.lgr3, .lgr7)
    )
    func delayBeforeRetryRejectsInvalidAttempt() throws {
        let policy = try RemoteRetryPolicy.make(
            maxAttempts: 3, backoff: .constant(seconds: 1)
        )
        // attempt 0 (below range)
        do {
            _ = try policy.delayBeforeRetry(attempt: 0)
            Issue.record("expected .invalidRetryPolicy for attempt = 0")
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
        // attempt == maxAttempts (last attempt has no successor)
        do {
            _ = try policy.delayBeforeRetry(attempt: 3)
            Issue.record("expected .invalidRetryPolicy for attempt == maxAttempts")
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
        // attempt > maxAttempts
        do {
            _ = try policy.delayBeforeRetry(attempt: 4)
            Issue.record("expected .invalidRetryPolicy for attempt > maxAttempts")
        } catch {
            #expect(error == .invalidRetryPolicy)
        }
    }
}

// MARK: - RemoteBatchPolicy boundary behavior

extension RemoteDeliveryContractTests {
    @Test(
        "`RemoteBatchPolicy.make` rejects zero entry cap",
        .tags(.lgr4, .lgr7)
    )
    func batchPolicyRejectsZeroEntryCap() {
        do {
            _ = try RemoteBatchPolicy.make(maxEntryCount: 0, maxByteCount: 1)
            Issue.record("expected .invalidBatchPolicy for entry cap = 0")
        } catch {
            #expect(error == .invalidBatchPolicy)
        }
    }

    @Test(
        "`RemoteBatchPolicy.make` rejects zero byte cap",
        .tags(.lgr4, .lgr7)
    )
    func batchPolicyRejectsZeroByteCap() {
        do {
            _ = try RemoteBatchPolicy.make(maxEntryCount: 1, maxByteCount: 0)
            Issue.record("expected .invalidBatchPolicy for byte cap = 0")
        } catch {
            #expect(error == .invalidBatchPolicy)
        }
    }

    @Test(
        "Batch boundary: equal-to-cap fits, strictly-greater fires",
        .tags(.lgr4)
    )
    func batchBoundaryEqualToCapFits() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 4, maxByteCount: 100
        )
        // Adding one more entry pushes count from 3 → 4 (== cap, fits).
        #expect(try policy.wouldExceed(
            currentEntryCount: 3, currentByteCount: 0, nextEntryByteCount: 0
        ) == false)
        // Adding one more entry pushes count from 4 → 5 (> cap, fires).
        #expect(try policy.wouldExceed(
            currentEntryCount: 4, currentByteCount: 0, nextEntryByteCount: 0
        ) == true)
        // Bytes equal-to-cap fit.
        #expect(try policy.wouldExceed(
            currentEntryCount: 0, currentByteCount: 50, nextEntryByteCount: 50
        ) == false)
        // Bytes strictly-greater fire.
        #expect(try policy.wouldExceed(
            currentEntryCount: 0, currentByteCount: 50, nextEntryByteCount: 51
        ) == true)
    }

    @Test(
        "Batch boundary: pathological byte-count overflow fires the boundary",
        .tags(.lgr4)
    )
    func batchBoundaryByteOverflowFires() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: .max, maxByteCount: .max
        )
        // current + next would overflow Int → addingReportingOverflow
        // returns overflow, helper must fire the boundary instead of
        // trapping.
        #expect(try policy.wouldExceed(
            currentEntryCount: 1,
            currentByteCount: .max,
            nextEntryByteCount: 1
        ) == true)
    }

    @Test(
        "Batch boundary: entry-count overflow fires the boundary",
        .tags(.lgr4)
    )
    func batchBoundaryEntryCountOverflowFires() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: .max, maxByteCount: .max
        )
        // currentEntryCount = .max, +1 overflows Int → helper must
        // fire the boundary, not trap.
        #expect(try policy.wouldExceed(
            currentEntryCount: .max,
            currentByteCount: 0,
            nextEntryByteCount: 0
        ) == true)
    }

    @Test(
        "Batch boundary: negative inputs surface .invalidBatchState",
        .tags(.lgr4, .lgr7)
    )
    func batchBoundaryNegativeInputsRejected() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 4, maxByteCount: 100
        )
        for inputs in [
            (-1, 0, 0),
            (0, -1, 0),
            (0, 0, -1)
        ] {
            do {
                _ = try policy.wouldExceed(
                    currentEntryCount: inputs.0,
                    currentByteCount: inputs.1,
                    nextEntryByteCount: inputs.2
                )
                Issue.record(
                    "expected .invalidBatchState for inputs \(inputs)"
                )
            } catch {
                #expect(error == .invalidBatchState)
            }
        }
    }

    @Test(
        "Batch boundary: current state already beyond policy surfaces .invalidBatchState",
        .tags(.lgr4, .lgr7)
    )
    func batchBoundaryCurrentBeyondPolicyRejected() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 4, maxByteCount: 100
        )
        // currentEntryCount past cap.
        do {
            _ = try policy.wouldExceed(
                currentEntryCount: 5, currentByteCount: 0, nextEntryByteCount: 0
            )
            Issue.record(
                "expected .invalidBatchState for currentEntryCount > maxEntryCount"
            )
        } catch {
            #expect(error == .invalidBatchState)
        }
        // currentByteCount past cap.
        do {
            _ = try policy.wouldExceed(
                currentEntryCount: 0, currentByteCount: 101, nextEntryByteCount: 0
            )
            Issue.record(
                "expected .invalidBatchState for currentByteCount > maxByteCount"
            )
        } catch {
            #expect(error == .invalidBatchState)
        }
    }

    @Test(
        "Batch boundary: oversized single entry rejected as .batchSizeExceeded",
        .tags(.lgr4, .lgr7)
    )
    func batchBoundaryOversizedSingleEntryRejected() throws {
        let policy = try RemoteBatchPolicy.make(
            maxEntryCount: 4, maxByteCount: 100
        )
        do {
            _ = try policy.wouldExceed(
                currentEntryCount: 0,
                currentByteCount: 0,
                nextEntryByteCount: 101
            )
            Issue.record(
                "expected .batchSizeExceeded for oversized single entry"
            )
        } catch {
            #expect(error == .batchSizeExceeded(limit: 100, actual: 101))
        }
    }
}

// MARK: - RemoteDeliveryEntry identifier semantics

extension RemoteDeliveryContractTests {
    @Test(
        "Delivery entry identifier is correlation identity only, no ordering",
        .tags(.lgr1)
    )
    func deliveryEntryIdentifierParticipatesInEqualityButCarriesNoOrdering() {
        let payload = Data([0x01, 0x02])
        let metadata = ["sink": "elastic"]

        let first = RemoteDeliveryEntry(
            identifier: 1, payload: payload, metadata: metadata
        )
        let second = RemoteDeliveryEntry(
            identifier: 2, payload: payload, metadata: metadata
        )

        #expect(first != second)
        #expect(first.payload == second.payload)
        #expect(first.metadata == second.metadata)
    }
}

// MARK: - RemoteTransportResponse preservation

extension RemoteDeliveryContractTests {
    @Test(
        "Transport response preserves opaque bytes and metadata",
        .tags(.lgr5)
    )
    func transportResponsePreservesOpaqueBytesAndMetadata() {
        let bytes = Data([0x7B, 0x7D])
        let response = RemoteTransportResponse(
            responseBytes: bytes,
            responseMetadata: ["vendor-code": "ok"]
        )
        let defaultMetadata = RemoteTransportResponse(responseBytes: bytes)

        #expect(response.responseBytes == bytes)
        #expect(response.responseMetadata == ["vendor-code": "ok"])
        #expect(defaultMetadata.responseMetadata.isEmpty)
    }
}
