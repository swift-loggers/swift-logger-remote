import Foundation
import Testing

@testable import LoggerRemote

/// Coverage for remote-engine contract value types.
@Suite("Remote engine contract")
struct RemoteDeliveryContractTests {}

// MARK: - Sendable conformance

extension RemoteDeliveryContractTests {
    @Test(
        "Contract value types are Sendable",
        .tags(.lgr1, .lgr2, .lgr3, .lgr4, .lgr5, .lgr7)
    )
    func contractValueTypesAreSendable() {
        let entry: any Sendable = RemoteDeliveryEntry(
            identifier: 1, payload: Data([0x01])
        )
        let result: any Sendable = RemoteDeliveryResult.success
        let retry: any Sendable = try? RemoteRetryPolicy.make(
            maxAttempts: 1, backoff: .constant(seconds: 1)
        )
        let batch: any Sendable = try? RemoteBatchPolicy.make(
            maxEntryCount: 1, maxByteCount: 1
        )
        let response: any Sendable = RemoteTransportResponse(
            responseBytes: Data()
        )
        let error: any Sendable = RemoteDeliveryError.batchEmpty
        // Reaching this assertion proves each type satisfies the
        // existential `Sendable` requirement at compile time.
        #expect(entry is RemoteDeliveryEntry)
        #expect(result is RemoteDeliveryResult)
        #expect(retry is RemoteRetryPolicy?)
        #expect(batch is RemoteBatchPolicy?)
        #expect(response is RemoteTransportResponse)
        #expect(error is RemoteDeliveryError)
    }
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

// MARK: - RemoteEngine placeholder

extension RemoteDeliveryContractTests {
    @Test("`RemoteEngine` placeholder is constructible with no public dispatch surface")
    func remoteEnginePlaceholderConstructs() async {
        let engine = RemoteEngine()
        // Current scope intentionally exposes no dispatch API.
        _ = engine
    }
}
