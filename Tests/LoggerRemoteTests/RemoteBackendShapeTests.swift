import Testing

@testable import LoggerRemote

/// Backend-shape sanity coverage for sink-neutral result taxonomy.
///
/// Internal Elastic and Splunk fixtures must classify into
/// `RemoteDeliveryResult` without leaking vendor-specific response
/// fields into engine-visible errors.
@Suite("Remote engine backend-shape sanity")
struct RemoteBackendShapeTests {}

// MARK: - Elastic `_bulk` fixture

extension RemoteBackendShapeTests {
    /// Internal Elastic `_bulk` classifier fixture.
    private static func classifyElasticBulkItems(
        statusCodes: [Int]
    ) -> [RemoteDeliveryResult] {
        statusCodes.map { code in
            switch code {
            case 200, 201:
                return .success
            case 429, 502, 503, 504:
                return .retryable(reason: .transportRejected)
            default:
                return .terminal(reason: .transportRejected)
            }
        }
    }

    @Test(
        "Elastic bulk fixture maps vendor statuses to RemoteDeliveryResult",
        .tags(.lgr2, .lgr7, .lgr8, .lgr9)
    )
    func elasticBulkItemStatusesClassify() {
        // Mixed bulk: success, retryable (429), terminal (400).
        let results = Self.classifyElasticBulkItems(
            statusCodes: [201, 429, 400]
        )
        #expect(results == [
            .success,
            .retryable(reason: .transportRejected),
            .terminal(reason: .transportRejected)
        ])
    }
}

// MARK: - Splunk HEC fixture

extension RemoteBackendShapeTests {
    /// Internal Splunk HEC classifier fixture.
    private static func classifyHECResponse(
        httpStatusCode: Int,
        hecBodyCode: Int
    ) -> RemoteDeliveryResult {
        // Splunk HEC body codes: 0 = success, 9 = server-busy
        // (retryable), 13 = data too large (terminal).
        if httpStatusCode == 200, hecBodyCode == 0 {
            return .success
        }
        if hecBodyCode == 9 || httpStatusCode == 503 {
            return .retryable(reason: .transportRejected)
        }
        return .terminal(reason: .transportRejected)
    }

    @Test(
        "Splunk HEC fixture maps status and body code to RemoteDeliveryResult",
        .tags(.lgr2, .lgr7, .lgr8, .lgr9)
    )
    func hecResponseClassifies() {
        #expect(Self.classifyHECResponse(
            httpStatusCode: 200, hecBodyCode: 0
        ) == .success)
        #expect(Self.classifyHECResponse(
            httpStatusCode: 200, hecBodyCode: 9
        ) == .retryable(reason: .transportRejected))
        #expect(Self.classifyHECResponse(
            httpStatusCode: 503, hecBodyCode: 9
        ) == .retryable(reason: .transportRejected))
        #expect(Self.classifyHECResponse(
            httpStatusCode: 413, hecBodyCode: 13
        ) == .terminal(reason: .transportRejected))
    }
}
