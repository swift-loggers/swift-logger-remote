// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "swift-logger-remote",
    platforms: [
        .iOS("13.4"),
        .tvOS("13.4"),
        .macOS("10.15.4"),
        .watchOS("6.2"),
        .visionOS(.v1)
    ],
    products: [
        .library(
            name: "LoggerRemote",
            targets: ["LoggerRemote"]
        )
    ],
    dependencies: [
        // Pre-1.0 dependency: pin to the `0.1.x` patch range so a
        // future `0.2.0` does not auto-resolve through SwiftPM's
        // `from:` (up-to-next-major) semantics.
        .package(
            url: "https://github.com/swift-loggers/swift-logger-persistence.git",
            .upToNextMinor(from: "0.1.0")
        ),
        .package(url: "https://github.com/apple/swift-docc-plugin.git", from: "1.0.0")
    ],
    targets: [
        .target(
            name: "LoggerRemote",
            dependencies: [
                .product(name: "LoggerPersistence", package: "swift-logger-persistence"),
                .product(name: "LoggerFilePersistence", package: "swift-logger-persistence")
            ]
        ),
        .testTarget(
            name: "LoggerRemoteTests",
            dependencies: [
                "LoggerRemote",
                .product(name: "LoggerPersistence", package: "swift-logger-persistence"),
                .product(name: "LoggerFilePersistence", package: "swift-logger-persistence")
            ],
            exclude: [
                "CoverageMap.md"
            ]
        )
    ]
)
