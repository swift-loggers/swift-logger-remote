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
        .package(url: "https://github.com/apple/swift-docc-plugin.git", from: "1.0.0")
    ],
    targets: [
        .target(
            name: "LoggerRemote"
        ),
        .testTarget(
            name: "LoggerRemoteTests",
            dependencies: [
                "LoggerRemote"
            ],
            exclude: [
                "CoverageMap.md"
            ]
        )
    ]
)
