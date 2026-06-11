// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "Fleet",
    platforms: [
        .iOS(.v17),
        .macOS(.v14),
    ],
    products: [
        .library(
            name: "Fleet",
            targets: ["Fleet"]
        ),
    ],
    targets: [
        .target(
            name: "Fleet"
        ),
    ]
)
