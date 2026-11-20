// swift-tools-version: 6.0
import PackageDescription

// Library only. The runnable demo lives in a separate repository
// (agent-peer-sync-demo-app) that consumes this package as a remote,
// version-pinned dependency. There is deliberately no executable target here.
//
// Products:
// - AgentPeerSync: the sync engine (commit log, replica, policies, agent tool surface).
//   Foundation only, no Apple-only imports.
// - AgentPeerSyncTesting: an in-process transport with fault injection and the scripted
//   agent the demo plays. Link it from test targets or a demo, not from production code.
//
// Platforms: only what CI builds: macOS (tests), a generic iOS Simulator build, and
// Linux (tests).
let package = Package(
    name: "AgentPeerSync",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "AgentPeerSync", targets: ["AgentPeerSync"]),
        .library(name: "AgentPeerSyncTesting", targets: ["AgentPeerSyncTesting"]),
    ],
    targets: [
        .target(name: "AgentPeerSync"),
        .target(name: "AgentPeerSyncTesting", dependencies: ["AgentPeerSync"]),
        .testTarget(name: "AgentPeerSyncTests", dependencies: ["AgentPeerSync", "AgentPeerSyncTesting"]),
    ]
)
