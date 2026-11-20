import Foundation

/// A fixed-size, process-independent fingerprint of a mutation, so the server's replay
/// window holds 16 bytes per outcome instead of the whole (caller-sized) body.
///
/// Two 64-bit FNV-1a hashes with different offset bases over the mutation's canonical JSON
/// encoding (sorted keys). Not cryptographic: a collision could only make the server replay
/// an actor's *own* earlier outcome to that same actor, which is the actor harming itself.
/// Stable across processes and across the toolchains CI runs (macOS and Linux; unlike
/// `Hasher`, which is seeded per process). It depends on JSONEncoder's exact output, which
/// Foundation does not promise across versions, so a server that persists fingerprints
/// should store an encoding version with them, or hash its own canonical byte format.
struct Fingerprint: Hashable, Sendable {
    let high: UInt64
    let low: UInt64

    init(_ mutation: Mutation) {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        // Encoding these value types can't fail (no floats, no custom throwing encoders);
        // the empty fallback still yields a deterministic, if useless, fingerprint.
        let bytes = (try? encoder.encode(mutation)) ?? Data()
        var a: UInt64 = 0xCBF2_9CE4_8422_2325
        var b: UInt64 = 0x6C62_272E_07BB_0142
        for byte in bytes {
            a = (a ^ UInt64(byte)) &* 0x0000_0100_0000_01B3
            b = (b ^ UInt64(byte)) &* 0x0000_0100_0000_01B3
        }
        high = a
        low = b
    }

    var hex: String { String(format: "%016llx%016llx", high, low) }
}
