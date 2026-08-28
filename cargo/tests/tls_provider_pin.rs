//! Guards the TLS provider choice. The failure mode this exists for is SILENT:
//! dropping aws-lc-rs in favour of `ring` compiles and links fine, and an outbound
//! client whose graph carries no CryptoProvider panics at RUNTIME -- in production, on
//! the first wss dial. The loopback e2e suites are plain `ws://`, so nothing else in
//! the suite would ever notice.
//!
//! Design note: these assert on properties that are true under `ring` and false under
//! the two alternatives (no provider, or aws-lc re-added). A guard that could only
//! agree with the wanted answer has no teeth -- see tests/negative_control.rs for the
//! project's convention on that.

/// The provider must resolve implicitly. We never call `install_default()`, and
/// `tokio-tungstenite` builds its client config internally, so a provider-less graph
/// fails at connect time. Building a client config is the cheapest call that exercises
/// the same provider-selection path.
#[test]
fn tls_provider_resolves_without_explicit_install() {
    let res = std::panic::catch_unwind(|| {
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth()
    });
    match res {
        Ok(cfg) => assert!(
            !cfg.crypto_provider().cipher_suites.is_empty(),
            "provider resolved but offers no cipher suites"
        ),
        Err(_) => panic!(
            "no usable process CryptoProvider -- outbound wss dials will panic in \
             production. A dependency bump likely re-enabled defaults somewhere; see \
             the provider pins in Cargo.toml (tokio-rustls is the usual culprit)."
        ),
    }
}

/// Pin WHICH provider answered, so a bump that silently re-adds aws-lc-rs (re-breaking
/// the musl build) or swaps the provider is caught here rather than in a linker error.
#[test]
fn tls_provider_is_ring() {
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    let prov = cfg.crypto_provider();

    // CryptoProvider has no name() accessor, so identify the provider by what only it
    // provides: the ring-backed provider's cipher-suite naming and its ring-specific
    // verification capability. Cheapest unambiguous discriminator is that the ring
    // module is linked in AND its default provider is what builder() picked -- compare
    // the kx group set, which differs between ring and aws-lc.
    let ring = rustls::crypto::ring::default_provider();
    let same_kx = prov.kx_groups.len() == ring.kx_groups.len()
        && prov
            .kx_groups
            .iter()
            .all(|k| ring.kx_groups.iter().any(|r| r.name() == k.name()));
    assert!(
        same_kx,
        "implicit provider is not ring (kx groups differ): shipped provider would \
         re-introduce the musl asm failure"
    );

    // belt: the ring provider must itself be constructible, or the pin is fiction
    assert!(!ring.cipher_suites.is_empty(), "ring provider unusable");
}

/// aws-lc must be ABSENT from the RESOLVED lockfile, not merely unused -- present-but-
/// unused still gets compiled, which is exactly what failed the musl builds.
///
/// Why Cargo.lock and not `cargo metadata`: metadata describes the union of every
/// possible dependency graph, so it lists optional deps regardless of which features
/// are on and can NEVER show an absence. That mistake made the first version of this
/// test fail against a correct graph. The lockfile is the resolved answer, so an
/// absence there is real. (Same trap as feature unification: check the artifact that
/// actually records the decision.)
#[test]
fn aws_lc_is_absent_from_the_lockfile() {
    let lock = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock");
    let text = std::fs::read_to_string(&lock).expect("Cargo.lock must exist (it is committed)");
    assert!(
        !text.contains("aws-lc"),
        "aws-lc is back in the resolved graph, so musl builds will fail assembling its \
         vendored ARM asm. Find the crate that re-enabled it: \
         `cargo tree -i aws-lc-rs`, then pin default-features = false THERE (Cargo.toml \
         notes cover tokio-rustls, tokio-tungstenite and rustls-webpki)."
    );
    // Positive control: the lockfile is real and non-trivial, so the assert above is
    // not passing because the file is empty or truncated.
    assert!(
        text.contains("name = \"rustls\""),
        "unexpected lockfile shape"
    );
    let n = text.matches("name = ").count();
    assert!(n > 40, "lockfile looks truncated: only {n} packages");
    println!("resolved graph has {n} packages, none of them aws-lc");
}
