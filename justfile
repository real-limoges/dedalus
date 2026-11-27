# Every check dedalus runs, in one place. The pre-push hook and CI call these
# recipes rather than repeating the commands. `just` with no arguments lists
# them.

set shell := ["bash", "-euo", "pipefail", "-c"]

# The minimum Rust version, read from Cargo.toml so it is written down once.
msrv := `sed -n 's/^rust-version = "\(.*\)"$/\1/p' Cargo.toml`

default:
    @just --list

# Everything here is fast and needs no network beyond the advisory database.
# The release build and the MSRV check are left to CI.
# Lint, dependency checks and tests: what runs before every push.
pre-push: lint deny test

# Formatting, clippy and unused dependencies. Compiles, but runs nothing.
lint:
    cargo fmt --all -- --check
    cargo clippy --all-targets --locked -- -D warnings
    cargo machete

# nextest does not run doc tests, so `cargo test --doc` follows it.
# CI passes `ci`, a nextest profile that reports every failure.
# Unit and integration tests under nextest, then doc tests.
test profile="default":
    cargo nextest run --locked --profile {{ profile }}
    cargo test --doc --locked

# RustSec advisories, licenses, banned crates and sources, from deny.toml.
deny:
    cargo deny check

# The rust-version that Cargo.toml promises still compiles everything.
msrv:
    rustup toolchain install {{ msrv }} --profile minimal --no-self-update
    cargo +{{ msrv }} check --all-targets --locked

# Unit tests always build with unwinding, so this is the only check of the
# shipped profile (panic = 'abort', no overflow checks, LTO).
# The release binary end to end on a tiny dump: extract, merge, analytics, load.
smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release --locked
    work="$(mktemp -d)"
    trap 'rm -rf "$work"' EXIT
    bzip2 -c tests/fixtures/smoke.xml > "$work/smoke.xml.bz2"
    target/release/dedalus pipeline -i "$work/smoke.xml.bz2" -o "$work/out" -v
    # Three articles (the redirect is folded into its target), plus headers.
    test "$(wc -l < "$work/out/nodes.csv")" -eq 4
    # Every edge exactly; 2,1 exists only if the redirect `Rust lang` resolved.
    diff <(sort "$work/out/edges.csv") <(sort tests/fixtures/smoke.edges.csv)
    test "$(wc -l < "$work/out/analytics.csv")" -eq 4
    test -s "$work/out/wikipedia.duckdb"
    target/release/dedalus stats -o "$work/out"
    echo "Smoke run OK"
