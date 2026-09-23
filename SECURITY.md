# Security Policy

## Supported Versions

| Version | Supported |
|---|---|
| 0.x (pre-1.0) | Yes — fixes land on `main` |
| After 1.0 GA | The last two minor releases receive security backports for 12 months |

## Reporting a Vulnerability

Please report vulnerabilities **privately** through
[GitHub Security Advisories](https://github.com/cramen/tier_cache/security/advisories/new).
Do not open public issues for security reports.

You will receive an acknowledgment within 3 business days.

## Fix SLAs

| Severity | Target |
|---|---|
| Critical | Fix released within 7 days |
| High | Fix released within 30 days |

## Scope

This policy covers the published library modules and TCK, including their shipped
classifiers. The `examples/` applications are demonstration code and are out of scope.

## Supply-Chain Verification

The release-candidate workflow resolves the actual publication runtime graphs,
checks SBOM completeness (including shaded Caffeine and published test fixtures),
and scans them with a checksum-pinned Trivy binary. Unexcepted HIGH/CRITICAL
findings and incomplete or failed scans reject acceptance. Any checked-in
exception must name the exact vulnerability/artifact/version, owner, rationale and
future expiration date; exceptions require maintainer review.

The complete candidate evidence bundle is signed with Sigstore and receives
GitHub build provenance. Its manifest binds binaries, classifiers, publication
metadata, SBOMs and scan results to their exact SHA-256 bytes and source commit.
Final-mode evidence is retained with an existing GitHub release; ordinary
SNAPSHOT development and trial workflows remain allowed.

See [release evidence](docs/release-evidence.md) for the precise scope, scanner
self-test, exception policy, commands, signature verification and comparison with
Central downloads. A separately rebuilt artifact is not covered by the candidate
attestation unless its checksum matches. No historical release is retroactively
claimed to have passed this new evidence gate.


The [recorded trial](docs/release-evidence.md#recorded-trial-evidence--2026-09-22)
is dated evidence for a specific commit, not acceptance of a future release.
Application BOMs can override library versions; functional compatibility checks
in the [platform matrix](docs/compatibility.md) do not replace scanning the final
application dependency graph.
