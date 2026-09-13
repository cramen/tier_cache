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

This policy covers the published library modules (`tiercache-core`,
`tiercache-invalidation`, `tiercache-transport-redis`,
`tiercache-spring-boot-starter`, `tiercache-micrometer`,
`tiercache-kotlin`). The `examples/` applications are demonstration code
and are out of scope.

## Supply-Chain Verification

Release-candidate artifacts are built on GitHub Actions with
SLSA-style build provenance and Sigstore keyless signatures. To verify
an artifact:

```bash
gh attestation verify <jar> --repo cramen/tier_cache
cosign verify-blob --bundle <jar>.sigstore.json <jar> \
  --certificate-identity-regexp '^https://github.com/cramen/tier_cache/.github/workflows/release-candidate.yml@.*' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com'
```

Each CI build of `main` also publishes a CycloneDX SBOM as the `sbom`
workflow artifact, and the nightly pipeline checks that the core jar
builds reproducibly (byte-identical double build).
