# Release dependency evidence

Release acceptance checks the dependencies of the actual built publications. A
filesystem scan of Kotlin DSL sources, dependency checksums, and a successful
build are not substitutes for a vulnerability scan.

## What is covered

`./gradlew releaseEvidenceInputs` builds the files declared by the Maven
publications and generates an independent resolved-artifact inventory at
`build/release-inputs/inventory.json`. It includes all nine library/TCK modules,
main, sources and documentation JARs, core `unshaded`, `test-fixtures` and
`test-fixtures-sources`, TCK `tests`, and matching POM/Gradle module metadata.
It never collects arbitrary `build/libs/*.jar` leftovers.

Module CycloneDX SBOMs describe the selected runtime graph. Core additionally
includes its fixture runtime; TCK includes its published compliance-suite runtime.
Embedded Caffeine must be present in both the core inventory and SBOM, and the
shaded class must be present in the main JAR. Demo applications are excluded from
the release aggregate. Root coordinates are `io.github.cramen:tiercache:<version>`.

Completeness checks compare every module's resolved group/name/version set with
its SBOM, verify classifier coverage, check the aggregate, and compare publication
metadata and artifact hashes. Dependencies selected by conflict resolution are
recorded, not inferred from version catalog declarations. Optional application
integrations and consumer BOM overrides still require scanning the final
application's own graph.

## Vulnerability acceptance

The scanner is Trivy 0.74.0, installed from an upstream archive using checked-in
SHA-256 values. Updating the scanner requires reviewing that version and updating
its hashes. The scanner and database versions, timestamps, database hash, exact
SBOM hashes, raw JSON results and a readable summary are retained.

Each scan first validates a synthetic SBOM containing Log4j 2.14.1 and requires
recognition of CVE-2021-44228. No vulnerable JAR is added to runtime dependencies.
That self-test result is separate from shipped dependency findings.

Every module SBOM and the aggregate are scanned in SBOM mode. All package results
are requested, and reported package identities are compared with SBOM contents.
Missing/empty documents, omitted dependencies, unparsed scanner output, failed
scanner processes, unavailable or expired databases and unexcepted HIGH/CRITICAL
findings reject acceptance. Lower severities remain visible. Infrastructure
failure is reported as an incomplete scan, never a clean result.

`scripts/release/exceptions.json` starts empty. An exception requires exactly:

```json
{
  "id": "CVE-YYYY-NNNN",
  "package": "pkg:maven/exact.group/exact-artifact@exact-version",
  "owner": "accountable-owner",
  "reason": "Reviewed exposure, compensating control and remediation plan",
  "expires": "YYYY-MM-DD"
}
```

The expiration date must be strictly after the evaluation date in UTC. Wildcard,
empty, duplicate, expired and unmatched entries fail validation. Exceptions apply
only to the exact vulnerability/package/version and require maintainer review;
they are not generated automatically when a scan fails.

## Trial and final evidence

The manual `release-candidate` workflow requires an explicit source ref, intended
version and mode. Both modes resolve the ref to the checked-out commit and require
the intended version to equal `gradle.properties`.

- **trial** permits SNAPSHOT development and creates downloadable candidate
  evidence without publishing anything or attaching assets to a release.
- **final** requires a clean checkout, stable non-SNAPSHOT `X.Y.Z` version and
  `refs/tags/vX.Y.Z`. It attaches the signed evidence bundle to an **existing**
  GitHub release, including an existing draft. It creates/publishes no release
  and performs no Maven Central upload. Existing assets are not overwritten.

The current Redis keyspace v2 change still requires a major release and coordinated
cold cutover; a successful scan does not waive that migration/version policy.

CI uses a clean build with the selected ref's release settings. Staging creates a
new directory and checks the scan input hashes again, re-evaluates findings and
exceptions, and refuses expired database evidence. `manifest.json` records the
immutable source commit, version, mode and each staged file's coordinate (where
applicable) and SHA-256. A dirty local trial is explicitly labeled as such and is
not evidence of a reproducible build of an unmodified commit.

The complete bundle, including the manifest, binaries, classifiers, publication
metadata, SBOMs, inventory and scan reports, is archived as
`tiercache-evidence.tar.gz`. Cosign signs that archive; GitHub attests its exact
bytes. Build/scanning jobs have read-only repository permissions, signing has
OIDC/attestation permissions, and only the final release-attachment job can write
release assets. Tool and job deadlines are finite.

## Running locally

Docker is not required for dependency evidence generation. The build requires
its configured Java toolchain, Python 3.11+ and network access for scanner/database
installation. The checked-in installer supports Linux x86_64 and macOS arm64.

```bash
python3 -m unittest discover -s scripts/release -p 'test_*.py' -v
./gradlew releaseEvidenceInputs
python3 scripts/release/install_trivy.py /tmp/tiercache-trivy
python3 scripts/release/evidence.py scan \
  --trivy /tmp/tiercache-trivy/trivy \
  --output build/release-scan \
  --exceptions scripts/release/exceptions.json
python3 scripts/release/evidence.py stage \
  --scan build/release-scan --output dist \
  --ref <exact-commit> --version <version-from-gradle.properties> --mode trial
python3 scripts/release/evidence.py verify dist
```

Use a fresh output directory for each attempt; the scanner and stager refuse to
reuse an existing one. Failure diagnostics remain available. Local checks cannot
exercise GitHub OIDC signing or prove that a GitHub workflow run succeeded.

## Verifying downloaded and published bytes

First verify the downloaded archive's signature and provenance. Specify the
trusted **workflow ref** that dispatched the run; it is distinct from the selected
source ref stored in the manifest.

```bash
cosign verify-blob \
  --bundle tiercache-evidence.sigstore.json \
  --certificate-identity 'https://github.com/cramen/tier_cache/.github/workflows/release-candidate.yml@refs/heads/main' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  tiercache-evidence.tar.gz
gh attestation verify tiercache-evidence.tar.gz --repo cramen/tier_cache
mkdir verified-evidence
tar -xzf tiercache-evidence.tar.gz -C verified-evidence
python3 scripts/release/evidence.py verify verified-evidence
```

Check `manifest.json` for the intended source commit/version and accepted scan.
Final bundles and signatures are retained as release assets, rather than relying
only on the 90-day workflow artifact. Trial artifacts are not release acceptance.

If the maintainer separately builds/publishes to Central, download all matching
publication files (JARs/classifiers, POM and `.module`) into a directory, then run:

```bash
python3 scripts/release/evidence.py compare-published verified-evidence downloaded-central-files
```

Missing or differing bytes fail verification. RC provenance does not automatically
cover separately rebuilt Central artifacts, even if version strings match.
Checksums bind bytes, signatures authenticate evidence, provenance describes the
build invocation, and the CVE scan evaluates a particular dependency graph against
a dated database. None alone proves the others or guarantees no future CVEs.
