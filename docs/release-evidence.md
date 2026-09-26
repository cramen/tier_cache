# Verifying release artifacts

Use the evidence attached to the release you intend to deploy. A checksum checks
bytes, a signature authenticates evidence, provenance identifies a build, and a
vulnerability scan evaluates dependencies against a dated database. None of
these alone provides all the other guarantees.

## What to inspect

For releases that provide an evidence bundle, download `tiercache-evidence.tar.gz`,
its `tiercache-evidence.sigstore.json` signature bundle and the corresponding
GitHub attestation. The bundle contains a manifest, publication files, dependency
inventories, CycloneDX SBOMs and scan results.

Check that the manifest identifies the intended version and source commit and
that it is final release evidence rather than a development trial. A trial or
historical report does not certify another commit or a separately rebuilt artifact.

Publication coverage includes the library and TCK modules, their sources and
documentation, published classifiers such as core test fixtures and TCK tests,
and POM/Gradle metadata. Shaded Caffeine is included in dependency evidence.
Demo applications and your application's final dependency graph are not covered
by the library release scan.

## Verify the signature and provenance

Run the following with Cosign and the GitHub CLI installed. Use the trusted
workflow ref that actually dispatched the release evidence; it is distinct from
the source commit recorded in the manifest. The `main` workflow ref below is an
example, not a reason to trust an unexpected signer.

```bash
cosign verify-blob \
  --bundle tiercache-evidence.sigstore.json \
  --certificate-identity 'https://github.com/cramen/tier_cache/.github/workflows/release-candidate.yml@refs/heads/main' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  tiercache-evidence.tar.gz
gh attestation verify tiercache-evidence.tar.gz --repo cramen/tier_cache
```

Then use the verification script from a checkout of the matching TierCache
release. Run these commands from that checkout with Python 3.11+ and the downloaded
archive in the current directory:

```bash
mkdir verified-evidence
tar -xzf tiercache-evidence.tar.gz -C verified-evidence
python3 scripts/release/evidence.py verify verified-evidence
```

Use a new extraction directory. Inspect `manifest.json` for the expected commit,
version and scan acceptance; do not rely only on a successful command exit.

## Compare Maven Central downloads

If artifacts were built separately for publication, their version strings alone
do not establish that the candidate evidence covers them. Download the matching
publication files, including classifiers, POMs and `.module` metadata, then compare:

```bash
python3 scripts/release/evidence.py compare-published verified-evidence downloaded-central-files
```

Missing or different bytes fail verification. An attestation of candidate bytes
does not automatically attest a separately rebuilt Central artifact.

## Interpret dependency results

Read the scan timestamp, database identity, findings and reviewed exceptions.
An incomplete scan is not a clean result. Release acceptance rejects incomplete
evidence and unexcepted HIGH/CRITICAL findings; lower severities and any exceptions
remain relevant to your deployment.

A Spring BOM or other dependency constraints can select different Lettuce, Netty
or framework versions. Scan the final resolved application graph as well, and
check the [platform matrix](compatibility.md) for tested combinations and limits.
No dated scan guarantees the absence of future vulnerabilities.

For how maintainers build, scan, sign and attach evidence, see the separate
[release procedure](../maintenance/release-evidence.md).
