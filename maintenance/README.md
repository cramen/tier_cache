# Maintainer procedures

These instructions are for developing and releasing TierCache from a repository
checkout. Run commands from the repository root unless a guide says otherwise.
Application users should start with [user documentation](../docs/README.md).

- [Repository quality gates](quality-gates.md): TCK, soak, JFR and benchmark tasks.
- [Virtual-thread validation](virtual-thread-validation.md): startup and steady-state policies.
- [Platform validation](platform-validation.md): server matrix, consumer builds and Sentinel.
- [Shared L1 benchmark](l1-contention-benchmark.md): workload controls and measurement protocol.
- [Release evidence generation](release-evidence.md): dependency acceptance, staging and signing.

Keep run-specific logs, recordings, measurements and acceptance reports in ignored
build output or external evidence storage. Preserve source revisions, environment
and artifact identities with those results. This directory contains reusable
procedures, not a history of individual experiments. Earlier tracked reports are
available in Git history.
