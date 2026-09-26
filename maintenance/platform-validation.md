# Reproducing the platform matrix

See the [user-facing platform matrix](../docs/compatibility.md) for supported combinations and limitations.

Run from the repository root with Docker, Python 3 and a Java 17 toolchain:

```bash
# All standalone profiles, or pass redis62 / redis74 / redis8 / valkey.
python3 compatibility/run-server-matrix.py

# Independent consumer builds against the locally built artifacts.
./gradlew stageCompatibilityArtifacts
python3 compatibility/run-consumers.py

# Planned promotion, primary kill, full outage; individual names are accepted.
./gradlew :tiercache-spring-boot-starter:sentinelRuntime
docker pull eclipse-temurin:17-jre
python3 compatibility/run-sentinel.py
```

Artifacts go to `build/compatibility-evidence/`: per-profile JUnit results and
image identity, consumer dependency graphs, and Sentinel topology events and
container logs. `sentinelRuntime` includes the test module's runtime; it is not
used as a substitute for the separately published-artifact consumer tests.
The fixture JVM image is a Java 17 runtime; its exact runtime version is logged.

PR CI runs the compact standalone matrix and both Spring consumers on Java 17.
Nightly runs the three Sentinel scenarios with a bounded job timeout. Existing
JDK 21/25 build checks remain separate; this is deliberately not a Cartesian
product of all JDKs, servers and topology failures.
