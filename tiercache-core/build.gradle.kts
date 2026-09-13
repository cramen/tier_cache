plugins {
    `java-library`
    `java-test-fixtures`
    `maven-publish`
    jacoco
    alias(libs.plugins.shadow)
    alias(libs.plugins.jmh)
    alias(libs.plugins.pitest)
    alias(libs.plugins.vanniktech.publish)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    // The only dependency visible to consumers. Caffeine is an
    // implementation detail, shaded into the jar (see shadowJar below).
    api(libs.slf4j.api)
    implementation(libs.caffeine) {
        // Annotations-only artifacts; not needed at runtime.
        exclude(group = "com.google.errorprone", module = "error_prone_annotations")
        exclude(group = "org.jspecify", module = "jspecify")
    }

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(testFixtures(project))

    "jmhImplementation"(testFixtures(project))

    testFixturesImplementation(libs.junit.jupiter)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// --- Shading (design D3): Caffeine is relocated so it never clashes with a
// user-managed Caffeine on the classpath. Pinned plugin + disabled manifest
// timestamps keep the artifact reproducible.
tasks.shadowJar {
    // The shaded jar IS the main artifact: tiercache-core-<version>.jar
    archiveClassifier.set("")
    relocate("com.github.benmanes.caffeine", "io.tiercache.internal.caffeine")
    mergeServiceFiles()
    isZip64 = false
}

// The unshaded jar stays available (under a classifier) because Gradle's
// test/test-fixtures classpath resolution needs the main output.
tasks.jar {
    archiveClassifier.set("unshaded")
}

// --- Publishing (release automation): the shaded jar is the published main
// artifact; the unshaded jar ships alongside with its `unshaded` classifier,
// matching the jar layout above. Caffeine is relocated into the shaded jar,
// so the published POM must not declare it: consumers get Caffeine inside the
// jar, not from Central. (The dependency audit below asserts the same about
// the external runtime classpath.)
mavenPublishing {
    pom {
        name.set("tiercache-core")
        description.set(
            "Two-level JVM cache core: L1 in-process (Caffeine, shaded) + L2 Redis/Valkey" +
                " cascade, singleflight, cluster-wide rebuild coordination, circuit breaker"
        )
    }
}

publishing {
    // The publication is registered by the publishing plugin in afterEvaluate,
    // so match lazily by type instead of looking it up by name. The java
    // component already contributes the shaded jar as the main artifact plus
    // the unshaded jar under its classifier (Shadow plugin integration), so
    // only the POM needs fixing here.
    publications.withType<MavenPublication>().configureEach {
        pom.withXml {
            val dependencies = asNode().children()
                .filterIsInstance<groovy.util.Node>()
                .firstOrNull { it.name().toString().endsWith("dependencies") }
                ?: return@withXml
            dependencies.children().removeIf { dependency ->
                dependency is groovy.util.Node && dependency.children().any { child ->
                    child is groovy.util.Node &&
                        child.name().toString().endsWith("artifactId") && child.text() == "caffeine"
                }
            }
        }
    }
}

// --- Dependency audit (task 1.4): the runtime classpath may expose only
// SLF4J API externally; Caffeine is allowed because it is shaded away.
val dependencyAudit = tasks.register("dependencyAudit") {
    description = "Fails if tiercache-core exposes mandatory dependencies beyond SLF4J API."
    group = "verification"

    val allowed = setOf(
        "org.slf4j:slf4j-api",
        "com.github.ben-manes.caffeine:caffeine" // shaded into the artifact, never exposed
    )
    val runtimeClasspath = configurations.runtimeClasspath

    doLast {
        val found = runtimeClasspath.get().incoming.artifactView {
            lenient(true)
        }.artifacts.artifacts.mapNotNull { artifact ->
            val id = artifact.variant.owner as? ModuleComponentIdentifier
            id?.let { "${it.group}:${it.module}" }
        }.toSortedSet()

        val unexpected = found - allowed
        if (unexpected.isNotEmpty()) {
            throw GradleException(
                "tiercache-core must not expose mandatory dependencies beyond SLF4J API. " +
                    "Unexpected modules on runtime classpath: $unexpected"
            )
        }
        logger.lifecycle("dependencyAudit OK: external modules = $found (caffeine is shaded)")
    }
}

tasks.check { dependsOn(dependencyAudit) }

// --- Coverage (task 4.3): branch coverage gate for core per AGENTS.md quality gates.
jacoco {
    toolVersion = "0.8.12"
}

tasks.jacocoTestReport {
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

tasks.jacocoTestCoverageVerification {
    violationRules {
        rule {
            element = "BUNDLE"
            limit {
                counter = "BRANCH"
                value = "COVEREDRATIO"
                minimum = "0.90".toBigDecimal()
            }
        }
    }
}

tasks.check { dependsOn(tasks.jacocoTestCoverageVerification) }

// --- JMH (task 4.2): baseline for the L1-hit hot path.
jmh {
    profilers = listOf("gc")
}

// --- PIT mutation testing (hardening-gates design D3): on-demand gate,
// intentionally NOT wired into `check` (runs in CI/nightly later).
// Scope: io.tiercache.internal.* — read-path coordination, circuit breaker,
// degradation glue. io.tiercache.spi.* stays out of scope: those types are
// interfaces and immutable value carriers with no mutatable logic, so
// including them would only dilute the score, not make it more honest.
pitest {
    targetClasses.set(setOf("io.tiercache.internal.*"))
    // Tests live in io.tiercache.* (not only io.tiercache.internal.*), so the
    // default tests filter derived from targetClasses would miss most of them.
    targetTests.set(setOf("io.tiercache.*"))
    mutationThreshold.set(75)
    junit5PluginVersion.set("1.2.3")
    outputFormats.set(setOf("HTML", "XML"))
    // Mutation history keeps incremental runs cheap; the file is a build
    // artifact and is regenerated when absent.
    historyInputLocation.set(layout.buildDirectory.file("pitest/history.bin"))
    historyOutputLocation.set(layout.buildDirectory.file("pitest/history.bin"))
}
