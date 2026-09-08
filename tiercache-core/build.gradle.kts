plugins {
    `java-library`
    `java-test-fixtures`
    jacoco
    alias(libs.plugins.shadow)
    alias(libs.plugins.jmh)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    // N-06: the only dependency visible to consumers. Caffeine is an
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
// timestamps keep the artifact reproducible (S-02).
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

// --- Dependency audit (task 1.4): the runtime classpath may expose only
// SLF4J API externally; Caffeine is allowed because it is shaded away.
val dependencyAudit = tasks.register("dependencyAudit") {
    description = "Fails if tiercache-core exposes mandatory dependencies beyond SLF4J API (N-06)."
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
                "tiercache-core must not expose mandatory dependencies beyond SLF4J API (N-06). " +
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

// --- JMH (task 4.2): baseline for the L1-hit hot path (N-01/N-03).
jmh {
    profilers = listOf("gc")
}
