import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.attributes.java.TargetJvmVersion
import org.gradle.api.component.AdhocComponentWithVariants

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

// The unshaded jar stays available under a classifier: its coordinates must
// not collide with the shaded main artifact, and the unshadedRuntimeElements
// variant below publishes it for consumers that manage Caffeine themselves.
tasks.jar {
    archiveClassifier.set("unshaded")
}

// --- Publishing (release automation): the shaded jar is the published main
// artifact; the unshaded jar ships alongside with its `unshaded` classifier,
// matching the jar layout above. Caffeine is relocated into the shaded jar,
// so published metadata must not declare it: consumers get Caffeine inside
// the jar, not from Central. (The dependency audit below asserts the same
// about the external runtime classpath.)
mavenPublishing {
    pom {
        name.set("tiercache-core")
        description.set(
            "Two-level JVM cache core: L1 in-process (Caffeine, shaded) + L2 Redis/Valkey" +
                " cascade, singleflight, cluster-wide rebuild coordination, circuit breaker"
        )
    }
}

// Gradle module metadata carries variants, not a flat dependency list, so it
// needs more than the POM treatment: the standard variants must serve the
// shaded jar (the same artifact Maven consumers get as the main jar) and
// declare only what it needs externally — SLF4J API. Nothing resolves
// runtimeElements locally (it is consumable only), so trimming its hierarchy
// affects published metadata exclusively.
configurations.apiElements {
    outgoing.artifacts.clear()
    outgoing.artifact(tasks.shadowJar)
}
configurations.runtimeElements {
    // implementation deps are embedded (relocated) in the shaded jar; the
    // published runtime variant exposes only the api deps (SLF4J API).
    setExtendsFrom(listOf(configurations.api.get()))
    outgoing.artifacts.clear()
    outgoing.artifact(tasks.shadowJar)
}

shadow {
    // The standard variants above already carry the shaded jar; the plugin's
    // extra shadow variant would only duplicate it (with an empty dependency
    // set that drops even SLF4J).
    addShadowVariantIntoJavaComponent = false
}

// The unshaded jar stays selectable through its own optional variant. It is
// NOT shaded, so unlike the default variants it genuinely needs Caffeine from
// the repository — this variant must keep that dependency. The Bundling
// attribute is deliberately left unset: default consumers (which prefer
// `external`) keep matching runtimeElements exactly, while this variant is
// only reachable through an explicit artifact-view/attribute request.
val unshadedRuntimeElements = configurations.consumable("unshadedRuntimeElements") {
    extendsFrom(configurations.implementation.get(), configurations.api.get())
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 17)
    }
    outgoing.artifact(tasks.jar)
}
(components["java"] as AdhocComponentWithVariants)
    .addVariantsFromConfiguration(unshadedRuntimeElements.get()) {}

publishing {
    // The publication is registered by the publishing plugin in afterEvaluate,
    // so match lazily by type instead of looking it up by name. The POM is a
    // flat view over every component variant, so the unshaded variant's
    // Caffeine dependency (which only applies to the unshaded classifier jar)
    // would leak into it — strip it. The default (shaded) artifact embeds
    // Caffeine relocated; declaring it would put a second, clashing copy on
    // every consumer's classpath.
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
