import com.vanniktech.maven.publish.MavenPublishBaseExtension
import org.cyclonedx.gradle.CyclonedxAggregateTask
import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.model.Component

plugins {
    // Root project only aggregates; module-specific config lives in each module.
    // Applied (not apply false) so the root gets the whole-build aggregate
    // cyclonedxBom and every module gets cyclonedxDirectBom.
    alias(libs.plugins.cyclonedx)
    // Publishable modules apply this themselves; declared here (apply false)
    // so subprojects can use the alias without a version and the shared
    // publishing config below can reference the plugin types.
    alias(libs.plugins.vanniktech.publish) apply false
    // Declared here (apply false) so the Kotlin plugin and the publishing
    // plugin above share one classloader; tiercache-kotlin applies it without
    // a version.
    kotlin("jvm") version "2.2.21" apply false
}

// Whole-repo aggregate SBOM, named like the module ones so CI can collect a
// flat directory of *-sbom.json files.
tasks.named<CyclonedxAggregateTask>("cyclonedxBom") {
    jsonOutput.set(layout.buildDirectory.file("reports/cyclonedx/${project.name}-sbom.json"))
    xmlOutput.set(layout.buildDirectory.file("reports/cyclonedx/${project.name}-sbom.xml"))
}

subprojects {
    group = "io.github.cramen"
    version = providers.gradleProperty("version").get()

    repositories {
        mavenCentral()
    }

    // SBOM (supply-chain design D1): each module gets its own cyclonedxBom
    // task (an aggregate of just itself; the direct-task guard in the plugin
    // skips re-registering cyclonedxDirectBom). Only the runtime classpath
    // feeds the component list, because that is what ships. For
    // tiercache-core this means Caffeine appears - correct: Caffeine is
    // shaded into the core jar, so it is part of the shipped artifact.
    apply(plugin = "org.cyclonedx.bom")
    tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom") {
        projectType.set(Component.Type.LIBRARY)
        includeConfigs.set(listOf("runtimeClasspath"))
    }
    tasks.named<CyclonedxAggregateTask>("cyclonedxBom") {
        projectType.set(Component.Type.LIBRARY)
        jsonOutput.set(layout.buildDirectory.file("reports/cyclonedx/${project.name}-sbom.json"))
        xmlOutput.set(layout.buildDirectory.file("reports/cyclonedx/${project.name}-sbom.xml"))
    }

    // Publishing (release automation): shared Central Portal target and POM
    // metadata for the modules that apply com.vanniktech.maven.publish; each
    // module sets only its own name/description. Signing is mandatory on
    // Maven Central but must not break keyless local builds: it activates
    // only when GPG credentials are present (in ~/.gradle/gradle.properties).
    pluginManager.withPlugin("com.vanniktech.maven.publish") {
        extensions.configure<MavenPublishBaseExtension> {
            publishToMavenCentral()
            pom {
                url.set("https://github.com/cramen/tier_cache")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("cramen")
                        name.set("cramen")
                        url.set("https://github.com/cramen")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/cramen/tier_cache.git")
                    developerConnection.set("scm:git:https://github.com/cramen/tier_cache.git")
                    url.set("https://github.com/cramen/tier_cache")
                }
            }
            if (providers.gradleProperty("signing.keyId").isPresent) {
                signAllPublications()
            }
        }
    }
}
