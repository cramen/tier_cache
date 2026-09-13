import org.cyclonedx.gradle.CyclonedxAggregateTask
import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.model.Component

plugins {
    // Root project only aggregates; module-specific config lives in each module.
    // Applied (not apply false) so the root gets the whole-build aggregate
    // cyclonedxBom and every module gets cyclonedxDirectBom.
    alias(libs.plugins.cyclonedx)
}

// Whole-repo aggregate SBOM, named like the module ones so CI can collect a
// flat directory of *-sbom.json files.
tasks.named<CyclonedxAggregateTask>("cyclonedxBom") {
    jsonOutput.set(layout.buildDirectory.file("reports/cyclonedx/${project.name}-sbom.json"))
    xmlOutput.set(layout.buildDirectory.file("reports/cyclonedx/${project.name}-sbom.xml"))
}

subprojects {
    group = "io.tiercache"
    version = "0.1.0-SNAPSHOT"

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
}
