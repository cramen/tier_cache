plugins {
    `java-library`
    alias(libs.plugins.pitest)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    api(project(":tiercache-core"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(testFixtures(project(":tiercache-core")))
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// --- PIT mutation testing (hardening-gates design D3): on-demand gate,
// intentionally NOT wired into `check` (runs in CI/nightly later).
// Scope: the whole invalidation protocol package — journal, replay,
// last-write-wins, message codec.
pitest {
    targetClasses.set(setOf("io.tiercache.invalidation.*"))
    mutationThreshold.set(75)
    junit5PluginVersion.set("1.2.3")
    outputFormats.set(setOf("HTML", "XML"))
    // Mutation history keeps incremental runs cheap; the file is a build
    // artifact and is regenerated when absent.
    historyInputLocation.set(layout.buildDirectory.file("pitest/history.bin"))
    historyOutputLocation.set(layout.buildDirectory.file("pitest/history.bin"))
}
