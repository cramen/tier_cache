plugins {
    `java-library`
    alias(libs.plugins.vanniktech.publish)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    api(project(":tiercache-core"))
    api(libs.micrometer.core)
    api(libs.otel.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(testFixtures(project(":tiercache-core")))
    testImplementation(libs.otel.sdk.testing)
    testImplementation(libs.otel.sdk)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// --- Publishing (release automation): shared Central Portal target, license,
// and scm metadata come from the root build script.
mavenPublishing {
    pom {
        name.set("tiercache-micrometer")
        description.set(
            "Micrometer metrics, OpenTelemetry tracing and JMX inspection" +
                " for the Tiercache two-level cache"
        )
    }
}
